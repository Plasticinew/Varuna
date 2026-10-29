#include <arpa/inet.h>
#include "zQP.h"
#include <gperftools/profiler.h>
#include <set>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <cstdlib>

#define RECOVERY

// #define POLLTHREAD

// #define SEND_TWICE

// Backup path uses DCQP for read/write/CAS; do not wait for RC rebuild.
#define DCQP_FAILOVER

// #define NO_ERROR_HANDLE

#define SEND_LOG

#define LOG

namespace zrdma
{

    std::mutex zQP_connect_mutex;
    std::atomic<int> g_backup_connect_started{0};

    static void log_path_event(const char *name, int device)
    {
        printf("EVENT t_ns=%lu name=%s device=%d\n",
               (unsigned long)TIME_NOW.time_since_epoch().count(), name, device);
        fflush(stdout);
    }

    static bool full_retransmit_enabled()
    {
        const char *v = getenv("VARUNA_FULL_RETRANSMIT");
        return v != NULL && v[0] == '1';
    }

    static bool dcqp_failover_enabled()
    {
#ifdef DCQP_FAILOVER
        static int cached = -1;
        if (cached < 0)
        {
            const char *v = getenv("VARUNA_DCQP");
            cached = (v != NULL && v[0] == '0') ? 0 : 1;
        }
        return cached != 0;
#else
        return false;
#endif
    }

    static int wr_slot(int index)
    {
        int slot = index % WR_ENTRY_NUM;
        return slot < 0 ? slot + WR_ENTRY_NUM : slot;
    }

    static void wr_log_advance(zQP *qp);
    static int pop_recovered_wc(zQP *qp, ibv_wc &wc);

    struct ZqpSpan
    {
        zQP *qp;
        uint64_t wr_id;
        int begin;
        int end;
    };
    static const unsigned ZQP_SPAN_CAP = 256;
    static thread_local ZqpSpan zqp_spans[ZQP_SPAN_CAP];
    static thread_local unsigned zqp_span_head = 0;
    static thread_local unsigned zqp_span_tail = 0;

    static void zqp_span_push(zQP *qp, uint64_t wr_id, int begin, int end)
    {
        unsigned next = (zqp_span_tail + 1) % ZQP_SPAN_CAP;
        if (next == zqp_span_head)
        {
            zqp_span_head = (zqp_span_head + 1) % ZQP_SPAN_CAP;
        }
        zqp_spans[zqp_span_tail].qp = qp;
        zqp_spans[zqp_span_tail].wr_id = wr_id;
        zqp_spans[zqp_span_tail].begin = begin;
        zqp_spans[zqp_span_tail].end = end;
        zqp_span_tail = next;
    }

    static bool zqp_span_retire(zQP *qp, uint64_t wr_id)
    {
        if (zqp_span_head == zqp_span_tail)
        {
            return false;
        }
        ZqpSpan span = zqp_spans[zqp_span_head];
        if (span.qp != qp || span.wr_id != wr_id)
        {
            return false;
        }
        zqp_span_head = (zqp_span_head + 1) % ZQP_SPAN_CAP;
        for (int n = span.begin; n < span.end; n++)
        {
            int slot = wr_slot(n);
            qp->wr_entry_[slot].wr_addr = 0;
            qp->wr_entry_[slot].finished = 1;
        }
        return true;
    }

    static int wr_log_alloc(zQP *qp)
    {
        wr_log_advance(qp);
        if (qp->entry_end_.load() - qp->entry_start_.load() >= WR_ENTRY_NUM)
        {
            int slot = wr_slot(qp->entry_start_.load());
            qp->wr_entry_[slot].wr_addr = 0;
            qp->wr_entry_[slot].finished = 1;
            wr_log_advance(qp);
        }
        int index = qp->entry_end_.fetch_add(1);
        return wr_slot(index);
    }

    static void wr_log_advance(zQP *qp)
    {
        while (true)
        {
            int start = qp->entry_start_.load();
            int end = qp->entry_end_.load();
            if (start >= end)
            {
                return;
            }
            int slot = wr_slot(start);
            if (qp->wr_entry_[slot].wr_addr != 0 && qp->wr_entry_[slot].finished != 1)
            {
                return;
            }
            if (qp->entry_start_.compare_exchange_strong(start, start + 1))
            {
                continue;
            }
        }
    }

    static void wr_log_complete(zQP *qp, uint32_t time_stamp, uint64_t wr_addr)
    {
        int start = qp->entry_start_.load();
        int end = qp->entry_end_.load();
        if (end - start > WR_ENTRY_NUM)
        {
            start = end - WR_ENTRY_NUM;
        }
        for (int i = start; i < end; i++)
        {
            int slot = wr_slot(i);
            if (qp->wr_entry_[slot].time_stamp == time_stamp &&
                qp->wr_entry_[slot].wr_addr == wr_addr)
            {
                qp->wr_entry_[slot].wr_addr = 0;
                qp->wr_entry_[slot].finished = 1;
            }
        }
        wr_log_advance(qp);
    }

    static void wr_log_complete_by_wr_id(zQP *qp, uint64_t wr_id)
    {
        if (wr_id == 0)
        {
            wr_log_advance(qp);
            return;
        }
        if (zqp_span_retire(qp, wr_id))
        {
            wr_log_advance(qp);
            return;
        }
        int start = qp->entry_start_.load();
        int end = qp->entry_end_.load();
        if (end - start > WR_ENTRY_NUM)
        {
            start = end - WR_ENTRY_NUM;
        }
        int ts = -1;
        for (int i = start; i < end; i++)
        {
            int slot = wr_slot(i);
            if (qp->wr_entry_[slot].finished == 1)
            {
                continue;
            }
            if (qp->wr_entry_[slot].reserved == wr_id)
            {
                ts = (int)qp->wr_entry_[slot].time_stamp;
                break;
            }
        }
        for (int i = start; i < end; i++)
        {
            int slot = wr_slot(i);
            if (qp->wr_entry_[slot].finished == 1)
            {
                continue;
            }
            if (qp->wr_entry_[slot].reserved == wr_id ||
                (ts >= 0 && (int)qp->wr_entry_[slot].time_stamp == ts))
            {
                qp->wr_entry_[slot].wr_addr = 0;
                qp->wr_entry_[slot].finished = 1;
            }
        }
        wr_log_advance(qp);
    }

    static void fill_logged_write_imm(ibv_send_wr *wr, uint32_t time_stamp, int slot)
    {
        wr->opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        wr->imm_data = htonl(pack_write_imm((uint16_t)time_stamp, (uint16_t)slot));
    }

    static void persist_write_imm(zWR_entry *log, uint32_t host_imm)
    {
        static_assert(sizeof(zWR_entry) == 16, "completion log slot must stay 16 bytes");
        uint16_t slot = write_imm_slot(host_imm);
        uint16_t ts = write_imm_time_stamp(host_imm);
        if (log == NULL || slot >= WR_ENTRY_NUM)
        {
            return;
        }
        uint64_t packed = pack_wr_header(0, ts, 0);
        volatile uint64_t *header = (volatile uint64_t *)((char *)log + slot * sizeof(zWR_entry) + sizeof(uint64_t));
        *header = packed;
    }

    static bool requestor_ready(zQP *qp, int device)
    {
        return device >= 0 &&
               device < (int)qp->m_requestors.size() &&
               qp->m_requestors[device] != NULL &&
               qp->m_requestors[device]->status_ == ZSTATUS_CONNECTED;
    }

    static uint32_t path_lkey(zQP *qp, uint32_t lkey);
    static uint32_t path_rkey(zQP *qp, uint32_t rkey);

#ifdef DCQP_FAILOVER
    static bool should_use_dcqp(zQP *qp)
    {
        return dcqp_failover_enabled() &&
               qp->current_device != 0 &&
               !requestor_ready(qp, qp->current_device) &&
               qp->m_targets.find(qp->current_device) != qp->m_targets.end() &&
               qp->current_device < (int)qp->m_pd->m_requestors.size() &&
               !qp->m_pd->m_requestors[qp->current_device].empty();
    }

    static void dcqp_recreate(zQP *qp)
    {
        int d = qp->current_device;
        zDCQP_requestor *old = qp->m_pd->m_requestors[d][0];
        zDCQP_requestor *neu = zDCQP_create_requestor(old->device_, old->pd_);
        qp->m_pd->m_requestors[d][0] = neu;
        fprintf(stderr, "recreated DCQP on device %d\n", d);
    }

    static int dcqp_read_retry(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length,
                               void *remote_addr, uint32_t rkey)
    {
        zTarget *t = qp->m_targets[qp->current_device];
        uint32_t lk = path_lkey(qp, lkey);
        uint32_t rk = path_rkey(qp, rkey);
        int rc = zDCQP_read(qp->m_pd->m_requestors[qp->current_device][0], t->ah,
                            local_addr, lk, length, remote_addr, rk, t->lid_, t->dct_num_);
        if (rc != 0)
        {
            dcqp_recreate(qp);
            lk = path_lkey(qp, lkey);
            rk = path_rkey(qp, rkey);
            rc = zDCQP_read(qp->m_pd->m_requestors[qp->current_device][0], t->ah,
                            local_addr, lk, length, remote_addr, rk, t->lid_, t->dct_num_);
        }
        if (rc == 0)
        {
            static int logged = 0;
            if (logged == 0)
            {
                logged = 1;
                printf("DCQP read ok device=%d at %lu lkey=%u rkey=%u len=%lu\n",
                       qp->current_device, (unsigned long)TIME_NOW.time_since_epoch().count(),
                       lk, rk, (unsigned long)length);
            }
        }
        return rc;
    }

    static int dcqp_write_retry(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length,
                                void *remote_addr, uint32_t rkey)
    {
        zTarget *t = qp->m_targets[qp->current_device];
        uint32_t lk = path_lkey(qp, lkey);
        uint32_t rk = path_rkey(qp, rkey);
        int rc = zDCQP_write(qp->m_pd->m_requestors[qp->current_device][0], t->ah,
                             local_addr, lk, length, remote_addr, rk, t->lid_, t->dct_num_);
        if (rc != 0)
        {
            dcqp_recreate(qp);
            lk = path_lkey(qp, lkey);
            rk = path_rkey(qp, rkey);
            rc = zDCQP_write(qp->m_pd->m_requestors[qp->current_device][0], t->ah,
                             local_addr, lk, length, remote_addr, rk, t->lid_, t->dct_num_);
        }
        return rc;
    }

    static int dcqp_cas_retry(zQP *qp, void *local_addr, uint32_t lkey, uint64_t new_val,
                              void *remote_addr, uint32_t rkey)
    {
        zTarget *t = qp->m_targets[qp->current_device];
        uint32_t lk = path_lkey(qp, lkey);
        uint32_t rk = path_rkey(qp, rkey);
        int rc = zDCQP_CAS(qp->m_pd->m_requestors[qp->current_device][0], t->ah,
                           local_addr, lk, new_val, remote_addr, rk, t->lid_, t->dct_num_);
        if (rc != 0)
        {
            dcqp_recreate(qp);
            lk = path_lkey(qp, lkey);
            rk = path_rkey(qp, rkey);
            rc = zDCQP_CAS(qp->m_pd->m_requestors[qp->current_device][0], t->ah,
                           local_addr, lk, new_val, remote_addr, rk, t->lid_, t->dct_num_);
        }
        return rc;
    }
#else
    static bool should_use_dcqp(zQP *qp)
    {
        (void)qp;
        return false;
    }
#endif

    static bool wc_is_path_failure(int status)
    {
        return status == IBV_WC_RETRY_EXC_ERR ||
               status == IBV_WC_RNR_RETRY_EXC_ERR ||
               status == IBV_WC_WR_FLUSH_ERR;
    }

    static int pick_rocev2_gid_index(ibv_context *ctx, ibv_gid *gid_out)
    {
        ibv_gid gid;
        int chosen = 1;
        ibv_gid chosen_gid;
        memset(&chosen_gid, 0, sizeof(chosen_gid));
        ibv_query_gid(ctx, 1, 1, &chosen_gid);
        for (int i = 0; i < 16; i++)
        {
            memset(&gid, 0, sizeof(gid));
            if (ibv_query_gid(ctx, 1, i, &gid) != 0)
            {
                continue;
            }
            bool all_zero = true;
            for (int b = 0; b < 16; b++)
            {
                if (gid.raw[b] != 0)
                {
                    all_zero = false;
                    break;
                }
            }
            if (all_zero)
            {
                continue;
            }
            bool v4mapped = true;
            for (int b = 0; b < 10; b++)
            {
                if (gid.raw[b] != 0)
                {
                    v4mapped = false;
                    break;
                }
            }
            if (gid.raw[10] != 0xff || gid.raw[11] != 0xff)
            {
                v4mapped = false;
            }
            if (v4mapped)
            {
                chosen = i;
                chosen_gid = gid;
            }
        }
        if (gid_out != NULL)
        {
            *gid_out = chosen_gid;
        }
        return chosen;
    }

    static int poll_cq_timeout(ibv_cq *cq, const char *op)
    {
        auto start = TIME_NOW;
        struct ibv_wc wc;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                std::cerr << "Error, " << op << " timeout" << std::endl;
                return -1;
            }
            int n = ibv_poll_cq(cq, 1, &wc);
        if (n > 0)
        {
            if (wc.status != IBV_WC_SUCCESS)
            {
                std::cerr << "Error, " << op << " failed: " << wc.status << std::endl;
                return -1;
            }
            return 0;
        }
            if (n < 0)
            {
                std::cerr << "Error, " << op << " poll failed" << std::endl;
                return -1;
            }
        }
    }

    static uint32_t path_lkey(zQP *qp, uint32_t lkey)
    {
        return translate_lkey(qp->m_pd, lkey, qp->current_device);
    }

    static uint32_t path_rkey(zQP *qp, uint32_t rkey)
    {
        return translate_rkey(qp->m_rkey_table, rkey, qp->current_device);
    }

    static int rc_read(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length,
                       void *remote_addr, uint32_t rkey)
    {
        zQP_requestor *requestor = qp->m_requestors[qp->current_device];
        struct ibv_sge sge;
        struct ibv_send_wr send_wr;
        struct ibv_send_wr *bad_send_wr;
        sge.addr = (uint64_t)local_addr;
        sge.length = length;
        sge.lkey = path_lkey(qp, lkey);
        send_wr.wr_id = 0;
        send_wr.sg_list = &sge;
        send_wr.num_sge = 1;
        send_wr.next = NULL;
        send_wr.opcode = IBV_WR_RDMA_READ;
        send_wr.send_flags = IBV_SEND_SIGNALED;
        send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
        send_wr.wr.rdma.rkey = path_rkey(qp, rkey);
        if (ibv_post_send(requestor->qp_, &send_wr, &bad_send_wr))
        {
            return -1;
        }
        return poll_cq_timeout(requestor->cq_, "recovery read");
    }

    static int path_read(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length,
                         void *remote_addr, uint32_t rkey)
    {
#ifdef DCQP_FAILOVER
        if (should_use_dcqp(qp))
        {
            return dcqp_read_retry(qp, local_addr, lkey, length, remote_addr, rkey);
        }
#endif
        if (requestor_ready(qp, qp->current_device))
        {
            return rc_read(qp, local_addr, lkey, length, remote_addr, rkey);
        }
        if (qp->current_device >= (int)qp->m_pd->m_requestors.size() ||
            qp->m_targets.find(qp->current_device) == qp->m_targets.end())
        {
            return -1;
        }
        return zDCQP_read(qp->m_pd->m_requestors[qp->current_device][0],
                          qp->m_targets[qp->current_device]->ah,
                          local_addr, path_lkey(qp, lkey), length, remote_addr,
                          path_rkey(qp, rkey),
                          qp->m_targets[qp->current_device]->lid_,
                          qp->m_targets[qp->current_device]->dct_num_);
    }

    static int path_write(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length,
                          void *remote_addr, uint32_t rkey)
    {
#ifdef DCQP_FAILOVER
        if (should_use_dcqp(qp))
        {
            return dcqp_write_retry(qp, local_addr, lkey, length, remote_addr, rkey);
        }
#endif
        if (requestor_ready(qp, qp->current_device))
        {
            zQP_requestor *requestor = qp->m_requestors[qp->current_device];
            struct ibv_sge sge;
            struct ibv_send_wr send_wr;
            struct ibv_send_wr *bad_send_wr;
            sge.addr = (uint64_t)local_addr;
            sge.length = length;
            sge.lkey = path_lkey(qp, lkey);
            send_wr.wr_id = 0;
            send_wr.sg_list = &sge;
            send_wr.num_sge = 1;
            send_wr.next = NULL;
            send_wr.opcode = IBV_WR_RDMA_WRITE;
            send_wr.send_flags = IBV_SEND_SIGNALED;
            send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
            send_wr.wr.rdma.rkey = path_rkey(qp, rkey);
            if (ibv_post_send(requestor->qp_, &send_wr, &bad_send_wr))
            {
                return -1;
            }
            return poll_cq_timeout(requestor->cq_, "recovery write");
        }
        if (qp->current_device >= (int)qp->m_pd->m_requestors.size() ||
            qp->m_targets.find(qp->current_device) == qp->m_targets.end())
        {
            return -1;
        }
        return zDCQP_write(qp->m_pd->m_requestors[qp->current_device][0],
                           qp->m_targets[qp->current_device]->ah,
                           local_addr, path_lkey(qp, lkey), length, remote_addr,
                           path_rkey(qp, rkey),
                           qp->m_targets[qp->current_device]->lid_,
                           qp->m_targets[qp->current_device]->dct_num_);
    }

    static int path_cas(zQP *qp, void *local_addr, uint32_t lkey, uint64_t new_val,
                        void *remote_addr, uint32_t rkey)
    {
#ifdef DCQP_FAILOVER
        if (should_use_dcqp(qp))
        {
            return dcqp_cas_retry(qp, local_addr, lkey, new_val, remote_addr, rkey);
        }
#endif
        if (requestor_ready(qp, qp->current_device))
        {
            zQP_requestor *requestor = qp->m_requestors[qp->current_device];
            uint64_t expected = *(uint64_t *)local_addr;
            struct ibv_sge sge;
            struct ibv_send_wr send_wr;
            struct ibv_send_wr *bad_send_wr;
            sge.addr = (uint64_t)local_addr;
            sge.length = sizeof(uint64_t);
            sge.lkey = path_lkey(qp, lkey);
            send_wr.wr_id = 0;
            send_wr.sg_list = &sge;
            send_wr.num_sge = 1;
            send_wr.next = NULL;
            send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
            send_wr.send_flags = IBV_SEND_SIGNALED;
            send_wr.wr.atomic.remote_addr = (uint64_t)remote_addr;
            send_wr.wr.atomic.rkey = path_rkey(qp, rkey);
            send_wr.wr.atomic.compare_add = expected;
            send_wr.wr.atomic.swap = new_val;
            if (ibv_post_send(requestor->qp_, &send_wr, &bad_send_wr))
            {
                return -1;
            }
            return poll_cq_timeout(requestor->cq_, "recovery cas");
        }
        if (qp->current_device >= (int)qp->m_pd->m_requestors.size() ||
            qp->m_targets.find(qp->current_device) == qp->m_targets.end())
        {
            return -1;
        }
        return zDCQP_CAS(qp->m_pd->m_requestors[qp->current_device][0],
                         qp->m_targets[qp->current_device]->ah,
                         local_addr, path_lkey(qp, lkey), new_val, remote_addr,
                         path_rkey(qp, rkey),
                         qp->m_targets[qp->current_device]->lid_,
                         qp->m_targets[qp->current_device]->dct_num_);
    }

    static zQP_requestor *cas_log_owner(zQP *qp)
    {
        if (requestor_ready(qp, qp->current_device))
        {
            return qp->m_requestors[qp->current_device];
        }
        if (!qp->m_requestors.empty() && qp->m_requestors[0] != NULL)
        {
            return qp->m_requestors[0];
        }
        for (size_t i = 0; i < qp->m_requestors.size(); i++)
        {
            if (qp->m_requestors[i] != NULL)
            {
                return qp->m_requestors[i];
            }
        }
        return NULL;
    }

    static uint32_t cas_log_rkey(zQP *qp, zQP_requestor *owner)
    {
        if (owner == NULL)
        {
            return 0;
        }
        uint32_t rk = owner->server_cmd_rkey_[qp->current_device];
        if (rk == 0)
        {
            rk = owner->server_cmd_rkey_[0];
        }
        return rk;
    }

    static uint32_t cas_msg_lkey(zQP *qp)
    {
        if (qp->msg_mr_[qp->current_device])
        {
            return qp->msg_mr_[qp->current_device]->lkey;
        }
        return qp->msg_mr_[0]->lkey;
    }

    static int write_cas_buffer(zQP *qp, int slot, uint64_t new_val, void *remote_addr,
                                uint32_t time_stamp)
    {
        zQP_requestor *owner = cas_log_owner(qp);
        if (owner == NULL)
        {
            return -1;
        }
        zAtomic_buffer *buffer = &qp->atomic_buffer_[slot];
        buffer->buffer = new_val;
        buffer->finished = 0;
        buffer->target_addr = (uint64_t)remote_addr;
        buffer->time_stamp = time_stamp;

        uint64_t remote = cas_buffer_remote_addr(owner->server_cmd_msg_, slot);
        if (requestor_ready(qp, qp->current_device))
        {
            zQP_requestor *req = qp->m_requestors[qp->current_device];
            struct ibv_sge *buffer_sge = &qp->sge_[slot];
            struct ibv_send_wr *buffer_wr = &qp->wr_[slot];
            struct ibv_send_wr *bad_send_wr;
            memset(buffer_sge, 0, sizeof(*buffer_sge));
            memset(buffer_wr, 0, sizeof(*buffer_wr));
            buffer_sge->addr = (uint64_t)buffer;
            buffer_sge->length = sizeof(zAtomic_buffer);
            buffer_sge->lkey = 0;
            buffer_wr->wr_id = 0;
            buffer_wr->sg_list = buffer_sge;
            buffer_wr->num_sge = 1;
            buffer_wr->next = NULL;
            buffer_wr->opcode = IBV_WR_RDMA_WRITE;
            buffer_wr->send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
            buffer_wr->wr.rdma.remote_addr = remote;
            buffer_wr->wr.rdma.rkey = cas_log_rkey(qp, owner);
            if (ibv_post_send(req->qp_, buffer_wr, &bad_send_wr) != 0)
            {
                std::cerr << "Error, ibv_post_send CAS log failed" << std::endl;
                return -1;
            }
            return poll_cq_timeout(req->cq_, "cas log");
        }
        memcpy(qp->cmd_msg_, buffer, sizeof(*buffer));
        return path_write(qp, qp->cmd_msg_, cas_msg_lkey(qp), sizeof(zAtomic_buffer),
                          (void *)(uintptr_t)remote, owner->server_cmd_rkey_[0]);
    }

    static int complete_cas_step2(zQP *qp, uint64_t uid, uint64_t new_val, void *target,
                                  uint32_t rkey, uint32_t lkey, void *local_addr,
                                  uint32_t time_stamp, int slot)
    {
        if (requestor_ready(qp, qp->current_device))
        {
            return zQP_CAS_step2(qp, new_val, target, rkey, time_stamp, slot);
        }
        *(uint64_t *)local_addr = uid;
        if (path_cas(qp, local_addr, lkey, new_val, target, rkey) != 0)
        {
            return -1;
        }
        zQP_requestor *owner = cas_log_owner(qp);
        if (owner != NULL)
        {
            zAtomic_buffer buf{};
            buf.buffer = new_val;
            buf.finished = 1;
            buf.target_addr = (uint64_t)target;
            buf.time_stamp = time_stamp;
            memcpy(qp->cmd_msg_, &buf, sizeof(buf));
            uint64_t remote = cas_buffer_remote_addr(owner->server_cmd_msg_, slot);
            (void)path_write(qp, qp->cmd_msg_, cas_msg_lkey(qp), sizeof(buf),
                             (void *)(uintptr_t)remote, owner->server_cmd_rkey_[0]);
        }
        return 0;
    }

    static int prepare_path(zQP *qp, bool run_recovery)
    {
        if (qp->in_recovery.load())
        {
            return 0;
        }
        int n = qp->m_ep->m_device_num;
        if (n <= 0)
        {
            return -1;
        }
        if (qp->m_ep->m_devices[qp->current_device]->status.load() != ZSTATUS_ERROR)
        {
            return 0;
        }
        int switched = 0;
        for (int i = 1; i < n; i++)
        {
            int cand = (qp->current_device + i) % n;
            if (qp->m_ep->m_devices[cand]->status.load() != ZSTATUS_ERROR)
            {
                z_switch(qp);
                switched = 1;
                break;
            }
        }
        if (switched && run_recovery)
        {
#ifdef RECOVERY
            return z_recovery(qp);
#endif
        }
        return 0;
    }

    int load_config(const char *fname, struct zDeviceConfig *config)
    {
        std::fstream config_fs(fname);

        boost::property_tree::ptree pt;
        try
        {
            boost::property_tree::read_json(config_fs, pt);
        }
        catch (boost::property_tree::ptree_error &e)
        {
            return -1;
        }

        try
        {
            config->node_id = pt.get<uint16_t>("node_id");

            int i = 0;
            BOOST_FOREACH (boost::property_tree::ptree::value_type &v, pt.get_child("eth_names"))
            {
                config->eth_names[i] = v.second.get<string>("");
                i++;
            }

            i = 0;
            BOOST_FOREACH (boost::property_tree::ptree::value_type &v, pt.get_child("mlx_names"))
            {
                config->mlx_names[i] = v.second.get<string>("");
                i++;
            }

            config->num_devices = i;
            i = 0;
            BOOST_FOREACH (boost::property_tree::ptree::value_type &v, pt.get_child("ports"))
            {
                config->ports[i] = v.second.get<string>("");
                i++;
            }

            i = 0;
            BOOST_FOREACH (boost::property_tree::ptree::value_type &v, pt.get_child("ips"))
            {
                config->ips[i] = v.second.get<string>("");
                i++;
            }
        }
        catch (boost::property_tree::ptree_error &e)
        {
            return -1;
        }
        return 0;
    }

    int load_config(const char *fname, struct zTargetConfig *config)
    {
        std::fstream config_fs(fname);

        boost::property_tree::ptree pt;
        try
        {
            boost::property_tree::read_json(config_fs, pt);
        }
        catch (boost::property_tree::ptree_error &e)
        {
            return -1;
        }

        try
        {
            int i = 0;
            BOOST_FOREACH (boost::property_tree::ptree::value_type &v, pt.get_child("target_ports"))
            {
                config->target_ports[i] = v.second.get<string>("");
                i++;
            }
            config->num_nodes = i;
            i = 0;
            BOOST_FOREACH (boost::property_tree::ptree::value_type &v, pt.get_child("target_ips"))
            {
                config->target_ips[i] = v.second.get<string>("");
                i++;
            }
        }
        catch (boost::property_tree::ptree_error &e)
        {
            return -1;
        }
        return 0;
    }

    zEndpoint *zEP_create(string config_file)
    {
        zEndpoint *endpoint = new zEndpoint();
        struct zDeviceConfig m_config;
        load_config(config_file.c_str(), &m_config);
        endpoint->m_device_num = m_config.num_devices;
        endpoint->m_node_id = m_config.node_id;
        endpoint->qp_num_.store(1);
        for (int i = 0; i < endpoint->m_device_num; i++)
        {
            zDevice *device = new zDevice();
            device->mlx_name = m_config.mlx_names[i];
            device->eth_name = m_config.eth_names[i];
            device->eth_ip = m_config.ips[i];
            device->port = m_config.ports[i];
            endpoint->m_devices.push_back(device);
            device->status.store(zStatus::ZSTATUS_INIT);
        }

        struct ibv_context **device_list;
        int device_num;
        device_list = rdma_get_devices(&device_num);
        for (int i = 0; i < device_num; i++)
        {
            for (int j = 0; j < endpoint->m_device_num; j++)
            {
                if (endpoint->m_devices[j]->mlx_name == device_list[i]->device->name)
                {
                    endpoint->m_devices[j]->context = device_list[i];
                    break;
                }
            }
        }

        for (int i = 0; i < endpoint->m_device_num; i++)
        {
            if (endpoint->m_devices[i]->context == NULL)
            {
                printf("Device %s not found\n", endpoint->m_devices[i]->mlx_name.c_str());
                exit(-1);
            }
        }

#ifdef POLLTHREAD
        std::thread *poll_thread = new std::thread(zQP_poll_thread, endpoint);
#endif

        return endpoint;
    }

    zPD *zPD_create(zEndpoint *ep, int pool_size)
    {
        zPD *pd = new zPD();
        for (int i = 0; i < ep->m_device_num; i++)
        {
            ibv_pd *pd_ = ibv_alloc_pd(ep->m_devices[i]->context);
            pd->m_pds.push_back(pd_);
        }
        // create DCQP requestor and responder
        for (int i = 0; i < ep->m_device_num; i++)
        {
            vector<zDCQP_requestor *> requestors;
            vector<zDCQP_responder *> responders;
            for (int j = 0; j < pool_size; j++)
            {
                zDCQP_requestor *requestor = zDCQP_create_requestor(ep->m_devices[i], pd->m_pds[i]);
                zDCQP_responder *responder = zDCQP_create_responder(ep->m_devices[i], pd->m_pds[i]);
                requestors.push_back(requestor);
                responders.push_back(responder);
            }
            pd->m_requestors.push_back(requestors);
            pd->m_responders.push_back(responders);
        }
        return pd;
    }

    void zEP_destroy(zEndpoint *ep)
    {
        ep->stop = true;
        delete ep;
    }
    void zPD_destroy(zPD *pd)
    {
        delete pd;
    }

    zQP *zQP_create(zPD *pd, zEndpoint *ep, rkeyTable *table, zQPType qp_type)
    {
        zQP *zqp = new zQP();
        zqp->m_ep = ep;
        zqp->m_pd = pd;
        zqp->m_rkey_table = table;
        zqp->qp_type = qp_type;
        zqp->time_stamp = random() % MAX_REQUESTOR_NUM;
        ibv_mr *msg_mr = mr_malloc_create(pd, (uint64_t &)zqp->cmd_msg_, sizeof(CmdMsgBlock));
        memset(zqp->cmd_msg_, 0, sizeof(CmdMsgBlock));
        for (int i = 0; i < pd->m_pds.size(); i++)
        {
            tbb::concurrent_hash_map<ibv_mr *, tbb::concurrent_vector<ibv_mr *>>::accessor a;
            if (!pd->m_mrs.find(a, msg_mr))
            {
                perror("ibv_reg_mr m_msg_mr_ fail");
                return NULL;
            }
            zqp->msg_mr_[i] = a->second[i];
        }

        ibv_mr *resp_mr = mr_malloc_create(pd, (uint64_t &)zqp->cmd_resp_, sizeof(CmdMsgRespBlock));
        memset(zqp->cmd_resp_, 0, sizeof(CmdMsgRespBlock));
        for (int i = 0; i < pd->m_pds.size(); i++)
        {
            tbb::concurrent_hash_map<ibv_mr *, tbb::concurrent_vector<ibv_mr *>>::accessor a;
            if (!pd->m_mrs.find(a, resp_mr))
            {
                perror("ibv_reg_mr m_msg_mr_ fail");
                return NULL;
            }
            zqp->resp_mr_[i] = a->second[i];
        }

        return zqp;
    }

    zQP_listener *zQP_listener_create(zPD *pd, zEndpoint *ep)
    {
        zQP_listener *listener = new zQP_listener();
        listener->m_pd = pd;
        listener->m_ep = ep;
        ibv_mr *mr = mr_malloc_create(pd, (uint64_t &)listener->qp_info, sizeof(qp_info_table) * MAX_QP_NUM);
        for (int i = 0; i < pd->m_pds.size(); i++)
        {
            tbb::concurrent_hash_map<ibv_mr *, tbb::concurrent_vector<ibv_mr *>>::accessor a;
            if (!pd->m_mrs.find(a, mr))
            {
                perror("ibv_reg_mr m_msg_mr_ fail");
                return NULL;
            }
            listener->qp_info_rkey[i] = a->second[i]->rkey;
        }
        listener->flush_thread_ = new std::thread(zQP_flush, listener);
        listener->imm_cq_thread_ = new std::thread(zQP_imm_cq_worker, listener);
        return listener;
    }

    void zQP_flush(zQP_listener *listener)
    {
        qp_info_table *qp_info = listener->qp_info;
        while (!listener->stop.load())
        {
            for (int i = 0; i < MAX_QP_NUM; i++)
            {
                if (listener->stop.load())
                {
                    return;
                }
                if (qp_info[i].addr == 0)
                {
                    continue;
                }
                zAtomic_buffer *atomic_buffer = cas_buffer_at((void *)(uintptr_t)qp_info[i].addr, 0);
                for (int j = 0; j < WR_ENTRY_NUM; j++)
                {
                    if (atomic_buffer[j].target_addr != 0 &&
                        atomic_buffer[j].finished == 0 &&
                        atomic_buffer[j].buffer != 0)
                    {
                        uint64_t target_addr = atomic_buffer[j].target_addr;
                        zAtomic_entry entry = *(zAtomic_entry *)(uintptr_t)target_addr;
                        if (entry.offset != j || entry.qp_id != i)
                        {
                            atomic_buffer[j].finished = 2;
                            continue;
                        }
                        *(volatile uint64_t *)target_addr = atomic_buffer[j].buffer;
                        atomic_buffer[j].finished = 1;
                        printf("flush qp %d entry %d: buffer=%lu\n", i, j, atomic_buffer[j].buffer);
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    int zQP_post_imm_recvs(ibv_qp *qp, int n)
    {
        if (qp == NULL || n <= 0)
        {
            return -1;
        }
        ibv_recv_wr wrs[64];
        ibv_recv_wr *bad = NULL;
        int posted = 0;
        while (posted < n)
        {
            int batch = n - posted;
            if (batch > 64)
            {
                batch = 64;
            }
            memset(wrs, 0, sizeof(wrs[0]) * batch);
            for (int i = 0; i < batch; i++)
            {
                wrs[i].num_sge = 0;
                wrs[i].sg_list = NULL;
                wrs[i].next = (i + 1 < batch) ? &wrs[i + 1] : NULL;
            }
            if (ibv_post_recv(qp, &wrs[0], &bad))
            {
                perror("ibv_post_recv imm failed");
                return -1;
            }
            posted += batch;
        }
        return 0;
    }

    void zQP_imm_cq_worker(zQP_listener *listener)
    {
        struct ibv_wc wc[64];
        int idle_spins = 0;
        std::vector<zImmRecvConn *> snap;
        while (!listener->stop.load())
        {
            if (snap.empty() || (idle_spins & 255) == 0)
            {
                std::lock_guard<std::mutex> lock(listener->imm_mu_);
                if (snap.size() != listener->imm_conns_.size())
                {
                    snap = listener->imm_conns_;
                }
            }
            bool did_work = false;
            for (size_t i = 0; i < snap.size(); i++)
            {
                zImmRecvConn *conn = snap[i];
                if (conn == NULL || conn->cq == NULL || conn->qp == NULL)
                {
                    continue;
                }
                int n = ibv_poll_cq(conn->cq, 64, wc);
                if (n <= 0)
                {
                    continue;
                }
                did_work = true;
                int need_repost = 0;
                for (int k = 0; k < n; k++)
                {
                    need_repost++;
                    if (wc[k].status != IBV_WC_SUCCESS)
                    {
                        continue;
                    }
                    if (wc[k].opcode != IBV_WC_RECV_RDMA_WITH_IMM &&
                        (wc[k].wc_flags & IBV_WC_WITH_IMM) == 0)
                    {
                        continue;
                    }
                    persist_write_imm(conn->log, ntohl(wc[k].imm_data));
                }
                if (zQP_post_imm_recvs(conn->qp, need_repost) != 0)
                {
                    fprintf(stderr, "imm recv replenish failed qp=%p\n", (void *)conn->qp);
                }
            }
            if (!did_work)
            {
                // A yield on every idle poll lets this thread get descheduled
                // while WRITE_WITH_IMM completions are arriving, and the
                // receive queue then returns RNR to the compute node.
                idle_spins++;
                if (idle_spins < 256)
                {
                    continue;
                }
                idle_spins = 0;
                std::this_thread::yield();
            }
            else
            {
                idle_spins = 0;
            }
        }
    }

    static void drain_endpoint_cqs(zEndpoint *zep)
    {
        struct ibv_wc wc[64];
        for (int i = 0; i < zep->m_device_num; i++)
        {
            if (zep->m_devices[i] == NULL)
            {
                continue;
            }
            for (size_t j = 0; j < zep->m_devices[i]->cq_list_.size(); j++)
            {
                ibv_cq *cq = zep->m_devices[i]->cq_list_[j];
                if (cq == NULL)
                {
                    continue;
                }
                int ne = ibv_poll_cq(cq, 64, wc);
                if (ne < 0)
                {
                    continue;
                }
                for (int k = 0; k < ne; k++)
                {
                    uint64_t wr_id = wc[k].wr_id;
                    if (wr_id == 0)
                    {
                        continue;
                    }
                    tbb::concurrent_hash_map<uint64_t, cq_info>::accessor a;
                    if (!zep->completed_table.insert(a, {wr_id, {wc[k].status, .valid = 1, .device_id = i}}))
                    {
                        a->second.status = wc[k].status;
                        a->second.valid = 1;
                        a->second.device_id = i;
                    }
                    if (wc[k].status != IBV_WC_SUCCESS)
                    {
                        static int logged_wc = 0;
                        if (logged_wc < 8)
                        {
                            logged_wc++;
                            fprintf(stderr, "CQ error status=%d wr_id=%lx device=%d\n",
                                    wc[k].status, (unsigned long)wr_id, i);
                        }
                    }
                }
            }
        }
    }

    std::atomic<int> poll_thread_count_{0};

    int zQP_poll_thread(zEndpoint *zep)
    {
        struct ibv_wc wc[1024];
        while (zep->stop == false)
        {
            for (int i = 0; i < zep->m_device_num; i++)
            {
                for (int j = 0; j < zep->m_devices[i]->cq_list_.size(); j++)
                {
                    if (zep->m_devices[i]->cq_list_[j] == NULL)
                    {
                        continue;
                    }
                    int ne = ibv_poll_cq(zep->m_devices[i]->cq_list_[j], 1024, wc);
                    if (ne < 0)
                    {
                        perror("poll cq failed");
                        return -1;
                    }
                    for (int k = 0; k < ne; k++)
                    {
                        uint64_t wr_id = wc[k].wr_id;
                        if (wr_id == 0)
                        {
                            continue;
                        }
                        tbb::concurrent_hash_map<uint64_t, cq_info>::accessor a;
                        bool result = zep->completed_table.insert(a, {wr_id, {wc[k].status, .valid = 1, .device_id = i}});
                        if (!result)
                        {
                            a->second.status = wc[k].status;
                            a->second.valid = 1;
                            a->second.device_id = i;
                        }
                        if (wc[k].status != IBV_WC_SUCCESS)
                        {
                            continue;
                        }
                    }
                }
            }
        }
        return 0;
    }

    zDCQP_requestor *zDCQP_create_requestor(zDevice *device, ibv_pd *pd)
    {
        zDCQP_requestor *dcqp = new zDCQP_requestor();
        dcqp->device_ = device;
        dcqp->pd_ = pd;
        struct mlx5dv_qp_init_attr dv_init_attr;
        struct ibv_qp_init_attr_ex init_attr;

        memset(&dv_init_attr, 0, sizeof(dv_init_attr));
        memset(&init_attr, 0, sizeof(init_attr));

        dcqp->cq_ = ibv_create_cq(device->context, 128, NULL, NULL, 0);
        ibv_gid local_gid;
        dcqp->gid_index_ = (uint8_t)pick_rocev2_gid_index(device->context, &local_gid);

        // 和正常QP创建相同，需要设置参数
        init_attr.qp_type = IBV_QPT_DRIVER;
        init_attr.send_cq = dcqp->cq_;
        init_attr.recv_cq = dcqp->cq_;
        init_attr.pd = pd;
        init_attr.cap.max_send_wr = 512;
        init_attr.cap.max_send_sge = 16;
        init_attr.sq_sig_all = 0;

        // DCQP需要额外设置的内容
        init_attr.comp_mask |= IBV_QP_INIT_ATTR_SEND_OPS_FLAGS | IBV_QP_INIT_ATTR_PD;
        init_attr.send_ops_flags |= IBV_QP_EX_WITH_SEND | IBV_QP_EX_WITH_RDMA_READ | IBV_QP_EX_WITH_RDMA_WRITE | IBV_QP_EX_WITH_ATOMIC_CMP_AND_SWP;

        dv_init_attr.comp_mask |=
            MLX5DV_QP_INIT_ATTR_MASK_DC |
            MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS;
        dv_init_attr.create_flags |=
            MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;

        // 类型为发送端DCI
        dv_init_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCI;

        dcqp->qp_ = mlx5dv_create_qp(device->context, &init_attr, &dv_init_attr);

        if (dcqp->qp_ == NULL)
        {
            perror("create dcqp failed!");
        }

        struct ibv_qp_attr qp_attr_to_init;
        struct ibv_qp_attr qp_attr_to_rtr;
        struct ibv_qp_attr qp_attr_to_rts;

        memset(&qp_attr_to_init, 0, sizeof(qp_attr_to_init));
        memset(&qp_attr_to_rtr, 0, sizeof(qp_attr_to_rtr));
        memset(&qp_attr_to_rts, 0, sizeof(qp_attr_to_rts));

        // 状态切换为INIT
        qp_attr_to_init.qp_state = IBV_QPS_INIT;
        qp_attr_to_init.pkey_index = 0;
        qp_attr_to_init.port_num = 1;

        // 状态切换为RTR
        qp_attr_to_rtr.qp_state = IBV_QPS_RTR;
        qp_attr_to_rtr.path_mtu = IBV_MTU_4096;
        qp_attr_to_rtr.min_rnr_timer = 10;
        qp_attr_to_rtr.ah_attr.port_num = 1;
        qp_attr_to_rtr.ah_attr.is_global = 1;

        // 状态切换为RTS
        qp_attr_to_rts.qp_state = IBV_QPS_RTS;
        qp_attr_to_rts.timeout = 14;
        qp_attr_to_rts.retry_cnt = 7;
        qp_attr_to_rts.rnr_retry = 7;
        qp_attr_to_rts.sq_psn = 114;
        qp_attr_to_rts.max_rd_atomic = 16;

        int ret = ibv_modify_qp(dcqp->qp_, &qp_attr_to_init, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT);
        if (ret)
        {
            printf("%d\n", ret);
            perror("init state failed\n");
            abort();
        }

        ret = ibv_modify_qp(dcqp->qp_, &qp_attr_to_rtr, IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_AV);
        if (ret)
        {
            printf("%d\n", ret);
            perror("rtr state failed\n");
            abort();
        }

        ret = ibv_modify_qp(dcqp->qp_, &qp_attr_to_rts, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
        if (ret)
        {
            printf("%d\n", ret);
            perror("rts state failed\n");
            abort();
        }

        struct ibv_qp_attr attr;
        struct ibv_qp_init_attr init_attr_;

        // 获取qp的信息，主要是dct_num
        // lid和port在IB网络下有效，RoCE网络下不会用到
        ibv_query_qp(dcqp->qp_, &attr,
                     IBV_QP_STATE, &init_attr_);

        dcqp->lid_ = attr.ah_attr.dlid;
        dcqp->port_num_ = attr.ah_attr.port_num;
        dcqp->dct_num_ = dcqp->qp_->qp_num;

        // printf("%d, %d, %d, %d\n", attr.qp_state ,dcqp->lid_, dcqp->port_num_, dcqp->dct_num_);

        // 提取ex qp与mlx qp
        dcqp->qp_ex_ = ibv_qp_to_qp_ex(dcqp->qp_);

        dcqp->qp_mlx_ex_ = mlx5dv_qp_ex_from_ibv_qp_ex(dcqp->qp_ex_);

        return dcqp;
    }

    zDCQP_responder *zDCQP_create_responder(zDevice *device, ibv_pd *pd)
    {

        zDCQP_responder *dcqp = new zDCQP_responder();
        dcqp->device_ = device;
        dcqp->pd_ = pd;
        struct mlx5dv_qp_init_attr dv_init_attr;
        struct ibv_qp_init_attr_ex init_attr;

        memset(&dv_init_attr, 0, sizeof(dv_init_attr));
        memset(&init_attr, 0, sizeof(init_attr));

        dcqp->cq_ = ibv_create_cq(device->context, 1024, NULL, NULL, 0);
        struct ibv_srq_init_attr srq_init_attr;

        memset(&srq_init_attr, 0, sizeof(srq_init_attr));

        srq_init_attr.attr.max_wr = 1;
        srq_init_attr.attr.max_sge = 1;

        dcqp->srq_ = ibv_create_srq(pd, &srq_init_attr);

        init_attr.qp_type = IBV_QPT_DRIVER;
        init_attr.send_cq = dcqp->cq_;
        init_attr.recv_cq = dcqp->cq_;
        init_attr.pd = pd;
        init_attr.cap.max_send_wr = 1;
        init_attr.cap.max_recv_wr = 1;
        init_attr.cap.max_send_sge = 1;
        init_attr.cap.max_recv_sge = 1;
        init_attr.cap.max_inline_data = 256;
        init_attr.sq_sig_all = 0;

        init_attr.comp_mask |= IBV_QP_INIT_ATTR_PD;
        init_attr.srq = dcqp->srq_;
        dv_init_attr.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_DC;
        // 类型为接收端DCT
        dv_init_attr.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
        // 需要设置访问的key
        dv_init_attr.dc_init_attr.dct_access_key = 114514;

        dcqp->qp_ = mlx5dv_create_qp(device->context, &init_attr, &dv_init_attr);

        if (dcqp->qp_ == NULL)
        {
            perror("create dcqp failed!");
        }

        // 切换为INIT
        ibv_qp_attr qp_attr{};
        qp_attr.qp_state = IBV_QPS_INIT;
        qp_attr.pkey_index = 0;
        qp_attr.port_num = 1;
        qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE |
                                  IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;

        int attr_mask =
            IBV_QP_STATE | IBV_QP_ACCESS_FLAGS | IBV_QP_PORT | IBV_QP_PKEY_INDEX;

        int ret = ibv_modify_qp(dcqp->qp_, &qp_attr, attr_mask);
        if (ret)
        {
            printf("%d\n", ret);
            perror("change state failed\n");
            abort();
        }

        // 切换为RTR
        qp_attr.qp_state = IBV_QPS_RTR;
        qp_attr.path_mtu = IBV_MTU_4096;
        qp_attr.min_rnr_timer = 12;
        qp_attr.ah_attr.is_global = 1;
        qp_attr.ah_attr.grh.hop_limit = 64;
        qp_attr.ah_attr.grh.traffic_class = 0;
        ibv_gid local_gid;
        dcqp->gid_index_ = (uint8_t)pick_rocev2_gid_index(device->context, &local_gid);
        qp_attr.ah_attr.grh.sgid_index = dcqp->gid_index_;
        qp_attr.ah_attr.port_num = 1;

        attr_mask = IBV_QP_STATE | IBV_QP_MIN_RNR_TIMER | IBV_QP_AV | IBV_QP_PATH_MTU;

        ret = ibv_modify_qp(dcqp->qp_, &qp_attr, attr_mask);
        if (ret)
        {
            printf("%d\n", ret);
            perror("change state failed\n");
            abort();
        }

        struct ibv_port_attr port_attr;

        memset(&port_attr, 0, sizeof(port_attr));

        // 查询dct_num与gid
        ibv_query_port(device->context, 1,
                       &port_attr);

        dcqp->lid_ = port_attr.lid;
        dcqp->port_num_ = 1;

        ibv_gid gid;
        ibv_query_gid(device->context, 1, dcqp->gid_index_, &gid);

        dcqp->dct_num_ = dcqp->qp_->qp_num;
        dcqp->gid1 = *(uint64_t *)gid.raw;
        dcqp->gid2 = *((uint64_t *)(gid.raw) + 1);
        dcqp->interface = gid.global.interface_id;
        dcqp->subnet = gid.global.subnet_prefix;
        z_debug("DCT %s gid_index=%u gid=%016lx:%016lx dct_num=%u\n",
                device->mlx_name.c_str(), dcqp->gid_index_,
                dcqp->gid1, dcqp->gid2, dcqp->dct_num_);

        return dcqp;
    }

    ibv_ah *zDCQP_create_ah(zDCQP_requestor *requestor, uint64_t input_gid1, uint64_t input_gid2, uint64_t input_interface, uint64_t input_subnet, uint32_t input_lid)
    {

        ibv_gid gid;
        *(uint64_t *)gid.raw = input_gid1;
        *((uint64_t *)(gid.raw) + 1) = input_gid2;
        gid.global.interface_id = input_interface;
        gid.global.subnet_prefix = input_subnet;

        struct ibv_ah_attr ah_attr;
        memset(&ah_attr, 0, sizeof(ah_attr));
        ah_attr.dlid = input_lid;
        ah_attr.port_num = 1;
        ah_attr.is_global = 1;
        ah_attr.grh.hop_limit = 64;
        ah_attr.grh.sgid_index = requestor->gid_index_;
        ah_attr.grh.dgid = gid;

        ibv_ah *ah = ibv_create_ah(requestor->pd_, &ah_attr);
        if (ah == NULL)
        {
            perror("ibv_create_ah failed");
        }
        return ah;
    }

    int zDCQP_send(zQP *zqp, zDCQP_requestor *requestor, ibv_ah *ah, ibv_send_wr *send_wr, uint32_t lid, uint32_t dct_num, int max_depth)
    {
        std::lock_guard<std::mutex> guard(requestor->lock);
        ibv_wr_start(requestor->qp_ex_);
        ibv_send_wr *p = send_wr;
        int i = 0;
        int depth = 0;
        uint64_t length = 0;
        while (p != NULL)
        {
            if (max_depth >= 0 && depth++ >= max_depth)
            {
                break;
            }
            assert(p->num_sge == 1);
            requestor->qp_ex_->wr_id = i++;
            if (p->next != NULL && (max_depth < 0 || depth < max_depth))
            {
                requestor->qp_ex_->wr_flags = 0;
            }
            else
            {
                requestor->qp_ex_->wr_flags = IBV_SEND_SIGNALED;
            }
            uint32_t new_lkey = path_lkey(zqp, p->sg_list[0].lkey);
            uint32_t new_rkey = p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP
                                    ? path_rkey(zqp, p->wr.atomic.rkey)
                                    : path_rkey(zqp, p->wr.rdma.rkey);
            if (p->opcode == IBV_WR_RDMA_WRITE)
            {
                ibv_wr_rdma_write(requestor->qp_ex_, new_rkey, p->wr.rdma.remote_addr);
            }
            else if (p->opcode == IBV_WR_RDMA_READ)
            {
                ibv_wr_rdma_read(requestor->qp_ex_, new_rkey, p->wr.rdma.remote_addr);
            }
            else if (p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
            {
                ibv_wr_atomic_cmp_swp(requestor->qp_ex_, new_rkey, p->wr.atomic.remote_addr, p->wr.atomic.compare_add, p->wr.atomic.swap);
            }
            else if (p->opcode == IBV_WR_SEND)
            {
                ibv_wr_send(requestor->qp_ex_);
            }
            else
            {
                std::cerr << "Error, unsupported opcode in zDCQP_send" << std::endl;
                ibv_wr_complete(requestor->qp_ex_);
                return -1;
            }
            ibv_wr_set_sge(requestor->qp_ex_, new_lkey, p->sg_list[0].addr, p->sg_list[0].length);
            mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
            length += p->sg_list->length;
            p = p->next;
        }
        ibv_wr_complete(requestor->qp_ex_);
        (void)lid;
        (void)length;
        return poll_cq_timeout(requestor->cq_, "dcqp send");
    }

    int zDCQP_read(zDCQP_requestor *requestor, ibv_ah *ah, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num)
    {
        std::lock_guard<std::mutex> guard(requestor->lock);
        ibv_wr_start(requestor->qp_ex_);
        requestor->qp_ex_->wr_id = 0;
        requestor->qp_ex_->wr_flags = IBV_SEND_SIGNALED;
        ibv_wr_rdma_read(requestor->qp_ex_, rkey, (uint64_t)remote_addr);
        ibv_wr_set_sge(requestor->qp_ex_, lkey, (uint64_t)local_addr, length);
        mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
        ibv_wr_complete(requestor->qp_ex_);
        (void)lid;
        int rc = poll_cq_timeout(requestor->cq_, "dcqp read");
        if (rc != 0)
        {
            fprintf(stderr, "dcqp read nic ctx lkey=%u rkey=%u len=%lu local=%p remote=%lx dct=%u ah=%p\n",
                    lkey, rkey, (unsigned long)length, local_addr, (unsigned long)remote_addr,
                    dct_num, (void *)ah);
        }
        return rc;
    }

    int zDCQP_write(zDCQP_requestor *requestor, ibv_ah *ah, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num)
    {
        std::lock_guard<std::mutex> guard(requestor->lock);
        ibv_wr_start(requestor->qp_ex_);
        requestor->qp_ex_->wr_id = 0;
        requestor->qp_ex_->wr_flags = IBV_SEND_SIGNALED;
        ibv_wr_rdma_write(requestor->qp_ex_, rkey, (uint64_t)remote_addr);
        ibv_wr_set_sge(requestor->qp_ex_, lkey, (uint64_t)local_addr, length);
        mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
        ibv_wr_complete(requestor->qp_ex_);
        (void)lid;
        return poll_cq_timeout(requestor->cq_, "dcqp write");
    }

    int zDCQP_CAS(zDCQP_requestor *requestor, ibv_ah *ah, void *local_addr, uint32_t lkey, uint64_t new_val, void *remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num)
    {
        std::lock_guard<std::mutex> guard(requestor->lock);
        uint64_t expected = *(uint64_t *)local_addr;
        ibv_wr_start(requestor->qp_ex_);
        requestor->qp_ex_->wr_id = 0;
        requestor->qp_ex_->wr_flags = IBV_SEND_SIGNALED;
        ibv_wr_atomic_cmp_swp(requestor->qp_ex_, rkey, (uint64_t)remote_addr, expected, new_val);
        ibv_wr_set_sge(requestor->qp_ex_, lkey, (uint64_t)local_addr, sizeof(uint64_t));
        mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
        ibv_wr_complete(requestor->qp_ex_);
        (void)lid;
        return poll_cq_timeout(requestor->cq_, "dcqp cas");
    }

    int zQP_connect(zQP *qp, int nic_index, string ip, string port)
    {
        std::lock_guard<std::mutex> lock(zQP_connect_mutex);
        auto start = TIME_NOW;
        if (nic_index < qp->m_requestors.size() && (qp->m_requestors[nic_index] != NULL && qp->m_requestors[nic_index]->status_ == ZSTATUS_CONNECTED))
        {
            return -1;
        }
        zQPType qp_type = qp->qp_type;
        zQP_requestor *qp_instance = new zQP_requestor();
        qp_instance->status_ = ZSTATUS_INIT;
        qp_instance->channel_ = rdma_create_event_channel();
        qp_instance->pd_ = qp->m_pd->m_pds[nic_index];
        int result = rdma_create_id(qp_instance->channel_, &(qp_instance->cm_id_), NULL, RDMA_PS_TCP);
        assert(result == 0);

        rdma_addrinfo *t = NULL, *s = NULL;
        rdma_addrinfo *res, *src;
        int counter = 0;
        // rdma_bind_addr(qp_instance->cm_id_, (struct sockaddr *)&src_addr);
        while (t == NULL && counter < 100)
        {
            counter += 1;
            struct sockaddr_in src_addr; // 设置源地址（指定网卡设备）
            memset(&src_addr, 0, sizeof(src_addr));
            src_addr.sin_family = AF_INET;
            inet_pton(AF_INET, qp->m_ep->m_devices[nic_index]->eth_ip.c_str(), &src_addr.sin_addr); // 本地网卡IP地址
            // struct sockaddr_in target_addr;
            // memset(&target_addr, 0, sizeof(target_addr));
            // target_addr.sin_family = AF_INET;
            // inet_pton(AF_INET, ip.c_str(), &target_addr.sin_addr);
            // target_addr.sin_port = htons(stoi(port));
            // rdma_addrinfo target;
            // target.ai_src_addr = (struct sockaddr*)&src_addr;
            // rdma_bind_addr(qp_instance->cm_id_, (struct sockaddr *)&src_addr);
            result = rdma_getaddrinfo(ip.c_str(), port.c_str(), NULL, &res);
            // assert(result == 0);
            // result = rdma_resolve_addr(qp_instance->cm_id_, (struct sockaddr *)&src_addr, (struct sockaddr *)&target_addr, RESOLVE_TIMEOUT_MS);
            // while(result != 0) {
            //     result = rdma_resolve_addr(qp_instance->cm_id_, (struct sockaddr *)&src_addr, (struct sockaddr *)&target_addr, RESOLVE_TIMEOUT_MS);
            // }
            assert(result == 0);
            for (t = res; t; t = t->ai_next)
            {
                // struct sockaddr_in *ipv4 = (struct sockaddr_in *)t->ai_dst_addr;
                // char ipAddress[INET_ADDRSTRLEN];
                // inet_ntop(AF_INET, &(ipv4->sin_addr), ipAddress, INET_ADDRSTRLEN);
                // printf("The IP address is: %s:%d\n", ipAddress, ntohs(ipv4->sin_port));
                if (!rdma_resolve_addr(qp_instance->cm_id_, (struct sockaddr *)&src_addr, t->ai_dst_addr, RESOLVE_TIMEOUT_MS))
                {
                    break;
                }
            }
        }
        // printf("rdma_resolve_addr tried %d times\n", counter);
        if (t == NULL)
        {
            printf("rdma_resolve_addr failed nic=%d dst=%s:%s\n", nic_index, ip.c_str(), port.c_str());
            return -1;
        }

        rdma_cm_event *event;
        result = rdma_get_cm_event(qp_instance->channel_, &event);
        if (result != 0)
        {
            perror("rdma_get_cm_event failed");
            return -1;
        }
        if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
        {
            printf("rdma_get_cm_event addr resolve failed: %d\n", event->event);
            rdma_ack_cm_event(event);
            return -1;
        }
        rdma_ack_cm_event(event);
        // Addr resolve finished, make route resolve

        result = rdma_resolve_route(qp_instance->cm_id_, RESOLVE_TIMEOUT_MS);
        if (result != 0)
        {
            perror("rdma_resolve_route failed");
            return -1;
        }
        result = rdma_get_cm_event(qp_instance->channel_, &event);
        if (result != 0 || event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
        {
            printf("rdma_resolve_route event failed: %d\n", event ? event->event : -1);
            if (result == 0)
            {
                rdma_ack_cm_event(event);
            }
            return -1;
        }
        rdma_ack_cm_event(event);
        // printf("QP connected in %ld us: qp_id=%u, nic_index=%d, server_qp_id=%u\n", TIME_DURATION_US(start, TIME_NOW), qp->qp_id_, nic_index, qp_instance->qp_id_);

        // Addr route resolve finished
        ibv_cq *cq = NULL;
        // if(qp->m_ep->m_devices[nic_index]->cq_ == NULL) {
        ibv_comp_channel *comp_channel = ibv_create_comp_channel(qp->m_ep->m_devices[nic_index]->context);
        assert(comp_channel != NULL);
        cq = ibv_create_cq(qp->m_ep->m_devices[nic_index]->context, 4096, NULL, comp_channel, 0);
        assert(cq != NULL);
        result = ibv_req_notify_cq(cq, 0);
        qp->m_ep->m_devices[nic_index]->cq_list_.push_back(cq);
        assert(result == 0);
        // } else {
        //     cq = qp->m_ep->m_devices[nic_index]->cq_list_.;
        // }

        ibv_qp_init_attr qp_init_attr;
        memset(&qp_init_attr, 0, sizeof(qp_init_attr));
        qp_init_attr.send_cq = cq;
        qp_init_attr.recv_cq = cq;
        qp_init_attr.qp_type = IBV_QPT_RC;
        qp_init_attr.cap.max_send_wr = 2048;
        qp_init_attr.cap.max_recv_wr = 1;
        qp_init_attr.cap.max_send_sge = 16;
        qp_init_attr.cap.max_recv_sge = 16;
        qp_init_attr.cap.max_inline_data = 256;
        qp_init_attr.sq_sig_all = 0;
        result = rdma_create_qp(qp_instance->cm_id_, qp->m_pd->m_pds[nic_index], &qp_init_attr);
        assert(result == 0);

        CNodeInit init_msg = {qp->qp_id_, qp_type};
        rdma_conn_param conn_param;
        memset(&conn_param, 0, sizeof(conn_param));
        conn_param.responder_resources = 16;
        conn_param.initiator_depth = 16;
        conn_param.private_data = &init_msg;
        conn_param.private_data_len = sizeof(CNodeInit);
        conn_param.retry_count = RETRY_TIMEOUT;
        conn_param.rnr_retry_count = RETRY_TIMEOUT;
        result = rdma_connect(qp_instance->cm_id_, &conn_param);
        if (result != 0)
        {
            perror("rdma_connect failed");
            return -1;
        }
        result = rdma_get_cm_event(qp_instance->channel_, &event);
        if (result != 0 || event->event != RDMA_CM_EVENT_ESTABLISHED)
        {
            printf("rdma_connect event failed: %d\n", event ? event->event : -1);
            if (result == 0)
            {
                rdma_ack_cm_event(event);
            }
            return -1;
        }
        // assert(result == 0);
        // assert(event->event == RDMA_CM_EVENT_ESTABLISHED);

        struct PData server_pdata;
        memset(&server_pdata, 0, sizeof(server_pdata));
        memcpy(&server_pdata, event->param.conn.private_data, sizeof(server_pdata));
        printf("QP connect nic=%d dst=%s:%s private_data_len=%u pdata=%zu nic_num=%u\n",
               nic_index, ip.c_str(), port.c_str(),
               event->param.conn.private_data_len, sizeof(server_pdata),
               server_pdata.nic_num_);

        if (server_pdata.nic_num_ > qp->m_ep->m_devices.size())
        {
            server_pdata.nic_num_ = qp->m_ep->m_devices.size();
        }
        qp_instance->server_cmd_msg_ = server_pdata.buf_addr;
        for (int i = 0; i < MAX_NIC_NUM; i++)
        {
            qp_instance->server_cmd_rkey_[i] = server_pdata.buf_rkey[i];
        }
        // qp_instance->server_cmd_rkey_ = server_pdata.buf_rkey;
        qp_instance->conn_id_ = server_pdata.conn_id;
        qp_instance->qp_id_ = server_pdata.qp_id;
        if (qp->qp_id_ == 0)
        {
            qp->qp_id_ = server_pdata.qp_id;
        }
        for (int i = 0; i < server_pdata.nic_num_; i++)
        {
            zTarget *target;
            if (qp->m_targets.find(i) != qp->m_targets.end())
            {
                target = qp->m_targets[i];
                assert(qp->m_targets[i] == target);
                assert(target->lid_ == server_pdata.lid_[i]);
                assert(target->dct_num_ == server_pdata.dct_num_[i]);
                assert(target->ip == string(server_pdata.ip[i]));
                assert(target->port == server_pdata.port[i]);
            }
            else
            {
                target = new zTarget();
                qp->m_targets[i] = target;
                target->ah = zDCQP_create_ah(qp->m_pd->m_requestors[i][0], server_pdata.gid1[i], server_pdata.gid2[i], server_pdata.gid2[i], server_pdata.gid1[i], server_pdata.lid_[i]);
                target->lid_ = server_pdata.lid_[i];
                target->dct_num_ = server_pdata.dct_num_[i];
                target->ip = string(server_pdata.ip[i]);
                target->port = server_pdata.port[i];
            }
            printf("  target[%d] ip=%s port=%s dct=%u lid=%u ah=%p\n",
                   i, target->ip.c_str(), target->port.c_str(),
                   target->dct_num_, target->lid_, (void *)target->ah);
        }

        assert(server_pdata.size == sizeof(CmdMsgBlock));
        qp_instance->cmd_msg_ = qp->cmd_msg_;
        qp_instance->msg_mr_ = qp->msg_mr_[nic_index];

        qp_instance->cmd_resp_ = qp->cmd_resp_;
        qp_instance->resp_mr_ = qp->resp_mr_[nic_index];

        if (qp->remote_atomic_table_addr == 0)
        {
            qp->remote_atomic_table_addr = server_pdata.atomic_table_addr;
            for (int i = 0; i < MAX_NIC_NUM; i++)
            {
                qp->remote_atomic_table_rkey[i] = server_pdata.atomic_table_rkey[i];
            }
            for (int i = 0; i < MAX_NIC_NUM; i++)
            {
                if (qp->m_rkey_table->find(qp->remote_atomic_table_rkey[0]) == qp->m_rkey_table->end())
                    (*qp->m_rkey_table)[qp->remote_atomic_table_rkey[0]] = std::vector<uint32_t>();
                qp->m_rkey_table->at(qp->remote_atomic_table_rkey[0]).push_back(qp->remote_atomic_table_rkey[i]);
            }
        }
        else if (qp->remote_atomic_table_addr != server_pdata.atomic_table_addr)
        {
            std::cerr << "Error, atomic table address mismatch" << std::endl;
            return -1;
        }
        if (qp->remote_qp_info_addr == 0)
        {
            qp->remote_qp_info_addr = server_pdata.qp_info_addr;
            for (int i = 0; i < MAX_NIC_NUM; i++)
            {
                qp->remote_qp_info_rkey[i] = server_pdata.qp_info_rkey[i];
            }
            for (int i = 0; i < MAX_NIC_NUM; i++)
            {
                if (qp->m_rkey_table->find(qp->remote_qp_info_rkey[0]) == qp->m_rkey_table->end())
                    (*qp->m_rkey_table)[qp->remote_qp_info_rkey[0]] = std::vector<uint32_t>();
                qp->m_rkey_table->at(qp->remote_qp_info_rkey[0]).push_back(qp->remote_qp_info_rkey[i]);
            }
        }
        else if (qp->remote_qp_info_addr != server_pdata.qp_info_addr)
        {
            std::cerr << "Error, qp info address mismatch" << std::endl;
            return -1;
        }

        // qp_instance->cmd_msg_ = new CmdMsgBlock();
        // memset(qp_instance->cmd_msg_, 0, sizeof(CmdMsgBlock));
        // qp_instance->msg_mr_ = mr_create(qp->m_pd->m_pds[nic_index], (void *)qp_instance->cmd_msg_, sizeof(CmdMsgBlock));
        // if (!qp_instance->msg_mr_) {
        //     perror("ibv_reg_mr m_msg_mr_ fail");
        //     return -1;
        // }

        // qp_instance->cmd_resp_ = new CmdMsgRespBlock();
        // memset(qp_instance->cmd_resp_, 0, sizeof(CmdMsgRespBlock));
        // qp_instance->resp_mr_ =
        //      mr_create(qp->m_pd->m_pds[nic_index], (void *)qp_instance->cmd_resp_, sizeof(CmdMsgRespBlock));
        // if (!qp_instance->resp_mr_) {
        //     perror("ibv_reg_mr m_resp_mr_ fail");
        //     return -1;
        // }

        rdma_ack_cm_event(event);

        rdma_freeaddrinfo(res);

        // Connect finished
        qp_instance->cq_ = cq;
        qp_instance->qp_ = qp_instance->cm_id_->qp;

        if ((int)qp->m_requestors.size() <= nic_index)
        {
            qp->m_requestors.grow_to_at_least(nic_index + 1);
        }
        qp->m_requestors[nic_index] = qp_instance;
        qp_instance->status_ = ZSTATUS_CONNECTED;
        // printf("QP connected: qp_id=%u, nic_index=%d, server_qp_id=%u\n", qp->qp_id_, nic_index, qp_instance->qp_id_);
        // ProfilerStop();
        return 0;
    }

    int z_simple_write(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        struct ibv_sge sge;
        struct ibv_send_wr send_wr;
        struct ibv_send_wr *bad_send_wr;

        sge.addr = (uint64_t)local_addr;
        sge.length = length;
        if (zqp->current_device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge.lkey = a->second[zqp->current_device];
        }
        else
        {
            sge.lkey = lkey;
        }

        send_wr.wr_id = 0;
        send_wr.sg_list = &sge;
        send_wr.num_sge = 1;
        send_wr.next = NULL;
        send_wr.opcode = IBV_WR_RDMA_WRITE;
        send_wr.send_flags = IBV_SEND_SIGNALED;
        send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
        if (zqp->current_device != 0)
        {
            send_wr.wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
        }
        else
        {
            send_wr.wr.rdma.rkey = rkey;
        }
        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, &send_wr, &bad_send_wr))
        {
            z_switch(zqp);
            return -1;
        }
        int result;
        auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                z_switch(zqp);
                result = -1;
                // std::cerr << "Error, write timeout" << std::endl;
                break;
            }
            if (ibv_poll_cq(cq, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    z_switch(zqp);
                    result = -1;
                    break;
                }
                result = 0;
                break;
            }
        }
        zqp->size_counter_.fetch_add(length);
        return result;
    }

    int z_simple_write_async(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        struct ibv_sge sge;
        struct ibv_send_wr send_wr;
        struct ibv_send_wr *bad_send_wr;

        sge.addr = (uint64_t)local_addr;
        sge.length = length;
        if (zqp->current_device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge.lkey = a->second[zqp->current_device];
        }
        else
        {
            sge.lkey = lkey;
        }

        send_wr.wr_id = ++(zqp->time_stamp);
        send_wr.sg_list = &sge;
        send_wr.num_sge = 1;
        send_wr.next = NULL;
        send_wr.opcode = IBV_WR_RDMA_WRITE;
        send_wr.send_flags = IBV_SEND_SIGNALED;
        send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
        if (zqp->current_device != 0)
        {
            send_wr.wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
        }
        else
        {
            send_wr.wr.rdma.rkey = rkey;
        }
        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, &send_wr, &bad_send_wr))
        {
            z_switch(zqp);
            return -1;
        }
        wr_ids->push_back(send_wr.wr_id);
        zqp->size_counter_.fetch_add(length);
        return 0;
    }

    int z_simple_read(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        struct ibv_sge sge;
        struct ibv_send_wr send_wr;
        struct ibv_send_wr *bad_send_wr;
        sge.addr = (uint64_t)local_addr;
        sge.length = length;
        if (zqp->current_device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge.lkey = a->second[zqp->current_device];
        }
        else
        {
            sge.lkey = lkey;
        }

        send_wr.wr_id = 0;
        send_wr.sg_list = &sge;
        send_wr.num_sge = 1;
        send_wr.next = NULL;
        send_wr.opcode = IBV_WR_RDMA_READ;
        send_wr.send_flags = IBV_SEND_SIGNALED;
        send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
        if (zqp->current_device != 0)
        {
            send_wr.wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
        }
        else
        {
            send_wr.wr.rdma.rkey = rkey;
        }
        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, &send_wr, &bad_send_wr))
        {
            z_switch(zqp);
            return -1;
        }
        int result = -1;
        auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                z_switch(zqp);
                break;
            }
            if (ibv_poll_cq(cq, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    result = -1;
                    z_switch(zqp);
                    break;
                }
                result = 0;
                break;
            }
        }
        return result;
    }

    int z_simple_read_async(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        struct ibv_sge sge;
        struct ibv_send_wr send_wr;
        struct ibv_send_wr *bad_send_wr;

        sge.addr = (uint64_t)local_addr;
        sge.length = length;
        if (zqp->current_device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge.lkey = a->second[zqp->current_device];
        }
        else
        {
            sge.lkey = lkey;
        }

        send_wr.wr_id = ++(zqp->time_stamp);
        send_wr.sg_list = &sge;
        send_wr.num_sge = 1;
        send_wr.next = NULL;
        send_wr.opcode = IBV_WR_RDMA_READ;
        send_wr.send_flags = IBV_SEND_SIGNALED;
        send_wr.wr.rdma.remote_addr = (uint64_t)remote_addr;
        if (zqp->current_device != 0)
        {
            send_wr.wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
        }
        else
        {
            send_wr.wr.rdma.rkey = rkey;
        }
        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, &send_wr, &bad_send_wr))
        {
            z_switch(zqp);
            return -1;
        }
        wr_ids->push_back(send_wr.wr_id);
        zqp->size_counter_.fetch_add(length);
        return 0;
    }

    int z_simple_CAS(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void *remote_addr, uint32_t rkey)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        int entry_index = wr_log_alloc(zqp);
        if (entry_index < 0)
        {
            return -1;
        }
        struct ibv_sge *sge = &zqp->sge_[entry_index];
        struct ibv_send_wr *send_wr = &zqp->wr_[entry_index];
        struct ibv_send_wr *bad_send_wr;

        sge->addr = (uint64_t)local_addr;
        sge->length = sizeof(uint64_t);
        if (zqp->current_device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge->lkey = a->second[zqp->current_device];
        }
        else
        {
            sge->lkey = lkey;
        }

        send_wr->wr_id = 0;
        send_wr->sg_list = sge;
        send_wr->num_sge = 1;
        send_wr->next = NULL;
        send_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        send_wr->send_flags = IBV_SEND_SIGNALED;
        send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
        if (zqp->current_device != 0)
        {
            send_wr->wr.atomic.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
        }
        else
        {
            send_wr->wr.atomic.rkey = rkey;
        }
        send_wr->wr.atomic.compare_add = *(uint64_t *)local_addr;
        send_wr->wr.atomic.swap = new_val;
        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            std::cout << "Send error" << std::endl;
            z_switch(zqp);
            return -1;
        }
        int result;
        auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                z_switch(zqp);
                result = -1;
                std::cout << "Error, read timeout" << std::endl;
                break;
            }
            if (ibv_poll_cq(cq, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    std::cout << "Wc error " << wc.status << std::endl;
                    z_switch(zqp);
                    result = -1;
                    break;
                }
                result = 0;
                break;
            }
        }
        return result;
    }

    int z_simple_CAS_async(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void *remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        struct ibv_sge *sge = new ibv_sge();
        struct ibv_send_wr *send_wr = new ibv_send_wr();
        struct ibv_send_wr *bad_send_wr;
        struct ibv_sge log_sge;
        struct ibv_send_wr log_wr;

        sge->addr = (uint64_t)local_addr;
        sge->length = sizeof(uint64_t);
        if (zqp->current_device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge->lkey = a->second[zqp->current_device];
        }
        else
        {
            sge->lkey = lkey;
        }

        send_wr->wr_id = ++(zqp->time_stamp);
        send_wr->sg_list = sge;
        send_wr->num_sge = 1;
        send_wr->next = NULL;
        send_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        send_wr->send_flags = IBV_SEND_SIGNALED;
        send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
        if (zqp->current_device != 0)
        {
            send_wr->wr.atomic.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
        }
        else
        {
            send_wr->wr.atomic.rkey = rkey;
        }
        send_wr->wr.atomic.compare_add = *(uint64_t *)local_addr;
        send_wr->wr.atomic.swap = new_val;
        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            z_switch(zqp);
            return -1;
        }
        wr_ids->push_back(send_wr->wr_id);
        return 0;
    }

    int zQP_read(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, uint32_t time_stamp)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        int entry_index = wr_log_alloc(zqp);
        if (entry_index < 0)
        {
            wr_log_advance(zqp);
            drain_endpoint_cqs(zqp->m_ep);
            entry_index = wr_log_alloc(zqp);
            if (entry_index < 0)
            {
                return -1;
            }
        }
        struct ibv_sge *sge = &zqp->sge_[entry_index];
        struct ibv_send_wr *send_wr = &zqp->wr_[entry_index];
        struct ibv_send_wr *bad_send_wr;
        memset(sge, 0, sizeof(*sge));
        memset(send_wr, 0, sizeof(*send_wr));
        sge->addr = (uint64_t)local_addr;
        sge->length = length;
        sge->lkey = lkey;

        zqp->wr_entry_[entry_index].time_stamp = time_stamp;
        zqp->wr_entry_[entry_index].wr_addr = (uint64_t)send_wr;
        zqp->wr_entry_[entry_index].finished = 0;
        uint64_t wr_id = *((uint64_t *)(&zqp->wr_entry_[entry_index]) + 1);
        vector<uint64_t> wr_ids;
        wr_ids.push_back(wr_id);

        send_wr->wr_id = wr_id;
        send_wr->sg_list = sge;
        send_wr->num_sge = 1;
        send_wr->next = NULL;
        send_wr->opcode = IBV_WR_RDMA_READ;
        send_wr->send_flags = IBV_SEND_SIGNALED;
        send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
        send_wr->wr.rdma.rkey = rkey;
        sge->lkey = path_lkey(zqp, lkey);
        send_wr->wr.rdma.rkey = path_rkey(zqp, rkey);
        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            sge->lkey = lkey;
            send_wr->wr.rdma.rkey = rkey;
            zqp->wr_entry_[entry_index].finished = 1;
            std::cerr << "Error, ibv_post_send failed" << std::endl;
            return -1;
        }
        sge->lkey = lkey;
        send_wr->wr.rdma.rkey = rkey;

        send_wr->next = NULL;
#ifndef POLLTHREAD
        int result;
        auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                z_switch(zqp);
#ifdef RECOVERY
                z_recovery(zqp);
#endif
                result = -1;
                break;
            }
            if (ibv_poll_cq(cq, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    z_switch(zqp);
#ifdef RECOVERY
                    z_recovery(zqp);
#endif
                    result = -1;
                    break;
                }
                result = 0;
                break;
            }
        }
        if (result == 0)
        {
            wr_log_complete(zqp, time_stamp, (uint64_t)send_wr);
            zqp->size_counter_.fetch_add(length);
        }
        return result;
#else
        return z_poll_completion(zqp, wr_id);
#endif
    }


    int z_poll_till_completion(zQP *qp) {
        if (!requestor_ready(qp, qp->current_device))
        {
            return 0;
        }
        zQP_requestor *requestor = qp->m_requestors[qp->current_device];
        int result;
        auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                z_switch(qp);
#ifdef RECOVERY
                z_recovery(qp);
#endif
                result = 0;
                break;
            }
            if (ibv_poll_cq(cq, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    z_switch(qp);
#ifdef RECOVERY
                    z_recovery(qp);
#endif
                    result = 0;
                    break;
                }
                wr_log_complete_by_wr_id(qp, wc.wr_id);
                result = 0;
                break;
            }
        }
        return result;
    }

    int z_poll_send_completion(zQP *qp, ibv_wc& wc) {
        if (pop_recovered_wc(qp, wc) > 0)
        {
            return 1;
        }
        int ret = 0;
        int nreq = (int)qp->m_requestors.size();
        int upto = qp->current_device;
        if (upto >= nreq)
        {
            upto = nreq - 1;
        }
        for (int i = 0; i <= upto; i++)
        {
            zQP_requestor *requestor = qp->m_requestors[i];
            if (requestor == NULL || requestor->status_ != ZSTATUS_CONNECTED || requestor->cq_ == NULL)
            {
                continue;
            }
            ret = ibv_poll_cq(requestor->cq_, 1, &wc);
            if (ret <= 0)
            {
                continue;
            }
            if (wc.status != IBV_WC_SUCCESS)
            {
                if (i == qp->current_device)
                {
                    z_switch(qp);
#ifdef RECOVERY
                    z_recovery(qp);
#endif
                }
                if (pop_recovered_wc(qp, wc) > 0)
                {
                    return 1;
                }
                wc.status = IBV_WC_SUCCESS;
                return 0;
            }
            wr_log_complete_by_wr_id(qp, wc.wr_id);
            return ret;
        }
        return 0;
    }

    int zQP_write(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, uint32_t time_stamp, bool use_log)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        int entry_index = wr_log_alloc(zqp);
        if (entry_index < 0)
        {
            return -1;
        }
        struct ibv_sge *sge = &zqp->sge_backup[entry_index];
        struct ibv_send_wr *send_wr = &zqp->wr_backup[entry_index];
        struct ibv_send_wr *bad_send_wr;

        zWR_entry entry;
        entry.time_stamp = time_stamp;
        entry.wr_addr = (uint64_t)send_wr;
        entry.finished = 0;
        entry.reserved = 0;

        zqp->wr_entry_[entry_index].time_stamp = entry.time_stamp;
        zqp->wr_entry_[entry_index].wr_addr = entry.wr_addr;
        zqp->wr_entry_[entry_index].finished = 0;
        uint64_t wr_id = pack_wr_header(entry.wr_addr, entry.time_stamp, 0);

        sge->addr = (uint64_t)local_addr;
        sge->length = length;
        sge->lkey = lkey;

        send_wr->wr_id = wr_id;
        send_wr->sg_list = sge;
        send_wr->num_sge = 1;
        send_wr->next = NULL;
        send_wr->opcode = IBV_WR_RDMA_WRITE;
        send_wr->imm_data = 0;
        send_wr->send_flags = IBV_SEND_SIGNALED;
        if (use_log)
        {
            fill_logged_write_imm(send_wr, time_stamp, entry_index);
        }
        send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
        send_wr->wr.rdma.rkey = rkey;
        sge->lkey = path_lkey(zqp, lkey);
        send_wr->wr.rdma.rkey = path_rkey(zqp, rkey);
        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            sge->lkey = lkey;
            send_wr->wr.rdma.rkey = rkey;
            z_switch(zqp);
            z_recovery(zqp);
            perror("Error, ibv_post_send failed");
            return -1;
        }
        sge->lkey = lkey;
        send_wr->wr.rdma.rkey = rkey;

#ifndef POLLTHREAD
        int result;
        auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                z_switch(zqp);
                z_recovery(zqp);
                result = -1;
                // std::cerr << "Error, read timeout" << std::endl;
                return result;
            }
            if (ibv_poll_cq(cq, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    z_switch(zqp);
                    z_recovery(zqp);
                    result = -1;
                    return result;
                }
                result = 0;
                break;
            }
        }
        // zqp->wr_entry_[entry_index].finished = 1;
        // zqp->wr_entry_[entry_index].wr_addr = 0;

        wr_log_complete(zqp, time_stamp, (uint64_t)send_wr);
        zqp->size_counter_.fetch_add(length);
        return result;
#else
        return z_poll_completion(zqp, wr_id);
#endif
    }

    int zQP_CAS(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void *remote_addr, uint32_t rkey, uint32_t time_stamp)
    {
        uint64_t expected = *(uint64_t *)local_addr;

        int offset = wr_log_alloc(zqp);
        if (offset < 0)
        {
            return -1;
        }
        struct ibv_sge *sge = &zqp->sge_backup[offset];
        struct ibv_send_wr *send_wr = &zqp->wr_backup[offset];
        struct ibv_send_wr *bad_send_wr;
        memset(sge, 0, sizeof(*sge));
        memset(send_wr, 0, sizeof(*send_wr));
        sge->addr = (uint64_t)local_addr;
        sge->length = sizeof(uint64_t);
        sge->lkey = lkey;

        zAtomic_entry entry;
        entry.time_stamp = time_stamp;
        entry.qp_id = zqp->qp_id_;
        entry.offset = offset;
        uint64_t uid = *(uint64_t *)&entry;

        zqp->wr_entry_[offset].time_stamp = time_stamp;
        zqp->wr_entry_[offset].wr_addr = (uint64_t)send_wr;
        zqp->wr_entry_[offset].finished = 0;
        zqp->wr_entry_[offset].reserved = new_val;
        uint64_t wr_id = *((uint64_t *)(&zqp->wr_entry_[offset]) + 1);

        send_wr->wr_id = 0;
        send_wr->sg_list = sge;
        send_wr->num_sge = 1;
        send_wr->next = NULL;
        send_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        send_wr->send_flags = IBV_SEND_SIGNALED;
        send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
        send_wr->wr.atomic.rkey = rkey;
        send_wr->wr.atomic.compare_add = expected;
        send_wr->wr.atomic.swap = uid;

        int result = 0;
        if (write_cas_buffer(zqp, offset, new_val, remote_addr, time_stamp) != 0)
        {
            sge->lkey = lkey;
            send_wr->wr.atomic.rkey = rkey;
            zqp->wr_entry_[offset].finished = 1;
            if (z_switch(zqp) == 0)
            {
#ifdef RECOVERY
                z_recovery(zqp);
#endif
            }
            return -1;
        }

        if (requestor_ready(zqp, zqp->current_device))
        {
            zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
            sge->lkey = path_lkey(zqp, lkey);
            send_wr->wr.atomic.rkey = path_rkey(zqp, rkey);
            if ((result = ibv_post_send(requestor->qp_, send_wr, &bad_send_wr)) != 0)
            {
                sge->lkey = lkey;
                send_wr->wr.atomic.rkey = rkey;
                zqp->wr_entry_[offset].finished = 1;
                std::cerr << "Error, ibv_post_send CAS failed:" << result << std::endl;
                return -1;
            }
            sge->lkey = lkey;
            send_wr->wr.atomic.rkey = rkey;

#ifndef POLLTHREAD
            auto start = TIME_NOW;
            struct ibv_wc wc;
            ibv_cq *cq = requestor->cq_;
            while (true)
            {
                if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
                {
                    if (z_switch(zqp) == 0)
                    {
#ifdef RECOVERY
                        z_recovery(zqp);
#endif
                    }
                    result = -1;
                    break;
                }
                if (ibv_poll_cq(cq, 1, &wc) > 0)
                {
                    if (wc.status != IBV_WC_SUCCESS)
                    {
                        printf("CAS wc status=%d device=%d\n", wc.status, zqp->current_device);
                        if (wc_is_path_failure(wc.status))
                        {
                            if (z_switch(zqp) == 0)
                            {
#ifdef RECOVERY
                                z_recovery(zqp);
#endif
                            }
                        }
                        result = -1;
                        break;
                    }
                    result = 0;
                    break;
                }
            }
            if (result >= 0 && expected == *(uint64_t *)local_addr)
            {
                complete_cas_step2(zqp, uid, new_val, remote_addr, rkey, lkey, local_addr,
                                   time_stamp, offset);
            }
            else if (result < 0)
            {
                printf("CAS failed, expect %lu, get %lu device=%d\n",
                       expected, *(uint64_t *)local_addr, zqp->current_device);
            }
            return result;
#else
            result = z_poll_completion(zqp, wr_id);
            if (result >= 0 && expected == *(uint64_t *)local_addr)
            {
                complete_cas_step2(zqp, uid, new_val, remote_addr, rkey, lkey, local_addr,
                                   time_stamp, offset);
            }
            else
            {
                printf("CAS failed, expect %lu, get %lu\n", expected, *(uint64_t *)local_addr);
            }
            return 0;
#endif
        }

        *(uint64_t *)local_addr = expected;
        result = path_cas(zqp, local_addr, lkey, uid, remote_addr, rkey);
        if (result != 0)
        {
            sge->lkey = lkey;
            send_wr->wr.atomic.rkey = rkey;
            zqp->wr_entry_[offset].finished = 1;
            if (z_switch(zqp) == 0)
            {
#ifdef RECOVERY
                z_recovery(zqp);
#endif
            }
            return -1;
        }
        if (expected == *(uint64_t *)local_addr)
        {
            if (complete_cas_step2(zqp, uid, new_val, remote_addr, rkey, lkey, local_addr,
                                   time_stamp, offset) != 0)
            {
                return -1;
            }
            *(uint64_t *)local_addr = expected;
        }
        return result;
    }

    int zQP_CAS_step2(zQP *zqp, uint64_t new_val, void *remote_addr, uint32_t rkey, uint32_t time_stamp, uint64_t offset)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];

        zAtomic_entry entry;
        entry.time_stamp = time_stamp;
        entry.qp_id = zqp->qp_id_;
        entry.offset = offset;

        struct ibv_sge sge;
        sge.addr = (uint64_t)requestor->cmd_resp_;
        sge.length = sizeof(uint64_t);
        sge.lkey = requestor->resp_mr_->lkey;

        struct ibv_send_wr send_wr;
        struct ibv_send_wr *bad_send_wr;
        send_wr.wr_id = 0;
        send_wr.sg_list = &sge;
        send_wr.num_sge = 1;
        send_wr.next = NULL;
        send_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        send_wr.send_flags = 0;
        send_wr.wr.atomic.remote_addr = (uint64_t)remote_addr;
        send_wr.wr.atomic.rkey = path_rkey(zqp, rkey);
        send_wr.wr.atomic.compare_add = *(uint64_t *)&entry;
        send_wr.wr.atomic.swap = new_val;
        // printf("CAS: %lu --> %lu\n", *(uint64_t*)entry, new_val);

        struct ibv_sge buffer_sge;
        struct ibv_send_wr buffer_wr;
        zAtomic_buffer buffer{};
        buffer.buffer = new_val;
        buffer.finished = 0;
        buffer.target_addr = (uint64_t)(remote_addr);
        buffer.time_stamp = time_stamp;
        uint64_t old_val = *((uint64_t *)(&buffer) + 1);
        buffer.finished = 1;
        uint64_t check_val = *((uint64_t *)(&buffer) + 1);
        // printf("old: %lu, check: %lu\n", old_val, check_val);

        buffer_sge.addr = (uint64_t)((uint64_t *)(requestor->cmd_resp_) + 1);
        buffer_sge.length = sizeof(uint64_t);
        buffer_sge.lkey = requestor->resp_mr_->lkey;
        buffer_wr.wr_id = 0;
        buffer_wr.sg_list = &buffer_sge;
        buffer_wr.num_sge = 1;
        buffer_wr.next = NULL;
        buffer_wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        buffer_wr.send_flags = 0;
        buffer_wr.wr.atomic.remote_addr = cas_buffer_remote_addr(requestor->server_cmd_msg_, entry.offset) + sizeof(uint64_t);
        buffer_wr.wr.atomic.rkey = cas_log_rkey(zqp, requestor);
        buffer_wr.wr.atomic.compare_add = old_val;
        buffer_wr.wr.atomic.swap = check_val;
        send_wr.next = &buffer_wr;

        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, &send_wr, &bad_send_wr))
        {
            std::cerr << "Error, ibv_post_send failed" << std::endl;
            return -1;
        }
// #ifndef POLLTHREAD
//         int result;
//         auto start = TIME_NOW;
//         struct ibv_wc wc;
//         ibv_cq *cq = requestor->cq_;
//         while (true)
//         {
//             if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
//             {
//                 z_switch(zqp);
//                 result = -1;
//                 // std::cerr << "Error, read timeout" << std::endl;
//                 break;
//             }
//             if (ibv_poll_cq(cq, 1, &wc) > 0)
//             {
//                 if (wc.status != IBV_WC_SUCCESS)
//                 {
//                     z_switch(zqp);
//                     result = -1;
//                     break;
//                 }
//                 result = 0;
//                 break;
//             }
//         }
//         return result;
// #endif
        return 0;
    }

    int zQP_post_send(zQP *zqp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr, bool non_idempotent,
                      uint32_t time_stamp, bool unique_cas, int max_depth)
    {
        int entry_index = wr_log_alloc(zqp);
        if (entry_index < 0)
        {
            return -1;
        }
        ibv_send_wr *copy_wr = &zqp->wr_backup[entry_index];
        ibv_send_wr *chain_head = copy_wr;
        ibv_send_wr *prev_q = NULL;
        ibv_send_wr *p = send_wr;
        ibv_send_wr *q = copy_wr;
        std::set<ibv_send_wr *> wr_set;
        vector<uint64_t> wr_ids;
        int start_index = -1;
        int end_index = 0;
        bool retry = false;
        // deep copy
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        uint64_t length = 0;
        int depth = 0;
        while (p != NULL)
        {
            if (max_depth >= 0 && depth++ >= max_depth)
            {
                q->next = NULL;
                break;
            }
            memcpy(q, p, sizeof(ibv_send_wr));
            if(p->send_flags & IBV_SEND_SIGNALED) {
                // remove the signaled flag, we will handle it later
                wr_ids.push_back(p->wr_id);
            }
            ibv_sge *sge = &zqp->sge_backup[entry_index];
            // if (p->sg_list != NULL)
            // {
            // memcpy(sge, p->sg_list, sizeof(ibv_sge) * p->num_sge);
            sge->addr = p->sg_list->addr;
            sge->length = p->sg_list->length;
            sge->lkey = p->sg_list->lkey;
            length += p->sg_list->length;
            // }
            // else
            // {
                // sge = NULL;
            // }
            q->sg_list = sge;
            if (p->opcode == IBV_WR_SEND || p->opcode == IBV_WR_RDMA_WRITE ||
                p->opcode == IBV_WR_RDMA_WRITE_WITH_IMM || p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
            {
                retry = true;
            }
            if (zqp->current_device != 0)
            {
                for (int i = 0; i < p->num_sge; i++)
                {
                    q->sg_list[i].lkey = path_lkey(zqp, p->sg_list[i].lkey);
                }
                if (q->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
                {
                    q->wr.atomic.rkey = path_rkey(zqp, p->wr.atomic.rkey);
                }
                else
                {
                    q->wr.rdma.rkey = path_rkey(zqp, p->wr.rdma.rkey);
                }
                // p = p->next;
            }
#ifdef LOG
            if (!unique_cas && q->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
#else
            if (false)
#endif
            {
                zAtomic_entry entry;
                entry.time_stamp = time_stamp;
                entry.qp_id = zqp->qp_id_;
                entry.offset = entry_index;

                // struct ibv_send_wr *send_wr = new ibv_send_wr();
                // struct ibv_send_wr *bad_send_wr;
                zqp->wr_entry_[entry_index].time_stamp = time_stamp;
                zqp->wr_entry_[entry_index].wr_addr = (uint64_t)q;
                zqp->wr_entry_[entry_index].finished = 0;
                zqp->wr_entry_[entry_index].reserved = q->wr.atomic.swap;
                uint64_t orig_swap = q->wr.atomic.swap;

                q->wr.atomic.swap = *(uint64_t *)&entry;

                struct ibv_sge *buffer_sge = &zqp->sge_[entry_index];
                struct ibv_send_wr *buffer_wr = &zqp->wr_[entry_index];
                zAtomic_buffer *buffer = &zqp->atomic_buffer_[entry_index];
                buffer->buffer = orig_swap;
                buffer->finished = 0;
                buffer->target_addr = (uint64_t)(q->wr.atomic.remote_addr);
                buffer->time_stamp = time_stamp;
                uint64_t wr_id = *((uint64_t *)(&zqp->wr_entry_[entry_index]) + 1);
                buffer_sge->addr = (uint64_t)(buffer);
                buffer_sge->length = sizeof(zAtomic_buffer);
                buffer_sge->lkey = 0;
                buffer_wr->wr_id = wr_id;
                buffer_wr->sg_list = buffer_sge;
                buffer_wr->num_sge = 1;
                buffer_wr->next = q;
                buffer_wr->opcode = IBV_WR_RDMA_WRITE;
                buffer_wr->send_flags = IBV_SEND_INLINE;
                buffer_wr->wr.rdma.remote_addr = cas_buffer_remote_addr(requestor->server_cmd_msg_, entry_index);
                buffer_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[zqp->current_device];
                if (prev_q != NULL)
                {
                    prev_q->next = buffer_wr;
                }
                else
                {
                    chain_head = buffer_wr;
                }
                q->send_flags &= ~IBV_SEND_SIGNALED;
            }
#ifdef LOG
            else if (p->opcode == IBV_WR_RDMA_WRITE || p->opcode == IBV_WR_RDMA_WRITE_WITH_IMM)
            {
                zqp->wr_entry_[entry_index].time_stamp = time_stamp;
                zqp->wr_entry_[entry_index].wr_addr = (uint64_t)q;
                zqp->wr_entry_[entry_index].finished = 0;
                wr_set.insert(q);
                if (start_index == -1)
                {
                    start_index = entry_index;
                }
                end_index = entry_index;
                fill_logged_write_imm(q, time_stamp, entry_index);
            }
            else if (p->opcode == IBV_WR_SEND || p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
            {
                // int entry_index = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;
                struct ibv_sge *log_sge = zqp->sge_ + entry_index;
                struct ibv_send_wr *log_wr = zqp->wr_ + entry_index;
                zWR_entry *entry = zqp->wr_entry_ + entry_index;
                entry->time_stamp = time_stamp;
                entry->wr_addr = (uint64_t)q;
                entry->finished = 1;
                wr_set.insert(q);
                if (start_index == -1)
                {
                    start_index = entry_index;
                }
                end_index = entry_index;
                uint64_t wr_id = pack_wr_header((uint64_t)q, time_stamp, 0);
                zqp->wr_entry_[entry_index].finished = 0;
                zqp->wr_entry_[entry_index].wr_addr = (uint64_t)q;
                
                log_sge->addr = (uint64_t)((uint64_t *)(&zqp->wr_entry_[entry_index]) + 1);
                log_sge->length = sizeof(uint64_t);
                log_sge->lkey = 0;
                log_wr->wr_id = wr_id;
                log_wr->sg_list = log_sge;
                log_wr->num_sge = 1;
                log_wr->next = q->next;
                log_wr->opcode = IBV_WR_RDMA_WRITE;
                log_wr->send_flags = IBV_SEND_INLINE;
                log_wr->wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zWR_entry) + sizeof(uint64_t);
                log_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[zqp->current_device];
                q->next = log_wr;
                q->send_flags &= ~IBV_SEND_SIGNALED;
                q = q->next;
                zqp->wr_entry_[entry_index].finished = 0;
            }
#endif
            else {
#ifdef SEND_LOG
                zWR_entry *entry = zqp->wr_entry_ + entry_index;
                entry->time_stamp = time_stamp;
                entry->wr_addr = (uint64_t)q;
                entry->finished = 0;
#endif
            }
            p = p->next;
            if (p != NULL && (max_depth < 0 || depth < max_depth))
            {
                prev_q = q;
                entry_index = wr_log_alloc(zqp);
                if (entry_index < 0)
                {
                    break;
                }
                q->next = &zqp->wr_backup[entry_index];
                q = q->next;
            }
            else
            {
                q->next = NULL;
                break;
            }
        }
        q->send_flags |= IBV_SEND_SIGNALED;

        if (retry != non_idempotent)
        {
            // std::cerr << "Warning, inconsistent non_idempotent flag" << std::endl;
            non_idempotent = retry;
        }

        // wr_ids->push_back(wr_id);
        // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;
        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, chain_head, bad_wr))
        {
            if (end_index < start_index)
                end_index += WR_ENTRY_NUM;
            for (int i = start_index; i <= end_index; i++)
                zqp->wr_entry_[i % WR_ENTRY_NUM].finished = 1;
            perror("Error, ibv_post_send failed");
        }
        // for (auto k = copy_wr; k != NULL;)
        // {
        //     ibv_send_wr *next = k->next;
        //     if (k->opcode == IBV_WR_RDMA_READ)
        //     {
        //         if (k->sg_list)
        //             free(k->sg_list);
        //         delete k;
        //     }
        //     k = next;
        // }
        // q->next = NULL;
#ifndef POLLTHREAD
        int result;
        auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                z_switch(zqp);
#ifdef RECOVERY
                z_recovery(zqp);
#endif
                result = -1;
                // std::cerr << "Error, read timeout" << std::endl;
                return result;
            }
            if (ibv_poll_cq(cq, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    z_switch(zqp);
#ifdef RECOVERY
                    z_recovery(zqp);
#endif
                    result = -1;
                    return result;
                }
                result = 0;
                break;
            }
        }
        // if (end_index < start_index)
        //     end_index += WR_ENTRY_NUM;
        // for (int i = start_index; i <= end_index; i++)
        //     printf("timestamp: %u, wr_addr: %lu, finished: %d\n", zqp->wr_entry_[i % WR_ENTRY_NUM].time_stamp, zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr, zqp->wr_entry_[i % WR_ENTRY_NUM].finished);
#ifdef SEND_LOG
        int start_ = zqp->entry_start_;
        int end_ = zqp->entry_end_.load() % WR_ENTRY_NUM;
        if (start_ > end_)
            end_ += WR_ENTRY_NUM;
        if (start_ != end_)
        {
            for (int i = start_; i < end_; i++)
            {
                if (zqp->wr_entry_[i % WR_ENTRY_NUM].time_stamp == time_stamp &&
                    wr_set.find((ibv_send_wr *)zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr) != wr_set.end())
                {
                    wr_set.erase((ibv_send_wr *)zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr);
                    // free(((ibv_send_wr *)zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr)->sg_list);
                    // delete (ibv_send_wr *)zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr;
                    zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr = 0;
                    zqp->wr_entry_[i % WR_ENTRY_NUM].finished = 1;
                }
                if ((zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr == 0 ||
                     zqp->wr_entry_[i % WR_ENTRY_NUM].finished == 1) &&
                    i % WR_ENTRY_NUM == zqp->entry_start_)
                {
                    zqp->entry_start_.fetch_add(1);
                }
            }
        }
#endif
        // zqp->size_counter_.fetch_add(length);
        return result;
#else
        return z_poll_completion(zqp, &wr_ids);
        // return z_poll_completion(zqp, wr_id);
#endif
    }

    // Motor doorbells are READ / WRITE / a single CAS. The steady path keeps
    // the completion log (WRITE_WITH_IMM, plus one inline CAS-intent write)
    // but posts from the per-slot wr_backup/sge_backup arrays. The previous
    // body malloc'd a WR and an SGE per operation and never freed them, and
    // it turned every CAS into an inline write plus three CASes.
    static bool zqp_fast_chain_ok(const ibv_send_wr *send_wr, int max_depth)
    {
        int depth = 0;
        int ncas = 0;
        for (const ibv_send_wr *p = send_wr; p != NULL; p = p->next)
        {
            if (max_depth >= 0 && depth >= max_depth)
            {
                break;
            }
            depth++;
            if (depth > 120 || p->num_sge != 1 || p->sg_list == NULL)
            {
                return false;
            }
            if (p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
            {
                ncas++;
                if (ncas > 8)
                {
                    return false;
                }
            }
            else if (p->opcode != IBV_WR_RDMA_READ &&
                     p->opcode != IBV_WR_RDMA_WRITE &&
                     p->opcode != IBV_WR_RDMA_WRITE_WITH_IMM)
            {
                return false;
            }
        }
        return depth > 0;
    }

    static int zQP_post_send_async_fast(zQP *zqp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr,
                                        uint32_t time_stamp, vector<uint64_t> *wr_ids, int max_depth,
                                        int wr_id_override)
    {
        ibv_send_wr *user_wrs[128];
        int slots[128];
        int nuser = 0;
        ibv_send_wr *nic_head = NULL;
        ibv_send_wr *nic_tail = NULL;
        int end_index = -1;
        int depth = 0;
        int raw_begin = zqp->entry_end_.load();

        for (ibv_send_wr *p = send_wr; p != NULL; p = p->next)
        {
            if (max_depth >= 0 && depth >= max_depth)
            {
                break;
            }
            depth++;
            int entry_index = wr_log_alloc(zqp);
            end_index = entry_index;
            ibv_send_wr *q = &zqp->wr_backup[entry_index];
            ibv_sge *sge = &zqp->sge_backup[entry_index];
            memcpy(q, p, sizeof(*q));
            *sge = p->sg_list[0];
            q->sg_list = sge;
            q->num_sge = 1;
            q->next = NULL;

            zqp->wr_entry_[entry_index].time_stamp = time_stamp;
            zqp->wr_entry_[entry_index].wr_addr = (uint64_t)q;
            zqp->wr_entry_[entry_index].finished = 0;
            zqp->wr_entry_[entry_index].reserved = q->wr_id;

            if (q->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
            {
                // Keep the intended swap in the local log for retransmission.
                // An extra inline log write ahead of the CAS added a full
                // message to every lock doorbell.
                zqp->wr_entry_[entry_index].reserved = q->wr.atomic.swap;
            }
            else if (q->opcode == IBV_WR_RDMA_WRITE || q->opcode == IBV_WR_RDMA_WRITE_WITH_IMM)
            {
#ifdef LOG
                fill_logged_write_imm(q, time_stamp, entry_index);
#endif
            }
            if (nic_tail != NULL)
            {
                nic_tail->next = q;
            }
            else
            {
                nic_head = q;
            }
            nic_tail = q;
            user_wrs[nuser] = q;
            slots[nuser] = entry_index;
            nuser++;
        }

        if (nic_head == NULL || nic_tail == NULL)
        {
            return -1;
        }
        nic_tail->send_flags |= IBV_SEND_SIGNALED;
        if (wr_id_override != -1)
        {
            nic_tail->wr_id = (uint64_t)wr_id_override;
            if (nic_tail->opcode != IBV_WR_ATOMIC_CMP_AND_SWP && end_index >= 0)
            {
                zqp->wr_entry_[end_index].reserved = (uint64_t)(uint32_t)wr_id_override;
            }
        }
        int rc = ibv_post_send(zqp->m_requestors[0]->qp_, nic_head, bad_wr);
        for (int i = 0; i < nuser; i++)
        {
            user_wrs[i]->next = (i + 1 < nuser) ? user_wrs[i + 1] : NULL;
        }
        if (rc != 0)
        {
            for (int i = 0; i < nuser; i++)
            {
                zqp->wr_entry_[slots[i]].finished = 1;
                zqp->wr_entry_[slots[i]].wr_addr = 0;
            }
            wr_log_advance(zqp);
            perror("Error, ibv_post_send failed");
            return -1;
        }
        zqp_span_push(zqp, nic_tail->wr_id, raw_begin, zqp->entry_end_.load());
        if (wr_ids != nullptr)
        {
            wr_ids->push_back(nic_tail->wr_id);
        }
        return 0;
    }

    int zQP_post_send_async(zQP *zqp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr, bool non_idempotent,
                            uint32_t time_stamp, vector<uint64_t> *wr_ids, bool unique_cas, int max_depth, int wr_id)
    {
        if (!unique_cas && zqp->current_device == 0 &&
            !zqp->m_requestors.empty() && zqp->m_requestors[0] != NULL &&
            zqp->m_requestors[0]->status_ == ZSTATUS_CONNECTED &&
            zqp_fast_chain_ok(send_wr, max_depth))
        {
            return zQP_post_send_async_fast(zqp, send_wr, bad_wr, time_stamp, wr_ids, max_depth, wr_id);
        }
        static int slow_logs = 0;
        if (slow_logs < 4)
        {
            slow_logs++;
            fprintf(stderr, "zQP_post_send_async slow path device=%d unique_cas=%d opcode=%d\n",
                    zqp->current_device, (int)unique_cas,
                    send_wr != NULL ? (int)send_wr->opcode : -1);
        }
                vector<ibv_send_wr*> copy_wrs;
        vector<ibv_sge*> copy_sges;
        vector<zWR_entry*> wr_entries;
        ibv_send_wr *copy_wr = new ibv_send_wr();
        ibv_send_wr *p = send_wr;
        ibv_send_wr *q = copy_wr;
        ibv_send_wr *prev_q = NULL;
        std::set<ibv_send_wr *> wr_set;
        int start_index = -1;
        int end_index = 0;
        bool retry = false;
        // deep copy
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        int depth = 0;
        while (p != NULL)
        {
            if (max_depth >= 0 && depth++ >= max_depth)
            {
                q->next = NULL;
                break;
            }
            memcpy(q, p, sizeof(ibv_send_wr));
            ibv_sge *sge = (ibv_sge *)malloc(sizeof(ibv_sge) * p->num_sge);
            if (p->sg_list != NULL)
            {
                memcpy(sge, p->sg_list, sizeof(ibv_sge) * p->num_sge);
            }
            else
            {
                sge = NULL;
            }
            q->sg_list = sge;
            if (p->opcode == IBV_WR_SEND || p->opcode == IBV_WR_RDMA_WRITE ||
                p->opcode == IBV_WR_RDMA_WRITE_WITH_IMM || p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
            {
                retry = true;
            }
            if (zqp->current_device != 0)
            {
                for (int i = 0; i < p->num_sge; i++)
                {
                    tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                    zqp->m_pd->m_lkey_table.find(a, p->sg_list[i].lkey);
                    q->sg_list[i].lkey = a->second[zqp->current_device];
                }
                if (q->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
                {
                    q->wr.atomic.rkey = zqp->m_rkey_table->at(p->wr.atomic.rkey)[zqp->current_device];
                }
                else
                {
                    q->wr.rdma.rkey = zqp->m_rkey_table->at(p->wr.rdma.rkey)[zqp->current_device];
                }
                // p = p->next;
            }
            if (!unique_cas && q->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
            {
                int entry_index = wr_log_alloc(zqp);
                if (entry_index < 0)
                {
                    return -1;
                }
                if (start_index == -1)
                {
                    start_index = entry_index;
                }
                end_index = entry_index;
                zAtomic_entry *entry = new zAtomic_entry();
                entry->time_stamp = time_stamp;
                entry->qp_id = zqp->qp_id_;
                entry->offset = entry_index;
                uint64_t orig_swap = q->wr.atomic.swap;
                uint64_t uid = *(uint64_t *)entry;
                zqp->wr_entry_[entry_index].time_stamp = time_stamp;
                zqp->wr_entry_[entry_index].wr_addr = (uint64_t)q;
                zqp->wr_entry_[entry_index].finished = 0;
                zqp->wr_entry_[entry_index].reserved = orig_swap;
                wr_set.insert(q);

                q->wr.atomic.swap = uid;
                q->send_flags &= ~IBV_SEND_SIGNALED;

                struct ibv_sge *buffer_sge = new ibv_sge();
                struct ibv_send_wr *buffer_wr = new ibv_send_wr();
                zAtomic_buffer *buffer = new zAtomic_buffer();
                buffer->buffer = orig_swap;
                buffer->finished = 0;
                buffer->target_addr = (uint64_t)q->wr.atomic.remote_addr;
                buffer->time_stamp = time_stamp;
                copy_wrs.push_back(buffer_wr);
                copy_sges.push_back(buffer_sge);
                uint32_t log_rkey = requestor->server_cmd_rkey_[zqp->current_device];
                if (log_rkey == 0)
                {
                    log_rkey = requestor->server_cmd_rkey_[0];
                }
                buffer_sge->addr = (uint64_t)buffer;
                buffer_sge->length = sizeof(zAtomic_buffer);
                buffer_sge->lkey = 0;
                buffer_wr->wr_id = 0;
                buffer_wr->sg_list = buffer_sge;
                buffer_wr->num_sge = 1;
                buffer_wr->opcode = IBV_WR_RDMA_WRITE;
                buffer_wr->send_flags = IBV_SEND_INLINE;
                buffer_wr->wr.rdma.remote_addr = cas_buffer_remote_addr(requestor->server_cmd_msg_, entry_index);
                buffer_wr->wr.rdma.rkey = log_rkey;

                ibv_mr *step_mr = requestor->resp_mr_;
                CmdMsgRespBlock *step_local = requestor->cmd_resp_;
                struct ibv_sge *step_sge = new ibv_sge();
                struct ibv_send_wr *step_wr = new ibv_send_wr();
                copy_wrs.push_back(step_wr);
                copy_sges.push_back(step_sge);
                step_sge->addr = (uint64_t)step_local;
                step_sge->length = sizeof(uint64_t);
                step_sge->lkey = step_mr ? step_mr->lkey : 0;
                step_wr->wr_id = 0;
                step_wr->sg_list = step_sge;
                step_wr->num_sge = 1;
                step_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
                step_wr->send_flags = 0;
                step_wr->wr.atomic.remote_addr = q->wr.atomic.remote_addr;
                step_wr->wr.atomic.rkey = q->wr.atomic.rkey;
                step_wr->wr.atomic.compare_add = uid;
                step_wr->wr.atomic.swap = orig_swap;

                zAtomic_buffer *done_buf = new zAtomic_buffer();
                *done_buf = *buffer;
                done_buf->finished = 0;
                uint64_t done_cmp = *((uint64_t *)done_buf + 1);
                done_buf->finished = 1;
                uint64_t done_swap = *((uint64_t *)done_buf + 1);
                struct ibv_sge *done_sge = new ibv_sge();
                struct ibv_send_wr *done_wr = new ibv_send_wr();
                copy_wrs.push_back(done_wr);
                copy_sges.push_back(done_sge);
                done_sge->addr = (uint64_t)((uint64_t *)step_local + 1);
                done_sge->length = sizeof(uint64_t);
                done_sge->lkey = step_mr ? step_mr->lkey : 0;
                done_wr->wr_id = 0;
                done_wr->sg_list = done_sge;
                done_wr->num_sge = 1;
                done_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
                done_wr->send_flags = 0;
                done_wr->wr.atomic.remote_addr =
                    cas_buffer_remote_addr(requestor->server_cmd_msg_, entry_index) + sizeof(uint64_t);
                done_wr->wr.atomic.rkey = log_rkey;
                done_wr->wr.atomic.compare_add = done_cmp;
                done_wr->wr.atomic.swap = done_swap;

                ibv_send_wr *cas_wr = q;
                buffer_wr->next = cas_wr;
                cas_wr->next = step_wr;
                step_wr->next = done_wr;
                done_wr->next = NULL;
                if (copy_wr == cas_wr)
                {
                    copy_wr = buffer_wr;
                }
                else if (prev_q != NULL)
                {
                    prev_q->next = buffer_wr;
                }
                q = done_wr;
            }
            else if (p->opcode == IBV_WR_RDMA_WRITE || p->opcode == IBV_WR_RDMA_WRITE_WITH_IMM)
            {
                int entry_index = wr_log_alloc(zqp);
                if (entry_index < 0)
                {
                    return -1;
                }
                if (start_index == -1)
                {
                    start_index = entry_index;
                }
                end_index = entry_index;
                zqp->wr_entry_[entry_index].time_stamp = time_stamp;
                zqp->wr_entry_[entry_index].wr_addr = (uint64_t)q;
                zqp->wr_entry_[entry_index].finished = 0;
                zqp->wr_entry_[entry_index].reserved = q->wr_id;
                wr_set.insert(q);
#ifdef LOG
                fill_logged_write_imm(q, time_stamp, entry_index);
#endif
            }
#ifdef LOG
            else if (p->opcode == IBV_WR_SEND || p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
            {
                struct ibv_sge *log_sge = new ibv_sge();
                struct ibv_send_wr *log_wr = new ibv_send_wr();
                copy_wrs.push_back(log_wr);
                copy_sges.push_back(log_sge);
                zWR_entry *entry = new zWR_entry();
                wr_entries.push_back(entry);
                entry->time_stamp = time_stamp;
                entry->wr_addr = (uint64_t)q;
                entry->finished = 1;
                wr_set.insert(q);
                int entry_index = wr_log_alloc(zqp);
        if (entry_index < 0)
        {
            return -1;
        }
                if (start_index == -1)
                {
                    start_index = entry_index;
                }
                end_index = entry_index;
                zqp->wr_entry_[entry_index].time_stamp = entry->time_stamp;
                zqp->wr_entry_[entry_index].wr_addr = entry->wr_addr;
                zqp->wr_entry_[entry_index].finished = 0;
                zqp->wr_entry_[entry_index].reserved = q->wr_id;
                uint64_t wr_id = *((uint64_t *)(&zqp->wr_entry_[entry_index]) + 1);

                log_sge->addr = (uint64_t)(entry);
                log_sge->length = sizeof(zWR_entry);
                log_sge->lkey = 0;
                log_wr->wr_id = wr_id;
                if(q->wr_id != 0) {
                    log_wr->wr_id = q->wr_id;
                }
                log_wr->sg_list = log_sge;
                log_wr->num_sge = 1;
                log_wr->next = q->next;
                log_wr->opcode = IBV_WR_RDMA_WRITE;
                log_wr->send_flags = IBV_SEND_INLINE;
                log_wr->wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zWR_entry);
                log_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[zqp->current_device];
                q->next = log_wr;
                q->send_flags &= ~IBV_SEND_SIGNALED;
                q = q->next;
            }
#endif
            else
            {
                int entry_index = wr_log_alloc(zqp);
                if (start_index == -1)
                {
                    start_index = entry_index;
                }
                end_index = entry_index;
                zqp->wr_entry_[entry_index].time_stamp = time_stamp;
                zqp->wr_entry_[entry_index].wr_addr = (uint64_t)q;
                zqp->wr_entry_[entry_index].finished = 0;
                zqp->wr_entry_[entry_index].reserved = q->wr_id;
            }
            prev_q = q;
            p = p->next;
            if (p != NULL && (max_depth < 0 || depth < max_depth))
            {
                q->next = new ibv_send_wr();
                q = q->next;
            }
            else
            {
                q->next = NULL;
                break;
            }
        }
        q->send_flags |= IBV_SEND_SIGNALED;
        if(wr_id != -1) {
            q->wr_id = wr_id;
            if (end_index >= 0)
            {
                zqp->wr_entry_[end_index % WR_ENTRY_NUM].reserved = (uint64_t)(uint32_t)wr_id;
            }
        }
        if (retry != non_idempotent)
        {
            non_idempotent = retry;
        }

        // wr_ids->push_back(wr_id);
        // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;
        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, copy_wr, bad_wr))
        {
            if (end_index < start_index)
                end_index += WR_ENTRY_NUM;
            for (int i = start_index; i <= end_index; i++)
                zqp->wr_entry_[i % WR_ENTRY_NUM].finished = 1;
            perror("Error, ibv_post_send failed");
        }
        if (wr_ids != nullptr)
        {
            wr_ids->push_back(q->wr_id);
        }
        return 0;
    }

    int zQP_read_async(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, uint32_t time_stamp, vector<uint64_t> *wr_ids)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        struct ibv_sge *sge = new ibv_sge();
        sge->addr = (uint64_t)local_addr;
        sge->length = length;
        if (zqp->current_device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge->lkey = a->second[zqp->current_device];
        }
        else
        {
            sge->lkey = lkey;
        }
        struct ibv_send_wr *send_wr = new ibv_send_wr();
        struct ibv_send_wr *bad_send_wr;

        int entry_index = wr_log_alloc(zqp);
        if (entry_index < 0)
        {
            return -1;
        }
        zqp->wr_entry_[entry_index].time_stamp = time_stamp;
        zqp->wr_entry_[entry_index].wr_addr = (uint64_t)send_wr;
        zqp->wr_entry_[entry_index].finished = 0;
        uint64_t wr_id = *((uint64_t *)(&zqp->wr_entry_[entry_index]) + 1);

        send_wr->wr_id = wr_id;
        send_wr->sg_list = sge;
        send_wr->num_sge = 1;
        send_wr->next = NULL;
        send_wr->opcode = IBV_WR_RDMA_READ;
        send_wr->send_flags = IBV_SEND_SIGNALED;
        send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
        if (zqp->current_device != 0)
        {
            send_wr->wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
        }
        else
        {
            send_wr->wr.rdma.rkey = rkey;
        }

        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            zqp->wr_entry_[entry_index].finished = 1;
            std::cerr << "Error, ibv_post_send failed" << std::endl;
            return -1;
        }
        wr_ids->push_back(wr_id);

        // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;

        send_wr->next = NULL;
        return 0;
    }

    int zQP_write_async(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey,
                        uint32_t time_stamp, bool use_log, vector<uint64_t> *wr_ids)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        int entry_index = wr_log_alloc(zqp);
        if (entry_index < 0)
        {
            static int logged_alloc = 0;
            if (logged_alloc < 5)
            {
                logged_alloc++;
                fprintf(stderr, "wr_log_alloc full start=%d end=%d\n",
                        zqp->entry_start_.load(), zqp->entry_end_.load());
            }
            return -2;
        }
        struct ibv_sge *sge = &zqp->sge_[entry_index];
        struct ibv_send_wr *send_wr = &zqp->wr_[entry_index];
        struct ibv_send_wr *bad_send_wr = NULL;
        memset(sge, 0, sizeof(*sge));
        memset(send_wr, 0, sizeof(*send_wr));

        zWR_entry *entry = &zqp->wr_entry_[entry_index];
        entry->time_stamp = time_stamp;
        entry->wr_addr = (uint64_t)send_wr;
        entry->finished = 1;
        uint64_t wr_id = *((uint64_t *)(&zqp->wr_entry_[entry_index]) + 1);

        zqp->wr_entry_[entry_index].time_stamp = entry->time_stamp;
        zqp->wr_entry_[entry_index].wr_addr = entry->wr_addr;
        zqp->wr_entry_[entry_index].finished = 0;

        sge->addr = (uint64_t)local_addr;
        sge->length = length;
        if (zqp->current_device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge->lkey = a->second[zqp->current_device];
        }
        else
        {
            sge->lkey = lkey;
        }
        send_wr->wr_id = wr_id;
        send_wr->sg_list = sge;
        send_wr->num_sge = 1;
        send_wr->next = NULL;
        send_wr->opcode = IBV_WR_RDMA_WRITE;
        send_wr->imm_data = 0;
        send_wr->send_flags = IBV_SEND_SIGNALED;
        if (use_log)
        {
            fill_logged_write_imm(send_wr, time_stamp, entry_index);
        }
        send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
        if (zqp->current_device != 0)
        {
            send_wr->wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
        }
        else
        {
            send_wr->wr.rdma.rkey = rkey;
        }
        ibv_qp *qp = requestor->qp_;
        if (int return_val = ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            zqp->wr_entry_[entry_index].finished = 1;
            wr_log_advance(zqp);
            static int logged = 0;
            if (logged < 5)
            {
                logged++;
                fprintf(stderr, "ibv_post_send failed: %s (nic=%d qp=%p)\n",
                        strerror(return_val), zqp->current_device, (void *)qp);
            }
            if (return_val == ENOMEM)
            {
                return -2;
            }
            return -1;
        }
        wr_ids->push_back(wr_id);

        // zqp->wr_entry_[zqp->entry_end_] = *entry;

        // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;

        // delete entry;
        // if (use_log)
        // {
        //     delete log_sge;
        //     delete log_wr;
        // }
        send_wr->next = NULL;
        return 0;
    }

    int zQP_CAS_async(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void *remote_addr, uint32_t rkey,
                      uint32_t time_stamp, vector<uint64_t> *wr_ids)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        uint64_t expected = *(uint64_t *)local_addr;

        struct ibv_sge *sge = new ibv_sge();
        sge->addr = (uint64_t)local_addr;
        sge->length = sizeof(uint64_t);
        if (zqp->current_device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge->lkey = a->second[zqp->current_device];
        }
        else
        {
            sge->lkey = lkey;
        }
        int entry_index = wr_log_alloc(zqp);
        if (entry_index < 0)
        {
            return -1;
        }

        // int offset = zqp->entry_end_;
        zAtomic_entry *entry = new zAtomic_entry();
        entry->time_stamp = time_stamp;
        entry->qp_id = zqp->qp_id_;
        entry->offset = entry_index;

        struct ibv_send_wr *send_wr = new ibv_send_wr();
        struct ibv_send_wr *bad_send_wr;
        zqp->wr_entry_[entry_index].time_stamp = time_stamp;
        zqp->wr_entry_[entry_index].wr_addr = (uint64_t)send_wr;
        zqp->wr_entry_[entry_index].finished = 0;
        zqp->wr_entry_[entry_index].reserved = new_val;
        uint64_t wr_id = *((uint64_t *)(&zqp->wr_entry_[entry_index]) + 1);

        send_wr->wr_id = wr_id;
        send_wr->sg_list = sge;
        send_wr->num_sge = 1;
        send_wr->next = NULL;
        send_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        send_wr->send_flags = 0;
        send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
        if (zqp->current_device != 0)
        {
            send_wr->wr.atomic.rkey = zqp->m_rkey_table->at(rkey)[zqp->current_device];
        }
        else
        {
            send_wr->wr.atomic.rkey = rkey;
        }
        send_wr->wr.atomic.compare_add = expected;
        send_wr->wr.atomic.swap = *(uint64_t *)entry;

        struct ibv_sge *buffer_sge = new ibv_sge();
        struct ibv_send_wr *buffer_wr = new ibv_send_wr();
        zAtomic_buffer *buffer = &zqp->atomic_buffer_[entry_index];
        buffer->buffer = new_val;
        buffer->finished = 0;
        buffer->target_addr = (uint64_t)(remote_addr);
        buffer->time_stamp = time_stamp;

        buffer_sge->addr = (uint64_t)(buffer);
        buffer_sge->length = sizeof(zAtomic_buffer);
        buffer_sge->lkey = 0;
        buffer_wr->wr_id = wr_id;
        buffer_wr->sg_list = buffer_sge;
        buffer_wr->num_sge = 1;
        buffer_wr->next = send_wr;
        buffer_wr->opcode = IBV_WR_RDMA_WRITE;
        buffer_wr->send_flags = IBV_SEND_INLINE;
        buffer_wr->wr.rdma.remote_addr = cas_buffer_remote_addr(requestor->server_cmd_msg_, entry_index);
        buffer_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[zqp->current_device];
        send_wr->send_flags = IBV_SEND_SIGNALED;

        ibv_qp *qp = requestor->qp_;
        int result;
        if (result = ibv_post_send(qp, buffer_wr, &bad_send_wr))
        {
            zqp->wr_entry_[entry_index].finished = 1;
            std::cerr << "Error, ibv_post_send failed:" << result << std::endl;
            return -1;
        }
        wr_ids->push_back(wr_id);

        delete entry;
        delete buffer_sge;
        delete buffer_wr;
        send_wr->next = NULL;
        return entry_index;
    }

    int zQP_CAS_step2_async(zQP *zqp, uint64_t new_val, void *remote_addr, uint32_t rkey, uint32_t time_stamp, uint64_t offset)
    {
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];

        zAtomic_entry *entry = new zAtomic_entry();
        entry->time_stamp = time_stamp;
        entry->qp_id = zqp->qp_id_;
        entry->offset = offset;

        struct ibv_sge *sge = new ibv_sge();
        sge->addr = (uint64_t)requestor->cmd_resp_;
        sge->length = sizeof(uint64_t);
        sge->lkey = requestor->resp_mr_->lkey;

        struct ibv_send_wr *send_wr = new ibv_send_wr();
        struct ibv_send_wr *bad_send_wr;
        send_wr->wr_id = 0;
        send_wr->sg_list = sge;
        send_wr->num_sge = 1;
        send_wr->next = NULL;
        send_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        send_wr->send_flags = 0;
        send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
        send_wr->wr.atomic.rkey = path_rkey(zqp, rkey);
        send_wr->wr.atomic.compare_add = *(uint64_t *)entry;
        send_wr->wr.atomic.swap = new_val;
        // printf("CAS: %lu --> %lu\n", *(uint64_t*)entry, new_val);

        struct ibv_sge *buffer_sge = new ibv_sge();
        struct ibv_send_wr *buffer_wr = new ibv_send_wr();
        zAtomic_buffer *buffer = new zAtomic_buffer();
        buffer->buffer = new_val;
        buffer->finished = 0;
        buffer->target_addr = (uint64_t)(remote_addr);
        buffer->time_stamp = time_stamp;
        uint64_t old_val = *((uint64_t *)(buffer) + 1);
        buffer->finished = 1;
        uint64_t check_val = *((uint64_t *)(buffer) + 1);
        // printf("old: %lu, check: %lu\n", old_val, check_val);

        buffer_sge->addr = (uint64_t)((uint64_t *)(requestor->cmd_resp_) + 1);
        buffer_sge->length = sizeof(uint64_t);
        buffer_sge->lkey = requestor->resp_mr_->lkey;
        buffer_wr->wr_id = 0;
        buffer_wr->sg_list = buffer_sge;
        buffer_wr->num_sge = 1;
        buffer_wr->next = NULL;
        buffer_wr->opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        buffer_wr->send_flags = 0;
        buffer_wr->wr.atomic.remote_addr = cas_buffer_remote_addr(requestor->server_cmd_msg_, entry->offset) + sizeof(uint64_t);
        buffer_wr->wr.atomic.rkey = cas_log_rkey(zqp, requestor);
        buffer_wr->wr.atomic.compare_add = old_val;
        buffer_wr->wr.atomic.swap = check_val;
        send_wr->next = buffer_wr;

        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            std::cerr << "Error, ibv_post_send failed" << std::endl;
            return -1;
        }

        return 0;
    }

    int zQP_send_wr(zQP *zqp, ibv_send_wr *send_wr)
    {
        struct ibv_send_wr *bad_send_wr;
        zQP_requestor *requestor = zqp->m_requestors[zqp->current_device];
        ibv_qp *qp_ = requestor->qp_;
        if (ibv_post_send(qp_, send_wr, &bad_send_wr))
        {
            perror("Error, ibv_post_send failed");
        }

        auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                // std::cerr << "Error, write timeout" << std::endl;
                break;
            }
            if (ibv_poll_cq(cq, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    std::cerr << "Error, write failed: " << wc.status << std::endl;
                    break;
                }
                int start_ = zqp->entry_start_;
                int end_ = zqp->entry_end_;
                if (start_ > end_)
                    end_ += WR_ENTRY_NUM;
                if (start_ != end_)
                {
                    for (int i = start_; i < end_; i++)
                    {
                        if (zqp->wr_entry_[i % WR_ENTRY_NUM].time_stamp == wc.wr_id)
                        {
                            delete (ibv_send_wr *)zqp->wr_entry_[zqp->entry_end_].wr_addr;
                            zqp->wr_entry_[zqp->entry_end_].wr_addr = 0;
                        }
                        if (zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr == 0 && i % WR_ENTRY_NUM == zqp->entry_start_)
                        {
                            zqp->entry_start_.fetch_add(1);
                        }
                    }
                }
                break;
            }
        }

        return 0;
    }

    void zQP_RPC_Alloc(zQP *qp, uint64_t *addr, uint32_t *rkey, size_t size)
    {
        zQP_requestor *requestor = qp->m_requestors[qp->current_device];
        memset(requestor->cmd_msg_, 0, sizeof(CmdMsgBlock));
        memset(requestor->cmd_resp_, 0, sizeof(CmdMsgRespBlock));
        requestor->cmd_resp_->notify = NOTIFY_IDLE;
        RegisterRequest *request = (RegisterRequest *)requestor->cmd_msg_;
        request->resp_addr = (uint64_t)requestor->cmd_resp_;
        request->resp_rkey = requestor->resp_mr_->rkey;
        request->id = requestor->conn_id_;
        request->type = MSG_REGISTER;
        request->size = size;
        requestor->cmd_msg_->notify = NOTIFY_WORK;

        // printf("cmd_resp: %lx, rkey: %u\n", requestor->cmd_resp_, requestor->resp_mr_->rkey);
        // printf("cmd_resp: %lx, rkey: %u\n", request->resp_addr, request->resp_rkey);

        /* send a request to sever */
        qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
#ifdef POLLTHREAD
        zQP_write(qp, (void *)requestor->cmd_msg_, requestor->msg_mr_->lkey, sizeof(CmdMsgBlock), (void *)requestor->server_cmd_msg_, requestor->server_cmd_rkey_[qp->current_device], qp->time_stamp, false);
#else
        z_simple_write(qp, (void *)requestor->cmd_msg_, requestor->msg_mr_->lkey, sizeof(CmdMsgBlock), (void *)requestor->server_cmd_msg_, requestor->server_cmd_rkey_[qp->current_device]);
#endif
        /* wait for response */
        auto start = TIME_NOW;
        while (requestor->cmd_resp_->notify == NOTIFY_IDLE)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US * 1000)
            {
                printf("wait for request completion timeout\n");
                return;
            }
        }
        RegisterResponse *resp_msg = (RegisterResponse *)requestor->cmd_resp_;
        if (resp_msg->status != RES_OK)
        {
            printf("register remote memory fail\n");
            return;
        }
        *addr = resp_msg->addr;
        for (int i = 0; i < MAX_NIC_NUM; i++)
        {
            if (qp->m_rkey_table->find(resp_msg->rkey[0]) == qp->m_rkey_table->end())
                (*qp->m_rkey_table)[resp_msg->rkey[0]] = std::vector<uint32_t>();
            qp->m_rkey_table->at(resp_msg->rkey[0]).push_back(resp_msg->rkey[i]);
        }
        *rkey = resp_msg->rkey[0];
        if (qp->m_fast_rkey && MAX_NIC_NUM > 1)
        {
            *(qp->m_fast_rkey) = resp_msg->rkey[1];
        }
        return;
    }

    void zQP_RPC_GetAddr(zQP *qp, uint64_t *addr, uint32_t *rkey)
    {
        zQP_requestor *requestor = qp->m_requestors[qp->current_device];
        memset(requestor->cmd_msg_, 0, sizeof(CmdMsgBlock));
        memset(requestor->cmd_resp_, 0, sizeof(CmdMsgRespBlock));
        requestor->cmd_resp_->notify = NOTIFY_IDLE;
        RegisterRequest *request = (RegisterRequest *)requestor->cmd_msg_;
        request->resp_addr = (uint64_t)requestor->cmd_resp_;
        request->resp_rkey = requestor->resp_mr_->rkey;
        request->id = requestor->conn_id_;
        request->type = MSG_FETCH;
        request->size = 0;
        requestor->cmd_msg_->notify = NOTIFY_WORK;

        // printf("cmd_resp: %lx, rkey: %u\n", requestor->cmd_resp_, requestor->resp_mr_->rkey);
        // printf("cmd_resp: %lx, rkey: %u\n", request->resp_addr, request->resp_rkey);

        /* send a request to sever */
        qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
#ifdef POLLTHREAD
        zQP_write(qp, (void *)requestor->cmd_msg_, requestor->msg_mr_->lkey, sizeof(CmdMsgBlock), (void *)requestor->server_cmd_msg_, requestor->server_cmd_rkey_[qp->current_device], qp->time_stamp, false);
#else
        z_simple_write(qp, (void *)requestor->cmd_msg_, requestor->msg_mr_->lkey, sizeof(CmdMsgBlock), (void *)requestor->server_cmd_msg_, requestor->server_cmd_rkey_[qp->current_device]);
#endif
        /* wait for response */
        auto start = TIME_NOW;
        while (requestor->cmd_resp_->notify == NOTIFY_IDLE)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US * 1000)
            {
                printf("wait for request completion timeout\n");
                return;
            }
        }
        RegisterResponse *resp_msg = (RegisterResponse *)requestor->cmd_resp_;
        if (resp_msg->status != RES_OK)
        {
            printf("register remote memory fail\n");
            return;
        }
        *addr = resp_msg->addr;
        for (int i = 0; i < MAX_NIC_NUM; i++)
        {
            if (qp->m_rkey_table->find(resp_msg->rkey[0]) == qp->m_rkey_table->end())
                (*qp->m_rkey_table)[resp_msg->rkey[0]] = std::vector<uint32_t>();
            qp->m_rkey_table->at(resp_msg->rkey[0]).push_back(resp_msg->rkey[i]);
        }
        *rkey = resp_msg->rkey[0];
        if (qp->m_fast_rkey && MAX_NIC_NUM > 1)
        {
            *(qp->m_fast_rkey) = resp_msg->rkey[1];
        }
        return;
    }

    int z_post_send_async(zQP *qp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr, bool non_idempotent,
                          uint32_t time_stamp, vector<uint64_t> *wr_ids, bool unique_cas, int max_depth, int wr_id)
    {
        if (prepare_path(qp, true) != 0)
        {
            return -1;
        }

        if (qp->m_requestors.size() > qp->current_device && qp->m_requestors[qp->current_device] != NULL && qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED)
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
            int result = zQP_post_send_async(qp, send_wr, bad_wr, non_idempotent, qp->time_stamp, wr_ids, unique_cas, max_depth, wr_id);
            return result;
        }
        else
        {
            int dcqp_idx = 0;
            if (qp->current_device >= 0 &&
                qp->current_device < (int)qp->m_pd->m_requestors.size() &&
                qp->bound_dcqps_ >= 0 &&
                qp->bound_dcqps_ < (int)qp->m_pd->m_requestors[qp->current_device].size())
            {
                dcqp_idx = qp->bound_dcqps_;
            }
            int result = zDCQP_send(qp, qp->m_pd->m_requestors[qp->current_device][dcqp_idx], qp->m_targets[qp->current_device]->ah, send_wr, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_, max_depth);
            return result;
        }
    }

    int z_read_async(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids)
    {
        if (prepare_path(qp, true) != 0)
        {
            return -1;
        }
#ifdef DCQP_FAILOVER
        if (should_use_dcqp(qp))
        {
            int result = dcqp_read_retry(qp, local_addr, lkey, length, remote_addr, rkey);
            if (result == 0)
            {
                qp->size_counter_.fetch_add(length);
            }
            return result;
        }
#endif
        uint32_t new_lkey = lkey;
        uint32_t new_rkey = rkey;
        if (qp->m_requestors.size() > qp->current_device && qp->m_requestors[qp->current_device] != NULL && qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED)
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
            int result = zQP_read_async(qp, local_addr, new_lkey, length, remote_addr, new_rkey, qp->time_stamp, wr_ids);
            if (result == -1)
            {
                z_switch(qp);
                return z_read_async(qp, local_addr, lkey, length, remote_addr, rkey, wr_ids);
            }
            return result;
        }
        else
        {
            if (qp->current_device != 0)
            {
                tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                qp->m_pd->m_lkey_table.find(a, lkey);
                new_lkey = a->second[qp->current_device];
                new_rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
            }
            int result = zDCQP_read(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, new_lkey, length, remote_addr, new_rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
            qp->size_counter_.fetch_add(length);
            return result;
        }
    }

    int z_write_async(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids)
    {
        for (int attempt = 0; attempt < 1024; attempt++)
        {
            if (prepare_path(qp, true) != 0)
            {
                return -1;
            }
            uint32_t new_lkey = lkey;
            uint32_t new_rkey = rkey;
#ifdef DCQP_FAILOVER
            if (should_use_dcqp(qp))
            {
                int result = dcqp_write_retry(qp, local_addr, lkey, length, remote_addr, rkey);
                if (result == 0)
                {
                    qp->size_counter_.fetch_add(length);
                    return result;
                }
                continue;
            }
#endif
            if (qp->m_requestors.size() > qp->current_device && qp->m_requestors[qp->current_device] != NULL && qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED)
            {
                qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
#ifdef LOG
                int result = zQP_write_async(qp, local_addr, new_lkey, length, remote_addr, new_rkey, qp->time_stamp, true, wr_ids);
#else
                int result = zQP_write_async(qp, local_addr, new_lkey, length, remote_addr, new_rkey, qp->time_stamp, false, wr_ids);
#endif
                if (result == -2)
                {
                    wr_log_advance(qp);
                    drain_endpoint_cqs(qp->m_ep);
                    if (wr_ids != NULL && !wr_ids->empty())
                    {
                        z_poll_completion(qp, wr_ids);
                    }
                    continue;
                }
                if (result == -1)
                {
                    z_switch(qp);
                    continue;
                }
                return result;
            }
            if (qp->current_device != 0)
            {
                tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                qp->m_pd->m_lkey_table.find(a, lkey);
                new_lkey = a->second[qp->current_device];
                new_rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
            }
            int result = zDCQP_write(qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, local_addr, new_lkey, length, remote_addr, new_rkey, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_);
            qp->size_counter_.fetch_add(length);
            return result;
        }
        return -1;
    }

    int z_CAS_async(zQP *qp, void *local_addr, uint32_t lkey, uint64_t new_val, void *remote_addr,
                    uint32_t rkey, vector<uint64_t> *wr_ids)
    {
        if (prepare_path(qp, true) != 0)
        {
            return -1;
        }
        if (requestor_ready(qp, qp->current_device))
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
            int result = zQP_CAS_async(qp, local_addr, lkey, new_val, remote_addr, rkey,
                                       qp->time_stamp, wr_ids);
            if (result == -1)
            {
                z_switch(qp);
                return z_CAS_async(qp, local_addr, lkey, new_val, remote_addr, rkey, wr_ids);
            }
            return 0;
        }
        qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
        int result = zQP_CAS(qp, local_addr, lkey, new_val, remote_addr, rkey, qp->time_stamp);
        if (result == 0)
        {
            qp->size_counter_.fetch_add(sizeof(uint64_t));
        }
        return result;
    }

    int z_write(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey)
    {
        for (int attempt = 0; attempt < 4; attempt++)
        {
            if (prepare_path(qp, true) != 0)
            {
                return -1;
            }
            int result;
#ifdef DCQP_FAILOVER
            if (should_use_dcqp(qp))
            {
                result = dcqp_write_retry(qp, local_addr, lkey, length, remote_addr, rkey);
                if (result == 0)
                {
                    qp->size_counter_.fetch_add(length);
                }
            }
            else
#endif
            if (requestor_ready(qp, qp->current_device))
            {
                qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
#ifdef LOG
                result = zQP_write(qp, local_addr, lkey, length, remote_addr, rkey, qp->time_stamp, true);
#else
                result = zQP_write(qp, local_addr, lkey, length, remote_addr, rkey, qp->time_stamp, false);
#endif
            }
            else
            {
                result = zDCQP_write(qp->m_pd->m_requestors[qp->current_device][0],
                                     qp->m_targets[qp->current_device]->ah, local_addr,
                                     path_lkey(qp, lkey), length, remote_addr, path_rkey(qp, rkey),
                                     qp->m_targets[qp->current_device]->lid_,
                                     qp->m_targets[qp->current_device]->dct_num_);
                if (result == 0)
                {
                    qp->size_counter_.fetch_add(length);
                }
            }
            if (result == 0)
            {
                return 0;
            }
        }
        return -1;
    }

    int z_read(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey)
    {
        for (int attempt = 0; attempt < 4; attempt++)
        {
            if (prepare_path(qp, true) != 0)
            {
                return -1;
            }
            int result;
#ifdef DCQP_FAILOVER
            if (should_use_dcqp(qp))
            {
                result = dcqp_read_retry(qp, local_addr, lkey, length, remote_addr, rkey);
                if (result == 0)
                {
                    qp->size_counter_.fetch_add(length);
                }
            }
            else
#endif
            if (requestor_ready(qp, qp->current_device))
            {
                qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
                result = zQP_read(qp, local_addr, lkey, length, remote_addr, rkey, qp->time_stamp);
            }
            else
            {
                result = zDCQP_read(qp->m_pd->m_requestors[qp->current_device][0],
                                    qp->m_targets[qp->current_device]->ah, local_addr,
                                    path_lkey(qp, lkey), length, remote_addr, path_rkey(qp, rkey),
                                    qp->m_targets[qp->current_device]->lid_,
                                    qp->m_targets[qp->current_device]->dct_num_);
                if (result == 0)
                {
                    qp->size_counter_.fetch_add(length);
                }
            }
            if (result == 0)
            {
                return 0;
            }
        }
        return -1;
    }

    int z_CAS(zQP *qp, void *local_addr, uint32_t lkey, uint64_t new_val, void *remote_addr, uint32_t rkey)
    {
        for (int attempt = 0; attempt < 4; attempt++)
        {
            if (prepare_path(qp, true) != 0)
            {
                return -1;
            }
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
            int result = zQP_CAS(qp, local_addr, lkey, new_val, remote_addr, rkey, qp->time_stamp);
            if (result == 0)
            {
                return 0;
            }
        }
        return -1;
    }

    int z_post_send(zQP *qp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr, bool non_idempotent,
                    uint32_t time_stamp, bool unique_cas, int max_depth)
    {
        if (prepare_path(qp, true) != 0)
        {
            return -1;
        }

        if (qp->m_requestors.size() > qp->current_device && qp->m_requestors[qp->current_device] != NULL && qp->m_requestors[qp->current_device]->status_ == ZSTATUS_CONNECTED)
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
            int result = zQP_post_send(qp, send_wr, bad_wr, non_idempotent, qp->time_stamp, unique_cas, max_depth);
            return result;
        }
        else
        {
            int result = zDCQP_send(qp, qp->m_pd->m_requestors[qp->current_device][0], qp->m_targets[qp->current_device]->ah, send_wr, qp->m_targets[qp->current_device]->lid_, qp->m_targets[qp->current_device]->dct_num_, max_depth);
            return result;
        }
    }

    int z_poll_completion(zQP *qp, uint64_t wr_id)
    {
        int return_val = 0;
        int error_device = -1;
        if (wr_id == 0)
        {
            return 0;
        }
        auto start = TIME_NOW;
        tbb::concurrent_hash_map<uint64_t, cq_info>::const_accessor a;
        while (qp->m_ep->completed_table.find(a, wr_id) == false)
        {
            drain_endpoint_cqs(qp->m_ep);
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                // std::cerr << "Error, poll completion timeout" << std::endl;
                z_switch(qp);
                z_recovery(qp);
                return -1;
            }
        }
        cq_info status_info = a->second;
        if (status_info.status != IBV_WC_SUCCESS)
        {
            // std::cerr << "Error, operation failed: " << status_info.status << " on device " << status_info.device_id << std::endl;
            error_device = status_info.device_id;
            return_val = -1;
            qp->m_ep->completed_table.erase(a);
        }
        else
        {
            zWR_header *header = (zWR_header *)&wr_id;
            ibv_send_wr *completed_wr = (ibv_send_wr *)(uintptr_t)header->wr_addr;
            if (completed_wr != NULL && completed_wr->sg_list != NULL)
            {
                qp->size_counter_.fetch_add(completed_wr->sg_list->length);
            }
            wr_log_complete(qp, header->time_stamp, header->wr_addr);
            qp->m_ep->completed_table.erase(a);
        }
        if (return_val < 0)
        {
            if (error_device != qp->current_device)
            {
                z_recovery(qp);
            }
            else
            {
                z_switch(qp);
                z_recovery(qp);
            }
        }
        return return_val;
    }

    int z_poll_completion(zQP *qp, vector<uint64_t> *wr_ids)
    {
        int return_val = 0;
        int error_device = -1;
        auto start = TIME_NOW;
        while (wr_ids->size() > 0)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                z_switch(qp);
#ifdef RECOVERY
                z_recovery(qp);
#endif
                return -1;
            }
            drain_endpoint_cqs(qp->m_ep);
            for (auto id : *wr_ids)
            {
                if (id == 0)
                {
                    wr_ids->erase(std::remove(wr_ids->begin(), wr_ids->end(), id), wr_ids->end());
                    break;
                }
                tbb::concurrent_hash_map<uint64_t, cq_info>::const_accessor a;
                // printf("Polling size: %lu, table size: %lu\n", wr_ids->size(), qp->completed_table.size());
                if (qp->m_ep->completed_table.find(a, id))
                {
                    cq_info status_info = a->second;
                    if (status_info.status != IBV_WC_SUCCESS)
                    {
                        // std::cerr << "Error, operation failed: " << status_info.status << " on device " << status_info.device_id << std::endl;
                        error_device = status_info.device_id;
                        return_val = -1;
                        wr_ids->erase(std::remove(wr_ids->begin(), wr_ids->end(), id), wr_ids->end());
                        qp->m_ep->completed_table.erase(a);
                        break;
                    }
                    zWR_header *header = (zWR_header *)&id;
                    ibv_send_wr *completed_wr = (ibv_send_wr *)(uintptr_t)header->wr_addr;
                    if (completed_wr != NULL && completed_wr->sg_list != NULL)
                    {
                        qp->size_counter_.fetch_add(completed_wr->sg_list->length);
                    }
                    wr_log_complete(qp, header->time_stamp, header->wr_addr);
                    wr_ids->erase(std::remove(wr_ids->begin(), wr_ids->end(), id), wr_ids->end());
                    qp->m_ep->completed_table.erase(a);
                    break;
                }
            }
        }
        if (return_val < 0)
        {
            if (error_device != qp->current_device)
            {
#ifdef RECOVERY
                z_recovery(qp);
#endif
            }
            else
            {
                z_switch(qp);
#ifdef RECOVERY
                z_recovery(qp);
#endif
            }
        }
        return return_val;
    }

    int z_switch(zQP *qp)
    {
        printf("Start switch on device %d at time %lu\n", qp->current_device, TIME_NOW.time_since_epoch().count());
#ifndef NO_ERROR_HANDLE
        int n = (int)qp->m_ep->m_devices.size();
        if (n <= 0)
        {
            return -1;
        }
        int old = qp->current_device;
        int next = -1;
        for (int i = 1; i < n; i++)
        {
            int cand = (old + i) % n;
            if (qp->m_ep->m_devices[cand]->status.load() != ZSTATUS_ERROR)
            {
                next = cand;
                break;
            }
        }
        if (next < 0)
        {
            printf("No healthy backup NIC from device %d, stay on current path\n", old);
            return -1;
        }
        qp->m_ep->m_devices[old]->status.store(ZSTATUS_ERROR);
        if (old < (int)qp->m_requestors.size() && qp->m_requestors[old] != NULL)
        {
            qp->m_requestors[old]->status_ = ZSTATUS_ERROR;
        }
        if (qp->qp_type == ZQP_RPC)
        {
            return 0;
        }
        qp->current_device = next;
        log_path_event("switch_start", old);
        if (dcqp_failover_enabled())
        {
            printf("Failover using DCQP on nic=%d (RC rebuild in background)\n", next);
            log_path_event("dcqp_active", next);
            if (!requestor_ready(qp, next) && qp->m_targets.find(next) != qp->m_targets.end())
            {
                if (g_backup_connect_started.exchange(1) == 0)
                {
                    string ip = qp->m_targets[next]->ip;
                    string port = qp->m_targets[next]->port;
                    log_path_event("rc_connect_begin", next);
                    std::thread([qp, next, ip, port]() {
                        int rc = zQP_connect(qp, next, ip, port);
                        if (rc == 0)
                        {
                            log_path_event("rc_ready", next);
                            printf("Switched back to RCQP on nic=%d at time %lu\n",
                                   next, (unsigned long)TIME_NOW.time_since_epoch().count());
                        }
                        else
                        {
                            log_path_event("rc_connect_fail", next);
                            printf("Backup RC connect failed on nic %d, stay on DCQP\n", next);
                        }
                    }).detach();
                }
            }
        }
        else if (!requestor_ready(qp, next) && qp->m_targets.find(next) != qp->m_targets.end())
        {
            printf("Connecting backup path nic=%d dst=%s:%s\n",
                   next, qp->m_targets[next]->ip.c_str(), qp->m_targets[next]->port.c_str());
            log_path_event("rc_connect_begin", next);
            if (zQP_connect(qp, next, qp->m_targets[next]->ip, qp->m_targets[next]->port) != 0)
            {
                log_path_event("rc_connect_fail", next);
                printf("Backup RC connect failed on nic %d, will use DCQP\n", next);
            }
            else
            {
                log_path_event("rc_ready", next);
            }
        }
        printf("Switched to device %d requestor_ready=%d dcqp=%d\n",
               qp->current_device, requestor_ready(qp, qp->current_device) ? 1 : 0,
               should_use_dcqp(qp) ? 1 : 0);
#endif
        return 0;
    }

    static bool is_app_coro_wr_id(uint64_t wr_id)
    {
        return wr_id != 0 && wr_id != magic_number && wr_id <= 64;
    }

    static bool note_recovered_wr_id(zQP *qp, uint64_t wr_id)
    {
        if (!is_app_coro_wr_id(wr_id))
        {
            return false;
        }
        std::lock_guard<std::mutex> g(qp->recovered_mu_);
        qp->recovered_wr_ids_.push_back(wr_id);
        return true;
    }

    static bool wr_owned_by_other_slot(zQP *qp, int start, int end, int self_slot, ibv_send_wr *wr)
    {
        if (wr == NULL)
        {
            return false;
        }
        uint64_t addr = (uint64_t)wr;
        for (int i = start; i < end; i++)
        {
            int slot = wr_slot(i);
            if (slot == self_slot)
            {
                continue;
            }
            if (qp->wr_entry_[slot].wr_addr == addr)
            {
                return true;
            }
        }
        return false;
    }

    // Motor counts one pending ACK per doorbell / post_send, not per log slot
    // and not per unique wr_id. Inject one fake CQE for each recovered post
    // that would have produced a signaled completion.
    static int queue_slot_completions(zQP *qp, ibv_send_wr *send_wr, int start, int end, int self_slot)
    {
        int n = 0;
        for (ibv_send_wr *p = send_wr; p != NULL; p = p->next)
        {
            if (p != send_wr && wr_owned_by_other_slot(qp, start, end, self_slot, p))
            {
                break;
            }
            if ((p->send_flags & IBV_SEND_SIGNALED) && note_recovered_wr_id(qp, p->wr_id))
            {
                n++;
            }
        }
        if (n == 0 && note_recovered_wr_id(qp, send_wr->wr_id))
        {
            n++;
        }
        return n;
    }

    static int pop_recovered_wc(zQP *qp, ibv_wc &wc)
    {
        std::lock_guard<std::mutex> g(qp->recovered_mu_);
        if (qp->recovered_wr_ids_.empty())
        {
            return 0;
        }
        memset(&wc, 0, sizeof(wc));
        wc.status = IBV_WC_SUCCESS;
        wc.wr_id = qp->recovered_wr_ids_.front();
        qp->recovered_wr_ids_.erase(qp->recovered_wr_ids_.begin());
        wc.opcode = IBV_WC_RDMA_WRITE;
        return 1;
    }

    static int recover_native_cas(zQP *qp, ibv_send_wr *send_wr, bool full)
    {
        uint64_t expected = send_wr->wr.atomic.compare_add;
        uint64_t swap = send_wr->wr.atomic.swap;
        void *local = (void *)(uintptr_t)send_wr->sg_list->addr;
        uint32_t lkey = send_wr->sg_list->lkey;
        void *target = (void *)send_wr->wr.atomic.remote_addr;
        uint32_t rkey = send_wr->wr.atomic.rkey;
        if (full)
        {
            *(uint64_t *)local = expected;
            int rc = path_cas(qp, local, lkey, swap, target, rkey);
            printf("FULL retransmit CAS cmp=%lu swap=%lu result=%lu rc=%d\n",
                   (unsigned long)expected, (unsigned long)swap,
                   (unsigned long)*(uint64_t *)local, rc);
            return rc;
        }
        uint64_t observed = 0;
        void *observe_buf = qp->cmd_resp_;
        uint32_t observe_lkey = qp->resp_mr_[qp->current_device]
                                    ? qp->resp_mr_[qp->current_device]->lkey
                                    : qp->resp_mr_[0]->lkey;
        if (path_read(qp, observe_buf, observe_lkey, sizeof(uint64_t), target, rkey) != 0)
        {
            return -1;
        }
        memcpy(&observed, observe_buf, sizeof(uint64_t));
        if (observed == swap)
        {
            *(uint64_t *)local = expected;
            return 0;
        }
        if (observed == expected)
        {
            *(uint64_t *)local = expected;
            return path_cas(qp, local, lkey, swap, target, rkey);
        }
        *(uint64_t *)local = observed;
        return 0;
    }

    static int recover_cas(zQP *qp, int slot, ibv_send_wr *send_wr, zAtomic_buffer *remote_buf)
    {
        uint64_t expected = send_wr->wr.atomic.compare_add;
        uint64_t new_val = qp->wr_entry_[slot].reserved;
        zAtomic_entry entry{};
        entry.time_stamp = qp->wr_entry_[slot].time_stamp;
        entry.qp_id = qp->qp_id_;
        entry.offset = slot;
        uint64_t uid = *(uint64_t *)&entry;
        void *target = (void *)send_wr->wr.atomic.remote_addr;
        uint32_t rkey = send_wr->wr.atomic.rkey;
        uint32_t lkey = send_wr->sg_list->lkey;
        void *local = (void *)(uintptr_t)send_wr->sg_list->addr;
        uint64_t observed = 0;
        void *observe_buf = qp->cmd_resp_;
        uint32_t observe_lkey = qp->resp_mr_[qp->current_device]
                                    ? qp->resp_mr_[qp->current_device]->lkey
                                    : qp->resp_mr_[0]->lkey;
        if (path_read(qp, observe_buf, observe_lkey, sizeof(uint64_t), target, rkey) != 0)
        {
            return -1;
        }
        memcpy(&observed, observe_buf, sizeof(uint64_t));
        bool buffer_written = (remote_buf != NULL &&
                               remote_buf->time_stamp == qp->wr_entry_[slot].time_stamp);

        if (observed == uid)
        {
            if (complete_cas_step2(qp, uid, new_val, target, rkey, lkey, local,
                                   qp->wr_entry_[slot].time_stamp, slot) != 0)
            {
                return -1;
            }
            *(uint64_t *)local = expected;
            return 0;
        }
        if (observed == new_val || (buffer_written && remote_buf->finished == 1))
        {
            *(uint64_t *)local = expected;
            return 0;
        }
        if (observed == expected)
        {
            if (!buffer_written)
            {
                if (write_cas_buffer(qp, slot, new_val, target, qp->wr_entry_[slot].time_stamp) != 0)
                {
                    return -1;
                }
            }
            *(uint64_t *)local = expected;
            if (path_cas(qp, local, lkey, uid, target, rkey) != 0)
            {
                return -1;
            }
            if (*(uint64_t *)local == expected)
            {
                if (complete_cas_step2(qp, uid, new_val, target, rkey, lkey, local,
                                       qp->wr_entry_[slot].time_stamp, slot) != 0)
                {
                    return -1;
                }
                *(uint64_t *)local = expected;
            }
            return 0;
        }
        *(uint64_t *)local = observed;
        return 0;
    }

    int z_recovery(zQP *qp)
    {
        if (qp->in_recovery.exchange(true))
        {
            return 0;
        }
        std::lock_guard<std::mutex> lock(qp->recovery_mutex);
        int n = (int)qp->m_ep->m_devices.size();
        int failed_device = prev_device_index(qp->current_device, n);
        zQP_requestor *log_src = NULL;
        if (failed_device >= 0 && failed_device < (int)qp->m_requestors.size())
        {
            log_src = qp->m_requestors[failed_device];
        }
        if (log_src == NULL)
        {
            log_src = cas_log_owner(qp);
        }
        if (log_src == NULL)
        {
            qp->in_recovery.store(false);
            return -1;
        }
        int start = qp->entry_start_.load();
        int end = qp->entry_end_.load();
        if (start >= end)
        {
            qp->in_recovery.store(false);
            return 0;
        }
        memset(qp->cmd_resp_, 0, sizeof(CmdMsgRespBlock));
        uint32_t log_lkey = qp->resp_mr_[qp->current_device]
                                ? qp->resp_mr_[qp->current_device]->lkey
                                : qp->resp_mr_[0]->lkey;
        uint32_t log_rkey = log_src->server_cmd_rkey_[qp->current_device];
        if (log_rkey == 0)
        {
            log_rkey = log_src->server_cmd_rkey_[0];
        }
        if (path_read(qp, (void *)qp->cmd_resp_, log_lkey, sizeof(CmdMsgRespBlock),
                      (void *)log_src->server_cmd_msg_,
                      log_rkey) != 0)
        {
            std::cerr << "Error, recovery log fetch failed" << std::endl;
            qp->in_recovery.store(false);
            return -1;
        }
        CmdMsgRespBlock log_copy;
        memcpy(&log_copy, qp->cmd_resp_, sizeof(log_copy));
        zWR_entry *entry = (zWR_entry *)&log_copy;
        int rc = 0;
        bool full = full_retransmit_enabled();
        int n_inflight = 0, n_rexmit = 0, n_skip = 0, n_cas = 0, n_wake = 0;
        for (int i = start; i < end; i++)
        {
            int slot = wr_slot(i);
            if (qp->wr_entry_[slot].wr_addr == 0 || qp->wr_entry_[slot].finished == 1)
            {
                continue;
            }
            struct ibv_send_wr *send_wr = (struct ibv_send_wr *)qp->wr_entry_[slot].wr_addr;
            if (send_wr == NULL)
            {
                continue;
            }
            n_inflight++;
            if (send_wr->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
            {
                n_cas++;
                zAtomic_buffer cas_buf{};
                uint32_t cas_lkey = qp->msg_mr_[qp->current_device]
                                        ? qp->msg_mr_[qp->current_device]->lkey
                                        : qp->msg_mr_[0]->lkey;
                uint64_t cas_remote = cas_buffer_remote_addr(log_src->server_cmd_msg_, slot);
                bool buffer_ok = path_read(qp, (void *)qp->cmd_msg_, cas_lkey, sizeof(zAtomic_buffer),
                                           (void *)cas_remote, log_rkey) == 0;
                if (buffer_ok)
                {
                    memcpy(&cas_buf, qp->cmd_msg_, sizeof(cas_buf));
                }
                bool two_phase = buffer_ok &&
                                 (cas_buf.time_stamp == qp->wr_entry_[slot].time_stamp);
                int cas_rc = 0;
                if (two_phase && !full)
                {
                    cas_rc = recover_cas(qp, slot, send_wr, &cas_buf);
                    n_skip++;
                }
                else
                {
                    cas_rc = recover_native_cas(qp, send_wr, full);
                    n_rexmit++;
                }
                if (cas_rc != 0)
                {
                    rc = -1;
                }
            }
            else
            {
                int local_time = qp->wr_entry_[slot].time_stamp;
                int remote_time = entry[slot].time_stamp;
                // READ never posts IMM into the completion log. Matching
                // timestamps here are leftover from a prior WRITE in the
                // same circular slot, so a skip leaves the local buffer
                // unfilled. Always re-read; only WRITE uses the log.
                bool is_read = (send_wr->opcode == IBV_WR_RDMA_READ);
                bool need_rexmit = full || is_read || (local_time != remote_time);
                if (need_rexmit)
                {
                    n_rexmit++;
                    if (is_read)
                    {
                        if (path_read(qp, (void *)send_wr->sg_list->addr, send_wr->sg_list->lkey,
                                      send_wr->sg_list->length, (void *)send_wr->wr.rdma.remote_addr,
                                      send_wr->wr.rdma.rkey) != 0)
                        {
                            rc = -1;
                        }
                    }
                    else if (send_wr->opcode == IBV_WR_RDMA_WRITE ||
                             send_wr->opcode == IBV_WR_RDMA_WRITE_WITH_IMM)
                    {
                        if (path_write(qp, (void *)send_wr->sg_list->addr, send_wr->sg_list->lkey,
                                       send_wr->sg_list->length, (void *)send_wr->wr.rdma.remote_addr,
                                       send_wr->wr.rdma.rkey) != 0)
                        {
                            rc = -1;
                        }
                    }
                    else
                    {
                        ibv_send_wr *bad_wr = NULL;
                        if (zDCQP_send(qp, qp->m_pd->m_requestors[qp->current_device][0],
                                       qp->m_targets[qp->current_device]->ah, send_wr,
                                       qp->m_targets[qp->current_device]->lid_,
                                       qp->m_targets[qp->current_device]->dct_num_, 1) != 0)
                        {
                            (void)bad_wr;
                            rc = -1;
                        }
                    }
                    if (send_wr->sg_list != NULL)
                    {
                        qp->size_counter_.fetch_add(send_wr->sg_list->length);
                    }
                }
                else
                {
                    n_skip++;
                }
            }
        }
        // Inject CQEs only after every in-flight READ has been refilled.
        // Queuing them inside the loop lets the Motor poller resume a
        // coroutine while later slots are still empty.
        for (int i = start; i < end; i++)
        {
            int slot = wr_slot(i);
            if (qp->wr_entry_[slot].wr_addr == 0 || qp->wr_entry_[slot].finished == 1)
            {
                continue;
            }
            struct ibv_send_wr *send_wr = (struct ibv_send_wr *)qp->wr_entry_[slot].wr_addr;
            if (send_wr != NULL)
            {
                n_wake += queue_slot_completions(qp, send_wr, start, end, slot);
            }
            qp->wr_entry_[slot].wr_addr = 0;
            qp->wr_entry_[slot].finished = 1;
        }
        wr_log_advance(qp);
        qp->in_recovery.store(false);
        printf("Finish recovery device=%d mode=%s inflight=%d rexmit=%d skip=%d cas=%d wake=%d rc=%d at %lu\n",
               qp->current_device, full ? "FULL" : "SELECTIVE",
               n_inflight, n_rexmit, n_skip, n_cas, n_wake, rc,
               (unsigned long)TIME_NOW.time_since_epoch().count());
        return rc;
    }

    ibv_mr *mr_create(ibv_pd *pd, void *addr, size_t length)
    {
        ibv_mr *mr = ibv_reg_mr(pd, addr, length, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_MW_BIND);
        return mr;
    }


    ibv_mr *mr_create(zPD *pd, void* addr, size_t length)
    {
        ibv_mr *primary_mr = mr_create(pd->m_pds[0], (void *)addr, length);
        if (primary_mr == NULL)
        {
            munmap((void *)addr, length);
            return NULL;
        }
        tbb::concurrent_hash_map<ibv_mr *, tbb::concurrent_vector<ibv_mr *>>::accessor a;
        if (pd->m_mrs.find(a, primary_mr))
        {
            a->second.push_back(primary_mr);
        }
        else
        {
            tbb::concurrent_vector<ibv_mr *> mr_list;
            mr_list.push_back(primary_mr);
            pd->m_mrs.insert(tbb::concurrent_hash_map<ibv_mr *, tbb::concurrent_vector<ibv_mr *>>::value_type(primary_mr, mr_list));
            // pd->m_mrs.insert(std::make_pair(primary_mr, mr_list));
        }
        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor b;
        if (pd->m_lkey_table.find(b, primary_mr->lkey))
        {
            b->second.push_back(primary_mr->lkey);
        }
        else
        {
            tbb::concurrent_vector<uint32_t> lkey_list;
            lkey_list.push_back(primary_mr->lkey);
            pd->m_lkey_table.insert(tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::value_type(primary_mr->lkey, lkey_list));
            // pd->m_lkey_table.insert(std::make_pair(primary_mr->lkey, lkey_list));
        }
        // pd->m_mrs[primary_mr].push_back(primary_mr);
        // pd->m_lkey_table[primary_mr->lkey].push_back(primary_mr->lkey);
        for (int i = 1; i < pd->m_pds.size(); i++)
        {
            ibv_mr *mr = mr_create(pd->m_pds[i], (void *)addr, length);
            tbb::concurrent_hash_map<ibv_mr *, tbb::concurrent_vector<ibv_mr *>>::accessor c;
            if (pd->m_mrs.find(c, primary_mr))
            {
                c->second.push_back(mr);
            }
            else
            {
                tbb::concurrent_vector<ibv_mr *> mr_list;
                mr_list.push_back(mr);
                // pd->m_mrs.insert(std::make_pair(primary_mr, mr_list));
                pd->m_mrs.insert(tbb::concurrent_hash_map<ibv_mr *, tbb::concurrent_vector<ibv_mr *>>::value_type(mr, mr_list));
            }
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor d;
            if (pd->m_lkey_table.find(d, primary_mr->lkey))
            {
                d->second.push_back(mr->lkey);
            }
            else
            {
                tbb::concurrent_vector<uint32_t> lkey_list;
                lkey_list.push_back(mr->lkey);
                pd->m_lkey_table.insert(tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::value_type(primary_mr->lkey, lkey_list));
                // pd->m_lkey_table.insert(std::make_pair(primary_mr->lkey, lkey_list));
            }
            // pd->m_mrs[primary_mr].push_back(mr);
            // pd->m_lkey_table[primary_mr->lkey].push_back(mr->lkey);
        }
        return primary_mr;
    }


    ibv_mr *mr_malloc_create(zPD *pd, uint64_t &addr, size_t length)
    {
        addr = (uint64_t)mmap(NULL, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (addr == (uint64_t)MAP_FAILED)
        {
            addr = 0;
            return NULL;
        }
        return mr_create(pd, (void *)addr, length);
    }

    int zQP_reg_mem(zQP_listener *zqp, void *addr, size_t length)
    {
        ibv_mr *mr = mr_create(zqp->m_pd, addr, length);
        if (mr == NULL)
        {
            printf("Error, register memory failed\n");
            return -1;
        }
        zqp->mem_ = mr;
        return 0;
    }

    int zQP_listen(zQP_listener *zqp, int nic_index, string ip, string port)
    {
        if (zqp->listeners.find(nic_index) != zqp->listeners.end())
        {
            return -1;
        }
        zQP_responder *qp = new zQP_responder();
        zqp->listeners[nic_index] = qp;
        qp->mem_ = zqp->mem_;

        qp->worker_info_ = new WorkerInfo *[MAX_SERVER_WORKER * MAX_SERVER_CLIENT];
        qp->worker_threads_ = new std::thread *[MAX_SERVER_WORKER];
        qp->pd_ = zqp->m_pd->m_pds[nic_index];
        for (uint32_t i = 0; i < MAX_SERVER_WORKER; i++)
        {
            qp->worker_info_[i] = nullptr;
            qp->worker_threads_[i] = nullptr;
        }
        qp->channel_ = rdma_create_event_channel();
        int result = rdma_create_id(qp->channel_, &(qp->cm_id_), NULL, RDMA_PS_TCP);

        assert(result == 0);
        sockaddr_in sin;
        memset(&sin, 0, sizeof(sin));
        sin.sin_family = AF_INET;
        sin.sin_port = htons(atoi(port.c_str()));
        sin.sin_addr.s_addr = inet_addr(ip.c_str());
        result = rdma_bind_addr(qp->cm_id_, (sockaddr *)&sin);
        assert(result == 0);
        result = rdma_listen(qp->cm_id_, 1024);
        assert(result == 0);
        z_debug("device %s listen on %s:%s\n", zqp->m_ep->m_devices[nic_index]->mlx_name.c_str(), ip.c_str(), port.c_str());
        while (true)
        {
            rdma_cm_event *event;
            int result = rdma_get_cm_event(qp->channel_, &event);
            assert(result == 0);
            if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
            {
                rdma_cm_id *new_cm_id = event->id;
                CNodeInit msg = *(CNodeInit *)event->param.conn.private_data;
                rdma_ack_cm_event(event);
                zQP_accept(zqp, nic_index, new_cm_id, msg.qp_type, msg.node_id);
            }
            else
            {
                rdma_ack_cm_event(event);
            }
        }
        return 0;
    }

    void zQP_worker(zPD *pd, zQP_responder *qp_instance, WorkerInfo *work_info, uint32_t num)
    {
        ibv_mr *cache_mr = NULL;
        uint64_t cache_addr = 0;
        printf("start worker %d\n", num);
        CmdMsgBlock *cmd_msg = work_info->cmd_msg;
        CmdMsgRespBlock *cmd_resp = work_info->cmd_resp_msg;
        struct ibv_mr *resp_mr = work_info->resp_mr;
        cmd_resp->notify = NOTIFY_WORK;
        int active_id = -1;
        int record = num;
        while (true)
        {
            for (int i = record; i < qp_instance->worker_num_; i += MAX_SERVER_WORKER)
            {
                if (qp_instance->worker_info_[i]->cmd_msg->notify != NOTIFY_IDLE)
                {
                    active_id = i;
                    cmd_msg = qp_instance->worker_info_[i]->cmd_msg;
                    record = i + MAX_SERVER_WORKER;
                    break;
                }
            }
            if (active_id == -1)
            {
                record = num;
                continue;
            }
            cmd_msg->notify = NOTIFY_IDLE;
            RequestsMsg *request = (RequestsMsg *)cmd_msg;
            if (active_id != request->id)
            {
                printf("find %d, receive from id:%d\n", active_id, request->id);
            }
            assert(active_id == request->id);
            work_info = qp_instance->worker_info_[request->id];
            cmd_resp = work_info->cmd_resp_msg;
            memset(cmd_resp, 0, sizeof(CmdMsgRespBlock));
            resp_mr = work_info->resp_mr;
            cmd_resp->notify = NOTIFY_WORK;
            active_id = -1;
            if (request->type == MSG_REGISTER)
            {

                RegisterRequest *reg_req = (RegisterRequest *)request;
                RegisterResponse *resp_msg = (RegisterResponse *)cmd_resp;
                ibv_mr *mr;
                if ((mr = mr_malloc_create(pd, resp_msg->addr,
                                           reg_req->size)) == NULL)
                {
                    resp_msg->status = RES_FAIL;
                }
                else
                {
                    tbb::concurrent_hash_map<ibv_mr *, tbb::concurrent_vector<ibv_mr *>>::accessor a;
                    pd->m_mrs.find(a, mr);
                    for (int i = 0; i < a->second.size(); i++)
                    {
                        resp_msg->rkey[i] = a->second[i]->rkey;
                    }
                    resp_msg->status = RES_OK;
                }

                worker_write(work_info->cm_id->qp, work_info->cq, (uint64_t)cmd_resp, resp_mr->lkey,
                             sizeof(CmdMsgRespBlock), reg_req->resp_addr,
                             reg_req->resp_rkey);
            }
            else if (request->type == MSG_FETCH)
            {
                RegisterRequest *reg_req = (RegisterRequest *)request;
                RegisterResponse *resp_msg = (RegisterResponse *)cmd_resp;
                ibv_mr *fetch_mr = qp_instance->mem_;
                if (fetch_mr == NULL)
                {
                    if (cache_mr == NULL)
                    {
                        uint64_t cache_size = (size_t)1024 * 1024 * 1024;
                        cache_mr = mr_malloc_create(pd, cache_addr, cache_size);
                    }
                    fetch_mr = cache_mr;
                }
                if (fetch_mr == NULL)
                {
                    resp_msg->status = RES_FAIL;
                }
                else
                {
                    resp_msg->addr = (uint64_t)(fetch_mr->addr);
                    tbb::concurrent_hash_map<ibv_mr *, tbb::concurrent_vector<ibv_mr *>>::accessor a;
                    pd->m_mrs.find(a, fetch_mr);
                    for (int i = 0; i < a->second.size(); i++)
                    {
                        resp_msg->rkey[i] = a->second[i]->rkey;
                    }
                    resp_msg->status = RES_OK;
                }
                worker_write(work_info->cm_id->qp, work_info->cq, (uint64_t)cmd_resp, resp_mr->lkey,
                             sizeof(CmdMsgRespBlock), reg_req->resp_addr,
                             reg_req->resp_rkey);
            }
            else if (request->type == MSG_FREE_FAST)
            {
            }
            else if (request->type == MSG_MW_REBIND)
            {
            }
            else if (request->type == RPC_FUSEE_SUBTABLE)
            {
            }
            else if (request->type == MSG_UNREGISTER)
            {
            }
            else if (request->type == MSG_PRINT_INFO)
            {
            }
            else if (request->type == MSG_MW_BATCH)
            {
            }
            else
            {
                printf("wrong request type\n");
            }
        }
    }

    int zQP_accept(zQP_listener *zqp, int nic_index, rdma_cm_id *cm_id, zQPType qp_type, int node_id)
    {
        zQP_responder *qp_instance = zqp->listeners[nic_index];

        int id = node_id;
        if (id == 0)
        {
            id = zqp->m_ep->qp_num_.fetch_add(1);
        }

        zQP_responder_connection *conn = new zQP_responder_connection();
        conn->cm_id_ = cm_id;

        ibv_comp_channel *comp_channel = ibv_create_comp_channel(zqp->m_ep->m_devices[nic_index]->context);
        assert(comp_channel != NULL);
        int cq_size = (qp_type == ZQP_ONESIDED) ? IMM_CQ_SIZE : 1;
        int max_recv_wr = (qp_type == ZQP_ONESIDED) ? IMM_RECV_DEPTH : 1;
        ibv_cq *cq = ibv_create_cq(zqp->m_ep->m_devices[nic_index]->context, cq_size, NULL, comp_channel, 0);
        assert(cq != NULL);
        int result = ibv_req_notify_cq(cq, 0);
        assert(result == 0);
        ibv_qp_init_attr qp_init_attr;
        memset(&qp_init_attr, 0, sizeof(qp_init_attr));
        qp_init_attr.send_cq = cq;
        qp_init_attr.recv_cq = cq;
        qp_init_attr.qp_type = IBV_QPT_RC;
        qp_init_attr.cap.max_send_wr = 1;
        qp_init_attr.cap.max_recv_wr = max_recv_wr;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;
        qp_init_attr.cap.max_inline_data = 256;
        qp_init_attr.sq_sig_all = 0;
        result = rdma_create_qp(cm_id, zqp->m_pd->m_pds[nic_index], &qp_init_attr);
        assert(result == 0);
        if (qp_type == ZQP_ONESIDED)
        {
            result = zQP_post_imm_recvs(cm_id->qp, IMM_RECV_DEPTH);
            assert(result == 0);
        }

        struct PData rep_pdata;
        memset(&rep_pdata, 0, sizeof(rep_pdata));
        CmdMsgBlock *cmd_msg = nullptr;
        CmdMsgRespBlock *cmd_resp = nullptr;
        struct ibv_mr *msg_mr = nullptr;
        struct ibv_mr *resp_mr = nullptr;
        cmd_msg = new CmdMsgBlock();
        memset(cmd_msg, 0, sizeof(CmdMsgBlock));
        msg_mr = mr_create(zqp->m_pd->m_pds[nic_index], (void *)cmd_msg, sizeof(CmdMsgBlock));

        cmd_resp = new CmdMsgRespBlock();
        memset(cmd_resp, 0, sizeof(CmdMsgRespBlock));
        resp_mr = mr_create(zqp->m_pd->m_pds[nic_index], (void *)cmd_resp, sizeof(CmdMsgRespBlock));

        // if(zqp->qp_log_list_[node_id] == NULL){
        //     zqp->qp_log_[node_id] = new CmdMsgBlock();
        //     memset(zqp->qp_log_[node_id], 0, sizeof(CmdMsgBlock));
        //     zqp->qp_log_list_[node_id] = mr_create(zqp->m_pd->m_pds[nic_index], (void *)zqp->qp_log_[node_id], sizeof(CmdMsgBlock));
        // }
        rep_pdata.qp_id = id;

        if (zqp->qp_info[id].addr == 0)
        {
            ibv_mr *mr = mr_malloc_create(zqp->m_pd, zqp->qp_info[id].addr,
                                          write_log_region_bytes() + sizeof(zAtomic_buffer) * WR_ENTRY_NUM);
            tbb::concurrent_hash_map<ibv_mr *, tbb::concurrent_vector<ibv_mr *>>::accessor a;
            zqp->m_pd->m_mrs.find(a, mr);
            for (int i = 0; i < a->second.size(); i++)
            {
                zqp->qp_info[id].rkey[i] = a->second[i]->rkey;
            }
        }
        // rep_pdata.id = -1;

        rep_pdata.conn_id = -1;
        if (qp_type == ZQP_RPC)
        {
            int num = qp_instance->worker_num_;
            if (num < MAX_SERVER_WORKER)
            {
                qp_instance->worker_info_[num] = new WorkerInfo();
                qp_instance->worker_info_[num]->cmd_msg = cmd_msg;
                qp_instance->worker_info_[num]->cmd_resp_msg = cmd_resp;
                qp_instance->worker_info_[num]->msg_mr = msg_mr;
                qp_instance->worker_info_[num]->resp_mr = resp_mr;
                qp_instance->worker_info_[num]->cm_id = cm_id;
                qp_instance->worker_info_[num]->cq = cq;
                qp_instance->worker_threads_[num] =
                    new std::thread(&zQP_worker, zqp->m_pd, qp_instance, qp_instance->worker_info_[num], num);
            }
            else
            {
                qp_instance->worker_info_[num] = new WorkerInfo();
                qp_instance->worker_info_[num]->cmd_msg = cmd_msg;
                qp_instance->worker_info_[num]->cmd_resp_msg = cmd_resp;
                qp_instance->worker_info_[num]->msg_mr = msg_mr;
                qp_instance->worker_info_[num]->resp_mr = resp_mr;
                qp_instance->worker_info_[num]->cm_id = cm_id;
                qp_instance->worker_info_[num]->cq = cq;
            }
            rep_pdata.conn_id = num;
            qp_instance->worker_num_ += 1;
        }
        if (qp_type == ZQP_RPC)
        {
            rep_pdata.buf_addr = (uintptr_t)cmd_msg;
            for (int i = 0; i < MAX_NIC_NUM; i++)
            {
                rep_pdata.buf_rkey[i] = msg_mr->rkey;
            }
        }
        else
        {
            // rep_pdata.buf_addr = (uintptr_t)zqp->qp_log_[node_id];
            // rep_pdata.buf_rkey = zqp->qp_log_list_[node_id]->rkey;
            rep_pdata.buf_addr = zqp->qp_info[id].addr;
            for (int i = 0; i < MAX_NIC_NUM; i++)
            {
                rep_pdata.buf_rkey[i] = zqp->qp_info[id].rkey[i];
            }
            // rep_pdata.buf_rkey = zqp->qp_info[id].rkey[nic_index];
        }
        rep_pdata.size = sizeof(CmdMsgRespBlock);
        rep_pdata.nic_num_ = zqp->m_pd->m_responders.size();
        rep_pdata.qp_info_addr = (uint64_t)(zqp->qp_info);
        for (int i = 0; i < MAX_NIC_NUM; i++)
        {
            rep_pdata.qp_info_rkey[i] = zqp->qp_info_rkey[i];
        }
        rep_pdata.atomic_table_addr = (uint64_t)(zqp->qp_info[id].addr);
        for (int i = 0; i < MAX_NIC_NUM; i++)
        {
            rep_pdata.atomic_table_rkey[i] = zqp->qp_info[id].rkey[i];
        }
        for (int i = 0; i < rep_pdata.nic_num_; i++)
        {
            rep_pdata.gid1[i] = zqp->m_pd->m_responders[i][0]->gid1;
            rep_pdata.gid2[i] = zqp->m_pd->m_responders[i][0]->gid2;
            // rep_pdata.interface[i] = zqp->m_pd->m_responders[i][0]->interface;
            // rep_pdata.subnet[i] = zqp->m_pd->m_responders[i][0]->subnet;
            rep_pdata.lid_[i] = zqp->m_pd->m_responders[i][0]->lid_;
            rep_pdata.dct_num_[i] = zqp->m_pd->m_responders[i][0]->dct_num_;
            strncpy(rep_pdata.ip[i], zqp->m_ep->m_devices[i]->eth_ip.c_str(), sizeof(rep_pdata.ip[i]) - 1);
            strncpy(rep_pdata.port[i], zqp->m_ep->m_devices[i]->port.c_str(), sizeof(rep_pdata.port[i]) - 1);
        }
        if (qp_type == ZQP_ONESIDED)
        {
            zImmRecvConn *imm = new zImmRecvConn();
            imm->qp = cm_id->qp;
            imm->cq = cq;
            imm->log = (zWR_entry *)(uintptr_t)zqp->qp_info[id].addr;
            std::lock_guard<std::mutex> lock(zqp->imm_mu_);
            zqp->imm_conns_.push_back(imm);
        }
        rdma_conn_param conn_param;
        memset(&conn_param, 0, sizeof(conn_param));
        conn_param.responder_resources = 16;
        conn_param.initiator_depth = 16;
        conn_param.private_data = &rep_pdata;
        conn_param.private_data_len = sizeof(rep_pdata);
        conn_param.retry_count = RETRY_TIMEOUT;
        conn_param.rnr_retry_count = RETRY_TIMEOUT;
        result = rdma_accept(cm_id, &conn_param);
        assert(result == 0);

        conn->cq_ = cq;
        conn->cm_id_ = cm_id;
        conn->status_ = zStatus::ZSTATUS_ACCEPTED;
        qp_instance->connections.push_back(conn);

        return 0;
    }

    int worker_write(ibv_qp *qp, ibv_cq *cq, uint64_t local_addr, uint32_t lkey, uint32_t length, uint64_t remote_addr, uint32_t rkey)
    {
        struct ibv_sge sge;
        sge.addr = (uintptr_t)local_addr;
        sge.length = length;
        sge.lkey = lkey;

        struct ibv_send_wr send_wr = {};
        struct ibv_send_wr *bad_send_wr;
        send_wr.wr_id = 0;
        send_wr.num_sge = 1;
        send_wr.next = NULL;
        send_wr.opcode = IBV_WR_RDMA_WRITE;
        send_wr.sg_list = &sge;
        send_wr.send_flags = IBV_SEND_SIGNALED;
        send_wr.wr.rdma.remote_addr = remote_addr;
        send_wr.wr.rdma.rkey = rkey;
        int error_code;
        if (error_code = ibv_post_send(qp, &send_wr, &bad_send_wr))
        {
            perror("ibv_post_send write fail");
            printf("error code %d\n", error_code);
            return -1;
        }

        auto start = TIME_NOW;
        struct ibv_wc wc;
        int ret = -1;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                perror("remote write timeout");
                return -1;
            }
            int rc = ibv_poll_cq(cq, 1, &wc);
            if (rc > 0)
            {
                if (IBV_WC_SUCCESS == wc.status)
                {
                    ret = 0;
                    break;
                }
                else if (IBV_WC_WR_FLUSH_ERR == wc.status)
                {
                    perror("cmd_send IBV_WC_WR_FLUSH_ERR");
                    break;
                }
                else if (IBV_WC_RNR_RETRY_EXC_ERR == wc.status)
                {
                    perror("cmd_send IBV_WC_RNR_RETRY_EXC_ERR");
                    break;
                }
                else
                {
                    perror("cmd_send ibv_poll_cq status error");
                    printf("%d\n", wc.status);
                    break;
                }
            }
            else if (0 == rc)
            {
                continue;
            }
            else
            {
                perror("ibv_poll_cq fail");
                break;
            }
        }
        return 0;
    }

}
