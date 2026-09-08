#pragma once

#include "zQP.h"
#include "rlib/logging.hpp"

namespace zrdma
{

enum ConnStatus {
  SUCC = 0,
  TIMEOUT = 1,
  WRONG_ARG = 2,
  ERR = 3,
  NOT_READY = 4,
  UNKNOWN = 5
};

constexpr struct timeval default_timeout = {0, 8000};
constexpr struct timeval no_timeout = {0, 0};  // it means forever

class RCQP{
    struct mr{
        uint64_t buf;
        uint32_t key;   
    };
public:
    RCQP(uint32_t *m_fast_rkey_, rkeyTable* rkey_table_, zTargetConfig& config, zEndpoint* ep_, zPD* pd_, string ip, string port, uint64_t local_addr, uint32_t lkey) {
        // m_fast_rkey_ = m_fast_rkey_;
        rpc_qp_ = zQP_create(pd_, ep_, rkey_table_, ZQP_RPC);
        rpc_qp_->m_fast_rkey = m_fast_rkey_;
        zQP_connect(rpc_qp_, 0, ip, port);
        qp_ = zQP_create(pd_, ep_, rkey_table_, ZQP_ONESIDED);
        qp_->m_fast_rkey = m_fast_rkey_;
        qp_->bound_dcqps_ = pd_->bound_dcqps_.fetch_add(1);
        zQP_connect(qp_, 0, ip, port);
        if (qp_->m_targets.find(1) != qp_->m_targets.end() && qp_->m_targets[1] != NULL)
        {
            zQP_connect(qp_, 1, qp_->m_targets[1]->ip, qp_->m_targets[1]->port);
        }
        zQP_RPC_GetAddr(rpc_qp_, &remote_mr_.buf, &remote_mr_.key);
        local_mr_.buf = local_addr;
        local_mr_.key = lkey;
        RDMA_LOG(INFO) << "RCQP created! local_addr: " << local_mr_.buf << ", remote_addr: " << remote_mr_.buf << ", remote_rkey: " << remote_mr_.key;
    }
    ~RCQP() = default;
    ConnStatus post_send(ibv_wr_opcode op, char* local_buf, uint32_t len, uint64_t off, int flags,
                       uint64_t wr_id = 0, uint32_t imm = 0) {
        ConnStatus ret = SUCC;
        struct ibv_send_wr* bad_sr;
            // setting the SGE
        struct ibv_sge sge{
        .addr = (uint64_t) local_buf,
        .length = len,
        .lkey = local_mr_.key
        };

        // setting sr, sr has to be initialized in this style
        struct ibv_send_wr sr;
        sr.wr_id = wr_id;
        sr.opcode = op;
        sr.num_sge = 1;
        sr.next = NULL;
        sr.sg_list = &sge;
        sr.send_flags = flags;
        sr.imm_data = imm;

        sr.wr.rdma.remote_addr = remote_mr_.buf + off;
        sr.wr.rdma.rkey = remote_mr_.key;
        // RDMA_LOG(INFO) << "post_send opcode: " << op << ", local_buf: " << (void*)sge.addr << ", len: " << sge.length 
        //               << ", remote_addr: " << sr.wr.rdma.remote_addr << ", rkey: " << sr.wr.rdma.rkey << ", wr_id: " << wr_id;
        z_post_send_async(qp_, &sr, &bad_sr, true, 0, nullptr, false, -1, wr_id);
        return SUCC;
    }

    ConnStatus post_batch(struct ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr, int num = 0) {
        int rc = z_post_send_async(qp_, send_sr, bad_sr_addr, true, 0, nullptr, false, -1);
        auto p = send_sr;
        int count = 0;
        // while(p != nullptr) {
        //     RDMA_LOG(INFO) << count << " post batch opcode: " << p->opcode << ", local_buf: " << (void*)p->sg_list->addr 
        //                   << ", len: " << p->sg_list->length << ", remote_addr: " << p->wr.rdma.remote_addr 
        //                   << ", rkey: " << p->wr.rdma.rkey << ", wr_id: " << p->wr_id;
        //     p = p->next;
        //     count++;
        // }
        // RDMA_LOG(INFO) << "post batch rc: " << rc;
        return rc == 0 ? SUCC : ERR;
    }

    ConnStatus post_cas(char* local_buf, uint64_t off,
                      uint64_t compare, uint64_t swap, int flags, uint64_t wr_id = 0) {
        printf("CAS is not supported in RCQP!\n");
        return ERR;
    }

    ConnStatus poll_till_completion(ibv_wc& wc, struct timeval timeout = default_timeout) {
        int result = z_poll_till_completion(qp_);
        // RDMA_LOG(INFO) << "poll till completion result: " << result;
        return result >= 0 ? SUCC : ERR;
    }

    int poll_send_completion(ibv_wc& wc) {
        int result = z_poll_send_completion(qp_, wc);
        // if(result > 0)
            // RDMA_LOG(INFO) << "poll send completion result: " << result;
        return result;
    }

    mr local_mr_;
    mr remote_mr_;
private:
    zQP* rpc_qp_{nullptr};
    zQP* qp_{nullptr};
    // uint32_t *m_fast_rkey_;

};

}