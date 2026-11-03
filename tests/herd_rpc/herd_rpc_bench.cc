// HERD request-path measurements.
//
// Native posts one 64-byte RC WRITE. Varuna posts one WRITE_WITH_IMM; the
// immediate packs a 16-bit stamp and the slot, and the server stores that
// stamp in the completion log when the receive completion arrives. Blind
// recovery always posts the request again. Varuna recovery RDMA-reads the
// log and posts the request again only when the stamp is absent. Both the
// log read and a Varuna replay go through a DCI to the server DCT.
//
//   herd_rpc_bench server <bind_ip> <port> <ib_dev>
//   herd_rpc_bench perf <server_ip> <port> <ib_dev> <seconds>
//   herd_rpc_bench correct <server_ip> <port> <ib_dev> <trials>
//   herd_rpc_bench timeline <server_ip> <port> <ib_dev> <record_ms> <fail_ms>

#include <arpa/inet.h>
#include <fcntl.h>
#include <infiniband/mlx5dv.h>
#include <infiniband/verbs.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace {

constexpr uint8_t kMicaOpPut = 112;
constexpr uint8_t kHerdMicaOffset = 10;
constexpr uint8_t kHerdOpPut = kMicaOpPut + kHerdMicaOffset;
constexpr int kMicaMaxValue = 64 - (16 + 1 + 1);
constexpr int kGidIndex = 3;
constexpr int kIbPort = 1;
constexpr int kBurst = 16;
constexpr int kRecvDepth = 128;
constexpr uint64_t kDcKey = 114514;

struct MicaKey {
    uint64_t unused;
    uint32_t bkt;
    uint16_t server;
    uint16_t tag;
};

struct HerdRequest {
    MicaKey key;
    uint8_t opcode;
    uint8_t val_len;
    uint8_t value[kMicaMaxValue];
};
static_assert(sizeof(HerdRequest) == 64, "HERD request is one cache line");

struct Slot {
    HerdRequest req;
    uint64_t log;
    uint64_t exec;
    uint64_t seen;
};

struct Remote {
    Slot slots[kBurst];
};

uint64_t field_off(int slot, size_t field) {
    return static_cast<uint64_t>(slot) * sizeof(Slot) + field;
}

struct Exch {
    uint32_t qpn;
    uint32_t psn;
    uint32_t rkey;
    uint32_t dct;
    uint64_t addr;
    uint8_t gid[16];
};

struct Conn {
    ibv_context *ctx = nullptr;
    ibv_pd *pd = nullptr;
    ibv_cq *cq = nullptr;
    ibv_cq *recv_cq = nullptr;
    ibv_qp *qp = nullptr;
    ibv_mr *mr = nullptr;
    void *buf = nullptr;
    uint32_t rkey = 0;
    uint64_t remote_addr = 0;
    int recv_depth = 0;
    int fd = -1;
};

struct DcIni {
    ibv_context *ctx = nullptr;
    ibv_pd *pd = nullptr;
    ibv_cq *cq = nullptr;
    ibv_qp *qp = nullptr;
    ibv_qp_ex *qp_ex = nullptr;
    mlx5dv_qp_ex *mlx = nullptr;
    ibv_ah *ah = nullptr;
    uint32_t dct = 0;
    uint64_t wr_id = 1;
};

[[noreturn]] void die(const char *what) {
    std::fprintf(stderr, "%s: %s\n", what, std::strerror(errno));
    std::exit(1);
}

void io_full(int fd, void *buf, size_t len, bool write) {
    auto *p = static_cast<unsigned char *>(buf);
    size_t done = 0;
    while (done < len) {
        ssize_t n = write ? ::send(fd, p + done, len - done, MSG_NOSIGNAL)
                          : ::recv(fd, p + done, len - done, 0);
        if (n <= 0) {
            die(write ? "send" : "recv");
        }
        done += static_cast<size_t>(n);
    }
}

void send_line(int fd, const char *line) {
    io_full(fd, const_cast<char *>(line), std::strlen(line), true);
}

void recv_line(int fd, char *buf, size_t cap) {
    size_t n = 0;
    while (n + 1 < cap) {
        char c = 0;
        io_full(fd, &c, 1, false);
        if (c == '\n') {
            break;
        }
        buf[n++] = c;
    }
    buf[n] = 0;
}

uint64_t now_ns() {
    timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000000000ull + static_cast<uint64_t>(ts.tv_nsec);
}

void spin_ns(uint64_t ns) {
    if (ns == 0) {
        return;
    }
    uint64_t start = now_ns();
    while (now_ns() - start < ns) {
    }
}

void drain_async(ibv_context *ctx) {
    int flags = fcntl(ctx->async_fd, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(ctx->async_fd, F_SETFL, flags | O_NONBLOCK);
    }
    for (;;) {
        ibv_async_event ev{};
        if (ibv_get_async_event(ctx, &ev) != 0) {
            break;
        }
        ibv_ack_async_event(&ev);
    }
}

ibv_context *open_dev(const char *name) {
    int num = 0;
    ibv_device **list = ibv_get_device_list(&num);
    if (list == nullptr) {
        die("ibv_get_device_list");
    }
    for (int i = 0; i < num; i++) {
        if (std::strcmp(ibv_get_device_name(list[i]), name) == 0) {
            ibv_context *ctx = ibv_open_device(list[i]);
            ibv_free_device_list(list);
            if (ctx == nullptr) {
                die("ibv_open_device");
            }
            return ctx;
        }
    }
    std::fprintf(stderr, "device %s not found\n", name);
    std::exit(1);
}

void modify_init(ibv_qp *qp) {
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = kIbPort;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) != 0) {
        die("modify QP INIT");
    }
}

void modify_rtr(ibv_qp *qp, const Exch &peer) {
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = IBV_MTU_1024;
    attr.dest_qp_num = peer.qpn;
    attr.rq_psn = peer.psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = kIbPort;
    std::memcpy(attr.ah_attr.grh.dgid.raw, peer.gid, 16);
    attr.ah_attr.grh.sgid_index = kGidIndex;
    attr.ah_attr.grh.hop_limit = 64;
    int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    if (ibv_modify_qp(qp, &attr, flags) != 0) {
        die("modify QP RTR");
    }
}

void modify_rts(ibv_qp *qp, uint32_t psn) {
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = psn;
    attr.max_rd_atomic = 1;
    int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                IBV_QP_MAX_QP_RD_ATOMIC;
    if (ibv_modify_qp(qp, &attr, flags) != 0) {
        die("modify QP RTS");
    }
}

void qp_to_err(Conn *c) {
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_ERR;
    if (ibv_modify_qp(c->qp, &attr, IBV_QP_STATE) != 0) {
        die("modify QP ERR");
    }
}

void qp_reset_init(Conn *c) {
    drain_async(c->ctx);
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_RESET;
    if (ibv_modify_qp(c->qp, &attr, IBV_QP_STATE) != 0) {
        die("modify QP RESET");
    }
    modify_init(c->qp);
}

enum ibv_mtu port_mtu(ibv_context *ctx) {
    ibv_port_attr attr{};
    if (ibv_query_port(ctx, kIbPort, &attr) != 0) {
        die("ibv_query_port");
    }
    return attr.active_mtu;
}

void post_recvs(Conn *c, int n) {
    for (int i = 0; i < n; i++) {
        ibv_recv_wr wr{};
        wr.num_sge = 0;
        ibv_recv_wr *bad = nullptr;
        if (ibv_post_recv(c->qp, &wr, &bad) != 0) {
            die("ibv_post_recv");
        }
    }
}

Conn make_conn(ibv_context *ctx, ibv_pd *pd, void *buf, ibv_mr *mr, int recv_depth) {
    Conn c;
    c.ctx = ctx;
    c.pd = pd;
    c.buf = buf;
    c.mr = mr;
    c.recv_depth = recv_depth;
    c.cq = ibv_create_cq(ctx, 256, nullptr, nullptr, 0);
    c.recv_cq = recv_depth > 0 ? ibv_create_cq(ctx, recv_depth * 2, nullptr, nullptr, 0) : c.cq;
    ibv_qp_init_attr init{};
    init.send_cq = c.cq;
    init.recv_cq = c.recv_cq;
    init.cap.max_send_wr = 64;
    init.cap.max_recv_wr = recv_depth > 0 ? recv_depth : 1;
    init.cap.max_send_sge = 1;
    init.cap.max_recv_sge = 1;
    init.qp_type = IBV_QPT_RC;
    c.qp = ibv_create_qp(pd, &init);
    if (c.cq == nullptr || c.recv_cq == nullptr || c.qp == nullptr) {
        die("create qp");
    }
    modify_init(c.qp);
    return c;
}

Conn open_conn(const char *dev, size_t bytes, int recv_depth) {
    ibv_context *ctx = open_dev(dev);
    ibv_pd *pd = ibv_alloc_pd(ctx);
    if (pd == nullptr) {
        die("ibv_alloc_pd");
    }
    void *buf = nullptr;
    if (posix_memalign(&buf, 4096, bytes) != 0) {
        die("posix_memalign");
    }
    std::memset(buf, 0, bytes);
    ibv_mr *mr = ibv_reg_mr(pd, buf, bytes, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (mr == nullptr) {
        die("ibv_reg_mr");
    }
    return make_conn(ctx, pd, buf, mr, recv_depth);
}

Exch local_exch(const Conn &c, uint32_t psn, uint64_t addr) {
    Exch ex{};
    ex.qpn = c.qp->qp_num;
    ex.psn = psn;
    ex.rkey = c.mr->rkey;
    ex.addr = addr;
    ibv_gid gid{};
    if (ibv_query_gid(c.ctx, kIbPort, kGidIndex, &gid) != 0) {
        die("ibv_query_gid");
    }
    std::memcpy(ex.gid, gid.raw, 16);
    return ex;
}

Exch handshake(Conn *c, const Exch &mine) {
    Exch peer{};
    io_full(c->fd, const_cast<Exch *>(&mine), sizeof(mine), true);
    io_full(c->fd, &peer, sizeof(peer), false);
    c->rkey = peer.rkey;
    c->remote_addr = peer.addr;
    modify_rtr(c->qp, peer);
    modify_rts(c->qp, mine.psn);
    if (c->recv_depth > 0) {
        post_recvs(c, c->recv_depth);
    }
    return peer;
}

void post_write(Conn *c, void *local, uint32_t len, uint64_t remote, bool with_imm, uint32_t imm_host) {
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(local);
    sge.length = len;
    sge.lkey = c->mr->lkey;
    ibv_send_wr wr{};
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = with_imm ? IBV_WR_RDMA_WRITE_WITH_IMM : IBV_WR_RDMA_WRITE;
    wr.imm_data = with_imm ? htonl(imm_host) : 0;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote;
    wr.wr.rdma.rkey = c->rkey;
    ibv_send_wr *bad = nullptr;
    if (ibv_post_send(c->qp, &wr, &bad) != 0) {
        die("ibv_post_send");
    }
}

uint32_t pack_imm(int slot, uint32_t stamp) {
    return (stamp << 16) | (static_cast<uint32_t>(slot) & 0xffffu);
}

uint32_t create_dct(ibv_context *ctx, ibv_pd *pd) {
    ibv_srq_init_attr srq_attr{};
    srq_attr.attr.max_wr = 1;
    srq_attr.attr.max_sge = 1;
    ibv_srq *srq = ibv_create_srq(pd, &srq_attr);
    if (srq == nullptr) {
        die("ibv_create_srq");
    }
    ibv_cq *cq = ibv_create_cq(ctx, 16, nullptr, nullptr, 0);
    mlx5dv_qp_init_attr dv{};
    ibv_qp_init_attr_ex init{};
    init.qp_type = IBV_QPT_DRIVER;
    init.send_cq = cq;
    init.recv_cq = cq;
    init.pd = pd;
    init.srq = srq;
    init.cap.max_send_wr = 1;
    init.cap.max_recv_wr = 1;
    init.cap.max_send_sge = 1;
    init.cap.max_recv_sge = 1;
    init.comp_mask = IBV_QP_INIT_ATTR_PD;
    dv.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_DC;
    dv.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCT;
    dv.dc_init_attr.dct_access_key = kDcKey;
    ibv_qp *qp = mlx5dv_create_qp(ctx, &init, &dv);
    if (cq == nullptr || qp == nullptr) {
        die("create dct");
    }
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = kIbPort;
    attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_ATOMIC;
    if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_ACCESS_FLAGS | IBV_QP_PORT | IBV_QP_PKEY_INDEX) != 0) {
        die("dct init");
    }
    std::memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = port_mtu(ctx);
    attr.min_rnr_timer = 12;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = kIbPort;
    attr.ah_attr.grh.hop_limit = 64;
    attr.ah_attr.grh.sgid_index = kGidIndex;
    if (ibv_modify_qp(qp, &attr, IBV_QP_STATE | IBV_QP_MIN_RNR_TIMER | IBV_QP_AV | IBV_QP_PATH_MTU) != 0) {
        die("dct rtr");
    }
    return qp->qp_num;
}

DcIni open_dci(Conn *backup, const Exch &server) {
    DcIni dc;
    dc.ctx = backup->ctx;
    dc.pd = backup->pd;
    dc.dct = server.dct;
    dc.cq = ibv_create_cq(dc.ctx, 128, nullptr, nullptr, 0);
    mlx5dv_qp_init_attr dv{};
    ibv_qp_init_attr_ex init{};
    init.qp_type = IBV_QPT_DRIVER;
    init.send_cq = dc.cq;
    init.recv_cq = dc.cq;
    init.pd = dc.pd;
    init.cap.max_send_wr = 128;
    init.cap.max_send_sge = 1;
    init.comp_mask = IBV_QP_INIT_ATTR_SEND_OPS_FLAGS | IBV_QP_INIT_ATTR_PD;
    init.send_ops_flags = IBV_QP_EX_WITH_RDMA_READ | IBV_QP_EX_WITH_RDMA_WRITE;
    dv.comp_mask = MLX5DV_QP_INIT_ATTR_MASK_DC | MLX5DV_QP_INIT_ATTR_MASK_QP_CREATE_FLAGS;
    dv.create_flags = MLX5DV_QP_CREATE_DISABLE_SCATTER_TO_CQE;
    dv.dc_init_attr.dc_type = MLX5DV_DCTYPE_DCI;
    dc.qp = mlx5dv_create_qp(dc.ctx, &init, &dv);
    if (dc.cq == nullptr || dc.qp == nullptr) {
        die("create dci");
    }
    ibv_qp_attr attr{};
    attr.qp_state = IBV_QPS_INIT;
    attr.pkey_index = 0;
    attr.port_num = kIbPort;
    if (ibv_modify_qp(dc.qp, &attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT) != 0) {
        die("dci init");
    }
    std::memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = port_mtu(dc.ctx);
    attr.min_rnr_timer = 10;
    attr.ah_attr.port_num = kIbPort;
    attr.ah_attr.is_global = 1;
    if (ibv_modify_qp(dc.qp, &attr, IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_AV) != 0) {
        die("dci rtr");
    }
    std::memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    attr.sq_psn = 114;
    attr.max_rd_atomic = 16;
    if (ibv_modify_qp(dc.qp, &attr,
                      IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                          IBV_QP_MAX_QP_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER) != 0) {
        die("dci rts");
    }
    dc.qp_ex = ibv_qp_to_qp_ex(dc.qp);
    dc.mlx = mlx5dv_qp_ex_from_ibv_qp_ex(dc.qp_ex);
    ibv_ah_attr ah{};
    ah.port_num = kIbPort;
    ah.is_global = 1;
    ah.grh.hop_limit = 64;
    ah.grh.sgid_index = kGidIndex;
    std::memcpy(ah.grh.dgid.raw, server.gid, 16);
    dc.ah = ibv_create_ah(dc.pd, &ah);
    if (dc.qp_ex == nullptr || dc.mlx == nullptr || dc.ah == nullptr) {
        die("dci ex");
    }
    return dc;
}

int dc_wait(DcIni *dc, uint64_t wr_id) {
    uint64_t start = now_ns();
    while (now_ns() - start < 2000000000ull) {
        ibv_wc wc{};
        int n = ibv_poll_cq(dc->cq, 1, &wc);
        if (n < 0) {
            die("dc poll");
        }
        if (n == 0) {
            continue;
        }
        if (wc.status != IBV_WC_SUCCESS) {
            std::fprintf(stderr, "dc CQE %s\n", ibv_wc_status_str(wc.status));
            return -1;
        }
        if (wc.wr_id == wr_id) {
            return 0;
        }
    }
    std::fprintf(stderr, "dc timeout\n");
    return -1;
}

int dc_io(DcIni *dc, bool write, void *local, uint32_t lkey, uint32_t len, uint64_t remote, uint32_t rkey) {
    uint64_t id = ++dc->wr_id;
    ibv_wr_start(dc->qp_ex);
    dc->qp_ex->wr_id = id;
    dc->qp_ex->wr_flags = IBV_SEND_SIGNALED;
    if (write) {
        ibv_wr_rdma_write(dc->qp_ex, rkey, remote);
    } else {
        ibv_wr_rdma_read(dc->qp_ex, rkey, remote);
    }
    ibv_wr_set_sge(dc->qp_ex, lkey, reinterpret_cast<uintptr_t>(local), len);
    mlx5dv_wr_set_dc_addr(dc->mlx, dc->ah, dc->dct, kDcKey);
    if (ibv_wr_complete(dc->qp_ex) != 0) {
        die("ibv_wr_complete");
    }
    return dc_wait(dc, id);
}

void post_read(Conn *c, void *local, uint32_t len, uint64_t remote) {
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(local);
    sge.length = len;
    sge.lkey = c->mr->lkey;
    ibv_send_wr wr{};
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_READ;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote;
    wr.wr.rdma.rkey = c->rkey;
    ibv_send_wr *bad = nullptr;
    if (ibv_post_send(c->qp, &wr, &bad) != 0) {
        die("ibv_post_send read");
    }
}

int poll_once(Conn *c, bool errors_ok) {
    ibv_wc wc[8]{};
    int n = ibv_poll_cq(c->cq, 8, wc);
    if (n < 0) {
        die("ibv_poll_cq");
    }
    for (int i = 0; i < n; i++) {
        if (wc[i].status != IBV_WC_SUCCESS && !errors_ok) {
            std::fprintf(stderr, "CQE status %s\n", ibv_wc_status_str(wc[i].status));
            std::exit(1);
        }
    }
    return n;
}

void wait_cqes(Conn *c, int want) {
    int got = 0;
    uint64_t start = now_ns();
    while (got < want) {
        got += poll_once(c, false);
        if (now_ns() - start > 2000000000ull) {
            std::fprintf(stderr, "CQ timeout want=%d got=%d\n", want, got);
            std::exit(1);
        }
    }
}

int drain_cq(Conn *c) {
    int got = 0;
    uint64_t start = now_ns();
    while (now_ns() - start < 2000000ull) {
        int n = poll_once(c, true);
        got += n;
        if (n == 0 && now_ns() - start > 200000ull) {
            break;
        }
    }
    return got;
}

uint64_t rdma_read_u64(Conn *c, uint64_t remote_off) {
    auto *local = static_cast<uint64_t *>(c->buf);
    *local = 0;
    post_read(c, local, sizeof(uint64_t), c->remote_addr + remote_off);
    wait_cqes(c, 1);
    uint64_t v = 0;
    std::memcpy(&v, local, sizeof(v));
    return v;
}

HerdRequest make_put(uint64_t token) {
    HerdRequest req{};
    req.key.bkt = 7;
    req.opcode = kHerdOpPut;
    req.val_len = sizeof(token);
    std::memcpy(req.value, &token, sizeof(token));
    return req;
}

HerdRequest *req_slot(Conn *c, int slot) {
    return reinterpret_cast<HerdRequest *>(static_cast<unsigned char *>(c->buf)) + slot;
}

void post_request_at(Conn *c, int slot, const HerdRequest &req, uint32_t stamp) {
    HerdRequest *local = req_slot(c, slot);
    std::memcpy(local, &req, sizeof(req));
    uint64_t remote = c->remote_addr + field_off(slot, offsetof(Slot, req));
    if (stamp == 0) {
        post_write(c, local, sizeof(req), remote, false, 0);
    } else {
        post_write(c, local, sizeof(req), remote, true, pack_imm(slot, stamp));
    }
}

uint64_t percentile(std::vector<uint64_t> lat, int pct) {
    if (lat.empty()) {
        return 0;
    }
    std::sort(lat.begin(), lat.end());
    size_t i = lat.size() * static_cast<size_t>(pct) / 100;
    if (i >= lat.size()) {
        i = lat.size() - 1;
    }
    return lat[i];
}

struct ServerPoll {
    Remote *remote = nullptr;
    Conn *qps[2] = {};
    int n = 0;
    std::atomic<bool> *run = nullptr;
};

void poll_remote(ServerPoll *sp) {
    Remote *remote = sp->remote;
    while (sp->run->load(std::memory_order_relaxed)) {
        for (int q = 0; q < sp->n; q++) {
            Conn *c = sp->qps[q];
            ibv_wc wc[32];
            int n = ibv_poll_cq(c->recv_cq, 32, wc);
            if (n < 0) {
                continue;
            }
            for (int i = 0; i < n; i++) {
                if (wc[i].status == IBV_WC_SUCCESS && (wc[i].wc_flags & IBV_WC_WITH_IMM) != 0) {
                    uint32_t imm = ntohl(wc[i].imm_data);
                    unsigned slot = imm & 0xffffu;
                    uint32_t stamp = imm >> 16;
                    if (slot < static_cast<unsigned>(kBurst) && stamp != 0) {
                        __atomic_store_n(&remote->slots[slot].log, static_cast<uint64_t>(stamp), __ATOMIC_RELEASE);
                    }
                }
                if (wc[i].status == IBV_WC_SUCCESS) {
                    ibv_recv_wr wr{};
                    wr.num_sge = 0;
                    ibv_recv_wr *bad = nullptr;
                    ibv_post_recv(c->qp, &wr, &bad);
                }
            }
        }
        for (int i = 0; i < kBurst; i++) {
            uint8_t op = *reinterpret_cast<volatile uint8_t *>(&remote->slots[i].req.opcode);
            if (op < kHerdOpPut) {
                continue;
            }
            remote->slots[i].req.opcode = static_cast<uint8_t>(op - kHerdMicaOffset);
            uint64_t seen = 0;
            std::memcpy(&seen, remote->slots[i].req.value, sizeof(seen));
            __atomic_store_n(&remote->slots[i].seen, seen, __ATOMIC_RELEASE);
            __atomic_fetch_add(&remote->slots[i].exec, 1, __ATOMIC_RELEASE);
        }
    }
}

int listen_on(const char *ip, int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket");
    }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, ip, &addr.sin_addr);
    if (bind(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        die("bind");
    }
    if (listen(fd, 4) != 0) {
        die("listen");
    }
    return fd;
}

void serve(const char *ip, int port, const char *dev, const char *backup_dev) {
    Conn primary = open_conn(dev, sizeof(Remote), kRecvDepth);
    Conn backup;
    Conn probe;
    if (std::strcmp(dev, backup_dev) == 0) {
        backup = make_conn(primary.ctx, primary.pd, primary.buf, primary.mr, kRecvDepth);
        probe = make_conn(primary.ctx, primary.pd, primary.buf, primary.mr, 0);
    } else {
        ibv_context *ctx = open_dev(backup_dev);
        ibv_pd *pd = ibv_alloc_pd(ctx);
        if (pd == nullptr) {
            die("backup pd");
        }
        ibv_mr *mr = ibv_reg_mr(pd, primary.buf, sizeof(Remote),
                                IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
        if (mr == nullptr) {
            die("backup mr");
        }
        backup = make_conn(ctx, pd, primary.buf, mr, kRecvDepth);
        probe = make_conn(ctx, pd, primary.buf, mr, 0);
    }
    uint32_t dct = create_dct(backup.ctx, backup.pd);
    int listen_fd = listen_on(ip, port);
    std::printf("herd bench listening %s:%d dev=%s backup=%s dct=%u imm=1\n", ip, port, dev, backup_dev, dct);
    std::fflush(stdout);
    for (;;) {
        int fd = accept(listen_fd, nullptr, nullptr);
        if (fd < 0) {
            die("accept");
        }
        primary.fd = backup.fd = probe.fd = fd;
        uint64_t addr = reinterpret_cast<uint64_t>(primary.buf);
        Exch back = local_exch(backup, 2, addr);
        back.dct = dct;
        handshake(&primary, local_exch(primary, 1, addr));
        handshake(&backup, back);
        handshake(&probe, local_exch(probe, 3, addr));
        send_line(fd, "UP\n");
        std::atomic<bool> run{true};
        ServerPoll sp;
        sp.remote = static_cast<Remote *>(primary.buf);
        sp.qps[0] = &primary;
        sp.qps[1] = &backup;
        sp.n = 2;
        sp.run = &run;
        std::thread poller(poll_remote, &sp);
        for (;;) {
            char line[64];
            recv_line(fd, line, sizeof(line));
            if (std::strcmp(line, "QUIT") == 0) {
                break;
            }
            if (std::strcmp(line, "RESET") == 0) {
                qp_reset_init(&primary);
                handshake(&primary, local_exch(primary, static_cast<uint32_t>(std::rand()) & 0xffffffu, addr));
                send_line(fd, "READY\n");
                continue;
            }
            std::fprintf(stderr, "unknown command %s\n", line);
            break;
        }
        run.store(false);
        poller.join();
        ::close(fd);
        qp_reset_init(&primary);
        qp_reset_init(&backup);
        qp_reset_init(&probe);
    }
}

int tcp_connect(const char *ip, int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket");
    }
    timeval tv{};
    tv.tv_sec = 5;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, ip, &addr.sin_addr);
    if (connect(fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        die("connect");
    }
    return fd;
}

struct Ends {
    Conn primary;
    Conn backup;
    Conn probe;
    DcIni dc;
    int fd = -1;
};

Ends connect_all(const char *ip, int port, const char *primary_dev, const char *backup_dev) {
    Ends e;
    e.primary = open_conn(primary_dev, 4096, 0);
    e.backup = open_conn(backup_dev, 4096, 0);
    void *pbuf = nullptr;
    if (posix_memalign(&pbuf, 4096, 4096) != 0) {
        die("posix_memalign");
    }
    std::memset(pbuf, 0, 4096);
    ibv_mr *pmr = ibv_reg_mr(e.backup.pd, pbuf, 4096,
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (pmr == nullptr) {
        die("probe mr");
    }
    e.probe = make_conn(e.backup.ctx, e.backup.pd, pbuf, pmr, 0);
    e.fd = tcp_connect(ip, port);
    e.primary.fd = e.backup.fd = e.probe.fd = e.fd;
    handshake(&e.primary, local_exch(e.primary, 11, 0));
    Exch back = handshake(&e.backup, local_exch(e.backup, 12, 0));
    handshake(&e.probe, local_exch(e.probe, 13, 0));
    char up[16];
    recv_line(e.fd, up, sizeof(up));
    if (std::strcmp(up, "UP") != 0) {
        std::fprintf(stderr, "peer not up: %s\n", up);
        std::exit(1);
    }
    if (back.dct == 0) {
        std::fprintf(stderr, "server did not advertise a DCT\n");
        std::exit(1);
    }
    e.dc = open_dci(&e.backup, back);
    std::printf("PATH write=imm recovery=dcqp dct=%u dev=%s backup=%s\n", back.dct, primary_dev, backup_dev);
    std::fflush(stdout);
    return e;
}

uint64_t dc_read_u64(Ends *e, uint64_t remote_off) {
    auto *local = static_cast<uint64_t *>(e->probe.buf);
    *local = 0;
    if (dc_io(&e->dc, false, local, e->probe.mr->lkey, sizeof(uint64_t),
              e->backup.remote_addr + remote_off, e->backup.rkey) != 0) {
        std::fprintf(stderr, "dc read failed\n");
        std::exit(1);
    }
    uint64_t value = 0;
    std::memcpy(&value, local, sizeof(value));
    return value;
}

uint64_t dc_read_log(Ends *e, int slot, uint32_t stamp) {
    uint64_t got = 0;
    uint64_t start = now_ns();
    do {
        got = dc_read_u64(e, field_off(slot, offsetof(Slot, log)));
        if (got == stamp) {
            return got;
        }
    } while (now_ns() - start < 1000000ull);
    return got;
}

void dc_replay(Ends *e, int slot, const HerdRequest &req) {
    HerdRequest *local = req_slot(&e->backup, slot);
    std::memcpy(local, &req, sizeof(req));
    if (dc_io(&e->dc, true, local, e->backup.mr->lkey, sizeof(req),
              e->backup.remote_addr + field_off(slot, offsetof(Slot, req)), e->backup.rkey) != 0) {
        std::fprintf(stderr, "dc write failed\n");
        std::exit(1);
    }
}

void reconnect_primary(Ends *e) {
    send_line(e->fd, "RESET\n");
    qp_reset_init(&e->primary);
    handshake(&e->primary, local_exch(e->primary, static_cast<uint32_t>(std::rand()) & 0xffffffu, 0));
    char line[16];
    recv_line(e->fd, line, sizeof(line));
    if (std::strcmp(line, "READY") != 0) {
        std::fprintf(stderr, "reset failed: %s\n", line);
        std::exit(1);
    }
}

void run_perf(const char *ip, int port, const char *dev, double seconds) {
    Ends e = connect_all(ip, port, dev, dev);
    HerdRequest sample = make_put(1);
    for (int i = 0; i < 200; i++) {
        post_request_at(&e.primary, 0, sample, 0);
        wait_cqes(&e.primary, 1);
    }
    auto measure = [&](const char *name, bool imm) {
        std::vector<uint64_t> lat;
        lat.reserve(1 << 20);
        uint64_t t_start = now_ns();
        uint64_t t_end = t_start + static_cast<uint64_t>(seconds * 1e9);
        uint64_t ops = 0;
        while (now_ns() < t_end) {
            uint64_t t0 = now_ns();
            post_request_at(&e.primary, 0, sample, imm ? 1u : 0u);
            wait_cqes(&e.primary, 1);
            lat.push_back(now_ns() - t0);
            ops++;
        }
        double sec = static_cast<double>(now_ns() - t_start) / 1e9;
        uint64_t p50 = percentile(lat, 50);
        uint64_t p99 = percentile(lat, 99);
        std::printf("PERF system=%s ops=%lu seconds=%.6f kops=%.3f p50_ns=%lu p99_ns=%lu\n", name,
                    static_cast<unsigned long>(ops), sec, ops / sec / 1000.0, static_cast<unsigned long>(p50),
                    static_cast<unsigned long>(p99));
        std::fflush(stdout);
    };
    measure("herd_native", false);
    measure("herd_varuna", true);
    send_line(e.fd, "QUIT\n");
}

struct Tally {
    int trials = 0;
    int correct = 0;
    int duplicate = 0;
    int lost = 0;
    int mismatch = 0;
};

void note_outcome(Tally *t, uint64_t delta, uint64_t seen, uint64_t token) {
    t->trials++;
    if (delta == 0) {
        t->lost++;
    } else if (delta == 1 && seen == token) {
        t->correct++;
    } else if (delta == 1) {
        t->mismatch++;
    } else {
        t->duplicate++;
    }
}

void run_correct(const char *ip, int port, const char *dev, int trials) {
    Ends e = connect_all(ip, port, dev, dev);
    const char *names[] = {"herd_native", "herd_blind", "herd_varuna"};
    std::printf("CORRECT_BURST slots=%d\n", kBurst);
    for (int policy = 0; policy < 3; policy++) {
        Tally tally;
        for (int trial = 0; trial < trials; trial++) {
            uint64_t before[kBurst];
            uint64_t tokens[kBurst];
            for (int s = 0; s < kBurst; s++) {
                before[s] = rdma_read_u64(&e.probe, field_off(s, offsetof(Slot, exec)));
                tokens[s] = static_cast<uint64_t>(policy + 1) * 10000000ull +
                            static_cast<uint64_t>(trial) * static_cast<uint64_t>(kBurst) + static_cast<uint64_t>(s + 1);
            }
            for (int s = 0; s < kBurst; s++) {
                uint32_t stamp = static_cast<uint32_t>(trial * kBurst + s + 1);
                post_request_at(&e.primary, s, make_put(tokens[s]), policy == 2 ? stamp : 0u);
            }
            qp_to_err(&e.primary);
            drain_cq(&e.primary);
            spin_ns(300000);
            int replay_wq = 0;
            for (int s = 0; s < kBurst; s++) {
                uint32_t stamp = static_cast<uint32_t>(trial * kBurst + s + 1);
                if (policy == 1) {
                    post_request_at(&e.backup, s, make_put(tokens[s]), 0);
                    replay_wq++;
                } else if (policy == 2) {
                    uint64_t remote_stamp = dc_read_log(&e, s, stamp);
                    if (remote_stamp != stamp) {
                        dc_replay(&e, s, make_put(tokens[s]));
                    }
                }
            }
            if (replay_wq > 0) {
                wait_cqes(&e.backup, replay_wq);
            }
            spin_ns(300000);
            for (int s = 0; s < kBurst; s++) {
                uint64_t after = rdma_read_u64(&e.probe, field_off(s, offsetof(Slot, exec)));
                uint64_t seen = rdma_read_u64(&e.probe, field_off(s, offsetof(Slot, seen)));
                uint64_t delta = after >= before[s] ? after - before[s] : 0;
                note_outcome(&tally, delta, seen, tokens[s]);
            }
            reconnect_primary(&e);
        }
        std::printf("CORRECT system=%s trials=%d correct=%d duplicate=%d lost=%d mismatch=%d\n", names[policy],
                    tally.trials, tally.correct, tally.duplicate, tally.lost, tally.mismatch);
        std::fflush(stdout);
    }
    send_line(e.fd, "QUIT\n");
}

void run_timeline(const char *ip, int port, const char *primary_dev, const char *backup_dev, int record_ms, int fail_ms) {
    Ends e = connect_all(ip, port, primary_dev, backup_dev);
    Conn *cur = &e.primary;
    HerdRequest sample = make_put(1);
    for (int i = 0; i < 1000; i++) {
        post_request_at(cur, 0, sample, 1);
        wait_cqes(cur, 1);
    }
    const uint64_t bucket_ns = 100000ull;
    uint64_t record_ns = static_cast<uint64_t>(record_ms) * 1000000ull;
    uint64_t fail_ns = static_cast<uint64_t>(fail_ms) * 1000000ull;
    int bins_n = static_cast<int>(record_ns / bucket_ns) + 2;
    std::vector<int> bins(static_cast<size_t>(bins_n), 0);
    auto mark = [&](uint64_t t0, uint64_t t) {
        if (t < t0 || t - t0 >= record_ns) {
            return;
        }
        size_t bin = static_cast<size_t>((t - t0) / bucket_ns);
        if (bin < bins.size()) {
            bins[bin]++;
        }
    };
    uint64_t t0 = now_ns();
    uint64_t recovery_ns = 0;
    bool failed = false;
    uint64_t token = 10;
    while (now_ns() - t0 < record_ns) {
        token++;
        uint32_t stamp = static_cast<uint32_t>(token & 0xffffu);
        if (stamp == 0) {
            stamp = 1;
        }
        HerdRequest put = make_put(token);
        if (!failed && now_ns() - t0 >= fail_ns) {
            post_request_at(cur, 0, put, stamp);
            uint64_t t_fail = now_ns();
            qp_to_err(&e.primary);
            drain_cq(&e.primary);
            uint64_t remote_stamp = dc_read_log(&e, 0, stamp);
            if (remote_stamp != stamp) {
                dc_replay(&e, 0, put);
            }
            uint64_t t_resolved = now_ns();
            recovery_ns = t_resolved - t_fail;
            mark(t0, t_resolved);
            cur = &e.backup;
            failed = true;
            continue;
        }
        uint64_t t_done_before = now_ns();
        post_request_at(cur, 0, put, stamp);
        wait_cqes(cur, 1);
        mark(t0, now_ns());
        (void)t_done_before;
    }
    std::printf("TIMELINE_META bucket_us=100 fail_us=%d recovery_us=%.3f record_us=%d\n", fail_ms * 1000,
                recovery_ns / 1000.0, record_ms * 1000);
    for (int i = 0; i < bins_n; i++) {
        double kops = bins[static_cast<size_t>(i)] / (bucket_ns / 1e9) / 1000.0;
        std::printf("TIMELINE_ROW t_us=%d kops=%.3f\n", i * 100, kops);
    }
    std::fflush(stdout);
    send_line(e.fd, "QUIT\n");
}

}  // namespace

int main(int argc, char **argv) {
    if (argc < 5) {
        std::fprintf(stderr,
                     "usage: %s server|perf|correct|timeline <ip> <port> <ib_dev> [seconds|trials|record_ms fail_ms]\n",
                     argv[0]);
        return 1;
    }
    std::srand(1);
    int port = std::atoi(argv[3]);
    if (std::strcmp(argv[1], "server") == 0) {
        const char *backup_dev = argc >= 6 ? argv[5] : argv[4];
        serve(argv[2], port, argv[4], backup_dev);
        return 0;
    }
    if (std::strcmp(argv[1], "perf") == 0) {
        double seconds = argc >= 6 ? std::atof(argv[5]) : 1.0;
        if (seconds < 0.05) {
            seconds = 0.05;
        }
        run_perf(argv[2], port, argv[4], seconds);
        return 0;
    }
    if (std::strcmp(argv[1], "correct") == 0) {
        int trials = argc >= 6 ? std::atoi(argv[5]) : 800;
        if (trials < 1) {
            trials = 1;
        }
        run_correct(argv[2], port, argv[4], trials);
        return 0;
    }
    if (std::strcmp(argv[1], "timeline") == 0) {
        int record_ms = argc >= 6 ? std::atoi(argv[5]) : 30;
        int fail_ms = argc >= 7 ? std::atoi(argv[6]) : 10;
        const char *backup = argc >= 8 ? argv[7] : "mlx5_3";
        if (record_ms < fail_ms + 5) {
            record_ms = fail_ms + 5;
        }
        run_timeline(argv[2], port, argv[4], backup, record_ms, fail_ms);
        return 0;
    }
    std::fprintf(stderr, "unknown mode %s\n", argv[1]);
    return 1;
}
