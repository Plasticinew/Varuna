// Cross-node HERD RPC request path.
//
// The slot layout and poll rule are efficient/rdma_bench (mica_op, HERD_OP_*
// = MICA opcode + 10, worker.c disarms by subtracting 10). The client posts
// that 64-byte request with an RC WRITE. rdma_bench uses a UC QP; the bytes
// and the worker check are the same.
//
// Each request also has a completion-log slot. z_recovery() in source/zQP.cc
// rewrites a logged WRITE only when full retransmission is on or the local
// timestamp differs from the log. A match leaves the request slot untouched.
// The log here is an 8-byte stamp written by a second WRITE, which is the
// paper's completion-log write, not a call into zQP. The UD SEND reply is
// not on this path.
//
//   herd_rpc_rdma server <bind_ip> <port> <ib_dev>
//   herd_rpc_rdma client <server_ip> <port> <ib_dev>

#include <arpa/inet.h>
#include <infiniband/verbs.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <netinet/in.h>
#include <random>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace {

constexpr uint8_t kMicaOpGet = 111;
constexpr uint8_t kMicaOpPut = 112;
constexpr uint8_t kHerdMicaOffset = 10;
constexpr uint8_t kHerdOpGet = kMicaOpGet + kHerdMicaOffset;
constexpr uint8_t kHerdOpPut = kMicaOpPut + kHerdMicaOffset;
constexpr int kMicaMaxValue = 64 - (16 + 1 + 1);
constexpr int kGidIndex = 3;
constexpr int kPort = 1;
constexpr int kLogSlots = 8;

struct MicaKey {
    uint64_t unused;
    uint32_t bkt;
    uint16_t server;
    uint16_t tag;
};
static_assert(sizeof(MicaKey) == 16, "HERD key is 16 bytes");

struct HerdRequest {
    MicaKey key;
    uint8_t opcode;
    uint8_t val_len;
    uint8_t value[kMicaMaxValue];
};
static_assert(sizeof(HerdRequest) == 64, "rdma_bench mica_op is one cache line");
static_assert(offsetof(HerdRequest, opcode) == 16, "worker polls opcode at byte 16");

struct Remote {
    HerdRequest slot;
    uint64_t log[kLogSlots];
    uint64_t exec;
    uint64_t seen_val;
};

struct Exch {
    uint32_t qpn;
    uint32_t psn;
    uint32_t rkey;
    uint32_t pad;
    uint64_t addr;
    uint8_t gid[16];
};

struct Stat {
    int puts = 0;
    int gets = 0;
    unsigned opcode = 0;
    unsigned vlen = 0;
    char val[64] = {};
};

struct Conn {
    ibv_context *ctx = nullptr;
    ibv_pd *pd = nullptr;
    ibv_cq *cq = nullptr;
    ibv_qp *qp = nullptr;
    ibv_mr *mr = nullptr;
    void *buf = nullptr;
    uint32_t rkey = 0;
    uint64_t remote_addr = 0;
    int fd = -1;
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

ibv_context *open_dev(const char *name) {
    int num = 0;
    ibv_device **list = ibv_get_device_list(&num);
    if (list == nullptr) {
        die("ibv_get_device_list");
    }
    for (int i = 0; i < num; i++) {
        if (std::strcmp(ibv_get_device_name(list[i]), name) == 0) {
            ibv_context *ctx = ibv_open_device(list[i]);
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
    attr.port_num = kPort;
    attr.qp_access_flags =
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
    if (ibv_modify_qp(qp, &attr,
                      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS) != 0) {
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
    attr.ah_attr.port_num = kPort;
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

Conn setup_qp(const char *dev, size_t bytes) {
    Conn c;
    c.ctx = open_dev(dev);
    c.pd = ibv_alloc_pd(c.ctx);
    c.cq = ibv_create_cq(c.ctx, 256, nullptr, nullptr, 0);
    if (c.pd == nullptr || c.cq == nullptr) {
        die("alloc pd/cq");
    }
    ibv_qp_init_attr init{};
    init.send_cq = c.cq;
    init.recv_cq = c.cq;
    init.cap.max_send_wr = 16;
    init.cap.max_recv_wr = 128;
    init.cap.max_send_sge = 1;
    init.cap.max_recv_sge = 1;
    init.qp_type = IBV_QPT_RC;
    c.qp = ibv_create_qp(c.pd, &init);
    if (c.qp == nullptr) {
        die("ibv_create_qp");
    }
    if (posix_memalign(&c.buf, 4096, bytes) != 0) {
        die("posix_memalign");
    }
    std::memset(c.buf, 0, bytes);
    c.mr = ibv_reg_mr(c.pd, c.buf, bytes,
                      IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE);
    if (c.mr == nullptr) {
        die("ibv_reg_mr");
    }
    modify_init(c.qp);
    return c;
}

Exch local_exch(const Conn &c, uint32_t psn, uint64_t addr) {
    Exch ex{};
    ex.qpn = c.qp->qp_num;
    ex.psn = psn;
    ex.rkey = c.mr->rkey;
    ex.addr = addr;
    ibv_gid gid{};
    if (ibv_query_gid(c.ctx, kPort, kGidIndex, &gid) != 0) {
        die("ibv_query_gid");
    }
    std::memcpy(ex.gid, gid.raw, 16);
    return ex;
}

void handshake(Conn *c, const Exch &mine) {
    Exch peer{};
    io_full(c->fd, const_cast<Exch *>(&mine), sizeof(mine), true);
    io_full(c->fd, &peer, sizeof(peer), false);
    c->rkey = peer.rkey;
    c->remote_addr = peer.addr;
    modify_rtr(c->qp, peer);
    modify_rts(c->qp, mine.psn);
}

int poll_one(ibv_cq *cq) {
    timespec start{};
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (;;) {
        ibv_wc wc{};
        int n = ibv_poll_cq(cq, 1, &wc);
        if (n < 0) {
            die("ibv_poll_cq");
        }
        if (n > 0) {
            if (wc.status != IBV_WC_SUCCESS) {
                std::fprintf(stderr, "CQE status %s\n", ibv_wc_status_str(wc.status));
                std::exit(1);
            }
            return 0;
        }
        timespec now{};
        clock_gettime(CLOCK_MONOTONIC, &now);
        double sec = static_cast<double>(now.tv_sec - start.tv_sec) +
                     static_cast<double>(now.tv_nsec - start.tv_nsec) / 1e9;
        if (sec > 2.0) {
            std::fprintf(stderr, "CQ timeout\n");
            std::exit(1);
        }
    }
}

void rdma(Conn *c, ibv_wr_opcode op, void *local, uint32_t len, uint64_t remote) {
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(local);
    sge.length = len;
    sge.lkey = c->mr->lkey;
    ibv_send_wr wr{};
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = op;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote;
    wr.wr.rdma.rkey = c->rkey;
    ibv_send_wr *bad = nullptr;
    if (ibv_post_send(c->qp, &wr, &bad) != 0) {
        die("ibv_post_send");
    }
    poll_one(c->cq);
}

HerdRequest make_put(uint32_t key, const char *value) {
    HerdRequest req{};
    req.key.bkt = key;
    req.opcode = kHerdOpPut;
    req.val_len = static_cast<uint8_t>(std::strlen(value));
    std::memcpy(req.value, value, req.val_len);
    return req;
}

HerdRequest make_get(uint32_t key) {
    HerdRequest req{};
    req.key.bkt = key;
    req.opcode = kHerdOpGet;
    return req;
}

void apply_request(HerdRequest *slot, std::unordered_map<uint32_t, std::string> *values, int *puts,
                   int *gets) {
    uint8_t op = slot->opcode;
    if (op < kHerdOpGet) {
        return;
    }
    slot->opcode = static_cast<uint8_t>(op - kHerdMicaOffset);
    if (slot->opcode == kMicaOpPut) {
        (*values)[slot->key.bkt] =
            std::string(reinterpret_cast<char *>(slot->value), slot->val_len);
        (*puts)++;
        std::fprintf(stderr, "exec PUT key=%u val=%s puts=%d\n", slot->key.bkt,
                     (*values)[slot->key.bkt].c_str(), *puts);
    } else if (slot->opcode == kMicaOpGet) {
        (*gets)++;
        std::fprintf(stderr, "exec GET key=%u gets=%d\n", slot->key.bkt, *gets);
    }
}

void serve(const char *ip, int port, const char *dev) {
    Conn c = setup_qp(dev, sizeof(Remote));
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        die("socket");
    }
    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        die("inet_pton");
    }
    if (bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        die("bind");
    }
    if (listen(listen_fd, 1) != 0) {
        die("listen");
    }
    std::printf("herd worker listening %s:%d dev=%s\n", ip, port, dev);
    std::fflush(stdout);
    c.fd = accept(listen_fd, nullptr, nullptr);
    if (c.fd < 0) {
        die("accept");
    }
    uint32_t psn = static_cast<uint32_t>(std::rand()) & 0xffffffu;
    handshake(&c, local_exch(c, psn, reinterpret_cast<uint64_t>(c.buf)));
    send_line(c.fd, "UP\n");
    std::printf("QP up qpn=%u\n", c.qp->qp_num);
    std::fflush(stdout);

    auto *remote = static_cast<Remote *>(c.buf);
    std::unordered_map<uint32_t, std::string> values;
    int puts = 0;
    int gets = 0;
    char line[64];
    for (;;) {
        recv_line(c.fd, line, sizeof(line));
        if (std::strcmp(line, "QUIT") == 0) {
            break;
        }
        if (std::strcmp(line, "STAT") != 0) {
            die("unexpected command");
        }
        for (int i = 0; i < 1000; i++) {
            apply_request(&remote->slot, &values, &puts, &gets);
            if (remote->slot.opcode < kHerdOpGet) {
                break;
            }
        }
        const char *val = "-";
        auto it = values.find(remote->slot.key.bkt);
        if (it != values.end() && !it->second.empty()) {
            val = it->second.c_str();
        }
        char out[160];
        std::snprintf(out, sizeof(out), "puts=%d gets=%d opcode=%u vlen=%u val=%s\n", puts, gets,
                      remote->slot.opcode, static_cast<unsigned>(std::strlen(val)), val);
        send_line(c.fd, out);
    }
}

struct Client {
    Conn conn;
    HerdRequest *req;
    uint64_t *stamp;
    int failed = 0;
};

void write_bytes(Client *cl, const void *src, uint32_t len, uint64_t remote_off) {
    std::memcpy(cl->req, src, len);
    rdma(&cl->conn, IBV_WR_RDMA_WRITE, cl->req, len, cl->conn.remote_addr + remote_off);
}

constexpr uint32_t kGetReqSize = 17;

void post_request(Client *cl, const HerdRequest &posted) {
    uint32_t len = posted.opcode == kHerdOpGet ? kGetReqSize : sizeof(posted);
    write_bytes(cl, &posted, len, offsetof(Remote, slot));
}

void post_log(Client *cl, int slot, uint64_t stamp) {
    *cl->stamp = stamp;
    rdma(&cl->conn, IBV_WR_RDMA_WRITE, cl->stamp, sizeof(uint64_t),
         cl->conn.remote_addr + offsetof(Remote, log) + static_cast<uint64_t>(slot) * sizeof(uint64_t));
}

uint64_t read_log(Client *cl, int slot) {
    *cl->stamp = 0;
    rdma(&cl->conn, IBV_WR_RDMA_READ, cl->stamp, sizeof(uint64_t),
         cl->conn.remote_addr + offsetof(Remote, log) + static_cast<uint64_t>(slot) * sizeof(uint64_t));
    return *cl->stamp;
}

bool rewrites(uint64_t local_time, uint64_t remote_time, bool full) {
    return full || local_time != remote_time;
}

void recover(Client *cl, int slot, uint64_t local_time, const HerdRequest &posted, bool full) {
    uint64_t remote_time = read_log(cl, slot);
    if (rewrites(local_time, remote_time, full)) {
        post_request(cl, posted);
        post_log(cl, slot, local_time);
    }
}

Stat fetch_stat(Client *cl) {
    send_line(cl->conn.fd, "STAT\n");
    char line[160];
    recv_line(cl->conn.fd, line, sizeof(line));
    Stat st;
    if (std::sscanf(line, "puts=%d gets=%d opcode=%u vlen=%u val=%63s", &st.puts, &st.gets, &st.opcode,
                    &st.vlen, st.val) != 5) {
        std::fprintf(stderr, "bad stat line: %s\n", line);
        std::exit(1);
    }
    return st;
}

void expect_stat(Client *cl, const char *name, int puts, int gets, unsigned opcode, const char *val) {
    Stat st = fetch_stat(cl);
    bool ok = st.puts == puts && st.gets == gets && st.opcode == opcode && std::strcmp(st.val, val) == 0;
    if (ok) {
        std::printf("PASS %s\n", name);
        return;
    }
    std::printf("FAIL %s got puts=%d gets=%d opcode=%u val=%s\n", name, st.puts, st.gets, st.opcode, st.val);
    cl->failed++;
}

void run_client(const char *ip, int port, const char *dev) {
    Client cl;
    cl.conn = setup_qp(dev, 4096);
    cl.req = static_cast<HerdRequest *>(cl.conn.buf);
    cl.stamp = reinterpret_cast<uint64_t *>(static_cast<unsigned char *>(cl.conn.buf) + 256);
    cl.conn.fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (cl.conn.fd < 0) {
        die("socket");
    }
    timeval tv{};
    tv.tv_sec = 3;
    setsockopt(cl.conn.fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, ip, &addr.sin_addr);
    if (connect(cl.conn.fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        die("connect");
    }
    uint32_t psn = static_cast<uint32_t>(std::rand()) & 0xffffffu;
    handshake(&cl.conn, local_exch(cl.conn, psn, 0));
    char up[16];
    recv_line(cl.conn.fd, up, sizeof(up));
    if (std::strcmp(up, "UP") != 0) {
        std::fprintf(stderr, "peer not up: %s\n", up);
        std::exit(1);
    }
    std::printf("RC WRITE %s dev=%s -> %s\n", dev, dev, ip);

    HerdRequest v1 = make_put(7, "v1");
    post_request(&cl, v1);
    post_log(&cl, 0, 1);
    expect_stat(&cl, "PUT v1 lands once", 1, 0, kMicaOpPut, "v1");

    HerdRequest v2 = make_put(7, "v2");
    post_request(&cl, v2);
    post_log(&cl, 1, 2);
    expect_stat(&cl, "PUT v2 reuses the slot", 2, 0, kMicaOpPut, "v2");

    recover(&cl, 0, 1, v1, false);
    expect_stat(&cl, "matching log skips the old PUT", 2, 0, kMicaOpPut, "v2");

    post_request(&cl, v1);
    expect_stat(&cl, "blind rewrite rolls the key back", 3, 0, kMicaOpPut, "v1");

    HerdRequest v3 = make_put(7, "v3");
    recover(&cl, 2, 3, v3, false);
    expect_stat(&cl, "empty log rewrites a lost PUT once", 4, 0, kMicaOpPut, "v3");

    HerdRequest v4 = make_put(7, "v4");
    post_request(&cl, v4);
    expect_stat(&cl, "request lands but its log write does not", 5, 0, kMicaOpPut, "v4");
    recover(&cl, 3, 4, v4, false);
    expect_stat(&cl, "missing log replays that PUT", 6, 0, kMicaOpPut, "v4");

    HerdRequest get = make_get(7);
    post_request(&cl, get);
    expect_stat(&cl, "GET runs and leaves the value", 6, 1, kMicaOpGet, "v4");
    post_request(&cl, get);
    expect_stat(&cl, "replayed GET runs again", 6, 2, kMicaOpGet, "v4");

    post_log(&cl, 4, 5);
    HerdRequest again = make_put(7, "v4");
    recover(&cl, 4, 5, again, true);
    expect_stat(&cl, "full retransmission rewrites a matched PUT", 7, 2, kMicaOpPut, "v4");

    send_line(cl.conn.fd, "QUIT\n");
    if (cl.failed != 0) {
        std::printf("%d failed\n", cl.failed);
        std::exit(1);
    }
    std::printf("herd rpc rdma: all checks passed\n");
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

Conn add_qp(const Conn &base) {
    Conn c = base;
    c.cq = ibv_create_cq(base.ctx, 64, nullptr, nullptr, 0);
    ibv_qp_init_attr init{};
    init.send_cq = c.cq;
    init.recv_cq = c.cq;
    init.cap.max_send_wr = 32;
    init.cap.max_recv_wr = 1;
    init.cap.max_send_sge = 1;
    init.cap.max_recv_sge = 1;
    init.qp_type = IBV_QPT_RC;
    c.qp = ibv_create_qp(base.pd, &init);
    if (c.cq == nullptr || c.qp == nullptr) {
        die("add_qp");
    }
    modify_init(c.qp);
    return c;
}

void post_async(Conn *c, ibv_wr_opcode op, void *local, uint32_t len, uint64_t remote, uint64_t wr_id) {
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(local);
    sge.length = len;
    sge.lkey = c->mr->lkey;
    ibv_send_wr wr{};
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = op;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.rdma.remote_addr = remote;
    wr.wr.rdma.rkey = c->rkey;
    ibv_send_wr *bad = nullptr;
    if (ibv_post_send(c->qp, &wr, &bad) != 0) {
        die("ibv_post_send");
    }
}

int poll_available(Conn *c) {
    int got = 0;
    for (;;) {
        ibv_wc wc[8]{};
        int n = ibv_poll_cq(c->cq, 8, wc);
        if (n < 0) {
            die("ibv_poll_cq");
        }
        if (n == 0) {
            return got;
        }
        for (int i = 0; i < n; i++) {
            if (wc[i].status != IBV_WC_SUCCESS) {
                std::fprintf(stderr, "CQE status %s\n", ibv_wc_status_str(wc[i].status));
                std::exit(1);
            }
        }
        got += n;
    }
}

void wait_cqes(Conn *c, int want) {
    int got = 0;
    uint64_t start = now_ns();
    while (got < want) {
        got += poll_available(c);
        if (now_ns() - start > 2000000000ull) {
            std::fprintf(stderr, "CQ timeout want=%d got=%d\n", want, got);
            std::exit(1);
        }
    }
}

uint64_t rdma_read_u64(Conn *c, uint64_t *local, uint64_t remote_off) {
    *local = 0;
    post_async(c, IBV_WR_RDMA_READ, local, sizeof(uint64_t), c->remote_addr + remote_off, 9);
    wait_cqes(c, 1);
    uint64_t v = 0;
    std::memcpy(&v, local, sizeof(v));
    return v;
}

HerdRequest make_put_u64(uint32_t key, uint64_t value) {
    HerdRequest req{};
    req.key.bkt = key;
    req.opcode = kHerdOpPut;
    req.val_len = sizeof(value);
    std::memcpy(req.value, &value, sizeof(value));
    return req;
}

void exchange(int fd, Conn *c, uint64_t addr) {
    uint32_t psn = static_cast<uint32_t>(std::rand()) & 0xffffffu;
    c->fd = fd;
    handshake(c, local_exch(*c, psn, addr));
}

void post_one_recv(ibv_qp *qp) {
    ibv_recv_wr wr{};
    ibv_recv_wr *bad = nullptr;
    if (ibv_post_recv(qp, &wr, &bad) != 0) {
        die("ibv_post_recv");
    }
}

void imm_poll(ibv_qp *qp, ibv_cq *cq, Remote *remote, std::atomic<bool> *run) {
    while (run->load(std::memory_order_relaxed)) {
        ibv_wc wc{};
        int n = ibv_poll_cq(cq, 1, &wc);
        if (n == 0) {
            continue;
        }
        if (n < 0) {
            die("ibv_poll_cq");
        }
        if (wc.status != IBV_WC_SUCCESS) {
            std::fprintf(stderr, "recv CQE %s\n", ibv_wc_status_str(wc.status));
            std::exit(1);
        }
        if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM) {
            uint64_t stamp = ntohl(wc.imm_data);
            if (stamp >= 0x80000000ull) {
                __atomic_store_n(&remote->log[1], stamp, __ATOMIC_RELEASE);
            } else {
                __atomic_store_n(&remote->log[0], stamp, __ATOMIC_RELEASE);
            }
            post_one_recv(qp);
        }
    }
}

void post_write_imm(Conn *c, void *local, uint32_t len, uint64_t remote, uint32_t stamp, uint64_t wr_id) {
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(local);
    sge.length = len;
    sge.lkey = c->mr->lkey;
    ibv_send_wr wr{};
    wr.wr_id = wr_id;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.imm_data = htonl(stamp);
    wr.wr.rdma.remote_addr = remote;
    wr.wr.rdma.rkey = c->rkey;
    ibv_send_wr *bad = nullptr;
    if (ibv_post_send(c->qp, &wr, &bad) != 0) {
        die("ibv_post_send imm");
    }
}

void race_poll(Remote *remote, std::atomic<bool> *run) {
    while (run->load(std::memory_order_relaxed)) {
        uint8_t op = *reinterpret_cast<volatile uint8_t *>(&remote->slot.opcode);
        if (op < kHerdOpGet) {
            continue;
        }
        remote->slot.opcode = static_cast<uint8_t>(op - kHerdMicaOffset);
        if (remote->slot.opcode == kMicaOpPut) {
            uint64_t seen = 0;
            std::memcpy(&seen, remote->slot.value, sizeof(seen));
            __atomic_store_n(&remote->seen_val, seen, __ATOMIC_RELEASE);
            __atomic_fetch_add(&remote->exec, 1, __ATOMIC_RELEASE);
        }
    }
}

void serve_race(const char *ip, int port, const char *dev) {
    Conn data = setup_qp(dev, sizeof(Remote));
    Conn probe = add_qp(data);
    int listen_fd = ::socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, ip, &addr.sin_addr);
    if (bind(listen_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        die("bind");
    }
    if (listen(listen_fd, 1) != 0) {
        die("listen");
    }
    std::printf("herd race worker listening %s:%d dev=%s\n", ip, port, dev);
    std::fflush(stdout);
    int fd = accept(listen_fd, nullptr, nullptr);
    if (fd < 0) {
        die("accept");
    }
    exchange(fd, &data, reinterpret_cast<uint64_t>(data.buf));
    exchange(fd, &probe, reinterpret_cast<uint64_t>(data.buf));
    send_line(fd, "UP\n");
    std::printf("data qpn=%u probe qpn=%u\n", data.qp->qp_num, probe.qp->qp_num);
    std::fflush(stdout);
    std::atomic<bool> run{true};
    for (int i = 0; i < 128; i++) {
        post_one_recv(data.qp);
    }
    auto *remote = static_cast<Remote *>(data.buf);
    std::thread imm(imm_poll, data.qp, data.cq, remote, &run);
    std::thread poller(race_poll, remote, &run);
    char line[64];
    recv_line(fd, line, sizeof(line));
    run.store(false);
    imm.join();
    poller.join();
}

struct Tally {
    int trials = 0;
    int complete = 0;
    int retransmit = 0;
    int duplicate = 0;
    int once = 0;
    int missed = 0;
    int exec3 = 0;
};

uint64_t percentile(std::vector<uint64_t> *lat, int pct) {
    std::sort(lat->begin(), lat->end());
    size_t i = lat->size() * static_cast<size_t>(pct) / 100;
    if (i >= lat->size()) {
        i = lat->size() - 1;
    }
    return (*lat)[i];
}

void print_tally(const char *name, const Tally &t, uint64_t window_ns) {
    std::printf("%s trials=%d window_ns=%lu complete=%d retransmit=%d duplicate=%d once=%d missed=%d exec3=%d\n",
                name, t.trials, static_cast<unsigned long>(window_ns), t.complete, t.retransmit, t.duplicate, t.once,
                t.missed, t.exec3);
    if (t.trials > 0) {
        std::printf("%s P(duplicate)=%.1f%% (%d/%d)\n", name, 100.0 * t.duplicate / t.trials, t.duplicate, t.trials);
    }
    if (t.retransmit > 0) {
        std::printf("%s P(duplicate|retransmit)=%.1f%% (%d/%d)\n", name, 100.0 * t.duplicate / t.retransmit,
                    t.duplicate, t.retransmit);
    }
    std::fflush(stdout);
}

void note_delta(Tally *t, uint64_t delta, bool retransmitted) {
    t->trials++;
    if (retransmitted) {
        t->retransmit++;
    } else {
        t->complete++;
    }
    if (delta == 0) {
        t->missed++;
    } else if (delta == 1) {
        t->once++;
    } else {
        t->duplicate++;
        if (delta > 2) {
            t->exec3++;
        }
    }
}

uint64_t settle_exec(Conn *probe, uint64_t *local) {
    spin_ns(50000);
    uint64_t a = rdma_read_u64(probe, local, offsetof(Remote, exec));
    spin_ns(50000);
    uint64_t b = rdma_read_u64(probe, local, offsetof(Remote, exec));
    return b > a ? b : a;
}

void run_race(const char *ip, int port, const char *dev, int trials) {
    Conn data = setup_qp(dev, 4096);
    Conn probe = add_qp(data);
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
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
    exchange(fd, &data, 0);
    exchange(fd, &probe, 0);
    char up[16];
    recv_line(fd, up, sizeof(up));
    if (std::strcmp(up, "UP") != 0) {
        std::fprintf(stderr, "peer not up: %s\n", up);
        std::exit(1);
    }
    auto *req = static_cast<HerdRequest *>(data.buf);
    auto *stamp = reinterpret_cast<uint64_t *>(static_cast<unsigned char *>(data.buf) + 256);
    auto *scratch = reinterpret_cast<uint64_t *>(static_cast<unsigned char *>(data.buf) + 512);
    std::printf("race %s -> %s trials=%d\n", dev, ip, trials);
    std::fflush(stdout);

    HerdRequest sample = make_put_u64(7, 1);
    for (int i = 0; i < 20; i++) {
        std::memcpy(req, &sample, sizeof(sample));
        post_async(&data, IBV_WR_RDMA_WRITE, req, sizeof(sample), data.remote_addr + offsetof(Remote, slot), 1);
        wait_cqes(&data, 1);
    }
    std::vector<uint64_t> lat;
    lat.reserve(200);
    for (int i = 0; i < 200; i++) {
        std::memcpy(req, &sample, sizeof(sample));
        uint64_t t0 = now_ns();
        post_async(&data, IBV_WR_RDMA_WRITE, req, sizeof(sample), data.remote_addr + offsetof(Remote, slot), 1);
        wait_cqes(&data, 1);
        lat.push_back(now_ns() - t0);
    }
    uint64_t p50 = percentile(&lat, 50);
    uint64_t p99 = percentile(&lat, 99);
    uint64_t window = p99 > 1000 ? p99 : 1000;
    std::printf("write_latency_ns p50=%lu p99=%lu min=%lu max=%lu sample_window_ns=%lu\n",
                static_cast<unsigned long>(p50), static_cast<unsigned long>(p99),
                static_cast<unsigned long>(lat.front()), static_cast<unsigned long>(lat.back()),
                static_cast<unsigned long>(window));

    uint64_t last = settle_exec(&probe, scratch);
    std::mt19937 rng(1);

    Tally after_cqe;
    for (int i = 0; i < trials; i++) {
        HerdRequest put = make_put_u64(7, 2);
        std::memcpy(req, &put, sizeof(put));
        post_async(&data, IBV_WR_RDMA_WRITE, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), 1);
        wait_cqes(&data, 1);
        std::memcpy(req, &put, sizeof(put));
        post_async(&data, IBV_WR_RDMA_WRITE, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), 3);
        wait_cqes(&data, 1);
        uint64_t now = settle_exec(&probe, scratch);
        note_delta(&after_cqe, now - last, true);
        last = now;
    }
    print_tally("blind_after_cqe", after_cqe, 0);

    Tally blind;
    std::uniform_int_distribution<uint64_t> delay(0, window);
    for (int i = 0; i < trials; i++) {
        HerdRequest put = make_put_u64(7, 3);
        std::memcpy(req, &put, sizeof(put));
        post_async(&data, IBV_WR_RDMA_WRITE, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), 1);
        spin_ns(delay(rng));
        int arrived = poll_available(&data);
        bool rexmit = arrived == 0;
        if (rexmit) {
            post_async(&data, IBV_WR_RDMA_WRITE, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), 3);
        }
        wait_cqes(&data, (rexmit ? 2 : 1) - arrived);
        uint64_t now = settle_exec(&probe, scratch);
        note_delta(&blind, now - last, rexmit);
        last = now;
    }
    print_tally("blind_uniform", blind, window);

    std::vector<uint64_t> lat2;
    for (int i = 0; i < 200; i++) {
        HerdRequest put = make_put_u64(7, 4);
        std::memcpy(req, &put, sizeof(put));
        *stamp = 4;
        uint64_t t0 = now_ns();
        post_async(&data, IBV_WR_RDMA_WRITE, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), 1);
        post_async(&data, IBV_WR_RDMA_WRITE, stamp, sizeof(uint64_t), data.remote_addr + offsetof(Remote, log), 2);
        wait_cqes(&data, 2);
        lat2.push_back(now_ns() - t0);
    }
    uint64_t p50_log = percentile(&lat2, 50);
    uint64_t window2 = percentile(&lat2, 99);
    if (window2 < 1000) {
        window2 = 1000;
    }
    last = settle_exec(&probe, scratch);
    std::printf("logged_write_latency_ns p50=%lu p99=%lu\n", static_cast<unsigned long>(p50_log),
                static_cast<unsigned long>(window2));

    Tally varuna;
    std::uniform_int_distribution<uint64_t> delay2(0, window2);
    int skipped = 0;
    for (int i = 0; i < trials; i++) {
        uint64_t local_stamp = static_cast<uint64_t>(i + 1);
        HerdRequest put = make_put_u64(7, 5);
        std::memcpy(req, &put, sizeof(put));
        *stamp = local_stamp;
        post_async(&data, IBV_WR_RDMA_WRITE, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), 1);
        post_async(&data, IBV_WR_RDMA_WRITE, stamp, sizeof(uint64_t), data.remote_addr + offsetof(Remote, log), 2);
        spin_ns(delay2(rng));
        int arrived = poll_available(&data);
        bool inflight = arrived < 2;
        bool rexmit = false;
        if (inflight) {
            uint64_t remote_stamp = rdma_read_u64(&probe, scratch, offsetof(Remote, log));
            if (remote_stamp == local_stamp) {
                skipped++;
            } else {
                rexmit = true;
                post_async(&data, IBV_WR_RDMA_WRITE, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), 3);
                post_async(&data, IBV_WR_RDMA_WRITE, stamp, sizeof(uint64_t),
                           data.remote_addr + offsetof(Remote, log), 4);
            }
        }
        int need = 2 + (rexmit ? 2 : 0);
        wait_cqes(&data, need - arrived);
        uint64_t now = settle_exec(&probe, scratch);
        note_delta(&varuna, now - last, rexmit);
        last = now;
    }
    print_tally("varuna_uniform", varuna, window2);
    std::printf("varuna_uniform skipped=%d\n", skipped);

    *stamp = 0;
    post_async(&data, IBV_WR_RDMA_WRITE, stamp, sizeof(uint64_t), data.remote_addr + offsetof(Remote, log), 1);
    wait_cqes(&data, 1);
    std::vector<uint64_t> lat_imm;
    for (int i = 0; i < 20; i++) {
        HerdRequest put = make_put_u64(7, 6);
        std::memcpy(req, &put, sizeof(put));
        post_write_imm(&data, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), 1, 1);
        wait_cqes(&data, 1);
    }
    for (int i = 0; i < 200; i++) {
        HerdRequest put = make_put_u64(7, 6);
        std::memcpy(req, &put, sizeof(put));
        uint64_t t0 = now_ns();
        post_write_imm(&data, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), 1, 1);
        wait_cqes(&data, 1);
        lat_imm.push_back(now_ns() - t0);
    }
    uint64_t p50_imm = percentile(&lat_imm, 50);
    uint64_t window_imm = percentile(&lat_imm, 99);
    if (window_imm < 1000) {
        window_imm = 1000;
    }
    last = settle_exec(&probe, scratch);
    std::printf("write_with_imm_latency_ns p50=%lu p99=%lu\n", static_cast<unsigned long>(p50_imm),
                static_cast<unsigned long>(window_imm));

    Tally imm_tally;
    int imm_skipped = 0;
    std::uniform_int_distribution<uint64_t> delay_imm(0, window_imm);
    for (int i = 0; i < trials; i++) {
        uint32_t local_stamp = static_cast<uint32_t>(100000 + i + 1);
        HerdRequest put = make_put_u64(7, 6);
        std::memcpy(req, &put, sizeof(put));
        post_write_imm(&data, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), local_stamp, 1);
        spin_ns(delay_imm(rng));
        int arrived = poll_available(&data);
        bool rexmit = false;
        if (arrived == 0) {
            uint64_t remote_stamp = rdma_read_u64(&probe, scratch, offsetof(Remote, log));
            if (remote_stamp == local_stamp) {
                imm_skipped++;
            } else {
                rexmit = true;
                post_write_imm(&data, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), local_stamp, 3);
            }
        }
        wait_cqes(&data, (rexmit ? 2 : 1) - arrived);
        uint64_t now = settle_exec(&probe, scratch);
        note_delta(&imm_tally, now - last, rexmit);
        last = now;
    }
    print_tally("varuna_imm", imm_tally, window_imm);
    std::printf("varuna_imm skipped=%d\n", imm_skipped);

    Tally imm_wait;
    int wait_skipped = 0;
    for (int i = 0; i < trials; i++) {
        uint32_t local_stamp = static_cast<uint32_t>(200000 + i + 1);
        HerdRequest put = make_put_u64(7, 6);
        std::memcpy(req, &put, sizeof(put));
        uint64_t posted = now_ns();
        post_write_imm(&data, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), local_stamp, 1);
        spin_ns(delay_imm(rng));
        int arrived = poll_available(&data);
        bool rexmit = false;
        if (arrived == 0) {
            uint64_t now = now_ns();
            if (posted + window_imm > now) {
                spin_ns(posted + window_imm - now);
            }
            uint64_t remote_stamp = rdma_read_u64(&probe, scratch, offsetof(Remote, log));
            if (remote_stamp == local_stamp) {
                wait_skipped++;
            } else {
                rexmit = true;
                post_write_imm(&data, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), local_stamp, 3);
            }
        }
        wait_cqes(&data, (rexmit ? 2 : 1) - arrived);
        uint64_t exec_now = settle_exec(&probe, scratch);
        note_delta(&imm_wait, exec_now - last, rexmit);
        last = exec_now;
    }
    print_tally("varuna_imm_wait", imm_wait, window_imm);
    std::printf("varuna_imm_wait skipped=%d\n", wait_skipped);

    Tally imm_flush;
    int flush_skipped = 0;
    int flush_timeout = 0;
    for (int i = 0; i < trials; i++) {
        uint32_t local_stamp = static_cast<uint32_t>(300000 + i + 1);
        uint32_t flush_stamp = 0x80000000u + static_cast<uint32_t>(i + 1);
        HerdRequest put = make_put_u64(7, 6);
        std::memcpy(req, &put, sizeof(put));
        post_write_imm(&data, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), local_stamp, 1);
        spin_ns(delay_imm(rng));
        int arrived = poll_available(&data);
        bool rexmit = false;
        int extra = 0;
        if (arrived == 0) {
            *stamp = 0;
            post_write_imm(&data, stamp, sizeof(uint64_t), data.remote_addr + offsetof(Remote, log) + 16, flush_stamp,
                           7);
            extra = 1;
            uint64_t deadline = now_ns() + 100000;
            uint64_t got = 0;
            while (now_ns() < deadline) {
                got = rdma_read_u64(&probe, scratch, offsetof(Remote, log) + 8);
                if (got == flush_stamp) {
                    break;
                }
            }
            if (got != flush_stamp) {
                flush_timeout++;
            }
            uint64_t remote_stamp = rdma_read_u64(&probe, scratch, offsetof(Remote, log));
            if (remote_stamp == local_stamp) {
                flush_skipped++;
            } else {
                rexmit = true;
                post_write_imm(&data, req, sizeof(put), data.remote_addr + offsetof(Remote, slot), local_stamp, 3);
            }
        }
        wait_cqes(&data, 1 + extra + (rexmit ? 1 : 0) - arrived);
        uint64_t exec_now = settle_exec(&probe, scratch);
        note_delta(&imm_flush, exec_now - last, rexmit);
        last = exec_now;
    }
    print_tally("varuna_imm_flush", imm_flush, window_imm);
    std::printf("varuna_imm_flush skipped=%d flush_timeout=%d\n", flush_skipped, flush_timeout);

    int rollback = 0;
    int rollback_trials = trials < 1000 ? trials : 1000;
    for (int i = 0; i < rollback_trials; i++) {
        HerdRequest newer = make_put_u64(7, 20);
        HerdRequest older = make_put_u64(7, 10);
        std::memcpy(req, &newer, sizeof(newer));
        post_async(&data, IBV_WR_RDMA_WRITE, req, sizeof(newer), data.remote_addr + offsetof(Remote, slot), 1);
        wait_cqes(&data, 1);
        spin_ns(20000);
        std::memcpy(req, &older, sizeof(older));
        post_async(&data, IBV_WR_RDMA_WRITE, req, sizeof(older), data.remote_addr + offsetof(Remote, slot), 3);
        wait_cqes(&data, 1);
        spin_ns(20000);
        uint64_t seen = rdma_read_u64(&probe, scratch, offsetof(Remote, seen_val));
        if (seen == 10) {
            rollback++;
        }
        last = rdma_read_u64(&probe, scratch, offsetof(Remote, exec));
    }
    std::printf("blind_reuse_rollback=%d/%d (%.1f%%)\n", rollback, rollback_trials,
                100.0 * rollback / rollback_trials);

    send_line(fd, "QUIT\n");
}

}  // namespace

int main(int argc, char **argv) {
    if (argc < 5) {
        std::fprintf(stderr, "usage: %s server|client|race-server|race <ip> <port> <ib_dev> [trials]\n", argv[0]);
        return 1;
    }
    std::srand(static_cast<unsigned>(std::time(nullptr)));
    int port = std::atoi(argv[3]);
    if (std::strcmp(argv[1], "server") == 0) {
        serve(argv[2], port, argv[4]);
        return 0;
    }
    if (std::strcmp(argv[1], "client") == 0) {
        run_client(argv[2], port, argv[4]);
        return 0;
    }
    if (std::strcmp(argv[1], "race-server") == 0) {
        serve_race(argv[2], port, argv[4]);
        return 0;
    }
    if (std::strcmp(argv[1], "race") == 0) {
        int trials = argc >= 6 ? std::atoi(argv[5]) : 2000;
        if (trials < 1) {
            trials = 1;
        }
        run_race(argv[2], port, argv[4], trials);
        return 0;
    }
    std::fprintf(stderr, "usage: %s server|client|race-server|race <ip> <port> <ib_dev> [trials]\n", argv[0]);
    return 1;
}
