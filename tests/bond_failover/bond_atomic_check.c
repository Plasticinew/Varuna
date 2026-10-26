/* Bond slave-failure check for one RC QP.
 *
 * gcc -O2 -std=c11 -Wall -Wextra -o bond_atomic_check bond_atomic_check.c -lrdmacm -libverbs -pthread
 *
 * FAA: remote counter must increase once per execution.
 * WRITE_WITH_IMM: responder completion count is the execution count.
 * CAS: final value advances once per committed CAS. A second CAS with the
 * same compare/swap does not advance it again.
 *
 * blind resend of failed completions duplicates (executed - wc_success) ops
 * when that difference is greater than zero.
 */
#define _GNU_SOURCE

#include <arpa/inet.h>
#include <byteswap.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <infiniband/verbs.h>
#include <inttypes.h>
#include <net/if.h>
#include <netinet/in.h>
#include <pthread.h>
#include <rdma/rdma_cma.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

enum {
    MODE_FAA = 1,
    MODE_CAS = 2,
    MODE_WRITE = 3
};

enum { kMaxSlaves = 4, kHwMax = 8, kRecvSlots = 256 };

static const uint64_t kReportMagic = 0xB0ADF001ULL;

struct report {
    uint64_t magic;
    uint64_t posted;
    uint64_t wc_ok;
    uint64_t wc_retry;
    uint64_t wc_rnr;
    uint64_t wc_flush;
    uint64_t wc_other;
    uint64_t cas_bad;
    uint64_t cas_be;
    uint64_t inflight_left;
    uint32_t mode;
    uint32_t pad;
};

_Static_assert(sizeof(struct report) == 88, "report size");

struct hello {
    uint32_t mode;
    uint32_t depth;
};

struct xfer {
    uint64_t addr;
    uint32_t rkey;
    uint32_t pad;
};

struct slot {
    uint64_t result;
    struct ibv_sge sge;
    struct ibv_send_wr wr;
};

static volatile sig_atomic_t g_stop;
static char g_fault_if[IFNAMSIZ];
static volatile sig_atomic_t g_fault_down;
static pthread_mutex_t g_print_mu = PTHREAD_MUTEX_INITIALIZER;

static char g_slaves[kMaxSlaves][IFNAMSIZ];
static int g_nslaves;
static uint64_t g_tx0[kMaxSlaves];
static uint64_t g_rx0[kMaxSlaves];
static uint64_t g_tx_fault[kMaxSlaves];
static volatile sig_atomic_t g_fault_marked;

static char g_devname[64];
static const char *k_hw_names[kHwMax] = {
    "duplicate_request",
    "packet_seq_err",
    "out_of_sequence",
    "local_ack_timeout_err",
    "implied_nak_seq_err",
    "rnr_nak_retry_err",
    "req_cqe_error",
    "resp_cqe_error",
};
static uint64_t g_hw0[kHwMax];
static int g_hw_ok[kHwMax];

static void outf(const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    pthread_mutex_lock(&g_print_mu);
    vprintf(fmt, ap);
    fflush(stdout);
    pthread_mutex_unlock(&g_print_mu);
    va_end(ap);
}

static uint64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static const char *mode_name(uint32_t mode)
{
    if (mode == MODE_FAA)
        return "faa";
    if (mode == MODE_CAS)
        return "cas";
    if (mode == MODE_WRITE)
        return "write";
    return "unknown";
}

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

static int valid_ifname(const char *s)
{
    size_t n;
    size_t i;
    if (s == NULL || s[0] == '\0')
        return 0;
    n = strlen(s);
    if (n >= IFNAMSIZ)
        return 0;
    if (strncmp(s, "bond", 4) == 0)
        return 0;
    for (i = 0; i < n; i++) {
        unsigned char c = (unsigned char)s[i];
        if (!isalnum(c) && c != '_' && c != '-' && c != '.')
            return 0;
    }
    return 1;
}

static void restore_fault_if(void)
{
    char cmd[160];
    if (!g_fault_down || g_fault_if[0] == '\0')
        return;
    g_fault_down = 0;
    snprintf(cmd, sizeof(cmd), "ip link set dev %s up", g_fault_if);
    outf("RESTORE %s\n", cmd);
    if (system(cmd) != 0)
        outf("RESTORE failed; run: sudo ip link set dev %s up\n", g_fault_if);
}

static uint64_t sysfs_u64(const char *path)
{
    FILE *f = fopen(path, "r");
    unsigned long long v = 0;
    if (f == NULL)
        return 0;
    if (fscanf(f, "%llu", &v) != 1)
        v = 0;
    fclose(f);
    return (uint64_t)v;
}

static int load_slaves(const char *bond)
{
    char path[256];
    char line[512];
    char *save = NULL;
    char *tok;
    FILE *f;
    snprintf(path, sizeof(path), "/sys/class/net/%s/bonding/slaves", bond);
    f = fopen(path, "r");
    if (f == NULL)
        return 0;
    if (fgets(line, sizeof(line), f) == NULL) {
        fclose(f);
        return 0;
    }
    fclose(f);
    g_nslaves = 0;
    for (tok = strtok_r(line, " \n", &save); tok != NULL && g_nslaves < kMaxSlaves;
         tok = strtok_r(NULL, " \n", &save)) {
        snprintf(g_slaves[g_nslaves], IFNAMSIZ, "%s", tok);
        g_nslaves++;
    }
    return g_nslaves;
}

static void snap_slaves(uint64_t *tx, uint64_t *rx)
{
    int i;
    for (i = 0; i < g_nslaves; i++) {
        char path[256];
        if (tx != NULL) {
            snprintf(path, sizeof(path), "/sys/class/net/%s/statistics/tx_packets", g_slaves[i]);
            tx[i] = sysfs_u64(path);
        }
        if (rx != NULL) {
            snprintf(path, sizeof(path), "/sys/class/net/%s/statistics/rx_packets", g_slaves[i]);
            rx[i] = sysfs_u64(path);
        }
    }
}

static void snap_hw(uint64_t *out, int *ok)
{
    int i;
    for (i = 0; i < kHwMax; i++) {
        char path[320];
        ok[i] = 0;
        out[i] = 0;
        if (g_devname[0] == '\0')
            continue;
        snprintf(path, sizeof(path), "/sys/class/infiniband/%s/ports/1/hw_counters/%s", g_devname,
                 k_hw_names[i]);
        if (access(path, R_OK) != 0)
            continue;
        out[i] = sysfs_u64(path);
        ok[i] = 1;
    }
}

static void print_hw_delta(void)
{
    uint64_t nowv[kHwMax];
    int ok[kHwMax];
    int i;
    snap_hw(nowv, ok);
    for (i = 0; i < kHwMax; i++) {
        if (!g_hw_ok[i] || !ok[i])
            continue;
        outf("HW %s delta=%" PRIu64 "\n", k_hw_names[i], nowv[i] - g_hw0[i]);
    }
}

static void print_slave_delta(void)
{
    uint64_t tx1[kMaxSlaves];
    uint64_t rx1[kMaxSlaves];
    int hot = -1;
    uint64_t hot_tx = 0;
    int i;
    snap_slaves(tx1, rx1);
    for (i = 0; i < g_nslaves; i++) {
        uint64_t before;
        uint64_t after;
        if (g_fault_marked) {
            before = g_tx_fault[i] - g_tx0[i];
            after = tx1[i] >= g_tx_fault[i] ? tx1[i] - g_tx_fault[i] : 0;
        } else {
            before = tx1[i] - g_tx0[i];
            after = 0;
        }
        outf("SLAVE %s tx_before_fault=%" PRIu64 " tx_after_fault=%" PRIu64 " rx=%" PRIu64 "\n",
             g_slaves[i], before, after, rx1[i] - g_rx0[i]);
        if (before > hot_tx) {
            hot_tx = before;
            hot = i;
        }
    }
    if (g_fault_if[0] != '\0' && hot >= 0 && strcmp(g_slaves[hot], g_fault_if) != 0 && hot_tx > 1000) {
        outf("RESULT send queue before the fault was on %s; rerun with --fault-if %s\n",
             g_slaves[hot], g_slaves[hot]);
    }
}

static uint64_t as_host_counter(uint64_t raw, uint64_t hint, int *used_be)
{
    uint64_t be = bswap_64(raw);
    uint64_t dr = raw > hint ? raw - hint : hint - raw;
    uint64_t db = be > hint ? be - hint : hint - be;
    if (used_be != NULL)
        *used_be = db < dr;
    return db < dr ? be : raw;
}

static void print_verdict(const char *who, uint32_t mode, uint64_t executed, uint64_t posted,
                          uint64_t wc_ok)
{
    outf("RESULT %s mode=%s executed=%" PRIu64 " posted=%" PRIu64 " wc_success=%" PRIu64 "\n", who,
         mode_name(mode), executed, posted, wc_ok);
    if (executed == posted && wc_ok == posted)
        outf("RESULT %s pass: each posted op completed once\n", who);
    if (executed > posted)
        outf("RESULT %s nic_duplicate=%" PRIu64 "\n", who, executed - posted);
    if (executed > wc_ok) {
        outf("RESULT %s silent_commit=%" PRIu64 "\n", who, executed - wc_ok);
        if (wc_ok < posted) {
            outf("RESULT %s resend of the %" PRIu64
                 " failed WRs would execute %" PRIu64 " of them a second time\n",
                 who, posted - wc_ok, executed - wc_ok);
        }
    }
    if (executed < wc_ok)
        outf("RESULT %s silent_loss=%" PRIu64 "\n", who, wc_ok - executed);
    if (executed == wc_ok && executed < posted)
        outf("RESULT %s honest_loss=%" PRIu64 "\n", who, posted - executed);
    if (mode == MODE_CAS && executed > wc_ok) {
        outf("RESULT %s cas already applied; a later retry of the same compare/swap leaves memory unchanged\n",
             who);
    }
}

struct fault_arg {
    int delay_ms;
};

static void *fault_thread(void *arg)
{
    struct fault_arg *a = arg;
    uint64_t deadline = now_ms() + (uint64_t)a->delay_ms;
    char cmd[160];
    int rc;
    while (!g_stop && now_ms() < deadline)
        usleep(20000);
    if (g_stop)
        return NULL;
    snap_slaves(g_tx_fault, NULL);
    g_fault_marked = 1;
    snprintf(cmd, sizeof(cmd), "ip link set dev %s down", g_fault_if);
    outf("FAULT %s\n", cmd);
    rc = system(cmd);
    if (rc == 0)
        g_fault_down = 1;
    else
        outf("FAULT failed rc=%d; run the client with sudo, or down the slave yourself\n", rc);
    return NULL;
}

static int expect_cm(struct rdma_event_channel *ch, enum rdma_cm_event_type want,
                     struct rdma_cm_event **out)
{
    struct rdma_cm_event *ev = NULL;
    for (;;) {
        if (rdma_get_cm_event(ch, &ev) != 0) {
            if (errno == EINTR && g_stop)
                return -1;
            perror("rdma_get_cm_event");
            return -1;
        }
        if (ev->event == want) {
            *out = ev;
            return 0;
        }
        {
            enum rdma_cm_event_type kind = ev->event;
            int status = ev->status;
            fprintf(stderr, "cm event %s status %d\n", rdma_event_str(kind), status);
            rdma_ack_cm_event(ev);
            if (kind == RDMA_CM_EVENT_REJECTED || kind == RDMA_CM_EVENT_UNREACHABLE ||
                kind == RDMA_CM_EVENT_CONNECT_ERROR || kind == RDMA_CM_EVENT_ADDR_ERROR ||
                kind == RDMA_CM_EVENT_ROUTE_ERROR || kind == RDMA_CM_EVENT_DEVICE_REMOVAL ||
                kind == RDMA_CM_EVENT_DISCONNECTED)
                return -1;
        }
    }
}

static int tune_timeout(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    int rc;
    memset(&attr, 0, sizeof(attr));
    attr.timeout = 14;
    attr.retry_cnt = 7;
    attr.rnr_retry = 7;
    rc = ibv_modify_qp(qp, &attr, IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY);
    if (rc != 0)
        fprintf(stderr, "modify qp timeout failed rc=%d errno=%d\n", rc, errno);
    else
        outf("qp timeout=14 (~67ms base) retry_cnt=7\n");
    return rc;
}

static int qp_state(struct ibv_qp *qp)
{
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;
    memset(&attr, 0, sizeof(attr));
    memset(&init_attr, 0, sizeof(init_attr));
    if (ibv_query_qp(qp, &attr, IBV_QP_STATE, &init_attr) != 0)
        return -1;
    return (int)attr.qp_state;
}

static int post_op(struct ibv_qp *qp, struct slot *s, uint32_t lkey, uint64_t remote, uint32_t rkey,
                   uint64_t seq, uint32_t mode)
{
    struct ibv_send_wr *bad = NULL;
    memset(&s->wr, 0, sizeof(s->wr));
    s->sge.addr = (uintptr_t)&s->result;
    s->sge.length = 8;
    s->sge.lkey = lkey;
    s->wr.wr_id = seq;
    s->wr.sg_list = &s->sge;
    s->wr.num_sge = 1;
    s->wr.send_flags = IBV_SEND_SIGNALED;
    if (mode == MODE_FAA) {
        s->wr.opcode = IBV_WR_ATOMIC_FETCH_AND_ADD;
        s->wr.wr.atomic.remote_addr = remote;
        s->wr.wr.atomic.rkey = rkey;
        s->wr.wr.atomic.compare_add = 1;
    } else if (mode == MODE_CAS) {
        s->wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
        s->wr.wr.atomic.remote_addr = remote;
        s->wr.wr.atomic.rkey = rkey;
        s->wr.wr.atomic.compare_add = seq;
        s->wr.wr.atomic.swap = seq + 1;
    } else {
        s->result = seq + 1;
        s->wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
        s->wr.imm_data = htonl((uint32_t)(seq + 1));
        s->wr.wr.rdma.remote_addr = remote;
        s->wr.wr.rdma.rkey = rkey;
    }
    return ibv_post_send(qp, &s->wr, &bad);
}

static int fill_conn_param(struct rdma_conn_param *cp, const void *data, uint8_t len, int depth)
{
    int atom = depth;
    memset(cp, 0, sizeof(*cp));
    if (atom < 1)
        atom = 1;
    if (atom > 16)
        atom = 16;
    cp->private_data = data;
    cp->private_data_len = len;
    cp->responder_resources = (uint8_t)atom;
    cp->initiator_depth = (uint8_t)atom;
    cp->retry_count = 7;
    cp->rnr_retry_count = 7;
    return atom;
}

static int create_qp(struct rdma_cm_id *id, struct ibv_pd **pd, struct ibv_cq **cq, int send_wr,
                     int recv_wr)
{
    struct ibv_qp_init_attr attr;
    *pd = ibv_alloc_pd(id->verbs);
    if (*pd == NULL) {
        perror("ibv_alloc_pd");
        return -1;
    }
    *cq = ibv_create_cq(id->verbs, 4096, NULL, NULL, 0);
    if (*cq == NULL) {
        perror("ibv_create_cq");
        return -1;
    }
    memset(&attr, 0, sizeof(attr));
    attr.qp_type = IBV_QPT_RC;
    attr.send_cq = *cq;
    attr.recv_cq = *cq;
    attr.cap.max_send_wr = (uint32_t)send_wr;
    attr.cap.max_recv_wr = (uint32_t)recv_wr;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    if (rdma_create_qp(id, *pd, &attr) != 0) {
        perror("rdma_create_qp");
        return -1;
    }
    return 0;
}

struct server_ctx {
    volatile uint64_t *buf;
    volatile uint64_t imm;
    int report_port;
    volatile sig_atomic_t rep_ready;
    struct report rep;
};

static void *report_thread(void *arg)
{
    struct server_ctx *ctx = arg;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int yes = 1;
    struct sockaddr_in addr;
    if (fd < 0) {
        perror("socket");
        return NULL;
    }
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)ctx->report_port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(fd, 1) != 0) {
        perror("report bind");
        close(fd);
        return NULL;
    }
    outf("report listen tcp/%d\n", ctx->report_port);
    for (;;) {
        struct pollfd pfd;
        int cfd;
        struct report rep;
        uint8_t *p;
        size_t got;
        if (g_stop)
            break;
        pfd.fd = fd;
        pfd.events = POLLIN;
        if (poll(&pfd, 1, 200) <= 0)
            continue;
        cfd = accept(fd, NULL, NULL);
        if (cfd < 0)
            continue;
        p = (uint8_t *)&rep;
        got = 0;
        while (got < sizeof(rep)) {
            ssize_t n = read(cfd, p + got, sizeof(rep) - got);
            if (n <= 0)
                break;
            got += (size_t)n;
        }
        if (got != sizeof(rep) || rep.magic != kReportMagic) {
            outf("REPORT bad payload\n");
            close(cfd);
            continue;
        }
        ctx->rep = rep;
        ctx->rep_ready = 1;
        close(cfd);
        break;
    }
    close(fd);
    return NULL;
}

static void server_verdict(struct server_ctx *ctx)
{
    uint64_t raw = ctx->buf[0];
    uint64_t imm = ctx->buf[1];
    int used_be = 0;
    uint64_t executed;
    uint64_t hint;
    if (!ctx->rep_ready)
        return;
    hint = ctx->rep.wc_ok ? ctx->rep.wc_ok : ctx->rep.posted;
    if (ctx->rep.mode == MODE_WRITE)
        executed = imm;
    else
        executed = as_host_counter(raw, hint, &used_be);
    outf("SERVER mem_raw=%" PRIu64 " mem_bswap=%" PRIu64 " imm=%" PRIu64 " endian=%s\n", raw,
         bswap_64(raw), imm, used_be ? "bswap" : "raw");
    print_verdict("server", ctx->rep.mode, executed, ctx->rep.posted, ctx->rep.wc_ok);
    if (ctx->rep.inflight_left != 0) {
        outf("RESULT drain left inflight=%" PRIu64 "; increase --drain-seconds\n",
             ctx->rep.inflight_left);
    }
    if (ctx->rep.mode == MODE_CAS && (ctx->rep.cas_bad != 0 || ctx->rep.cas_be != 0)) {
        outf("RESULT cas_mismatch=%" PRIu64 " cas_match_bswap=%" PRIu64 "\n", ctx->rep.cas_bad,
             ctx->rep.cas_be);
    }
}

struct recv_slot {
    struct ibv_sge sge;
    struct ibv_recv_wr wr;
};

static int post_one_recv(struct ibv_qp *qp, struct recv_slot *s)
{
    struct ibv_recv_wr *bad = NULL;
    return ibv_post_recv(qp, &s->wr, &bad);
}

static int run_server(const char *bind_ip, int port, const char *bond)
{
    struct rdma_event_channel *ch = NULL;
    struct rdma_cm_id *listen_id = NULL;
    struct rdma_cm_id *id = NULL;
    struct rdma_cm_event *ev = NULL;
    struct sockaddr_in addr;
    struct ibv_pd *pd = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_mr *mr = NULL;
    struct ibv_mr *recv_mr = NULL;
    void *buf = NULL;
    void *recv_buf = NULL;
    struct recv_slot recvs[kRecvSlots];
    struct server_ctx ctx;
    pthread_t th;
    struct hello hello;
    struct xfer xfer;
    struct rdma_conn_param cp;
    int flags;
    uint32_t mode = MODE_FAA;
    uint64_t t0;
    uint64_t last_print;
    int i;

    memset(&ctx, 0, sizeof(ctx));
    memset(&hello, 0, sizeof(hello));
    ctx.report_port = port + 1;
    if (pthread_create(&th, NULL, report_thread, &ctx) != 0) {
        perror("pthread_create");
        return 1;
    }

    ch = rdma_create_event_channel();
    if (ch == NULL || rdma_create_id(ch, &listen_id, NULL, RDMA_PS_TCP) != 0) {
        perror("rdma listen id");
        return 1;
    }
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, bind_ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "bad --bind %s\n", bind_ip);
        return 1;
    }
    if (rdma_bind_addr(listen_id, (struct sockaddr *)&addr) != 0 || rdma_listen(listen_id, 1) != 0) {
        perror("rdma_bind/listen");
        return 1;
    }
    outf("READY rdma %s:%d bond=%s\n", bind_ip, port, bond);
    if (expect_cm(ch, RDMA_CM_EVENT_CONNECT_REQUEST, &ev) != 0)
        return 1;
    id = ev->id;
    if (ev->param.conn.private_data != NULL &&
        ev->param.conn.private_data_len >= sizeof(hello))
        memcpy(&hello, ev->param.conn.private_data, sizeof(hello));
    mode = hello.mode == 0 ? MODE_FAA : hello.mode;
    rdma_ack_cm_event(ev);
    ev = NULL;

    if (posix_memalign(&buf, 4096, 4096) != 0 || posix_memalign(&recv_buf, 4096, 4096) != 0) {
        perror("posix_memalign");
        return 1;
    }
    memset(buf, 0, 4096);
    memset(recv_buf, 0, 4096);
    ctx.buf = buf;
    if (create_qp(id, &pd, &cq, 16, kRecvSlots + 16) != 0)
        return 1;
    mr = ibv_reg_mr(pd, buf, 4096,
                    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
                        IBV_ACCESS_REMOTE_ATOMIC);
    recv_mr = ibv_reg_mr(pd, recv_buf, 4096, IBV_ACCESS_LOCAL_WRITE);
    if (mr == NULL || recv_mr == NULL) {
        perror("ibv_reg_mr");
        outf("If this is ENOMEM, run: ulimit -l unlimited\n");
        return 1;
    }
    if (mode == MODE_WRITE) {
        for (i = 0; i < kRecvSlots; i++) {
            memset(&recvs[i], 0, sizeof(recvs[i]));
            recvs[i].sge.addr = (uintptr_t)recv_buf + (size_t)(i % 64) * 64;
            recvs[i].sge.length = 64;
            recvs[i].sge.lkey = recv_mr->lkey;
            recvs[i].wr.wr_id = (uint64_t)i;
            recvs[i].wr.sg_list = &recvs[i].sge;
            recvs[i].wr.num_sge = 1;
            if (post_one_recv(id->qp, &recvs[i]) != 0) {
                perror("ibv_post_recv");
                return 1;
            }
        }
    }
    memset(&xfer, 0, sizeof(xfer));
    xfer.addr = (uint64_t)(uintptr_t)buf;
    xfer.rkey = mr->rkey;
    fill_conn_param(&cp, &xfer, sizeof(xfer), hello.depth ? (int)hello.depth : 16);
    if (rdma_accept(id, &cp) != 0) {
        perror("rdma_accept");
        return 1;
    }
    if (expect_cm(ch, RDMA_CM_EVENT_ESTABLISHED, &ev) != 0)
        return 1;
    rdma_ack_cm_event(ev);
    snprintf(g_devname, sizeof(g_devname), "%s", id->verbs->device->name);
    outf("ESTABLISHED device=%s mode=%s\n", g_devname, mode_name(mode));
    if (strstr(g_devname, "bond") == NULL)
        outf("RESULT device %s has no bond in its name; this QP is not on a RoCE LAG device\n",
             g_devname);
    tune_timeout(id->qp);
    load_slaves(bond);
    snap_slaves(g_tx0, g_rx0);
    snap_hw(g_hw0, g_hw_ok);

    flags = fcntl(ch->fd, F_GETFL, 0);
    fcntl(ch->fd, F_SETFL, flags | O_NONBLOCK);
    t0 = now_ms();
    last_print = t0;
    for (;;) {
        struct ibv_wc wc[32];
        int n;
        if (g_stop)
            break;
        n = ibv_poll_cq(cq, 32, wc);
        for (i = 0; i < n; i++) {
            int is_imm = wc[i].status == IBV_WC_SUCCESS &&
                         (wc[i].opcode == IBV_WC_RECV_RDMA_WITH_IMM ||
                          (wc[i].opcode == IBV_WC_RECV && (wc[i].wc_flags & IBV_WC_WITH_IMM)));
            if (is_imm) {
                ctx.imm++;
                ctx.buf[1] = ctx.imm;
                if (wc[i].wr_id < kRecvSlots)
                    post_one_recv(id->qp, &recvs[wc[i].wr_id]);
            }
        }
        if (rdma_get_cm_event(ch, &ev) == 0) {
            enum rdma_cm_event_type kind = ev->event;
            rdma_ack_cm_event(ev);
            ev = NULL;
            if (kind == RDMA_CM_EVENT_DISCONNECTED || kind == RDMA_CM_EVENT_DEVICE_REMOVAL)
                break;
            outf("cm event %s\n", rdma_event_str(kind));
        }
        if (ctx.rep_ready) {
            server_verdict(&ctx);
            ctx.rep_ready = 2;
        }
        if (now_ms() - last_print >= 1000) {
            last_print = now_ms();
            outf("SERVER t=%" PRIu64 "s mem_raw=%" PRIu64 " imm=%" PRIu64 "\n",
                 (last_print - t0) / 1000ull, ctx.buf[0], ctx.imm);
        }
        if (n <= 0 && mode != MODE_WRITE)
            usleep(1000);
    }
    if (ctx.rep_ready == 1)
        server_verdict(&ctx);
    outf("SERVER final mem_raw=%" PRIu64 " mem_bswap=%" PRIu64 " imm=%" PRIu64 "\n", ctx.buf[0],
         bswap_64(ctx.buf[0]), ctx.imm);
    print_slave_delta();
    print_hw_delta();
    g_stop = 1;
    pthread_join(th, NULL);
    return 0;
}

struct client_cfg {
    const char *src;
    const char *dst;
    int port;
    uint32_t mode;
    int seconds;
    int depth;
    int drain_seconds;
    uint64_t max_count;
    int fault_delay_ms;
    const char *bond;
};

static int write_full(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, p + off, len - off);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        if (n == 0)
            return -1;
        off += (size_t)n;
    }
    return 0;
}

static int connect_ms(int fd, const struct sockaddr *addr, socklen_t len, int timeout_ms)
{
    int flags = fcntl(fd, F_GETFL, 0);
    struct pollfd pfd;
    int err = 0;
    socklen_t errlen = sizeof(err);
    int rc;
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0)
        return -1;
    rc = connect(fd, addr, len);
    if (rc == 0)
        return 0;
    if (errno != EINPROGRESS)
        return -1;
    pfd.fd = fd;
    pfd.events = POLLOUT;
    if (poll(&pfd, 1, timeout_ms) <= 0)
        return -1;
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &errlen) < 0 || err != 0)
        return -1;
    return 0;
}

static int send_report(const char *dst, int port, const struct report *rep)
{
    int i;
    for (i = 0; i < 5; i++) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        struct sockaddr_in addr;
        if (fd < 0)
            return -1;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t)port);
        inet_pton(AF_INET, dst, &addr.sin_addr);
        if (connect_ms(fd, (struct sockaddr *)&addr, sizeof(addr), 300) == 0 &&
            write_full(fd, rep, sizeof(*rep)) == 0) {
            close(fd);
            return 0;
        }
        close(fd);
        if (g_stop)
            return -1;
        usleep(100000);
    }
    return -1;
}

static int poll_client(struct ibv_cq *cq, struct slot *slots, int depth, uint32_t mode,
                       uint64_t *wc_ok, uint64_t *wc_retry, uint64_t *wc_rnr, uint64_t *wc_flush,
                       uint64_t *wc_other, uint64_t *cas_bad, uint64_t *cas_be, int *err_prints)
{
    struct ibv_wc wc[32];
    int n = ibv_poll_cq(cq, 32, wc);
    int i;
    if (n <= 0)
        return 0;
    for (i = 0; i < n; i++) {
        if (wc[i].status == IBV_WC_SUCCESS) {
            if (wc[i].wr_id == UINT64_MAX)
                continue;
            (*wc_ok)++;
            if (mode == MODE_CAS) {
                uint64_t seq = wc[i].wr_id;
                uint64_t got = slots[seq % (uint64_t)depth].result;
                if (got != seq) {
                    if (bswap_64(got) == seq)
                        (*cas_be)++;
                    else
                        (*cas_bad)++;
                }
            }
        } else if (wc[i].status == IBV_WC_RETRY_EXC_ERR) {
            (*wc_retry)++;
        } else if (wc[i].status == IBV_WC_RNR_RETRY_EXC_ERR) {
            (*wc_rnr)++;
        } else if (wc[i].status == IBV_WC_WR_FLUSH_ERR) {
            (*wc_flush)++;
        } else {
            (*wc_other)++;
        }
        if (wc[i].status != IBV_WC_SUCCESS && *err_prints < 8) {
            fprintf(stderr, "wc status=%s opcode=%d wr_id=%" PRIu64 "\n",
                    ibv_wc_status_str(wc[i].status), wc[i].opcode, (uint64_t)wc[i].wr_id);
            (*err_prints)++;
        }
    }
    return n;
}

static int run_client(struct client_cfg *cfg)
{
    struct rdma_event_channel *ch = NULL;
    struct rdma_cm_id *id = NULL;
    struct rdma_cm_event *ev = NULL;
    struct sockaddr_in dst;
    struct ibv_pd *pd = NULL;
    struct ibv_cq *cq = NULL;
    struct ibv_mr *mr = NULL;
    struct ibv_mr *final_mr = NULL;
    struct ibv_device_attr dattr;
    struct hello hello;
    struct xfer xfer;
    struct rdma_conn_param cp;
    struct slot *slots = NULL;
    uint64_t *final_buf = NULL;
    pthread_t fault_th;
    struct fault_arg farg;
    int fault_started = 0;
    int depth;
    int err_prints = 0;
    uint64_t next_seq = 0;
    uint64_t posted = 0;
    uint64_t wc_ok = 0, wc_retry = 0, wc_rnr = 0, wc_flush = 0, wc_other = 0;
    uint64_t cas_bad = 0, cas_be = 0;
    int inflight = 0;
    int post_fail = 0;
    uint64_t t0, tend, last_print, drain_deadline;
    struct report rep;
    int state;

    ch = rdma_create_event_channel();
    if (ch == NULL || rdma_create_id(ch, &id, NULL, RDMA_PS_TCP) != 0) {
        perror("rdma id");
        return 1;
    }
    memset(&dst, 0, sizeof(dst));
    dst.sin_family = AF_INET;
    dst.sin_port = htons((uint16_t)cfg->port);
    if (inet_pton(AF_INET, cfg->dst, &dst.sin_addr) != 1) {
        fprintf(stderr, "bad address\n");
        return 1;
    }
    /*
     * Do not pass --src. The same address is also configured on the vxlan
     * devices, and rdma_resolve_addr binds the source with ip_dev_find before
     * the route lookup. That picks a vxlan, then neighbor resolution times out
     * (ADDR_ERROR -110) even though the route and ping use bond1. With a
     * wildcard source, the route to --dst selects bond1.
     */
    outf("resolve dst %s by route (src %s is also on vxlan)\n", cfg->dst, cfg->src);
    if (rdma_resolve_addr(id, NULL, (struct sockaddr *)&dst, 2000) != 0) {
        perror("rdma_resolve_addr");
        return 1;
    }
    if (expect_cm(ch, RDMA_CM_EVENT_ADDR_RESOLVED, &ev) != 0)
        return 1;
    rdma_ack_cm_event(ev);
    outf("resolved src %s\n", inet_ntoa(id->route.addr.src_sin.sin_addr));
    snprintf(g_devname, sizeof(g_devname), "%s", id->verbs->device->name);
    outf("device %s\n", g_devname);
    if (strstr(g_devname, "bond") == NULL)
        outf("RESULT device %s has no bond in its name; this QP is not on a RoCE LAG device\n",
             g_devname);
    memset(&dattr, 0, sizeof(dattr));
    if (ibv_query_device(id->verbs, &dattr) == 0) {
        outf("atomic_cap=%d max_qp_rd_atom=%d\n", dattr.atomic_cap, dattr.max_qp_rd_atom);
    }
    depth = cfg->depth;
    if ((cfg->mode == MODE_FAA || cfg->mode == MODE_CAS) && dattr.atomic_cap == IBV_ATOMIC_NONE) {
        outf("RESULT this device reports no atomics; rerun with --mode write\n");
        return 1;
    }
    if ((cfg->mode == MODE_FAA || cfg->mode == MODE_CAS) && dattr.max_qp_rd_atom > 0 &&
        depth > dattr.max_qp_rd_atom)
        depth = dattr.max_qp_rd_atom;
    if (depth < 1)
        depth = 1;

    if (rdma_resolve_route(id, 2000) != 0) {
        perror("rdma_resolve_route");
        return 1;
    }
    if (expect_cm(ch, RDMA_CM_EVENT_ROUTE_RESOLVED, &ev) != 0)
        return 1;
    rdma_ack_cm_event(ev);
    if (create_qp(id, &pd, &cq, depth + 16, 1) != 0)
        return 1;
    if (posix_memalign((void **)&slots, 64, sizeof(*slots) * (size_t)depth) != 0 ||
        posix_memalign((void **)&final_buf, 64, 16) != 0) {
        perror("posix_memalign");
        return 1;
    }
    memset(slots, 0, sizeof(*slots) * (size_t)depth);
    memset(final_buf, 0, 16);
    mr = ibv_reg_mr(pd, slots, sizeof(*slots) * (size_t)depth, IBV_ACCESS_LOCAL_WRITE);
    final_mr = ibv_reg_mr(pd, final_buf, 16, IBV_ACCESS_LOCAL_WRITE);
    if (mr == NULL || final_mr == NULL) {
        perror("ibv_reg_mr");
        outf("If this is ENOMEM, run: ulimit -l unlimited\n");
        return 1;
    }

    hello.mode = cfg->mode;
    hello.depth = (uint32_t)depth;
    fill_conn_param(&cp, &hello, sizeof(hello), depth);
    if (rdma_connect(id, &cp) != 0) {
        perror("rdma_connect");
        return 1;
    }
    if (expect_cm(ch, RDMA_CM_EVENT_ESTABLISHED, &ev) != 0)
        return 1;
    if (ev->param.conn.private_data == NULL || ev->param.conn.private_data_len < sizeof(xfer)) {
        fprintf(stderr, "missing server rkey\n");
        rdma_ack_cm_event(ev);
        return 1;
    }
    memcpy(&xfer, ev->param.conn.private_data, sizeof(xfer));
    rdma_ack_cm_event(ev);
    tune_timeout(id->qp);
    load_slaves(cfg->bond);
    snap_slaves(g_tx0, g_rx0);
    snap_hw(g_hw0, g_hw_ok);
    outf("GO mode=%s depth=%d seconds=%d remote=0x%" PRIx64 "\n", mode_name(cfg->mode), depth,
         cfg->seconds, xfer.addr);
    outf("Leave the failed slave down until DRAIN completes.\n");

    if (g_fault_if[0] != '\0') {
        farg.delay_ms = cfg->fault_delay_ms;
        if (pthread_create(&fault_th, NULL, fault_thread, &farg) == 0)
            fault_started = 1;
    }

    t0 = now_ms();
    tend = t0 + (uint64_t)cfg->seconds * 1000ull;
    last_print = t0;
    while (!g_stop && !post_fail && now_ms() < tend &&
           (cfg->max_count == 0 || posted < cfg->max_count)) {
        while (inflight < depth && (cfg->max_count == 0 || posted < cfg->max_count)) {
            uint64_t seq = next_seq;
            if (post_op(id->qp, &slots[seq % (uint64_t)depth], mr->lkey, xfer.addr, xfer.rkey, seq,
                        cfg->mode) != 0) {
                fprintf(stderr, "post_send failed errno=%d seq=%" PRIu64 "\n", errno, seq);
                post_fail = 1;
                break;
            }
            next_seq++;
            posted++;
            inflight++;
        }
        inflight -= poll_client(cq, slots, depth, cfg->mode, &wc_ok, &wc_retry, &wc_rnr, &wc_flush,
                                &wc_other, &cas_bad, &cas_be, &err_prints);
        if (inflight < 0)
            inflight = 0;
        if (now_ms() - last_print >= 1000) {
            last_print = now_ms();
            outf("CLIENT t=%" PRIu64 "s posted=%" PRIu64 " wc_success=%" PRIu64 " inflight=%d\n",
                 (last_print - t0) / 1000ull, posted, wc_ok, inflight);
        }
    }

    outf("DRAIN posted=%" PRIu64 " inflight=%d qp_state=%d\n", posted, inflight, qp_state(id->qp));
    drain_deadline = now_ms() + (uint64_t)cfg->drain_seconds * 1000ull;
    while (inflight > 0 && now_ms() < drain_deadline && !g_stop) {
        int got = poll_client(cq, slots, depth, cfg->mode, &wc_ok, &wc_retry, &wc_rnr, &wc_flush,
                              &wc_other, &cas_bad, &cas_be, &err_prints);
        inflight -= got;
        if (inflight < 0)
            inflight = 0;
        if (got == 0)
            usleep(1000);
    }
    state = qp_state(id->qp);
    outf("DRAIN done inflight=%d qp_state=%d wc_success=%" PRIu64 " wc_retry=%" PRIu64
         " wc_rnr=%" PRIu64 " wc_flush=%" PRIu64 " wc_other=%" PRIu64 "\n",
         inflight, state, wc_ok, wc_retry, wc_rnr, wc_flush, wc_other);

    if (inflight == 0 && state == IBV_QPS_RTS) {
        struct ibv_sge sge;
        struct ibv_send_wr wr;
        struct ibv_send_wr *bad = NULL;
        struct ibv_wc wc;
        uint64_t wait_until = now_ms() + 2000;
        memset(&wr, 0, sizeof(wr));
        memset(final_buf, 0, 16);
        sge.addr = (uintptr_t)final_buf;
        sge.length = 16;
        sge.lkey = final_mr->lkey;
        wr.wr_id = UINT64_MAX;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.opcode = IBV_WR_RDMA_READ;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = xfer.addr;
        wr.wr.rdma.rkey = xfer.rkey;
        if (ibv_post_send(id->qp, &wr, &bad) == 0) {
            int done = 0;
            while (!done && now_ms() < wait_until) {
                int n = ibv_poll_cq(cq, 1, &wc);
                if (n > 0 && wc.wr_id == UINT64_MAX) {
                    done = 1;
                    if (wc.status == IBV_WC_SUCCESS) {
                        int used_be = 0;
                        uint64_t hint = wc_ok ? wc_ok : posted;
                        uint64_t executed = cfg->mode == MODE_WRITE
                                                ? final_buf[1]
                                                : as_host_counter(final_buf[0], hint, &used_be);
                        outf("CLIENT read mem_raw=%" PRIu64 " imm_mirror=%" PRIu64 " endian=%s\n",
                             final_buf[0], final_buf[1], used_be ? "bswap" : "raw");
                        print_verdict("client", cfg->mode, executed, posted, wc_ok);
                    } else {
                        outf("CLIENT read status=%s\n", ibv_wc_status_str(wc.status));
                    }
                } else if (n == 0) {
                    usleep(1000);
                }
            }
        }
    } else {
        outf("CLIENT qp_state=%d, skipped final read; use the server RESULT line\n", state);
    }

    memset(&rep, 0, sizeof(rep));
    rep.magic = kReportMagic;
    rep.posted = posted;
    rep.wc_ok = wc_ok;
    rep.wc_retry = wc_retry;
    rep.wc_rnr = wc_rnr;
    rep.wc_flush = wc_flush;
    rep.wc_other = wc_other;
    rep.cas_bad = cas_bad;
    rep.cas_be = cas_be;
    rep.inflight_left = inflight > 0 ? (uint64_t)inflight : 0;
    rep.mode = cfg->mode;
    print_slave_delta();
    print_hw_delta();
    if (send_report(cfg->dst, cfg->port + 1, &rep) != 0) {
        outf("REPORT send failed while the slave is down; restoring and retrying\n");
        if (fault_started)
            pthread_join(fault_th, NULL);
        fault_started = 0;
        restore_fault_if();
        sleep(1);
        if (send_report(cfg->dst, cfg->port + 1, &rep) != 0)
            outf("REPORT send failed; compare CLIENT counts with SERVER final line\n");
        else
            outf("REPORT sent\n");
    } else {
        outf("REPORT sent\n");
    }
    rdma_disconnect(id);
    if (fault_started)
        pthread_join(fault_th, NULL);
    restore_fault_if();
    return 0;
}

static void usage(const char *argv0)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s server --bind IP [--port 18515] [--bond bond1]\n"
            "  %s client --src IP --dst IP [--port 18515] [--bond bond1]\n"
            "         --mode faa|cas|write [--seconds 8] [--depth 32]\n"
            "         [--drain-seconds 30] [--count N]\n"
            "         [--fault-if eth0 --fault-delay-ms 2000]\n"
            "\n"
            "Do not pass the bond device to --fault-if. The program brings the slave back up\n"
            "on exit. A second TCP port (RDMA port + 1) carries the completion report.\n",
            argv0, argv0);
}

int main(int argc, char **argv)
{
    const char *bind_ip = NULL;
    const char *src = NULL;
    const char *dst = NULL;
    const char *bond = "bond1";
    const char *mode_s = "faa";
    int port = 18515;
    int seconds = 8;
    int depth = 32;
    int drain_seconds = 30;
    int fault_delay_ms = 2000;
    uint64_t max_count = 0;
    int is_server = 0;
    struct sigaction sa;
    int c;

    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }
    if (strcmp(argv[1], "server") == 0)
        is_server = 1;
    else if (strcmp(argv[1], "client") != 0) {
        usage(argv[0]);
        return 1;
    }

    static struct option opts[] = {
        {"bind", required_argument, NULL, 'b'},
        {"src", required_argument, NULL, 's'},
        {"dst", required_argument, NULL, 'd'},
        {"port", required_argument, NULL, 'p'},
        {"mode", required_argument, NULL, 'm'},
        {"seconds", required_argument, NULL, 't'},
        {"depth", required_argument, NULL, 'q'},
        {"bond", required_argument, NULL, 'B'},
        {"fault-if", required_argument, NULL, 'f'},
        {"fault-delay-ms", required_argument, NULL, 'D'},
        {"drain-seconds", required_argument, NULL, 'r'},
        {"count", required_argument, NULL, 'n'},
        {"help", no_argument, NULL, 'h'},
        {NULL, 0, NULL, 0},
    };
    while ((c = getopt_long(argc - 1, argv + 1, "b:s:d:p:m:t:q:B:f:D:r:n:h", opts, NULL)) != -1) {
        switch (c) {
        case 'b':
            bind_ip = optarg;
            break;
        case 's':
            src = optarg;
            break;
        case 'd':
            dst = optarg;
            break;
        case 'p':
            port = atoi(optarg);
            break;
        case 'm':
            mode_s = optarg;
            break;
        case 't':
            seconds = atoi(optarg);
            break;
        case 'q':
            depth = atoi(optarg);
            break;
        case 'B':
            bond = optarg;
            break;
        case 'f':
            snprintf(g_fault_if, sizeof(g_fault_if), "%s", optarg);
            break;
        case 'D':
            fault_delay_ms = atoi(optarg);
            break;
        case 'r':
            drain_seconds = atoi(optarg);
            break;
        case 'n':
            max_count = strtoull(optarg, NULL, 10);
            break;
        default:
            usage(argv[0]);
            return 1;
        }
    }

    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    atexit(restore_fault_if);
    setvbuf(stdout, NULL, _IOLBF, 0);

    if (is_server) {
        if (bind_ip == NULL) {
            usage(argv[0]);
            return 1;
        }
        return run_server(bind_ip, port, bond);
    }

    {
        struct client_cfg cfg;
        uint32_t mode = MODE_FAA;
        if (src == NULL || dst == NULL) {
            usage(argv[0]);
            return 1;
        }
        if (strcmp(mode_s, "faa") == 0)
            mode = MODE_FAA;
        else if (strcmp(mode_s, "cas") == 0)
            mode = MODE_CAS;
        else if (strcmp(mode_s, "write") == 0)
            mode = MODE_WRITE;
        else {
            fprintf(stderr, "mode must be faa, cas, or write\n");
            return 1;
        }
        if (g_fault_if[0] != '\0' && !valid_ifname(g_fault_if)) {
            fprintf(stderr, "--fault-if must be a slave name, not the bond\n");
            return 1;
        }
        if (seconds < 1)
            seconds = 1;
        if (drain_seconds < 1)
            drain_seconds = 1;
        if (depth < 1)
            depth = 1;
        if (depth > 128)
            depth = 128;
        memset(&cfg, 0, sizeof(cfg));
        cfg.src = src;
        cfg.dst = dst;
        cfg.port = port;
        cfg.mode = mode;
        cfg.seconds = seconds;
        cfg.depth = depth;
        cfg.drain_seconds = drain_seconds;
        cfg.max_count = max_count;
        cfg.fault_delay_ms = fault_delay_ms;
        cfg.bond = bond;
        if (g_fault_if[0] != '\0') {
            outf("Will set %s down after %d ms. This affects every flow on that slave.\n",
                 g_fault_if, fault_delay_ms);
        }
        return run_client(&cfg);
    }
}
