#include "zQP.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace zrdma;

static const uint64_t kWriteBase = 0xC0FFEE0000000001ULL;
static const uint64_t kCasBase = 0xCA50000000000001ULL;

static int env_int(const char *name, int fallback)
{
    const char *v = getenv(name);
    if (v == NULL || v[0] == '\0')
    {
        return fallback;
    }
    return atoi(v);
}

static const char *env_str(const char *name, const char *fallback)
{
    const char *v = getenv(name);
    return (v == NULL || v[0] == '\0') ? fallback : v;
}

static int run_cmd(const std::string &cmd)
{
    return system(cmd.c_str());
}

static int set_link(const std::string &host, const std::string &ifname, bool up)
{
    if (ifname.empty())
    {
        return -1;
    }
    std::string inner = std::string("sudo -n ip link set ") + ifname + (up ? " up" : " down");
    if (host.empty())
    {
        return run_cmd(inner);
    }
    std::string cmd = std::string("ssh -o StrictHostKeyChecking=no -o ConnectTimeout=8 ") +
                      host + " '" + inner + "'";
    return run_cmd(cmd);
}

static int ensure_ip(const std::string &host, const std::string &ifname, const std::string &cidr)
{
    std::string inner = std::string("ip -4 addr show ") + ifname + " | grep -q " + cidr +
                        " || sudo -n ip addr add " + cidr + " dev " + ifname;
    if (host.empty())
    {
        return run_cmd(inner);
    }
    std::string cmd = std::string("ssh -o StrictHostKeyChecking=no -o ConnectTimeout=8 ") +
                      host + " \"" + inner + "\"";
    return run_cmd(cmd);
}

struct FaultPlan
{
    std::string mode;
    std::string mn_ssh;
    std::string cn_if;
    std::string mn_if;
    std::string cn_ip;
    std::string mn_ip;
    std::string mlx;
    int delay_us;
    int flap_us;
    int settle_us;
};

static FaultPlan g_plan;
static std::string g_cn_if;
static std::string g_mn_ssh;
static std::string g_mn_if;
static std::string g_cn_ip;
static std::string g_mn_ip;

static void restore_paths()
{
    if (!g_cn_if.empty())
    {
        set_link("", g_cn_if, true);
        usleep(400000);
        if (!g_cn_ip.empty())
        {
            ensure_ip("", g_cn_if, g_cn_ip);
        }
        if (!g_plan.mlx.empty())
        {
            run_cmd(std::string("sudo -n rdma link set ") + g_plan.mlx + " state active >/dev/null 2>&1");
        }
    }
    if (!g_mn_ssh.empty() && !g_mn_if.empty())
    {
        set_link(g_mn_ssh, g_mn_if, true);
        usleep(400000);
        if (!g_mn_ip.empty())
        {
            ensure_ip(g_mn_ssh, g_mn_if, g_mn_ip);
        }
    }
}

static void inject_fault(const FaultPlan &plan)
{
    if (plan.delay_us > 0)
    {
        usleep(plan.delay_us);
    }
    printf("INJECT mode=%s delay_us=%d\n", plan.mode.c_str(), plan.delay_us);
    fflush(stdout);
    if (plan.mode == "none")
    {
        return;
    }
    if (plan.mode == "cn_ip" || plan.mode == "both_ip" || plan.mode == "cn_flap")
    {
        set_link("", plan.cn_if, false);
    }
    if (plan.mode == "cn_ifconfig")
    {
        run_cmd(std::string("sudo -n ifconfig ") + plan.cn_if + " down");
    }
    if (plan.mode == "cn_rdma")
    {
        int rc = run_cmd(std::string("sudo -n rdma link set ") + plan.mlx + " state down");
        printf("rdma link set rc=%d, also taking netdev down\n", rc);
        set_link("", plan.cn_if, false);
    }
    if (plan.mode == "mn_ip" || plan.mode == "both_ip" || plan.mode == "mn_flap")
    {
        set_link(plan.mn_ssh, plan.mn_if, false);
    }
    if (plan.mode == "cn_flap" || plan.mode == "mn_flap")
    {
        usleep(plan.flap_us > 0 ? plan.flap_us : 200000);
        restore_paths();
        printf("INJECT flap restored\n");
        fflush(stdout);
    }
}

static int write_all(zQP *qp, uint64_t *local, uint32_t lkey, uint64_t addr, uint32_t rkey,
                     int nslots, uint64_t base, int fault_after, std::thread **fault_thr,
                     const FaultPlan *plan)
{
    int fail = 0;
    for (int i = 0; i < nslots; i++)
    {
        if (plan != NULL && fault_thr != NULL && *fault_thr == NULL && i == fault_after &&
            plan->mode != "none")
        {
            *fault_thr = new std::thread(inject_fault, *plan);
            if (plan->settle_us > 0)
            {
                usleep(plan->settle_us);
            }
        }
        local[i] = base + (uint64_t)i;
        if (z_write(qp, &local[i], lkey, sizeof(uint64_t), (void *)(addr + (uint64_t)i * 8), rkey) != 0)
        {
            fail++;
            if (z_write(qp, &local[i], lkey, sizeof(uint64_t), (void *)(addr + (uint64_t)i * 8), rkey) != 0)
            {
                fail++;
            }
        }
    }
    return fail;
}

static int read_check(zQP *qp, uint64_t *local, uint32_t lkey, uint64_t addr, uint32_t rkey,
                      int nslots, uint64_t base, const char *tag, int *first_bad)
{
    memset(local, 0, (size_t)nslots * sizeof(uint64_t));
    int mismatch = 0;
    int read_fail = 0;
    *first_bad = -1;
    for (int i = 0; i < nslots; i++)
    {
        if (z_read(qp, &local[i], lkey, sizeof(uint64_t), (void *)(addr + (uint64_t)i * 8), rkey) != 0)
        {
            read_fail++;
        }
        uint64_t expect = base + (uint64_t)i;
        if (local[i] != expect)
        {
            if (*first_bad < 0)
            {
                *first_bad = i;
                printf("%s first mismatch slot=%d expect=0x%lx got=0x%lx\n",
                       tag, i, (unsigned long)expect, (unsigned long)local[i]);
            }
            mismatch++;
        }
    }
    printf("%s read_fail=%d mismatch=%d/%d device=%d\n",
           tag, read_fail, mismatch, nslots, qp->current_device);
    return mismatch + read_fail;
}

static uint64_t slot_expect(uint64_t base, bool plus_i, int i)
{
    return plus_i ? (base + (uint64_t)i) : base;
}

static int cas_all(zQP *qp, uint64_t *local, uint32_t lkey, uint64_t addr, uint32_t rkey,
                   int nslots, uint64_t expected_base, bool expected_plus_i, uint64_t swap_base,
                   int fault_after, std::thread **fault_thr, const FaultPlan *plan)
{
    int fail = 0;
    for (int i = 0; i < nslots; i++)
    {
        if (plan != NULL && fault_thr != NULL && *fault_thr == NULL && i == fault_after &&
            plan->mode != "none")
        {
            *fault_thr = new std::thread(inject_fault, *plan);
            if (plan->settle_us > 0)
            {
                usleep(plan->settle_us);
            }
        }
        uint64_t expected = slot_expect(expected_base, expected_plus_i, i);
        uint64_t swap = swap_base + (uint64_t)i;
        local[i] = expected;
        int rc = z_CAS(qp, &local[i], lkey, swap, (void *)(addr + (uint64_t)i * 8), rkey);
        uint64_t observed = 0;
        bool need_read = (rc != 0) || (local[i] != expected);
        if (need_read)
        {
            if (z_read(qp, &observed, lkey, sizeof(uint64_t), (void *)(addr + (uint64_t)i * 8), rkey) != 0)
            {
                fail++;
                continue;
            }
            if (observed == swap)
            {
                continue;
            }
            if (observed == expected)
            {
                local[i] = expected;
                if (z_CAS(qp, &local[i], lkey, swap, (void *)(addr + (uint64_t)i * 8), rkey) != 0)
                {
                    fail++;
                }
            }
            else
            {
                printf("CAS leftover slot=%d expected=0x%lx swap=0x%lx observed=0x%lx\n",
                       i, (unsigned long)expected, (unsigned long)swap, (unsigned long)observed);
                fail++;
            }
        }
    }
    return fail;
}

static int async_write_burst(zQP *qp, uint64_t *local, uint32_t lkey, uint64_t addr, uint32_t rkey,
                             int nslots, uint64_t base, int fault_after, std::thread **fault_thr,
                             const FaultPlan *plan)
{
    std::vector<uint64_t> wr_ids;
    wr_ids.reserve((size_t)nslots);
    int post_fail = 0;
    for (int i = 0; i < nslots; i++)
    {
        if (plan != NULL && fault_thr != NULL && *fault_thr == NULL && i == fault_after &&
            plan->mode != "none")
        {
            *fault_thr = new std::thread(inject_fault, *plan);
        }
        local[i] = base + (uint64_t)i;
        if (z_write_async(qp, &local[i], lkey, sizeof(uint64_t), (void *)(addr + (uint64_t)i * 8),
                          rkey, &wr_ids) != 0)
        {
            post_fail++;
        }
    }
    if (plan != NULL && plan->settle_us > 0)
    {
        usleep(plan->settle_us);
    }
    int poll_rc = 0;
    if (!wr_ids.empty())
    {
        poll_rc = z_poll_completion(qp, &wr_ids);
    }
    printf("ASYNC post_fail=%d poll_rc=%d leftover_wr=%zu device=%d\n",
           post_fail, poll_rc, wr_ids.size(), qp->current_device);
    return post_fail;
}

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        printf("Usage: %s <local_config> <target_config>\n", argv[0]);
        printf("Env: VARUNA_FAULT=none|cn_ip|cn_ifconfig|cn_rdma|mn_ip|both_ip|cn_flap|mn_flap\n");
        printf("     VARUNA_FAULT_PHASE=write|cas|both|async  VARUNA_FAULT_AFTER VARUNA_FAULT_DELAY_US\n");
        return 2;
    }

    int nslots = env_int("VARUNA_NSLOTS", 128);
    int fault_after = env_int("VARUNA_FAULT_AFTER", 32);
    if (nslots < 8)
    {
        nslots = 8;
    }
    if (fault_after < 0 || fault_after >= nslots)
    {
        fault_after = nslots / 4;
    }

    zDeviceConfig local_cfg;
    zTargetConfig target_cfg;
    if (load_config(argv[1], &local_cfg) != 0 || load_config(argv[2], &target_cfg) != 0)
    {
        printf("Failed to load config\n");
        return 2;
    }

    FaultPlan plan;
    plan.mode = env_str("VARUNA_FAULT", "cn_ip");
    plan.mn_ssh = env_str("VARUNA_MN_SSH", "node-1.x1aoyang-314032.adslremotememory-pg0.utah.cloudlab.us");
    plan.cn_if = local_cfg.eth_names[0];
    plan.mn_if = env_str("VARUNA_MN_IFACE", "ens3f0");
    plan.cn_ip = local_cfg.ips[0] + "/24";
    plan.mn_ip = env_str("VARUNA_MN_IP", "10.10.1.2/24");
    plan.mlx = local_cfg.mlx_names[0] + "/1";
    plan.delay_us = env_int("VARUNA_FAULT_DELAY_US", 0);
    plan.flap_us = env_int("VARUNA_FLAP_US", 200000);
    plan.settle_us = env_int("VARUNA_FAULT_SETTLE_US", 0);
    g_plan = plan;
    g_cn_if = plan.cn_if;
    g_mn_ssh = plan.mn_ssh;
    g_mn_if = plan.mn_if;
    g_cn_ip = plan.cn_ip;
    g_mn_ip = plan.mn_ip;
    atexit(restore_paths);

    restore_paths();
    usleep(500000);

    std::string phase = env_str("VARUNA_FAULT_PHASE", "write");
    printf("recover_correct fault=%s phase=%s nslots=%d fault_after=%d delay_us=%d\n",
           plan.mode.c_str(), phase.c_str(), nslots, fault_after, plan.delay_us);

    zEndpoint *ep = zEP_create(argv[1]);
    zPD *pd = zPD_create(ep, 1);
    rkeyTable *table = new rkeyTable();
    zQP *qp = zQP_create(pd, ep, table, ZQP_ONESIDED);
    zQP *rpc = zQP_create(pd, ep, table, ZQP_RPC);
    zQP_connect(qp, 0, target_cfg.target_ips[0], target_cfg.target_ports[0]);
    zQP_connect(rpc, 0, target_cfg.target_ips[0], target_cfg.target_ports[0]);

    size_t bytes = (size_t)nslots * sizeof(uint64_t);
    uint64_t addr = 0;
    uint32_t rkey = 0;
    zQP_RPC_Alloc(rpc, &addr, &rkey, bytes);
    void *local_raw = NULL;
    ibv_mr *mr = mr_malloc_create(pd, (uint64_t &)local_raw, bytes);
    if (mr == NULL)
    {
        printf("MR failed\n");
        return 2;
    }
    uint64_t *local = (uint64_t *)local_raw;

    std::thread *fault_thr = NULL;
    int write_fail = 0, write_mismatch = 0, zero_fail = 0;
    int cas_fail = 0, cas_mismatch = 0, bump_fail = 0, bump_mismatch = 0;
    int write_bad = 0, cas_bad = 0, bump_bad = 0;

    if (phase == "async")
    {
        memset(local, 0, bytes);
        for (int i = 0; i < nslots; i++)
        {
            z_write(qp, &local[i], mr->lkey, sizeof(uint64_t), (void *)(addr + (uint64_t)i * 8), rkey);
        }
        write_fail = async_write_burst(qp, local, mr->lkey, addr, rkey, nslots, kWriteBase,
                                       fault_after, &fault_thr, &plan);
        write_mismatch = read_check(qp, local, mr->lkey, addr, rkey, nslots, kWriteBase,
                                    "ASYNC", &write_bad);
        if (fault_thr != NULL)
        {
            fault_thr->join();
            delete fault_thr;
            fault_thr = NULL;
        }
    }
    else
    {
        const FaultPlan *write_plan = (phase == "cas") ? NULL : &plan;
        const FaultPlan *cas_plan = (phase == "write") ? NULL : &plan;

        write_fail = write_all(qp, local, mr->lkey, addr, rkey, nslots, kWriteBase,
                               fault_after, &fault_thr, write_plan);
        write_mismatch = read_check(qp, local, mr->lkey, addr, rkey, nslots, kWriteBase,
                                    "WRITE", &write_bad);

        memset(local, 0, bytes);
        for (int i = 0; i < nslots; i++)
        {
            if (z_write(qp, &local[i], mr->lkey, sizeof(uint64_t), (void *)(addr + (uint64_t)i * 8), rkey) != 0)
            {
                zero_fail++;
            }
        }

        if (fault_thr != NULL)
        {
            fault_thr->join();
            delete fault_thr;
            fault_thr = NULL;
        }

        cas_fail = cas_all(qp, local, mr->lkey, addr, rkey, nslots, 0, false, kCasBase,
                           fault_after, &fault_thr, cas_plan);
        cas_mismatch = read_check(qp, local, mr->lkey, addr, rkey, nslots, kCasBase,
                                  "CAS", &cas_bad);

        bump_fail = cas_all(qp, local, mr->lkey, addr, rkey, nslots, kCasBase, true,
                            kCasBase + 0x1000, nslots + 1, &fault_thr, NULL);
        bump_mismatch = read_check(qp, local, mr->lkey, addr, rkey, nslots, kCasBase + 0x1000,
                                   "CAS2", &bump_bad);

        if (fault_thr != NULL)
        {
            fault_thr->join();
            delete fault_thr;
        }
    }

    int pass = (write_fail == 0 && write_mismatch == 0 && zero_fail == 0 &&
                cas_fail == 0 && cas_mismatch == 0 && bump_fail == 0 && bump_mismatch == 0);
    printf("RESULT fault=%s phase=%s pass=%d device=%d write_fail=%d write_mismatch=%d "
           "cas_fail=%d cas_mismatch=%d cas2_fail=%d cas2_mismatch=%d\n",
           plan.mode.c_str(), phase.c_str(), pass, qp->current_device, write_fail,
           write_mismatch, cas_fail, cas_mismatch, bump_fail, bump_mismatch);
    fflush(stdout);
    return pass ? 0 : 1;
}
