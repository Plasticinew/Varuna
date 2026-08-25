#include "zQP.h"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace zrdma;

static int env_int(const char *name, int fallback)
{
    const char *v = getenv(name);
    if (v == NULL || v[0] == '\0')
    {
        return fallback;
    }
    return atoi(v);
}

static int set_link(const string &ifname, bool up)
{
    if (ifname.empty())
    {
        return -1;
    }
    string cmd = string("sudo ip link set ") + ifname + (up ? " up" : " down");
    return system(cmd.c_str());
}

static bool requestor_connected(zQP *qp, int device)
{
    return device >= 0 &&
           device < (int)qp->m_requestors.size() &&
           qp->m_requestors[device] != NULL &&
           qp->m_requestors[device]->status_ == ZSTATUS_CONNECTED;
}

static const char *path_name(zQP *qp, bool dcqp_mode)
{
    if (dcqp_mode && qp->current_device != 0 && !requestor_connected(qp, qp->current_device))
    {
        return "dcqp";
    }
    return "rc";
}

int main(int argc, char **argv)
{
    if (argc < 3)
    {
        printf("Usage: %s <local_config> <target_config> [out_csv]\n", argv[0]);
        printf("Env: VARUNA_DCQP=0|1 VARUNA_IO_SIZE VARUNA_DEPTH VARUNA_SAMPLE_US\n");
        printf("     VARUNA_WARMUP_MS VARUNA_POST_MS VARUNA_TIMEOUT_MS\n");
        return -1;
    }

    string local_cfg_path = argv[1];
    string target_cfg_path = argv[2];
    bool dcqp_mode = true;
    if (const char *v = getenv("VARUNA_DCQP"))
    {
        dcqp_mode = v[0] != '0';
    }

    uint64_t io_size = (uint64_t)env_int("VARUNA_IO_SIZE", 65536);
    int depth = env_int("VARUNA_DEPTH", 16);
    int sample_us = env_int("VARUNA_SAMPLE_US", 1000);
    int warmup_ms = env_int("VARUNA_WARMUP_MS", 400);
    int post_ms = env_int("VARUNA_POST_MS", 1500);
    int timeout_ms = env_int("VARUNA_TIMEOUT_MS", 8000);
    if (depth < 1)
    {
        depth = 1;
    }
    if (sample_us < 200)
    {
        sample_us = 200;
    }

    string out_csv = argc >= 4 ? argv[3]
                               : (dcqp_mode ? string("results/bw_dcqp.csv") : string("results/bw_rcqp.csv"));

    zDeviceConfig local_cfg;
    if (load_config(local_cfg_path.c_str(), &local_cfg) != 0 || local_cfg.num_devices < 1)
    {
        printf("Failed to load local config %s\n", local_cfg_path.c_str());
        return -1;
    }
    string fault_ifname = local_cfg.eth_names[0];
    string backup_ifname = local_cfg.num_devices > 1 ? local_cfg.eth_names[1] : local_cfg.eth_names[0];
    set_link(fault_ifname, true);
    set_link(backup_ifname, true);
    sleep(1);

    zEndpoint *ep = zEP_create(local_cfg_path);
    zPD *pd = zPD_create(ep, 1);
    rkeyTable *table = new rkeyTable();
    zTargetConfig target_cfg;
    load_config(target_cfg_path.c_str(), &target_cfg);

    zQP *qp = zQP_create(pd, ep, table, ZQP_ONESIDED);
    zQP *rpc_qp = zQP_create(pd, ep, table, ZQP_RPC);
    zQP_connect(qp, 0, target_cfg.target_ips[0], target_cfg.target_ports[0]);
    zQP_connect(rpc_qp, 0, target_cfg.target_ips[0], target_cfg.target_ports[0]);

    uint64_t addr = 0;
    uint32_t rkey = 0;
    zQP_RPC_Alloc(rpc_qp, &addr, &rkey, io_size);
    void *local_buf = NULL;
    ibv_mr *mr = mr_malloc_create(pd, (uint64_t &)local_buf, io_size);
    if (mr == NULL)
    {
        printf("Memory registration failed\n");
        return -1;
    }
    memset(local_buf, 1, io_size);

    printf("bw_timeline mode=%s io_size=%lu depth=%d sample_us=%d warmup_ms=%d out=%s\n",
           dcqp_mode ? "dcqp" : "rcqp", (unsigned long)io_size, depth, sample_us, warmup_ms,
           out_csv.c_str());

    std::atomic<bool> stop{false};
    std::atomic<bool> fault_injected{false};
    std::atomic<uint64_t> link_down_us{0};
    std::atomic<int> sample_count{0};
    auto t0 = TIME_NOW;

    std::ofstream csv(out_csv.c_str());
    csv << "t_us,mbps,bytes,device,path\n";

    std::thread sampler([&]() {
        uint64_t prev = qp->size_counter_.load();
        auto prev_t = TIME_NOW;
        while (!stop.load())
        {
            usleep(sample_us);
            auto now = TIME_NOW;
            uint64_t cur = qp->size_counter_.load();
            uint64_t dt = TIME_DURATION_US(prev_t, now);
            if (dt == 0)
            {
                dt = 1;
            }
            uint64_t delta = cur >= prev ? cur - prev : 0;
            double mbps = (double)delta / (double)dt;
            uint64_t t_us = TIME_DURATION_US(t0, now);
            csv << t_us << "," << mbps << "," << delta << "," << qp->current_device << ","
                << path_name(qp, dcqp_mode) << "\n";
            sample_count.fetch_add(1);
            prev = cur;
            prev_t = now;
        }
        csv.flush();
    });

    std::thread faulter([&]() {
        usleep((useconds_t)warmup_ms * 1000);
        link_down_us.store(TIME_DURATION_US(t0, TIME_NOW));
        fault_injected.store(true);
        printf("EVENT t_ns=%lu name=link_down device=0 t_us=%lu\n",
               (unsigned long)TIME_NOW.time_since_epoch().count(),
               (unsigned long)link_down_us.load());
        fflush(stdout);
        set_link(fault_ifname, false);
        printf("Injected link down on %s at t_us=%lu\n",
               fault_ifname.c_str(), (unsigned long)link_down_us.load());
    });

    std::vector<uint64_t> wr_ids;
    uint64_t rc_ready_us = 0;
    uint64_t first_dcqp_us = 0;
    while (true)
    {
        int rc = z_write_async(qp, local_buf, mr->lkey, io_size, (void *)addr, rkey, &wr_ids);
        if (rc != 0)
        {
            if (wr_ids.size() > 0)
            {
                z_poll_completion(qp, &wr_ids);
            }
            wr_ids.clear();
        }
        else if ((int)wr_ids.size() >= depth)
        {
            z_poll_completion(qp, &wr_ids);
        }

        uint64_t now_us = TIME_DURATION_US(t0, TIME_NOW);
        if (first_dcqp_us == 0 && qp->current_device != 0 &&
            !requestor_connected(qp, qp->current_device))
        {
            first_dcqp_us = now_us;
        }
        if (rc_ready_us == 0 && qp->current_device != 0 &&
            requestor_connected(qp, qp->current_device))
        {
            rc_ready_us = now_us;
            printf("bw_timeline observed rc_ready t_us=%lu device=%d\n",
                   (unsigned long)rc_ready_us, qp->current_device);
        }

        if (fault_injected.load())
        {
            uint64_t since_fault = now_us - link_down_us.load();
            bool recovered = rc_ready_us != 0;
            if (recovered && (now_us - rc_ready_us) >= (uint64_t)post_ms * 1000ULL)
            {
                break;
            }
            if (since_fault >= (uint64_t)timeout_ms * 1000ULL)
            {
                printf("bw_timeline timeout after fault, rc_ready=%d\n", recovered ? 1 : 0);
                break;
            }
        }
    }

    if (wr_ids.size() > 0)
    {
        z_poll_completion(qp, &wr_ids);
    }
    stop.store(true);
    faulter.join();
    sampler.join();
    csv.close();

    set_link(fault_ifname, true);
    printf("bw_timeline done samples=%d link_down_us=%lu first_dcqp_us=%lu rc_ready_us=%lu out=%s\n",
           sample_count.load(), (unsigned long)link_down_us.load(),
           (unsigned long)first_dcqp_us, (unsigned long)rc_ready_us, out_csv.c_str());
    return 0;
}
