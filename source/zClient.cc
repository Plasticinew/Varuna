#include "zQP.h"

using namespace zrdma;

pthread_barrier_t barrier_start;
string g_fault_ifname;
string g_backup_ifname;

static bool inject_fault_enabled()
{
    const char *v = getenv("VARUNA_INJECT_FAULT");
    if (v == NULL)
    {
        return true;
    }
    return v[0] != '0';
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

void test_zQP_shared(string config_file, string remote_config_file, zEndpoint* ep, zPD* pd, int thread_id) {
    // zEndpoint *ep = zEP_create(config_file);
    // zPD *pd = zPD_create(ep, 1);
    rkeyTable *table = new rkeyTable();
    vector<zQP*> qps;
    vector<zQP*> rpc_qps;
    zTargetConfig config;
    load_config(remote_config_file.c_str(), &config);
    for(int i = 0; i < config.num_nodes; i ++) {
        zQP* qp = zQP_create(pd, ep, table, ZQP_ONESIDED);
        zQP_connect(qp, 0, config.target_ips[i], config.target_ports[i]);
        qps.push_back(qp);
        zQP* rpc_qp = zQP_create(pd, ep, table, ZQP_RPC);
        zQP_connect(rpc_qp, 0, config.target_ips[i], config.target_ports[i]);
        rpc_qps.push_back(rpc_qp);
    }
    size_t alloc_size = 1024;
    for(int i = 0; i < qps.size(); i ++) {
        uint64_t addr;
        uint32_t rkey;
        zQP_RPC_Alloc(rpc_qps[i], &addr, &rkey, alloc_size);
        printf("Allocated memory at addr: %lx, rkey: %u\n", addr, rkey);
        void* local_buf; 
        ibv_mr* mr = mr_malloc_create(pd, (uint64_t&)local_buf, alloc_size);
         if (mr == NULL) {
            printf("Memory registration failed\n");
            return;
        }
         if (table->find(rkey) == table->end()) {
            printf("RKey %u not found in rkey table\n", rkey);
            return;
        }
        printf("Local MR lkey: %u\n", mr->lkey);
        printf("Remote RKey list: ");
        for(auto rk : table->at(rkey)) {
            printf("%u ", rk);
        }
        printf("\n");
        memset(local_buf, 1, alloc_size);
        std::thread* t = nullptr;
        std::vector<uint64_t> wr_ids;
        int niter = 1000;
        if (const char *iters_env = getenv("VARUNA_ITERS"))
        {
            niter = atoi(iters_env);
            if (niter < 1)
            {
                niter = 1;
            }
        }
        auto star_time = TIME_NOW;
        for(int k = 0; k < niter; k++) {
            for(int j = 0; j < alloc_size ; j++){
                z_write_async(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j), rkey, &wr_ids);
                if (thread_id == 0 && inject_fault_enabled() && t == nullptr && k == 0 && j == 64) {
                    printf("Injecting link down on %s after 64 writes, device=%d\n",
                           g_fault_ifname.c_str(), qps[i]->current_device);
                    t = new std::thread(set_link, g_fault_ifname, false);
                }
                if (wr_ids.size() >= 64) {
                    z_poll_completion(qps[i], &wr_ids);
                }
            }
            z_poll_completion(qps[i], &wr_ids);
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        double throughput = (double)(alloc_size * niter) / (total_us);
        printf("Thread %d, QP %d, Time spend: %f us, Throughput: %f MB/s, device=%d\n",
               thread_id, i, total_us, throughput, qps[i]->current_device);
        if (t != nullptr) {
            usleep(500000);
        }
        memset(local_buf, 0, alloc_size);
        int read_fail = 0;
        for(int j = 0; j < alloc_size; j++){
            if (z_read(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j * sizeof(char)), rkey) != 0) {
                read_fail++;
            }
        }
        int mismatch = 0;
        for(int j = 0; j < alloc_size; j++) {
            if(((char*)local_buf)[j] != 1) {
                mismatch++;
            }
        }
        printf("Read verify: fail=%d mismatch=%d/%zu device=%d\n",
               read_fail, mismatch, alloc_size, qps[i]->current_device);

        sleep(1);
        pthread_barrier_wait(&barrier_start);
        int cas_fail = 0;
        for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
            if (z_CAS(qps[i], ((uint64_t*)local_buf)+j, mr->lkey, 2, (void*)(addr + j * sizeof(uint64_t)), rkey) != 0) {
                cas_fail++;
            }
        }
        memset(local_buf, 0, alloc_size);
        z_read(qps[i], local_buf, mr->lkey, alloc_size, (void*)addr, rkey);
        int cas_mismatch = 0;
        for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
            if(((uint64_t*)local_buf)[j] != 2) {
                cas_mismatch++;
            }
        }
        printf("CAS verify: fail=%d mismatch=%d/%zu device=%d\n",
               cas_fail, cas_mismatch, alloc_size / sizeof(uint64_t), qps[i]->current_device);
        if(t != nullptr) {
            t->join();
        }
        if (mismatch || cas_mismatch || read_fail) {
            printf("Test failed\n");
            return;
        }
    }
    printf("Test completed successfully\n");
}

void test_zQP(string config_file, string remote_config_file, int thread_id) {
    zEndpoint *ep = zEP_create(config_file);
    zPD *pd = zPD_create(ep, 1);
    rkeyTable *table = new rkeyTable();
    vector<zQP*> qps;
    vector<zQP*> rpc_qps;
    zTargetConfig config;
    load_config(remote_config_file.c_str(), &config);
    for(int i = 0; i < config.num_nodes; i ++) {
        zQP* qp = zQP_create(pd, ep, table, ZQP_ONESIDED);
        zQP_connect(qp, 0, config.target_ips[i], config.target_ports[i]);
        qps.push_back(qp);
        zQP* rpc_qp = zQP_create(pd, ep, table, ZQP_RPC);
        zQP_connect(rpc_qp, 0, config.target_ips[i], config.target_ports[i]);
        rpc_qps.push_back(rpc_qp);
    }
    size_t alloc_size = 1024;
    for(int i = 0; i < qps.size(); i ++) {
        uint64_t addr;
        uint32_t rkey;
        zQP_RPC_Alloc(rpc_qps[i], &addr, &rkey, alloc_size);
        printf("Allocated memory at addr: %lx, rkey: %u\n", addr, rkey);
        void* local_buf; 
        ibv_mr* mr = mr_malloc_create(pd, (uint64_t&)local_buf, alloc_size);
         if (mr == NULL) {
            printf("Memory registration failed\n");
            return;
        }
         if (table->find(rkey) == table->end()) {
            printf("RKey %u not found in rkey table\n", rkey);
            return;
        }
        printf("Local MR lkey: %u\n", mr->lkey);
        printf("Remote RKey list: ");
        for(auto rk : table->at(rkey)) {
            printf("%u ", rk);
        }
        printf("\n");
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++){
        //     ((uint64_t*)local_buf)[j] = 1;
        // }
        memset(local_buf, 1, alloc_size);
        // system("sudo ip link set ens1f0 down");
        // system("sudo ip link set ens1f1 down");
        // usleep(3000);
        std::thread* t = nullptr;
        if(thread_id == 0 && inject_fault_enabled()) {
            t = new std::thread(set_link, g_fault_ifname, false);
        }
        int nic_index = 0;
        std::vector<uint64_t> wr_ids;
        auto star_time = TIME_NOW;
        for(int k = 0; k < 1000; k++) {
            for(int j = 0; j < alloc_size ; j++){
                // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
                z_write(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j), rkey);
                // z_write_async(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j), rkey, &wr_ids);
            }
            // z_poll_completion(qps[i], &wr_ids);
            // printf("Thread %d, QP %d, write iteration %d completed\n", thread_id, i, k);
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        double throughput = (double)(alloc_size * 1000) / (total_us);
        printf("Thread %d, QP %d, Time spend: %f us, Throughput: %f MB/s\n", thread_id, i, total_us, throughput);
        // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
        // z_write(qps[i], local_buf, mr->lkey, alloc_size, (void*)addr, rkey);
        memset(local_buf, 0, alloc_size);
        // zDCQP_read(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
        // z_read(qps[i], local_buf, mr->lkey, alloc_size, (void*)addr, rkey);
        for(int j = 0; j < alloc_size; j++){
            // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
            z_read(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j * sizeof(char)), rkey);
            // z_read_async(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j * sizeof(char)), rkey, &wr_ids);
        }
        // z_poll_completion(qps[i], &wr_ids);
        for(int j = 0; j < alloc_size; j++) {
            if(((char*)local_buf)[j] != 1) {
                printf("Data mismatch at byte %d: expected 1, got %d\n", j, ((char*)local_buf)[j]);
                break;
            }
        }

        sleep(1);
        pthread_barrier_wait(&barrier_start);
        for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
            // zDCQP_CAS(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, ((uint64_t*)local_buf)+j, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], 2, (void*)((uint64_t)addr + j * sizeof(uint64_t)), table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
            z_CAS(qps[i], ((uint64_t*)local_buf)+j, mr->lkey, 2, (void*)(addr + j * sizeof(uint64_t)), rkey);
        }
        memset(local_buf, 0, alloc_size);
        // sleep(10);
        z_read(qps[i], local_buf, mr->lkey, alloc_size, (void*)addr, rkey);
        for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
            if(((uint64_t*)local_buf)[j] != 2) {
                printf("CAS Data mismatch at uint64 %d, value: %lu\n", j, ((uint64_t*)local_buf)[j]);
                // break;
            }
        }
        if(t != nullptr) {
            t->join();
        }
    }
    printf("Test completed successfully\n");
}

int main(int argc, char** argv) {
    if(argc < 4) {
        printf("Usage: %s <local_config_file> <remote_config_file> <thread_num>\n", argv[0]);
        return -1;
    }
    string config_file = argv[1];
    string remote_config_file = argv[2];
    int thread_num = atoi(argv[3]);
    zDeviceConfig local_cfg;
    if (load_config(config_file.c_str(), &local_cfg) != 0 || local_cfg.num_devices < 1)
    {
        printf("Failed to load local config %s\n", config_file.c_str());
        return -1;
    }
    g_fault_ifname = local_cfg.eth_names[0];
    g_backup_ifname = local_cfg.num_devices > 1 ? local_cfg.eth_names[1] : local_cfg.eth_names[0];
    set_link(g_fault_ifname, true);
    set_link(g_backup_ifname, true);
    sleep(1);
    pthread_barrier_init(&barrier_start, NULL, thread_num);
    std::vector<std::thread*> threads;
    zEndpoint *ep = zEP_create(config_file);
    zPD *pd = zPD_create(ep, 1);
    for(int i = 0; i < thread_num; i ++) {
        // threads.push_back(new std::thread(test_zQP, config_file, remote_config_file, i));
        threads.push_back(new std::thread(test_zQP_shared, config_file, remote_config_file, ep, pd, i));
    }
    for(int i = 0; i < thread_num; i ++) {
        threads[i]->join();
    }
    pthread_barrier_destroy(&barrier_start);
    return 0;
}