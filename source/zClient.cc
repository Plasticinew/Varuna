#include "zQP.h"

using namespace zrdma;

pthread_barrier_t barrier_start;

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
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++){
        //     ((uint64_t*)local_buf)[j] = 1;
        // }
        memset(local_buf, 1, alloc_size);
        // system("sudo ip link set ens1f0 down");
        // system("sudo ip link set ens1f1 down");
        // usleep(3000);
        std::thread* t;
        if(thread_id == 0) {
            t = new std::thread(system, "sudo ip link set ens1f0 down");
        }
        int nic_index = 0;
        std::vector<uint64_t> wr_ids;
        auto star_time = TIME_NOW;
        for(int k = 0; k < 1000; k++) {
            for(int j = 0; j < alloc_size ; j++){
                // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
                // z_write(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j), rkey);
                z_write_async(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j), rkey, &wr_ids);
            }
            z_poll_completion(qps[i], &wr_ids);
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
            // z_read(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j * sizeof(char)), rkey);
            z_read_async(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j * sizeof(char)), rkey, &wr_ids);
        }
        z_poll_completion(qps[i], &wr_ids);
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
        if(thread_id == 0) {
            t->join();
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
        std::thread* t;
        if(thread_id == 0) {
            t = new std::thread(system, "sudo ip link set ens1f0 down");
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
        if(thread_id == 0) {
            t->join();
        }
    }
    printf("Test completed successfully\n");
}

int main(int argc, char** argv) {
    system("sudo ip link set ens1f0 up");
    system("sudo ip link set ens1f1 up");
    sleep(1);
    if(argc < 4) {
        printf("Usage: %s <local_config_file> <remote_config_file> <thread_num>\n", argv[0]);
        return -1;
    }
    string config_file = argv[1];
    string remote_config_file = argv[2];
    int thread_num = atoi(argv[3]);
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