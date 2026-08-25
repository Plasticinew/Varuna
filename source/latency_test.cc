#include "zQP.h"
#include <fstream>
#include <random>
#include <gperftools/profiler.h>

using namespace zrdma;

// #define SHARED_EP_NUM 4

pthread_barrier_t barrier_start;

std::atomic<uint64_t> global_counter{0};
std::atomic<uint64_t> global_latency{0};
double bandwidth[16];
double recovery_latency[16];
string g_fault_ifname;
string g_backup_ifname;
string g_fault_cidr;
// #define SIMPLE_TEST
// #define PRECONNECT_BACKUP

enum TestType
{
    READ_SYNC,
    READ_ASYNC,
    READ_BATCH,
    WRITE_SYNC,
    WRITE_ASYNC,
    WRITE_BATCH,
    CAS_SYNC,
    CAS_ASYNC,
    CAS_BATCH
};

int request_number = 100000;
int async_number = 10;
int batch_number = 16;

void nic_shutdown_handler()
{
    // usleep(rand() % 500000 + 500000);
    usleep(3000000);
    if (g_fault_ifname.empty())
    {
        return;
    }
    const char *v = getenv("VARUNA_INJECT_FAULT");
    if (v != NULL && v[0] == '0')
    {
        return;
    }
    string cmd = string("sudo ip link set ") + g_fault_ifname + " down";
    system(cmd.c_str());
    printf("%s down\n", g_fault_ifname.c_str());
}

void bandwidth_counting_thread(tbb::concurrent_vector<std::atomic<uint64_t> *> &counter_vec)
{
    std::ofstream log_file("bandwidth_log.txt", std::ios::app);
    uint64_t zero_counter = 0;
    while (true)
    {   
        uint64_t start_size = 0;
        for (auto counter : counter_vec)
        {
            start_size += counter->load();
        }
        usleep(1000); // 100 ms
        uint64_t end_size = 0;
        for (auto counter : counter_vec)
        {
            end_size += counter->load();
        }
        double mbps = (end_size - start_size) * 1000 / (1024.0 * 1024.0);
        // print to log file
        if(mbps < 1.0) {
            zero_counter++;
        } else {
            zero_counter = 0;
        }
        if(zero_counter <= 10) {
            log_file << mbps << std::endl;        
        }
    }
}

// const int wr_num = 64;

ibv_send_wr *gen_wr(uint64_t local_addr, uint32_t lkey, uint64_t remote_addr, uint32_t rkey, uint64_t compare,
                    uint64_t swap, ibv_send_wr *wr, ibv_sge *sge, ibv_wr_opcode opcode, uint64_t size)
{
    for (int i = 0; i < batch_number; i++)
    {
        memset(&wr[i], 0, sizeof(ibv_send_wr));
        memset(&sge[i], 0, sizeof(ibv_sge));
        wr[i].sg_list = &sge[i];
        wr[i].sg_list->addr = local_addr + i * size;
        wr[i].sg_list->length = size;
        wr[i].sg_list->lkey = lkey;
        wr[i].opcode = opcode;
        if (opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
        {
            if( i % 4 == 0){
                wr[i].wr.atomic.remote_addr = remote_addr + i * size;
                wr[i].wr.atomic.compare_add = compare;
                wr[i].wr.atomic.swap = swap;
                wr[i].wr.atomic.rkey = rkey;
                wr[i].opcode = opcode;
            }
            else {
                wr[i].opcode = IBV_WR_RDMA_READ;
                wr[i].wr.rdma.remote_addr = remote_addr + i * size;
                wr[i].wr.rdma.rkey = rkey;
            }
        }
        // if (opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
        // {
        //     wr[i].wr.atomic.remote_addr = remote_addr + i * size;
        //     wr[i].wr.atomic.compare_add = compare;
        //     wr[i].wr.atomic.swap = swap;
        //     wr[i].wr.atomic.rkey = rkey;
        //     wr[i].opcode = opcode;
            
        // }
        else if (opcode == IBV_WR_RDMA_WRITE)
        {
            wr[i].wr.rdma.remote_addr = remote_addr + i * size;
            wr[i].wr.rdma.rkey = rkey;
        }
        else if (opcode == IBV_WR_RDMA_READ)
        {
            wr[i].wr.rdma.remote_addr = remote_addr + i * size;
            wr[i].wr.rdma.rkey = rkey;
        }
        wr[i].num_sge = 1;
        // wr[i].opcode = opcode;
        if (i < batch_number - 1)
        {
            wr[i].next = &wr[i + 1];
        }
        else
        {
            wr[batch_number - 1].next = nullptr;
            wr[batch_number - 1].send_flags = IBV_SEND_SIGNALED;
        }
    }
    // ibv_send_wr* new_wr = new ibv_send_wr;
    // ibv_sge* new_sge = new ibv_sge;
    // memset(new_wr, 0, sizeof(ibv_send_wr));
    // memset(new_sge, 0, sizeof(ibv_sge));
    // new_wr->sg_list = new_sge;
    // new_wr->sg_list->addr = local_addr + wr_num * 8;
    // new_wr->sg_list->length = 65536;
    // new_wr->sg_list->lkey = lkey;
    // new_wr->opcode = IBV_WR_RDMA_WRITE;
    // new_wr->wr.rdma.remote_addr = remote_addr + wr_num * 8;
    // new_wr->wr.rdma.rkey = rkey;
    // new_wr->num_sge = 1;
    // new_wr->send_flags = IBV_SEND_SIGNALED;
    // wr[wr_num - 1].next = new_wr;
    return wr;
    // return new_wr;
}

#ifdef SHARED_EP_NUM
void test_zQP_shared_p2p(string config_file, string remote_config_file, zEndpoint *ep, zPD *pd, int thread_id, tbb::concurrent_vector<std::atomic<uint64_t> *> &counter_vec, int thread_count, int write_size, TestType test_type)
{
#else
void test_zQP_shared_p2p(string config_file, string remote_config_file, int thread_id,
                         tbb::concurrent_vector<std::atomic<uint64_t> *> &counter_vec, int thread_count, int write_size, TestType test_type)
{
#endif
    rkeyTable *table = new rkeyTable();
    vector<zQP *> qps;
    zTargetConfig config;
#ifndef SHARED_EP_NUM
    zEndpoint *ep = zEP_create(config_file);
    zPD *pd = zPD_create(ep, 1);
#endif
    load_config(remote_config_file.c_str(), &config);
    zQP *rpc_qp = zQP_create(pd, ep, table, ZQP_RPC);
    zQP_connect(rpc_qp, 0, config.target_ips[0], config.target_ports[0]);
    int qp_per_thread = thread_count / thread_count;
    for (int i = 0; i < qp_per_thread; i++)
    {
        zQP *qp = zQP_create(pd, ep, table, ZQP_ONESIDED);
        zQP_connect(qp, 0, config.target_ips[0], config.target_ports[0]);
#ifdef PRECONNECT_BACKUP
        zQP_connect(qp, 1, qp->m_targets[1]->ip, qp->m_targets[1]->port);
#endif
        qps.push_back(qp);
        counter_vec.push_back(&qp->size_counter_);
    }
    size_t alloc_size = 1024 * 1024 * 64;
    // size_t write_size = 16384;
    uint64_t addr;
    uint32_t rkey;
    zQP_RPC_Alloc(rpc_qp, &addr, &rkey, alloc_size);
    void *local_buf;
    ibv_mr *mr = mr_malloc_create(pd, (uint64_t &)local_buf, alloc_size);
    if (mr == NULL)
    {
        printf("Memory registration failed\n");
        return;
    }
    if (table->find(rkey) == table->end())
    {
        printf("RKey %u not found in rkey table\n", rkey);
        return;
    }
    // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++){
    //     ((uint64_t*)local_buf)[j] = 1;
    // }
    memset(local_buf, 0, alloc_size);
    std::thread *t;

    int nic_index = 0;
    std::vector<uint64_t> wr_ids;
    // using random qps
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0, qp_per_thread - 1);
    uint64_t *counter = (uint64_t *)local_buf;
    for (int i = 0; i < batch_number; i++)
    {
        counter[i] = 0;
    }
    uint64_t *next = (uint64_t *)local_buf;
    // if(write_size > 8192) {
    //     next = counter + (8192 / sizeof(uint64_t)) - 1;
    // }
    ibv_send_wr *new_wr = new ibv_send_wr[batch_number];
    ibv_sge *new_sge = new ibv_sge[batch_number];
    // z_simple_write(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
    pthread_barrier_wait(&barrier_start);
    if (thread_id == 0)
    {
        // t = new std::thread(system, "sudo ip link set ens1f0 up");
        // t = new std::thread(system, "sudo ip link set ens3f0 down");
        t = new std::thread(nic_shutdown_handler);
    }
    pthread_barrier_wait(&barrier_start);
    auto star_time_bandwidth = TIME_NOW;
    switch (test_type)
    {
    case WRITE_SYNC:
    {
        auto star_time = TIME_NOW;
        for (int k = 0; k < request_number / (async_number); k++)
        {
            // int i = distribution(generator);
            int result;
            for (int j = 0; j < async_number; j++)
            {

                // *counter = k*1024 + j;
                // int prev[wr_num];
                // for(int i = 0; i < wr_num; i++) {
                //     prev[i] = counter[i];
                // }
                int target = k * batch_number + j + 1;
                *counter = target;
                // if(write_size > 4000) {
                //     int slice = write_size / 4000;
                //     for(int s = 1; s < slice; s++) {
                //         *(next + s * (4000 / sizeof(uint64_t)) - 1) = target;
                //     }
                // }
                // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
                // z_write_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_simple_read_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_read_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_simple_write_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // int result = z_simple_write(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
#ifdef SIMPLE_TEST
                result = z_simple_write(qps[0], ((char *)local_buf), mr->lkey, write_size, (void *)(addr), rkey);
#else
                int result = z_write(qps[0], ((char *)local_buf), mr->lkey, write_size, (void *)(addr), rkey);
#endif
                // ibv_send_wr *wr = gen_wr((uint64_t)local_buf, mr->lkey, addr, rkey, *counter, target, new_wr, new_sge);
                // int result = z_post_send(qps[0], wr, nullptr, true, 0, true);
                // int result = z_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                // int result = z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                // result = z_simple_CAS(qps[0], ((char*)local_buf), mr->lkey, target, (void*)(addr), rkey);
                // result = z_CAS(qps[0], ((char*)local_buf)+j*sizeof(uint64_t), mr->lkey, target, (void*)(addr)+j*sizeof(uint64_t), rkey);
                // result = z_CAS_async(qps[0], ((char*)local_buf)+j*sizeof(uint64_t), mr->lkey, target, (void*)(addr)+j*sizeof(uint64_t), rkey, &wr_ids);
                // printf("%d:%d\n", *counter, target);
                // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
                // printf("%d:%d\n", *counter, target);
                // for(int i = 0; i < wr_num; i++) {
                //     if(counter[i] != prev[i]){
                //         printf("CAS %d error! %lu -> %lu\n", i, prev[i], counter[i]);
                //     }
                // }
                // if(result < 0) {
                //     z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                //     // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
                //     // printf("%d:%d\n", *counter, target);
                //     if(*counter == target) {
                //         if(write_size > 4000) {            
                //             int slice = write_size / 4000;
                //             bool complete = true;
                //             for(int s = 1; s < slice; s++) {
                //                 if(*(next + s * (4000 / sizeof(uint64_t)) - 1) != target){
                //                     printf("oh....");
                //                     complete = false;
                //                     break;
                //                 }
                //             }
                //             if(complete) {
                //                 global_counter.fetch_add(1);
                //             } 
                //         } else {
                //             global_counter.fetch_add(1);
                //         }
                //     }
                //     break;
                // }
                // for(int i = 0; i < wr_num; i++) {
                //     counter[i] = target;
                // }
                // *(counter) = target;
                // z_CAS(qps[i], ((uint64_t*)local_buf), mr->lkey, 2, (void*)(addr), rkey);
                // z_write_async(qps[i], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
            }
            // if(result < 0) {
            //     break;
            // }
            // auto send_time = TIME_NOW;
            // z_poll_completion(qps[0], &wr_ids);
            // double write_us = TIME_DURATION_US(star_time, send_time);
            // double poll_us = TIME_DURATION_US(send_time, end_time);
            // printf("send time: %lf us, poll time: %lf us, total time: %lf us\n", write_us, poll_us, total_us);
            // printf("send time: %lf us, poll time: %lf us, total time: %lf us\n", TIME_DURATION_US(star_time, send_time), TIME_DURATION_US(send_time, end_time), total_us);
            // usleep(100);
            // printf("Thread %d, QP %d, write iteration %d completed\n", thread_id, i, k);
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        global_latency.fetch_add(total_us);
        auto end_time_bandwidth = TIME_NOW;
        double total_us_bandwidth = TIME_DURATION_US(star_time_bandwidth, end_time_bandwidth);
        uint64_t total_mbs = ((uint64_t)(request_number)*write_size) / (1024 * 1024);
        bandwidth[thread_id] = (double)(total_mbs) / (total_us_bandwidth / 1000000);
    }
    break;
    case WRITE_ASYNC:
    {
        auto star_time = TIME_NOW;
        for (int k = 0; k < request_number / (async_number); k++)
        {
            // int i = distribution(generator);
            int result;
            for (int j = 0; j < async_number; j++)
            {

                // *counter = k*1024 + j;
                // int prev[wr_num];
                // for(int i = 0; i < wr_num; i++) {
                //     prev[i] = counter[i];
                // }
                // int target = k * batch_number + j + 1;
                // *counter = target;
                // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
                // z_write_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_simple_read_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_read_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_simple_write_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // int result = z_simple_write(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
#ifdef SIMPLE_TEST
                z_simple_write_async(qps[0], ((char *)local_buf + j * write_size), mr->lkey, write_size, (void *)(addr + j * write_size), rkey, &wr_ids);
#else
                z_write_async(qps[0], ((char *)local_buf + j * write_size), mr->lkey, write_size, (void *)(addr + j * write_size), rkey, &wr_ids);
#endif
                // ibv_send_wr *wr = gen_wr((uint64_t)local_buf, mr->lkey, addr, rkey, *counter, target, new_wr, new_sge);
                // int result = z_post_send(qps[0], wr, nullptr, true, 0, true);
                // int result = z_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                // int result = z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                // result = z_simple_CAS(qps[0], ((char*)local_buf), mr->lkey, target, (void*)(addr), rkey);
                // result = z_CAS(qps[0], ((char*)local_buf)+j*sizeof(uint64_t), mr->lkey, target, (void*)(addr)+j*sizeof(uint64_t), rkey);
                // result = z_CAS_async(qps[0], ((char*)local_buf)+j*sizeof(uint64_t), mr->lkey, target, (void*)(addr)+j*sizeof(uint64_t), rkey, &wr_ids);
                // printf("%d:%d\n", *counter, target);
                // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
                // printf("%d:%d\n", *counter, target);
                // for(int i = 0; i < wr_num; i++) {
                //     if(counter[i] != prev[i]){
                //         printf("CAS %d error! %lu -> %lu\n", i, prev[i], counter[i]);
                //     }
                // }
                // if(result < 0) {
                //     // printf("%d:%d\n", *counter, target);
                //     z_read(qps[0], ((char*)local_buf), mr->lkey, 8 * wr_num, (void*)(addr), rkey);
                //     for(int i = 0; i < wr_num; i++) {
                //         if(counter[i] != target) {
                //             printf("After failure, CAS %d error! local %lu, remote %lu\n", i, counter[i], *((uint64_t*)((char*)local_buf + i * 8)));
                //             counter[i] = target;
                //         }
                //     }
                //     // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                //     // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
                //     // printf("%d:%d\n", *counter, target);
                //     // if(*counter == target) {
                //     //     global_counter.fetch_add(1);
                //     //     // printf("no need to send\n");
                //     // }
                //     // break;
                // }
                // for(int i = 0; i < wr_num; i++) {
                //     counter[i] = target;
                // }
                // *(counter) = target;
                // z_CAS(qps[i], ((uint64_t*)local_buf), mr->lkey, 2, (void*)(addr), rkey);
                // z_write_async(qps[i], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
            }
            // auto send_time = TIME_NOW;
            z_poll_completion(qps[0], &wr_ids);
            // double write_us = TIME_DURATION_US(star_time, send_time);
            // double poll_us = TIME_DURATION_US(send_time, end_time);
            // printf("send time: %lf us, poll time: %lf us, total time: %lf us\n", write_us, poll_us, total_us);
            // printf("send time: %lf us, poll time: %lf us, total time: %lf us\n", TIME_DURATION_US(star_time, send_time), TIME_DURATION_US(send_time, end_time), total_us);
            // usleep(100);
            // printf("Thread %d, QP %d, write iteration %d completed\n", thread_id, i, k);
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        global_latency.fetch_add(total_us);
        auto end_time_bandwidth = TIME_NOW;
        double total_us_bandwidth = TIME_DURATION_US(star_time_bandwidth, end_time_bandwidth);
        uint64_t total_mbs = (uint64_t)((uint64_t)(request_number)*write_size) / (1024 * 1024);
        bandwidth[thread_id] = (double)(total_mbs) / (total_us_bandwidth / 1000000);
    }
    break;
    case WRITE_BATCH:
    {
        // for(int k = 0; k < 2; k++) {
        //     int target = k % 2;
        //     memset(counter + k * batch_number * write_size / sizeof(uint64_t), target, batch_number * write_size);
        // }

        auto star_time = TIME_NOW;
        for (int k = 0; k < request_number / (batch_number); k++)
        {
            auto recovery_time = TIME_NOW;
            // int prev[batch_number];
            int target = k * batch_number +1;
            *(counter) = target;
            // int target = k % 2;
            // for(int i = 0; i < batch_number; i++) {
            //     *(counter + i * write_size) = target;
            // }
            uint32_t new_lkey = mr->lkey;
            uint32_t new_rkey = rkey;
#ifdef SIMPLE_TEST
            if (qps[0]->current_device != 0)
            {
                tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                qps[0]->m_pd->m_lkey_table.find(a, new_lkey);
                new_lkey = a->second[qps[0]->current_device];
                new_rkey = qps[0]->m_rkey_table->at(rkey)[qps[0]->current_device];
            }
#endif
            // ibv_send_wr *wr = gen_wr((uint64_t)(local_buf + (k%2) * write_size * batch_number), new_lkey, addr, new_rkey, *counter, 0, new_wr, new_sge, IBV_WR_RDMA_WRITE, write_size);
            ibv_send_wr *wr = gen_wr((uint64_t)(local_buf), new_lkey, addr, new_rkey, *counter, 0, new_wr, new_sge, IBV_WR_RDMA_WRITE, write_size);
#ifdef SIMPLE_TEST
            int result = ibv_post_send(qps[0]->m_requestors[0]->qp_, wr, nullptr);
            auto start = TIME_NOW;
            struct ibv_wc wc;
            ibv_cq *cq = qps[0]->m_requestors[0]->cq_;
            while (true)
            {
                if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
                {
                    std::cerr << "Error, read timeout" << std::endl;
                    z_switch(qps[0]);
                    break;
                }
                if (ibv_poll_cq(cq, 1, &wc) > 0)
                {
                    // result = 0;
                    if(wc.status != IBV_WC_SUCCESS) {
                        std::cerr << "CAS error, status: " << wc.status << std::endl;
                        z_switch(qps[0]);
                        result = -1;
                    }
                    break;
                }
            }
#else
            ibv_send_wr *bad_wr;
            int result = z_post_send(qps[0], wr, &bad_wr, true, 0, true);
#endif
            if(result < 0) {
                recovery_latency[thread_id] += TIME_DURATION_US(recovery_time, TIME_NOW);
                // printf("%d:%d\n", *counter, target);
                // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size * batch_number, (void*)(addr), rkey);
                // for(int i = 0; i < batch_number; i++) {
                //     bool post_failure = true;
                //     char* accessor = (char*)counter + i * (write_size);
                //     for(int j = 0; j < write_size; j++) {
                //         if(*( accessor + j ) != target) {
                //             // printf("%d : %d\n", *( accessor + j ), target);
                //             post_failure = false;
                //         } 
                //     }
                //     if(post_failure) {
                //         global_counter.fetch_add(1);
                //     }
                // }
                // break;
            }
            if(TIME_DURATION_US(star_time, TIME_NOW) > 10000000) {
                break;
            }
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        global_latency.fetch_add(total_us);
        auto end_time_bandwidth = TIME_NOW;
        double total_us_bandwidth = TIME_DURATION_US(star_time_bandwidth, end_time_bandwidth);
        uint64_t total_mbs = (uint64_t)((uint64_t)(request_number)*write_size) / (1024 * 1024);
        bandwidth[thread_id] = (double)(total_mbs) / (total_us_bandwidth / 1000000);
    }
    break;
    case CAS_SYNC:
    {
        auto star_time = TIME_NOW;
        for (int k = 0; k < request_number / (async_number); k++)
        {
            // int i = distribution(generator);
            int result;
            for (int j = 0; j < async_number; j++)
            {

                // *counter = k*1024 + j;
                // int prev[async_number];
                // for(int i = 0; i < async_number; i++) {
                //     prev[i] = counter[i];
                // }
                int target = k * batch_number + j + 1;
                // *counter = target;
                // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
                // z_write_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_simple_read_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_read_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_simple_write_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // int result = z_simple_write(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
#ifdef SIMPLE_TEST
                result = z_simple_CAS(qps[0], ((char*)local_buf), mr->lkey, target, (void*)(addr), rkey);
#else
                result = z_CAS(qps[0], ((char*)local_buf)+j*sizeof(uint64_t), mr->lkey, target, (void*)(addr)+j*sizeof(uint64_t), rkey);
#endif
                // ibv_send_wr *wr = gen_wr((uint64_t)local_buf, mr->lkey, addr, rkey, *counter, target, new_wr, new_sge);
                // int result = z_post_send(qps[0], wr, nullptr, true, 0, true);
                // int result = z_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                // int result = z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                // result = z_simple_CAS(qps[0], ((char*)local_buf), mr->lkey, target, (void*)(addr), rkey);
                // result = z_CAS(qps[0], ((char*)local_buf)+j*sizeof(uint64_t), mr->lkey, target, (void*)(addr)+j*sizeof(uint64_t), rkey);
                // result = z_CAS_async(qps[0], ((char*)local_buf)+j*sizeof(uint64_t), mr->lkey, target, (void*)(addr)+j*sizeof(uint64_t), rkey, &wr_ids);
                // printf("%d:%d\n", *counter, target);
                // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
                // printf("%d:%d\n", *counter, target);
                // for(int i = 0; i < wr_num; i++) {
                //     if(counter[i] != prev[i]){
                //         printf("CAS %d error! %lu -> %lu\n", i, prev[i], counter[i]);
                //     }
                // }
                // if(result < 0) {
                //     printf("%d:%d\n", *counter, target);
                //     // z_read(qps[0], ((char*)local_buf), mr->lkey, 8 * wr_num, (void*)(addr), rkey);
                //     // for(int i = 0; i < wr_num; i++) {
                //     //     if(counter[i] != target) {
                //     //         printf("After failure, CAS %d error! local %lu, remote %lu\n", i, counter[i], *((uint64_t*)((char*)local_buf + i * 8)));
                //     //         counter[i] = target;
                //     //     }
                //     }
                //     // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                //     // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
                //     // printf("%d:%d\n", *counter, target);
                //     // if(*counter == target) {
                //     //     global_counter.fetch_add(1);
                //     //     // printf("no need to send\n");
                //     // }
                //     // break;
                // }
                // for(int i = 0; i < wr_num; i++) {
                //     counter[i] = target;
                // }
                counter[j] = target;
                // z_CAS(qps[i], ((uint64_t*)local_buf), mr->lkey, 2, (void*)(addr), rkey);
                // z_write_async(qps[i], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
            }
            // auto send_time = TIME_NOW;
            // z_poll_completion(qps[0], &wr_ids);
            // double write_us = TIME_DURATION_US(star_time, send_time);
            // double poll_us = TIME_DURATION_US(send_time, end_time);
            // printf("send time: %lf us, poll time: %lf us, total time: %lf us\n", write_us, poll_us, total_us);
            // printf("send time: %lf us, poll time: %lf us, total time: %lf us\n", TIME_DURATION_US(star_time, send_time), TIME_DURATION_US(send_time, end_time), total_us);
            // usleep(100);
            // printf("Thread %d, QP %d, write iteration %d completed\n", thread_id, i, k);
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        global_latency.fetch_add(total_us);
        auto end_time_bandwidth = TIME_NOW;
        double total_us_bandwidth = TIME_DURATION_US(star_time_bandwidth, end_time_bandwidth);
        uint64_t total_mbs = ((uint64_t)(request_number)) / (1024);
        bandwidth[thread_id] = (double)(total_mbs) / (total_us_bandwidth / 1000000);
    }
    break;
    case CAS_ASYNC:
    {
        auto star_time = TIME_NOW;
        for (int k = 0; k < request_number / (async_number); k++)
        {
            // int i = distribution(generator);
            int result;
            for (int j = 0; j < async_number; j++)
            {

                // *counter = k*1024 + j;
                // int prev[wr_num];
                // for(int i = 0; i < wr_num; i++) {
                //     prev[i] = counter[i];
                // }
                // int target = k * batch_number + j + 1;
                // *counter = target;
                // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
                // z_write_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_simple_read_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_read_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // z_simple_write_async(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
                // int result = z_simple_write(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
#ifdef SIMPLE_TEST
                z_simple_write_async(qps[0], ((char *)local_buf + j * write_size), mr->lkey, write_size, (void *)(addr + j * write_size), rkey, &wr_ids);
#else
                z_write_async(qps[0], ((char *)local_buf + j * write_size), mr->lkey, write_size, (void *)(addr + j * write_size), rkey, &wr_ids);
#endif
                // ibv_send_wr *wr = gen_wr((uint64_t)local_buf, mr->lkey, addr, rkey, *counter, target, new_wr, new_sge);
                // int result = z_post_send(qps[0], wr, nullptr, true, 0, true);
                // int result = z_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                // int result = z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                // result = z_simple_CAS(qps[0], ((char*)local_buf), mr->lkey, target, (void*)(addr), rkey);
                // result = z_CAS(qps[0], ((char*)local_buf)+j*sizeof(uint64_t), mr->lkey, target, (void*)(addr)+j*sizeof(uint64_t), rkey);
                // result = z_CAS_async(qps[0], ((char*)local_buf)+j*sizeof(uint64_t), mr->lkey, target, (void*)(addr)+j*sizeof(uint64_t), rkey, &wr_ids);
                // printf("%d:%d\n", *counter, target);
                // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
                // printf("%d:%d\n", *counter, target);
                // for(int i = 0; i < wr_num; i++) {
                //     if(counter[i] != prev[i]){
                //         printf("CAS %d error! %lu -> %lu\n", i, prev[i], counter[i]);
                //     }
                // }
                // if(result < 0) {
                //     // printf("%d:%d\n", *counter, target);
                //     z_read(qps[0], ((char*)local_buf), mr->lkey, 8 * wr_num, (void*)(addr), rkey);
                //     for(int i = 0; i < wr_num; i++) {
                //         if(counter[i] != target) {
                //             printf("After failure, CAS %d error! local %lu, remote %lu\n", i, counter[i], *((uint64_t*)((char*)local_buf + i * 8)));
                //             counter[i] = target;
                //         }
                //     }
                //     // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
                //     // z_simple_read(qps[0], ((char*)local_buf), mr->lkey, sizeof(uint64_t), (void*)(addr), rkey);
                //     // printf("%d:%d\n", *counter, target);
                //     // if(*counter == target) {
                //     //     global_counter.fetch_add(1);
                //     //     // printf("no need to send\n");
                //     // }
                //     // break;
                // }
                // for(int i = 0; i < wr_num; i++) {
                //     counter[i] = target;
                // }
                // *(counter) = target;
                // z_CAS(qps[i], ((uint64_t*)local_buf), mr->lkey, 2, (void*)(addr), rkey);
                // z_write_async(qps[i], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey, &wr_ids);
            }
            // auto send_time = TIME_NOW;
            z_poll_completion(qps[0], &wr_ids);
            // double write_us = TIME_DURATION_US(star_time, send_time);
            // double poll_us = TIME_DURATION_US(send_time, end_time);
            // printf("send time: %lf us, poll time: %lf us, total time: %lf us\n", write_us, poll_us, total_us);
            // printf("send time: %lf us, poll time: %lf us, total time: %lf us\n", TIME_DURATION_US(star_time, send_time), TIME_DURATION_US(send_time, end_time), total_us);
            // usleep(100);
            // printf("Thread %d, QP %d, write iteration %d completed\n", thread_id, i, k);
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        global_latency.fetch_add(total_us);
        auto end_time_bandwidth = TIME_NOW;
        double total_us_bandwidth = TIME_DURATION_US(star_time_bandwidth, end_time_bandwidth);
        uint64_t total_mbs = (uint64_t)((uint64_t)(request_number)*write_size) / (1024 * 1024);
        bandwidth[thread_id] = (double)(total_mbs) / (total_us_bandwidth / 1000000);
    }
    break;
    case CAS_BATCH:
    {
        auto star_time = TIME_NOW;
        for (int k = 0; k < request_number / (batch_number); k++)
        {
            auto recovery_time = TIME_NOW;
            int prev[batch_number];
            for(int i = 0; i < batch_number; i++) {
                prev[i] = counter[i];
            }
            int target = k * batch_number + 1;
            uint32_t new_lkey = mr->lkey;
            uint32_t new_rkey = rkey;
#ifdef SIMPLE_TEST
            if (qps[0]->current_device != 0)
            {
                tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                qps[0]->m_pd->m_lkey_table.find(a, new_lkey);
                new_lkey = a->second[qps[0]->current_device];
                new_rkey = qps[0]->m_rkey_table->at(rkey)[qps[0]->current_device];
            }
#endif
            ibv_send_wr *wr = gen_wr((uint64_t)local_buf, new_lkey, addr, new_rkey, *counter, target, new_wr, new_sge, IBV_WR_ATOMIC_CMP_AND_SWP, write_size);
#ifdef SIMPLE_TEST
            int result = ibv_post_send(qps[0]->m_requestors[qps[0]->current_device]->qp_, wr, nullptr);
            auto start = TIME_NOW;
            struct ibv_wc wc;
            ibv_cq *cq = qps[0]->m_requestors[qps[0]->current_device]->cq_;
            while (true)
            {
                if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
                {
                    std::cerr << "Error, read timeout" << std::endl;
                    z_switch(qps[0]);
                    break;
                }
                if (ibv_poll_cq(cq, 1, &wc) > 0)
                {
                    // result = 0;
                    if(wc.status != IBV_WC_SUCCESS) {
                        std::cerr << "CAS error, status: " << wc.status << std::endl;
                        z_switch(qps[0]);
                        result = -1;
                    }
                    break;
                }
            }
#else
            ibv_send_wr *bad_wr;
            int result = z_post_send(qps[0], wr, &bad_wr, true, 0, false);
#endif
            if(result < 0) {
                recovery_latency[thread_id] += TIME_DURATION_US(recovery_time, TIME_NOW);
            //     // printf("%d:%d\n", *counter, target);
            //     z_simple_read(qps[0], ((char*)local_buf), mr->lkey, 8 * batch_number, (void*)(addr), rkey);
            //     bool post_failure = false;
            //     for(int i = 0; i < batch_number; i++) {
            //         if(counter[i] != target) {
            //             counter[i] = target;
            //         } else {
            //             global_counter.fetch_add(1);

            //             // post_failure = true;
            //         }
            //     }
                
            //     // if(post_failure) {
            //     //     global_counter.fetch_add(1);
            //     // }
            }
            if(TIME_DURATION_US(star_time, TIME_NOW) > 10000000) {
                break;
            }
            for(int i = 0; i < batch_number; i++) {
                counter[i] = target;
            }
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        global_latency.fetch_add(total_us);
        auto end_time_bandwidth = TIME_NOW;
        double total_us_bandwidth = TIME_DURATION_US(star_time_bandwidth, end_time_bandwidth);
        uint64_t total_mbs = (uint64_t)((uint64_t)(request_number)) / (1024);
        bandwidth[thread_id] = (double)(total_mbs) / (total_us_bandwidth / 1000000);
    }
    break;
        case READ_SYNC:
    {
        auto star_time = TIME_NOW;
        for (int k = 0; k < request_number / (async_number); k++)
        {
            // int i = distribution(generator);
            int result;
            for (int j = 0; j < async_number; j++)
            {

#ifdef SIMPLE_TEST
                int result = z_simple_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
#else
                int result = z_read(qps[0], ((char*)local_buf), mr->lkey, write_size, (void*)(addr), rkey);
#endif
            }
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        global_latency.fetch_add(total_us);
        auto end_time_bandwidth = TIME_NOW;
        double total_us_bandwidth = TIME_DURATION_US(star_time_bandwidth, end_time_bandwidth);
        uint64_t total_mbs = ((uint64_t)(request_number)*write_size) / (1024 * 1024);
        bandwidth[thread_id] = (double)(total_mbs) / (total_us_bandwidth / 1000000);
    }
    break;
    case READ_ASYNC:
    {
        auto star_time = TIME_NOW;
        for (int k = 0; k < request_number / (async_number); k++)
        {
            // int i = distribution(generator);
            int result;
            for (int j = 0; j < async_number; j++)
            {
#ifdef SIMPLE_TEST
                z_simple_write_async(qps[0], ((char *)local_buf + j * write_size), mr->lkey, write_size, (void *)(addr + j * write_size), rkey, &wr_ids);
#else
                z_write_async(qps[0], ((char *)local_buf + j * write_size), mr->lkey, write_size, (void *)(addr + j * write_size), rkey, &wr_ids);
#endif
            }
            // auto send_time = TIME_NOW;
            z_poll_completion(qps[0], &wr_ids);
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        global_latency.fetch_add(total_us);
        auto end_time_bandwidth = TIME_NOW;
        double total_us_bandwidth = TIME_DURATION_US(star_time_bandwidth, end_time_bandwidth);
        uint64_t total_mbs = (uint64_t)((uint64_t)(request_number)*write_size) / (1024 * 1024);
        bandwidth[thread_id] = (double)(total_mbs) / (total_us_bandwidth / 1000000);
    }
    break;
    case READ_BATCH:
    {
        auto star_time = TIME_NOW;
        for (int k = 0; k < request_number / (batch_number); k++)
        {
            int target = k * batch_number + 1;
            ibv_send_wr *wr = gen_wr((uint64_t)local_buf, mr->lkey, addr, rkey, *counter, target, new_wr, new_sge, IBV_WR_RDMA_READ, write_size);
#ifdef SIMPLE_TEST
            int result = ibv_post_send(qps[0]->m_requestors[0]->qp_, wr, nullptr);
            auto start = TIME_NOW;
            struct ibv_wc wc;
            ibv_cq *cq = qps[0]->m_requestors[0]->cq_;
            while (true)
            {
                if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
                {
                    // std::cerr << "Error, read timeout" << std::endl;
                    break;
                }
                if (ibv_poll_cq(cq, 1, &wc) > 0)
                {
                    // result = 0;
                    break;
                }
            }
#else
            ibv_send_wr *bad_wr;
            int result = z_post_send(qps[0], wr, &bad_wr, true, 0, false);
#endif
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        global_latency.fetch_add(total_us);
        auto end_time_bandwidth = TIME_NOW;
        double total_us_bandwidth = TIME_DURATION_US(star_time_bandwidth, end_time_bandwidth);
        uint64_t total_mbs = (uint64_t)((uint64_t)(request_number)*write_size) / (1024 * 1024);
        bandwidth[thread_id] = (double)(total_mbs) / (total_us_bandwidth / 1000000);
    }
    break;
    }
    // double throughput = (double)(alloc_size * 1000) / (total_us);
    // double average_latency = total_us / (200000);
    // printf("Thread %d, Time spend: %f us, Average Latency: %f us, Throughput: %f MB/s\n", thread_id, total_us, average_latency, throughput);
    // memset(local_buf, 0, alloc_size);
    // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++){
    //     // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
    //     // z_read(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j * sizeof(char)), rkey);
    //     z_read_async(qps[i], ((uint64_t*)local_buf)+j, mr->lkey, sizeof(uint64_t), (void*)(addr + j * sizeof(uint64_t)), rkey, &wr_ids);
    // }
    // z_poll_completion(qps[i], &wr_ids);
    // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
    //     if(((uint64_t*)local_buf)[j] != 1) {
    //         printf("Data mismatch at uint64 %d: expected 1, got %lu\n", j, ((uint64_t*)local_buf)[j]);
    //         break;
    //     }
    // }

    // sleep(1);
    // pthread_barrier_wait(&barrier_start);
    // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
    //     // zDCQP_CAS(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, ((uint64_t*)local_buf)+j, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], 2, (void*)((uint64_t)addr + j * sizeof(uint64_t)), table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
    //     z_CAS(qps[i], ((uint64_t*)local_buf)+j, mr->lkey, 2, (void*)(addr + j * sizeof(uint64_t)), rkey);
    // }
    // memset(local_buf, 0, alloc_size);
    // // sleep(10);
    // z_read(qps[i], local_buf, mr->lkey, alloc_size, (void*)addr, rkey);
    // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
    //     if(((uint64_t*)local_buf)[j] != 2) {
    //         printf("CAS Data mismatch at uint64 %d, value: %lu\n", j, ((uint64_t*)local_buf)[j]);
    //         // break;
    //     }
    // }
    if (thread_id == 0)
    {
        t->join();
    }
    // printf("Test completed successfully\n");
}

// void test_zQP_shared(string config_file, string remote_config_file, zEndpoint* ep, zPD* pd, int thread_id, tbb::concurrent_vector<zQP*>& listen_qps ) {
void test_zQP_shared(string config_file, string remote_config_file, int thread_id, tbb::concurrent_vector<std::atomic<uint64_t> *> &counter_vec)
{
    rkeyTable *table = new rkeyTable();
    vector<zQP *> qps;
    vector<zQP *> rpc_qps;
    zTargetConfig config;
    zEndpoint *ep = zEP_create(config_file);
    zPD *pd = zPD_create(ep, 1);
    load_config(remote_config_file.c_str(), &config);
    for (int i = 0; i < config.num_nodes; i++)
    {
        zQP *qp = zQP_create(pd, ep, table, ZQP_ONESIDED);
        zQP_connect(qp, 0, config.target_ips[i], config.target_ports[i]);
        qps.push_back(qp);
        counter_vec.push_back(&qp->size_counter_);
        zQP *rpc_qp = zQP_create(pd, ep, table, ZQP_RPC);
        zQP_connect(rpc_qp, 0, config.target_ips[i], config.target_ports[i]);
        rpc_qps.push_back(rpc_qp);
    }
    size_t alloc_size = 1024 * 1024;
    for (int i = 0; i < qps.size(); i++)
    {
        uint64_t addr;
        uint32_t rkey;
        zQP_RPC_Alloc(rpc_qps[i], &addr, &rkey, alloc_size);
        // printf("Allocated memory at addr: %lx, rkey: %u\n", addr, rkey);
        void *local_buf;
        ibv_mr *mr = mr_malloc_create(pd, (uint64_t &)local_buf, alloc_size);
        if (mr == NULL)
        {
            printf("Memory registration failed\n");
            return;
        }
        if (table->find(rkey) == table->end())
        {
            printf("RKey %u not found in rkey table\n", rkey);
            return;
        }
        // printf("Local MR lkey: %u\n", mr->lkey);
        // printf("Remote RKey list: ");
        for (auto rk : table->at(rkey))
        {
            printf("%u ", rk);
        }
        printf("\n");
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++){
        //     ((uint64_t*)local_buf)[j] = 1;
        // }
        memset(local_buf, 1, alloc_size);
        std::thread *t;
        if (thread_id == 0)
        {
            // t = new std::thread(system, "sudo ip link set ens1f0 up");
            // t = new std::thread(system, "sudo ip link set ens3f0 down");
            t = new std::thread(nic_shutdown_handler);
        }
        int nic_index = 0;
        std::vector<uint64_t> wr_ids;
        auto star_time = TIME_NOW;
        for (int k = 0; k < 4000; k++)
        {
            for (int j = 0; j < alloc_size / (4096); j++)
            {
                // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
                // z_write(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j), rkey);
                z_write_async(qps[i], ((char *)local_buf) + j * 4096, mr->lkey, 4096, (void *)(addr), rkey, &wr_ids);
            }
            z_poll_completion(qps[i], &wr_ids);
            // printf("Thread %d, QP %d, write iteration %d completed\n", thread_id, i, k);
        }
        auto end_time = TIME_NOW;
        double total_us = TIME_DURATION_US(star_time, end_time);
        double throughput = (double)(alloc_size * 1000) / (total_us);
        double average_latency = total_us / (alloc_size * 1000);
        // printf("Thread %d, QP %d, Time spend: %f us, Average Latency: %f us, Throughput: %f MB/s\n", thread_id, i, total_us, average_latency, throughput);
        // memset(local_buf, 0, alloc_size);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++){
        //     // zDCQP_write(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, local_buf, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], alloc_size, (void*)addr, table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
        //     // z_read(qps[i], ((char*)local_buf)+j, mr->lkey, sizeof(char), (void*)(addr + j * sizeof(char)), rkey);
        //     z_read_async(qps[i], ((uint64_t*)local_buf)+j, mr->lkey, sizeof(uint64_t), (void*)(addr + j * sizeof(uint64_t)), rkey, &wr_ids);
        // }
        // z_poll_completion(qps[i], &wr_ids);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
        //     if(((uint64_t*)local_buf)[j] != 1) {
        //         printf("Data mismatch at uint64 %d: expected 1, got %lu\n", j, ((uint64_t*)local_buf)[j]);
        //         break;
        //     }
        // }

        // sleep(1);
        // pthread_barrier_wait(&barrier_start);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
        //     // zDCQP_CAS(qps[i]->m_pd->m_requestors[nic_index][0], qps[i]->m_targets[nic_index]->ah, ((uint64_t*)local_buf)+j, qps[i]->m_pd->m_lkey_table[mr->lkey][nic_index], 2, (void*)((uint64_t)addr + j * sizeof(uint64_t)), table->at(rkey)[nic_index], qps[i]->m_targets[nic_index]->lid_, qps[i]->m_targets[nic_index]->dct_num_);
        //     z_CAS(qps[i], ((uint64_t*)local_buf)+j, mr->lkey, 2, (void*)(addr + j * sizeof(uint64_t)), rkey);
        // }
        // memset(local_buf, 0, alloc_size);
        // // sleep(10);
        // z_read(qps[i], local_buf, mr->lkey, alloc_size, (void*)addr, rkey);
        // for(int j = 0; j < alloc_size / sizeof(uint64_t); j++) {
        //     if(((uint64_t*)local_buf)[j] != 2) {
        //         printf("CAS Data mismatch at uint64 %d, value: %lu\n", j, ((uint64_t*)local_buf)[j]);
        //         // break;
        //     }
        // }
        if (thread_id == 0)
        {
            t->join();
        }
    }
    printf("Test completed successfully\n");
}

static void restore_links()
{
    if (!g_fault_ifname.empty())
    {
        string cmd = string("sudo ip link set ") + g_fault_ifname + " up";
        system(cmd.c_str());
        if (!g_fault_cidr.empty())
        {
            string ip_cmd = string("sudo ifconfig ") + g_fault_ifname + " " + g_fault_cidr;
            system(ip_cmd.c_str());
        }
    }
    if (!g_backup_ifname.empty())
    {
        string cmd = string("sudo ip link set ") + g_backup_ifname + " up";
        system(cmd.c_str());
    }
}

int main(int argc, char **argv)
{
    std::ofstream log_file("bandwidth_log.txt");
    std::ofstream result_file("result.txt", std::ios::app);
    log_file.close();
    if (argc < 7)
    {
        printf("Usage: %s <local_config_file> <remote_config_file> <thread_num> <test_type> <size> <batch>\n", argv[0]);
        return -1;
    }
    ProfilerStart("latency_test.prof");
    string config_file = argv[1];
    string remote_config_file = argv[2];
    int thread_num = atoi(argv[3]);
    string test_type = argv[4];
    int size = atoi(argv[5]);
    batch_number = atoi(argv[6]);
    zDeviceConfig local_cfg;
    if (load_config(config_file.c_str(), &local_cfg) != 0 || local_cfg.num_devices < 1)
    {
        printf("Failed to load local config %s\n", config_file.c_str());
        return -1;
    }
    g_fault_ifname = local_cfg.eth_names[0];
    g_backup_ifname = local_cfg.num_devices > 1 ? local_cfg.eth_names[1] : local_cfg.eth_names[0];
    g_fault_cidr = local_cfg.ips[0] + "/24";
    restore_links();
    sleep(2);
    TestType test_type_enum;
    if (test_type == "read_sync")
    {
        test_type_enum = READ_SYNC;
    }
    else if (test_type == "read_async")
    {
        test_type_enum = READ_ASYNC;
    }
    else if (test_type == "read_batch")
    {
        test_type_enum = READ_BATCH;
    }
    else if (test_type == "write_sync")
    {
        test_type_enum = WRITE_SYNC;
    }
    else if (test_type == "write_async")
    {
        test_type_enum = WRITE_ASYNC;
    }
    else if (test_type == "write_batch")
    {
        test_type_enum = WRITE_BATCH;
    }
    else if (test_type == "cas_sync")
    {
        test_type_enum = CAS_SYNC;
    }
    else if (test_type == "cas_async")
    {
        test_type_enum = CAS_ASYNC;
    }
    else if (test_type == "cas_batch")
    {
        test_type_enum = CAS_BATCH;
    }
    else
    {
        printf("Invalid test type: %s\n", test_type.c_str());
        return -1;
    }
    std::vector<std::thread *> threads;
    tbb::concurrent_vector<std::atomic<uint64_t> *> counter_vec;
    // result_file << "Size\t" << " Latency\t" << "Bandwidth\t" << "Post-Failure-Ratio" <<std::endl;
    int test_size[] = {16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576};
    // for (int j = 1; j < 9; j++)
    for (int j = 1; j < 2; j++)
    {
#ifdef SHARED_EP_NUM
        zEndpoint *ep[SHARED_EP_NUM];
        for (int i = 0; i < SHARED_EP_NUM; i++)
        {
            ep[i] = zEP_create(config_file);
        }
        zPD *pd[SHARED_EP_NUM];
        for (int i = 0; i < SHARED_EP_NUM; i++)
        {
            pd[i] = zPD_create(ep[i], 1);
        }
#endif
        // thread_num = 16;
        // batch_number = j*4;
        pthread_barrier_init(&barrier_start, NULL, thread_num);
        for (int i = 0; i < thread_num; i++)
        {
            // threads.push_back(new std::thread(test_zQP, config_file, remote_config_file, i));
            // threads.push_back(new std::thread(test_zQP_shared_p2p, config_file, remote_config_file, i, std::ref(counter_vec), thread_num, test_size[j]));
#ifdef SHARED_EP_NUM
            threads.push_back(new std::thread(test_zQP_shared_p2p, config_file, remote_config_file, ep[i % SHARED_EP_NUM], pd[i % SHARED_EP_NUM], i, std::ref(counter_vec), thread_num, 8, test_type_enum));
#else
            threads.push_back(new std::thread(test_zQP_shared_p2p, config_file, remote_config_file, i, std::ref(counter_vec), thread_num, size, test_type_enum));
#endif
            // threads.push_back(new std::thread(test_zQP_shared, config_file, remote_config_file, i, std::ref(counter_vec)));
            // threads.push_back(new std::thread(test_zQP_shared, config_file, remote_config_file, ep, pd, i, std::ref(listen_qps)));
        }
        std::thread *bw_thread = new std::thread(bandwidth_counting_thread, std::ref(counter_vec));
        for (int i = 0; i < thread_num; i++)
        {
            threads[i]->join();
        }
        restore_links();
        threads.clear();
        sleep(2);
        // printf("%lf\n", global_counter.load()/(thread_num*1.0));
        // printf("Size %d Average Latency: %lf us\n", test_size[j], global_latency.load()/(400000.0*thread_num));
        // result_file << "Size " << test_size[j] << " Average Latency: " << global_latency.load() / (400000.0 * thread_num) << " us, Average Bandwidth: " << global_bandwidth.load() / (thread_num * 1.0) << " MB/s" << std::endl;
        double total_bandwidth = 0.0;
        double total_recovery_latency = 0.0;
        double tail_recovery_latency = 0.0;
        for (int i = 0; i < thread_num; i++)
        {
            total_bandwidth += bandwidth[i];
            total_recovery_latency += recovery_latency[i];
            if (tail_recovery_latency < recovery_latency[i])
            {
                tail_recovery_latency = recovery_latency[i];
            }
        }
        // result_file << test_size[j] << "\t" << 1.0 * global_latency.load() / (request_number * thread_num) << "\t" << total_bandwidth << std::endl;
        result_file << size << "\t" << thread_num << "\t" << batch_number << "\t" << 1.0 * global_latency.load() / (request_number * thread_num) << "\t" << total_bandwidth << "\t" << 1.0 * global_counter.load() / (thread_num * batch_number) << "\t" << total_recovery_latency / thread_num << "\t" << tail_recovery_latency << std::endl;
        // printf("%lf\n", global_latency.load() / (400000.0 * thread_num));
        global_latency.store(0);
        global_counter.store(0);
        counter_vec.clear();
#ifdef SHARED_EP_NUM
        for (int i = 0; i < SHARED_EP_NUM; i++)
        {
            zEP_destroy(ep[i]);
        }
        for (int i = 0; i < SHARED_EP_NUM; i++)
        {
            zPD_destroy(pd[i]);
        }
#endif
    }
    // printf("%lf\n", global_counter.load()/(1000*thread_num*1.0));
    pthread_barrier_destroy(&barrier_start);
    ProfilerStop();
    result_file.close();
    return 0;
}
