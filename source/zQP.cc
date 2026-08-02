#include <arpa/inet.h>
#include "zQP.h"
#include <gperftools/profiler.h>
#include <set>

#define RECOVERY

// #define POLLTHREAD

// #define SEND_TWICE

// #define ASYNC_CONNECT

// #define NO_ERROR_HANDLE

#define SEND_LOG

#define LOG

namespace zrdma
{

    std::mutex zQP_connect_mutex;

    struct zRouteSnapshot
    {
        int device;
        zPathMode mode;
        uint64_t epoch;
    };

    static zRouteSnapshot z_get_route_snapshot(zQP *qp)
    {
        while (true)
        {
            uint64_t epoch = qp->route_epoch.load(std::memory_order_acquire);
            if (epoch & 1)
            {
                continue;
            }
            int device = qp->current_device.load(std::memory_order_relaxed);
            zPathMode mode = qp->path_mode.load(std::memory_order_relaxed);
            if (epoch == qp->route_epoch.load(std::memory_order_acquire))
            {
                return {device, mode, epoch};
            }
        }
    }

    static void z_publish_route(zQP *qp, int device, zPathMode mode)
    {
        qp->route_epoch.fetch_add(1, std::memory_order_acq_rel);
        qp->current_device.store(device, std::memory_order_relaxed);
        qp->path_mode.store(mode, std::memory_order_relaxed);
        qp->route_epoch.fetch_add(1, std::memory_order_release);
    }

    static zQP_requestor *z_get_active_requestor(zQP *qp, int device)
    {
        if (device < 0 || device >= MAX_NIC_NUM)
        {
            return nullptr;
        }
        return qp->active_requestors[device].load(std::memory_order_acquire);
    }

    static int z_resolve_device(zQP *qp, int device)
    {
        return device >= 0 ? device : z_get_route_snapshot(qp).device;
    }

    static bool z_route_uses_rc(zQP *qp, const zRouteSnapshot &route)
    {
        if (route.mode != ZPATH_RC_ACTIVE)
        {
            return false;
        }
        return z_get_active_requestor(qp, route.device) != nullptr;
    }

    static int z_prepare_route(zQP *qp, zRouteSnapshot &route)
    {
        while (true)
        {
            route = z_get_route_snapshot(qp);
            if (route.mode == ZPATH_FAILED || route.device < 0 ||
                route.device >= static_cast<int>(qp->m_ep->m_devices.size()))
            {
                return -1;
            }
            if (qp->m_ep->m_devices[route.device]->status.load() != ZSTATUS_ERROR &&
                (route.mode != ZPATH_RC_ACTIVE || z_get_active_requestor(qp, route.device) != nullptr))
            {
                return 0;
            }
            if (z_switch(qp, route.device) < 0)
            {
                return -1;
            }
        }
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
        zqp->path_mode.store(ZPATH_RC_ACTIVE);
        zqp->current_device.store(0);
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
        listener->flush_thread_ = new std::thread(zQP_flush, listener->qp_info);
        return listener;
    }

    void zQP_flush(qp_info_table *qp_info)
    {
        while (true)
        {
            for (int i = 0; i < MAX_QP_NUM; i++)
            {
                if (qp_info[i].addr == 0)
                {
                    continue;
                }
                zAtomic_buffer *atomic_buffer = (zAtomic_buffer *)qp_info[i].addr;
                for (int j = 0; j < WR_ENTRY_NUM; j++)
                {
                    if (atomic_buffer[j].target_addr != 0 && atomic_buffer[j].finished == 0)
                    {
                        uint64_t target_addr = atomic_buffer[j].target_addr;
                        zAtomic_entry entry = *(zAtomic_entry *)(uintptr_t)target_addr;
                        if (entry.offset != j || entry.qp_id != i || entry.offset != j)
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
            // std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
                    for (int i = 0; i < ne; i++)
                    {
                        uint64_t wr_id = wc[i].wr_id;
                        if (wr_id == 0)
                        {
                            continue;
                        }
                        // zqp->completed_table.[wr_id] = wc[i].status;
                        tbb::concurrent_hash_map<uint64_t, cq_info>::accessor a;
                        bool result = zep->completed_table.insert(a, {wr_id, {wc[i].status, .valid = 1, .device_id = i}});
                        if (!result)
                        {
                            // printf("Error, wr_id %lu already exists in completed_table\n", wr_id);
                            a->second.status = wc[i].status;
                            a->second.valid = 1;
                            a->second.device_id = i;
                        }
                        if (wc[i].status != IBV_WC_SUCCESS)
                        {
                            // printf("wc[%lu] error: %s\n", wr_id, ibv_wc_status_str(wc[i].status));
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
        qp_attr_to_rts.timeout = 0;
        qp_attr_to_rts.retry_cnt = 7;
        qp_attr_to_rts.rnr_retry = 7;
        qp_attr_to_rts.sq_psn = 114;
        qp_attr_to_rts.max_rd_atomic = 1;

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
        qp_attr.ah_attr.grh.hop_limit = 1;
        qp_attr.ah_attr.grh.traffic_class = 0;
        qp_attr.ah_attr.grh.sgid_index = 1;
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
        ibv_query_gid(device->context, 1, 1, &gid);

        dcqp->dct_num_ = dcqp->qp_->qp_num;

        // printf("%u, %u, %u\n", dcqp->lid_, dcqp->port_num_, dcqp->dct_num_);
        // printf("%lu, %lu, %lu, %lu\n", *(uint64_t*)gid.raw, *((uint64_t*)(gid.raw)+1), gid.global.interface_id, gid.global.subnet_prefix);
        dcqp->gid1 = *(uint64_t *)gid.raw;
        dcqp->gid2 = *((uint64_t *)(gid.raw) + 1);
        dcqp->interface = gid.global.interface_id;
        dcqp->subnet = gid.global.subnet_prefix;

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
        ah_attr.dlid = input_lid;
        ah_attr.port_num = 1;
        ah_attr.is_global = 1;
        ah_attr.grh.hop_limit = 1;
        ah_attr.grh.sgid_index = 1;
        ah_attr.grh.dgid = gid;

        ibv_ah *ah = ibv_create_ah(requestor->pd_, &ah_attr);
        return ah;
    }

    int zDCQP_send(zQP *zqp, zDCQP_requestor *requestor, ibv_ah *ah, ibv_send_wr *send_wr, uint32_t lid, uint32_t dct_num, int max_depth)
    {
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
            uint32_t new_lkey = p->sg_list[0].lkey;
            uint32_t new_rkey = p->wr.rdma.rkey;
            if (p->opcode == IBV_WR_RDMA_WRITE)
            {
                if (zqp->current_device != 0)
                {
                    tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                    zqp->m_pd->m_lkey_table.find(a, new_lkey);
                    new_lkey = a->second[zqp->current_device];
                    new_rkey = zqp->m_rkey_table->at(new_rkey)[zqp->current_device];
                }
                ibv_wr_rdma_write(requestor->qp_ex_, new_rkey, p->wr.rdma.remote_addr);
            }
            else if (p->opcode == IBV_WR_RDMA_READ)
            {
                if (zqp->current_device != 0)
                {
                    tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                    zqp->m_pd->m_lkey_table.find(a, new_lkey);
                    new_lkey = a->second[zqp->current_device];
                    new_rkey = zqp->m_rkey_table->at(new_rkey)[zqp->current_device];
                }
                ibv_wr_rdma_read(requestor->qp_ex_, new_rkey, p->wr.rdma.remote_addr);
            }
            else if (p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
            {
                new_rkey = p->wr.atomic.rkey;
                if (zqp->current_device != 0)
                {
                    tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                    zqp->m_pd->m_lkey_table.find(a, new_lkey);
                    new_lkey = a->second[zqp->current_device];
                    new_rkey = zqp->m_rkey_table->at(new_rkey)[zqp->current_device];
                }
                ibv_wr_atomic_cmp_swp(requestor->qp_ex_, new_rkey, p->wr.atomic.remote_addr, p->wr.atomic.compare_add, p->wr.atomic.swap);
            }
            else if (p->opcode == IBV_WR_SEND)
            {
                ibv_wr_send(requestor->qp_ex_);
            }
            else
            {
                std::cerr << "Error, unsupported opcode in zDCQP_send" << std::endl;
                return -1;
            }
            p->sg_list[0].lkey = new_lkey;
            ibv_wr_set_sge_list(requestor->qp_ex_, p->num_sge, p->sg_list);
            mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
            length += p->sg_list->length;
            p = p->next;
        }
        ibv_wr_complete(requestor->qp_ex_);
        auto start = TIME_NOW;
        struct ibv_wc wc;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                std::cerr << "Error, dcqp read timeout" << std::endl;
                break;
            }
            if (ibv_poll_cq(requestor->cq_, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    std::cerr << "Error, dcqp read failed: " << wc.status << std::endl;
                    break;
                }
                break;
            }
        }
        // zqp->size_counter_.fetch_add(length);
        return 0;
    }

    int zDCQP_read(zDCQP_requestor *requestor, ibv_ah *ah, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num)
    {
        ibv_wr_start(requestor->qp_ex_);
        requestor->qp_ex_->wr_id = 0;
        requestor->qp_ex_->wr_flags = IBV_SEND_SIGNALED;
        ibv_wr_rdma_read(requestor->qp_ex_, rkey, (uint64_t)remote_addr);
        ibv_wr_set_sge(requestor->qp_ex_, lkey, (uint64_t)local_addr, length);
        mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
        ibv_wr_complete(requestor->qp_ex_);
        auto start = TIME_NOW;
        struct ibv_wc wc;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                std::cerr << "Error, dcqp read timeout" << std::endl;
                break;
            }
            if (ibv_poll_cq(requestor->cq_, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    std::cerr << "Error, dcqp read failed: " << wc.status << std::endl;
                    break;
                }
                break;
            }
        }
        return 0;
    }

    int zDCQP_write(zDCQP_requestor *requestor, ibv_ah *ah, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num)
    {
        ibv_wr_start(requestor->qp_ex_);
        requestor->qp_ex_->wr_id = 0;
        requestor->qp_ex_->wr_flags = IBV_SEND_SIGNALED;
        ibv_wr_rdma_write(requestor->qp_ex_, rkey, (uint64_t)remote_addr);
        ibv_wr_set_sge(requestor->qp_ex_, lkey, (uint64_t)local_addr, length);
        mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
        ibv_wr_complete(requestor->qp_ex_);
        auto start = TIME_NOW;
        struct ibv_wc wc;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                std::cerr << "Error, dcqp write timeout" << std::endl;
                break;
            }
            if (ibv_poll_cq(requestor->cq_, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    std::cerr << "Error, dcqp write failed: " << wc.status << std::endl;
                    break;
                }
                break;
            }
        }
        return 0;
    }

    int zDCQP_CAS(zDCQP_requestor *requestor, ibv_ah *ah, void *local_addr, uint32_t lkey, uint64_t new_val, void *remote_addr, uint32_t rkey, uint32_t lid, uint32_t dct_num)
    {
        uint64_t expected = *(uint64_t *)local_addr;
        ibv_wr_start(requestor->qp_ex_);
        requestor->qp_ex_->wr_id = 0;
        requestor->qp_ex_->wr_flags = IBV_SEND_SIGNALED;
        ibv_wr_atomic_cmp_swp(requestor->qp_ex_, rkey, (uint64_t)remote_addr, expected, new_val);
        ibv_wr_set_sge(requestor->qp_ex_, lkey, (uint64_t)local_addr, sizeof(uint64_t));
        mlx5dv_wr_set_dc_addr(requestor->qp_mlx_ex_, ah, dct_num, 114514);
        ibv_wr_complete(requestor->qp_ex_);
        auto start = TIME_NOW;
        struct ibv_wc wc;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                std::cerr << "Error, dcqp cas timeout" << std::endl;
                break;
            }
            if (ibv_poll_cq(requestor->cq_, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    std::cerr << "Error, dcqp cas failed: " << wc.status << std::endl;
                    break;
                }
                break;
            }
        }
        return 0;
    }

    int zQP_connect(zQP *qp, int nic_index, string ip, string port)
    {
        // ProfilerStart("connect_test.prof");
        std::lock_guard<std::mutex> connect_lock(zQP_connect_mutex);
        auto start = TIME_NOW;
        if (qp == nullptr || qp->m_ep == nullptr || qp->m_pd == nullptr || qp->m_rkey_table == nullptr || nic_index < 0 ||
            nic_index >= MAX_NIC_NUM || nic_index >= static_cast<int>(qp->m_ep->m_devices.size()) ||
            nic_index >= static_cast<int>(qp->m_pd->m_pds.size()))
        {
            return -1;
        }
        zQP_requestor *active_requestor = z_get_active_requestor(qp, nic_index);
        if (active_requestor != nullptr && active_requestor->status_ == ZSTATUS_CONNECTED)
        {
            return -1;
        }
        zQPType qp_type = qp->qp_type;
        zQP_requestor *qp_instance = new zQP_requestor();
        qp_instance->status_ = ZSTATUS_INIT;
        qp_instance->cm_id_ = nullptr;
        qp_instance->channel_ = rdma_create_event_channel();
        if (qp_instance->channel_ == nullptr)
        {
            delete qp_instance;
            return -1;
        }
        qp_instance->pd_ = qp->m_pd->m_pds[nic_index];
        int result = rdma_create_id(qp_instance->channel_, &(qp_instance->cm_id_), NULL, RDMA_PS_TCP);
        if (result != 0)
        {
            rdma_destroy_event_channel(qp_instance->channel_);
            delete qp_instance;
            return -1;
        }

        rdma_addrinfo *t = nullptr;
        rdma_addrinfo *res = nullptr;
        ibv_comp_channel *comp_channel = nullptr;
        ibv_cq *cq = nullptr;
        auto fail_connect = [&](const char *message)
        {
            fprintf(stderr, "RCQP connect failed on device %d: %s\n", nic_index, message);
            qp_instance->status_ = ZSTATUS_ERROR;
            if (res != nullptr)
            {
                rdma_freeaddrinfo(res);
                res = nullptr;
            }
            if (qp_instance->cm_id_ != nullptr && qp_instance->cm_id_->qp != nullptr)
            {
                rdma_destroy_qp(qp_instance->cm_id_);
            }
            if (cq != nullptr)
            {
                ibv_destroy_cq(cq);
                cq = nullptr;
            }
            if (comp_channel != nullptr)
            {
                ibv_destroy_comp_channel(comp_channel);
                comp_channel = nullptr;
            }
            if (qp_instance->cm_id_ != nullptr)
            {
                rdma_destroy_id(qp_instance->cm_id_);
                qp_instance->cm_id_ = nullptr;
            }
            if (qp_instance->channel_ != nullptr)
            {
                rdma_destroy_event_channel(qp_instance->channel_);
                qp_instance->channel_ = nullptr;
            }
            delete qp_instance;
            return -1;
        };

        int counter = 0;
        // rdma_bind_addr(qp_instance->cm_id_, (struct sockaddr *)&src_addr);
        while (t == nullptr && counter < 3)
        {
            counter += 1;
            struct sockaddr_in src_addr; // 设置源地址（指定网卡设备）
            memset(&src_addr, 0, sizeof(src_addr));
            src_addr.sin_family = AF_INET;
            if (inet_pton(AF_INET, qp->m_ep->m_devices[nic_index]->eth_ip.c_str(), &src_addr.sin_addr) != 1)
            {
                return fail_connect("invalid local IP address");
            }
            // struct sockaddr_in target_addr;
            // memset(&target_addr, 0, sizeof(target_addr));
            // target_addr.sin_family = AF_INET;
            // inet_pton(AF_INET, ip.c_str(), &target_addr.sin_addr);
            // target_addr.sin_port = htons(stoi(port));
            // rdma_addrinfo target;
            // target.ai_src_addr = (struct sockaddr*)&src_addr;
            // rdma_bind_addr(qp_instance->cm_id_, (struct sockaddr *)&src_addr);
            result = rdma_getaddrinfo(ip.c_str(), port.c_str(), NULL, &res);
            if (result != 0)
            {
                usleep(1000 * counter);
                continue;
            }
            // result = rdma_resolve_addr(qp_instance->cm_id_, (struct sockaddr *)&src_addr, (struct sockaddr *)&target_addr, RESOLVE_TIMEOUT_MS);
            // while(result != 0) {
            //     result = rdma_resolve_addr(qp_instance->cm_id_, (struct sockaddr *)&src_addr, (struct sockaddr *)&target_addr, RESOLVE_TIMEOUT_MS);
            // }
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
            if (t == nullptr)
            {
                rdma_freeaddrinfo(res);
                res = nullptr;
                usleep(1000 * counter);
            }
        }
        // printf("rdma_resolve_addr tried %d times\n", counter);
        if (t == nullptr)
        {
            return fail_connect("address resolution failed");
        }

        rdma_cm_event *event = nullptr;
        result = rdma_get_cm_event(qp_instance->channel_, &event);
        if (result != 0)
        {
            return fail_connect("waiting for address event failed");
        }
        if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED)
        {
            rdma_ack_cm_event(event);
            return fail_connect("address resolution event rejected");
        }
        rdma_ack_cm_event(event);
        event = nullptr;
        // Addr resolve finished, make route resolve

        result = rdma_resolve_route(qp_instance->cm_id_, RESOLVE_TIMEOUT_MS);
        if (result != 0)
        {
            return fail_connect("route resolution request failed");
        }
        result = rdma_get_cm_event(qp_instance->channel_, &event);
        if (result != 0)
        {
            return fail_connect("waiting for route event failed");
        }
        if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)
        {
            rdma_ack_cm_event(event);
            return fail_connect("route resolution event rejected");
        }
        rdma_ack_cm_event(event);
        event = nullptr;
        // printf("QP connected in %ld us: qp_id=%u, nic_index=%d, server_qp_id=%u\n", TIME_DURATION_US(start, TIME_NOW), qp->qp_id_, nic_index, qp_instance->qp_id_);

        // Addr route resolve finished
        // if(qp->m_ep->m_devices[nic_index]->cq_ == NULL) {
        comp_channel = ibv_create_comp_channel(qp->m_ep->m_devices[nic_index]->context);
        if (comp_channel == nullptr)
        {
            return fail_connect("completion channel creation failed");
        }
        cq = ibv_create_cq(qp->m_ep->m_devices[nic_index]->context, 1024, NULL, comp_channel, 0);
        if (cq == nullptr)
        {
            return fail_connect("completion queue creation failed");
        }
        result = ibv_req_notify_cq(cq, 0);
        if (result != 0)
        {
            return fail_connect("completion notification setup failed");
        }
        // } else {
        //     cq = qp->m_ep->m_devices[nic_index]->cq_list_.;
        // }

        ibv_qp_init_attr qp_init_attr;
        memset(&qp_init_attr, 0, sizeof(qp_init_attr));
        qp_init_attr.send_cq = cq;
        qp_init_attr.recv_cq = cq;
        qp_init_attr.qp_type = IBV_QPT_RC;
        qp_init_attr.cap.max_send_wr = 512;
        qp_init_attr.cap.max_recv_wr = 1;
        qp_init_attr.cap.max_send_sge = 16;
        qp_init_attr.cap.max_recv_sge = 16;
        qp_init_attr.cap.max_inline_data = 256;
        qp_init_attr.sq_sig_all = 0;
        result = rdma_create_qp(qp_instance->cm_id_, qp->m_pd->m_pds[nic_index], &qp_init_attr);
        if (result != 0)
        {
            return fail_connect("queue pair creation failed");
        }

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
            return fail_connect("connection request failed");
        }
        result = rdma_get_cm_event(qp_instance->channel_, &event);
        if (result != 0)
        {
            return fail_connect("waiting for connection event failed");
        }
        if (event->event != RDMA_CM_EVENT_ESTABLISHED)
        {
            rdma_ack_cm_event(event);
            return fail_connect("connection was not established");
        }
        if (event->param.conn.private_data == nullptr || event->param.conn.private_data_len < sizeof(PData))
        {
            rdma_ack_cm_event(event);
            return fail_connect("invalid connection metadata");
        }

        struct PData server_pdata;
        memset(&server_pdata, 0, sizeof(server_pdata));
        memcpy(&server_pdata, event->param.conn.private_data, sizeof(server_pdata));

        server_pdata.nic_num_ = std::min<uint16_t>(
            server_pdata.nic_num_,
            static_cast<uint16_t>(std::min<size_t>(MAX_NIC_NUM, std::min(qp->m_ep->m_devices.size(), qp->m_pd->m_requestors.size()))));
        qp_instance->server_cmd_msg_ = server_pdata.buf_addr;
        for (int i = 0; i < MAX_NIC_NUM; i++)
        {
            qp_instance->server_cmd_rkey_[i] = server_pdata.buf_rkey[i];
        }
        if (qp->m_rkey_table->find(server_pdata.buf_rkey[0]) == qp->m_rkey_table->end())
        {
            vector<uint32_t> rkeys;
            for (int i = 0; i < MAX_NIC_NUM; i++)
            {
                rkeys.push_back(server_pdata.buf_rkey[i]);
            }
            (*qp->m_rkey_table)[server_pdata.buf_rkey[0]] = rkeys;
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
                if (target->lid_ != server_pdata.lid_[i] || target->dct_num_ != server_pdata.dct_num_[i] ||
                    target->ip != string(server_pdata.ip[i]) || target->port != server_pdata.port[i])
                {
                    rdma_ack_cm_event(event);
                    return fail_connect("server target metadata mismatch");
                }
            }
            else
            {
                if (qp->m_pd->m_requestors[i].empty())
                {
                    rdma_ack_cm_event(event);
                    return fail_connect("missing local DCQP requestor");
                }
                target = new zTarget();
                qp->m_targets[i] = target;
                target->ah = zDCQP_create_ah(qp->m_pd->m_requestors[i][0], server_pdata.gid1[i], server_pdata.gid2[i], server_pdata.gid2[i], server_pdata.gid1[i], server_pdata.lid_[i]);
                if (target->ah == nullptr)
                {
                    qp->m_targets.erase(i);
                    delete target;
                    rdma_ack_cm_event(event);
                    return fail_connect("address handle creation failed");
                }
                target->lid_ = server_pdata.lid_[i];
                target->dct_num_ = server_pdata.dct_num_[i];
                target->ip = string(server_pdata.ip[i]);
                target->port = server_pdata.port[i];
            }
        }

        if (server_pdata.size != sizeof(CmdMsgBlock))
        {
            rdma_ack_cm_event(event);
            return fail_connect("server command buffer size mismatch");
        }
        if (qp->msg_mr_.find(nic_index) == qp->msg_mr_.end() || qp->resp_mr_.find(nic_index) == qp->resp_mr_.end())
        {
            rdma_ack_cm_event(event);
            return fail_connect("missing local command memory registration");
        }
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
            rdma_ack_cm_event(event);
            return fail_connect("atomic table address mismatch");
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
            rdma_ack_cm_event(event);
            return fail_connect("QP info address mismatch");
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
        event = nullptr;

        rdma_freeaddrinfo(res);
        res = nullptr;

        // Connect finished
        qp_instance->cq_ = cq;
        qp_instance->qp_ = qp_instance->cm_id_->qp;

        qp_instance->status_ = ZSTATUS_CONNECTED;
        qp->m_ep->m_devices[nic_index]->cq_list_.push_back(cq);
        {
            std::lock_guard<std::mutex> lock(qp->route_mutex);
            while (qp->m_requestors.size() <= nic_index)
            {
                qp->m_requestors.push_back(nullptr);
            }
            qp->m_requestors[nic_index] = qp_instance;
            qp->active_requestors[nic_index].store(qp_instance, std::memory_order_release);
        }
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
        int result;
        auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                // std::cerr << "Error, read timeout" << std::endl;
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
        int entry_index = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;
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

    int zQP_read(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, uint32_t time_stamp, int device)
    {
        device = z_resolve_device(zqp, device);
        zQP_requestor *requestor = z_get_active_requestor(zqp, device);
        if (requestor == nullptr)
        {
            return -1;
        }
        int entry_index = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;
        struct ibv_sge *sge = &zqp->sge_[entry_index];
        sge->addr = (uint64_t)local_addr;
        sge->length = length;
        sge->lkey = lkey;
        struct ibv_send_wr *send_wr = &zqp->wr_[entry_index];
        struct ibv_send_wr *bad_send_wr;

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

        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            zqp->wr_entry_[entry_index].finished = 1;
            std::cerr << "Error, ibv_post_send failed" << std::endl;
            return -1;
        }

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
                z_switch(zqp, device);
                result = -1;
                // std::cerr << "Error, read timeout" << std::endl;
                break;
            }
            if (ibv_poll_cq(cq, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    z_switch(zqp, device);
                    result = -1;
                    break;
                }
                result = 0;
                break;
            }
        }
                int start_ = zqp->entry_start_;
                    int end_ = zqp->entry_end_.load() % WR_ENTRY_NUM;
                    if (start_ > end_)
                        end_ += WR_ENTRY_NUM;
                    if (start_ != end_)
                    {
                        for (int i = start_; i < end_; i++)
                        {
                            if (zqp->wr_entry_[i % WR_ENTRY_NUM].time_stamp == time_stamp &&
                                zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr == (uint64_t)send_wr)
                            {
                                // free(((ibv_send_wr *)qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr)->sg_list);
                                // delete (ibv_send_wr *)qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr;
                                zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr = 0;
                                zqp->wr_entry_[i % WR_ENTRY_NUM].finished = 1;
                            }
                            if ((zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr == 0 || zqp->wr_entry_[i % WR_ENTRY_NUM].finished == 1) && i % WR_ENTRY_NUM == zqp->entry_start_)
                            {
                                zqp->entry_start_ = (zqp->entry_start_ + 1) % WR_ENTRY_NUM;
                            }
                        }
                    }
        zqp->size_counter_.fetch_add(length);
        return result;
#else
        return z_poll_completion(zqp, wr_id, device);
#endif
    }


    int z_poll_till_completion(zQP *qp) {
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
                result = -1;
                // std::cerr << "Error, read timeout" << std::endl;
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
                    result = -1;
                    break;
                }
                result = 0;
                break;
            }
        }
        // qp->size_counter_.fetch_add(length);
        return result;
    }

    int z_poll_send_completion(zQP *qp, ibv_wc& wc) {
        zQP_requestor *requestor = qp->m_requestors[qp->current_device];
        ibv_cq *cq = requestor->cq_;
        int ret = ibv_poll_cq(cq, 1, &wc);
        if(ret > 0) {
            if (wc.status != IBV_WC_SUCCESS)
                {
                    z_switch(qp);
#ifdef RECOVERY
                    z_recovery(qp);
#endif
                }
        }
        return ret;
    }

    int zQP_write(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, uint32_t time_stamp, bool use_log, int device)
    {
        device = z_resolve_device(zqp, device);
        zQP_requestor *requestor = z_get_active_requestor(zqp, device);
        if (requestor == nullptr)
        {
            return -1;
        }
        int entry_index = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;
        struct ibv_sge *sge = &zqp->sge_backup[entry_index];
        struct ibv_send_wr *send_wr = &zqp->wr_backup[entry_index];
        struct ibv_send_wr *bad_send_wr;
        struct ibv_sge log_sge;
        struct ibv_send_wr log_wr;

        zWR_entry entry;
        entry.time_stamp = time_stamp;
        entry.wr_addr = (uint64_t)send_wr;
        entry.finished = 1;
        entry.reserved = 0;

        zqp->wr_entry_[entry_index].time_stamp = entry.time_stamp;
        zqp->wr_entry_[entry_index].wr_addr = entry.wr_addr;
        zqp->wr_entry_[entry_index].finished = 0;
        // uint64_t wr_id = *((uint64_t*)(&zqp->wr_entry_[entry_index])+1);
        zWR_header header{entry.wr_addr, entry.time_stamp, 1};
        uint64_t wr_id = *(uint64_t *)&header;

        if (use_log)
        {
            log_sge.addr = (uint64_t)(&entry);
            log_sge.length = sizeof(zWR_entry);
            log_sge.lkey = 0;
            log_wr.wr_id = wr_id;
            log_wr.sg_list = &log_sge;
            log_wr.num_sge = 1;
            log_wr.next = NULL;
            log_wr.opcode = IBV_WR_RDMA_WRITE;
            // log_wr.send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED ;
            log_wr.send_flags = IBV_SEND_INLINE;
            log_wr.wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zWR_entry);
            log_wr.wr.rdma.rkey = requestor->server_cmd_rkey_[device];
        }

        sge->addr = (uint64_t)local_addr;
        sge->length = length;
        sge->lkey = lkey;

        if (use_log)
            send_wr->wr_id = 0;
        else
            send_wr->wr_id = wr_id;
        send_wr->sg_list = sge;
        send_wr->num_sge = 1;
        if (use_log)
            send_wr->next = &log_wr;
        else
            send_wr->next = NULL;
        send_wr->opcode = IBV_WR_RDMA_WRITE;
        // if (use_log)
        //     send_wr->send_flags = 0;
        // else
            send_wr->send_flags = IBV_SEND_SIGNALED;
        send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
        send_wr->wr.rdma.rkey = rkey;
        ibv_qp *qp = requestor->qp_;
        if (ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            z_switch(zqp, device);
            z_recovery(zqp);
            // zqp->wr_entry_[entry_index].finished = 1;
            perror("Error, ibv_post_send failed");
            return -1;
        }

        send_wr->next = NULL;
        if (use_log)
            send_wr->send_flags = IBV_SEND_SIGNALED;
#ifndef POLLTHREAD
        int result;
        auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                z_switch(zqp, device);
                z_recovery(zqp);
                result = -1;
                // std::cerr << "Error, read timeout" << std::endl;
                return result;
            }
            if (ibv_poll_cq(cq, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    z_switch(zqp, device);
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

        int start_ = zqp->entry_start_;
                    int end_ = zqp->entry_end_.load() % WR_ENTRY_NUM;
                    if (start_ > end_)
                        end_ += WR_ENTRY_NUM;
                    if (start_ != end_)
                    {
                        for (int i = start_; i < end_; i++)
                        {
                            if (zqp->wr_entry_[i % WR_ENTRY_NUM].time_stamp == time_stamp &&
                                zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr == (uint64_t)send_wr)
                            {
                                // free(((ibv_send_wr *)qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr)->sg_list);
                                // delete (ibv_send_wr *)qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr;
                                zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr = 0;
                                zqp->wr_entry_[i % WR_ENTRY_NUM].finished = 1;
                            }
                            if ((zqp->wr_entry_[i % WR_ENTRY_NUM].wr_addr == 0 || zqp->wr_entry_[i % WR_ENTRY_NUM].finished == 1) && i % WR_ENTRY_NUM == zqp->entry_start_)
                            {
                                zqp->entry_start_ = (zqp->entry_start_ + 1) % WR_ENTRY_NUM;
                                // printf("advance entry_start_ to %d, %d\n", zqp->entry_start_, zqp->entry_end_.load());
                            }
                        }
                    }
                    // printf("start: %d, end: %d\n", zqp->entry_start_, zqp->entry_end_.load() % WR_ENTRY_NUM);
        zqp->size_counter_.fetch_add(length);
        return result;
#else
        return z_poll_completion(zqp, wr_id, device);
#endif
    }

    int zQP_CAS(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t new_val, void *remote_addr, uint32_t rkey, uint32_t time_stamp, int device)
    {
        device = z_resolve_device(zqp, device);
        zQP_requestor *requestor = z_get_active_requestor(zqp, device);
        if (requestor == nullptr)
        {
            return -1;
        }
        uint64_t expected = *(uint64_t *)local_addr;

        int offset = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;
        struct ibv_sge *sge = &zqp->sge_backup[offset];
        sge->addr = (uint64_t)local_addr;
        sge->length = sizeof(uint64_t);
        sge->lkey = lkey;

        zAtomic_entry entry;
        entry.time_stamp = time_stamp;
        entry.qp_id = zqp->qp_id_;
        entry.offset = offset;

        struct ibv_send_wr *send_wr = &zqp->wr_backup[offset];
        struct ibv_send_wr *bad_send_wr;
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
        send_wr->send_flags = 0;
        send_wr->wr.atomic.remote_addr = (uint64_t)remote_addr;
        send_wr->wr.atomic.rkey = rkey;
        send_wr->wr.atomic.compare_add = expected;
        send_wr->wr.atomic.swap = *(uint64_t *)&entry;
        // printf("CAS: %lu --> %lu\n", expected, *(uint64_t*)entry);

        struct ibv_sge *buffer_sge = &zqp->sge_[offset];
        struct ibv_send_wr *buffer_wr = &zqp->wr_[offset];
        zAtomic_buffer *buffer = (zAtomic_buffer *)&zqp->wr_entry_[offset];
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
        buffer_wr->next = NULL;
        buffer_wr->opcode = IBV_WR_RDMA_WRITE;
        buffer_wr->send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED;
        buffer_wr->wr.rdma.remote_addr = requestor->server_cmd_msg_ + offset * sizeof(zAtomic_buffer);
        buffer_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[device];
        send_wr->next = buffer_wr;

        ibv_qp *qp = requestor->qp_;
        int result;
        if (result = ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            zqp->wr_entry_[offset].finished = 1;
            std::cerr << "Error, ibv_post_send failed:" << result << std::endl;
            return -1;
        }

        // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;

        // delete entry;
        // delete buffer;
        // delete buffer_sge;
        // delete buffer_wr;
        send_wr->next = NULL;
#ifndef POLLTHREAD
        auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;
        while (true)
        {
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                z_switch(zqp, device);
                result = -1;
                // std::cerr << "Error, read timeout" << std::endl;
                break;
            }
            if (ibv_poll_cq(cq, 1, &wc) > 0)
            {
                if (wc.status != IBV_WC_SUCCESS)
                {
                    z_switch(zqp, device);
                    result = -1;
                    break;
                }
                result = 0;
                break;
            }
        }
        if (result >= 0 && expected == *(uint64_t *)local_addr)
        {
            zQP_CAS_step2(zqp, new_val, remote_addr, rkey, zqp->time_stamp, offset, device);
            // std::thread(zQP_CAS_step2, zqp, new_val, remote_addr, rkey, zqp->time_stamp, offset).detach();
        }
        else
        {
            printf("CAS failed, expect %lu, get %lu\n", expected, *(uint64_t *)local_addr);
        }
        return result;
#else
        result = z_poll_completion(zqp, wr_id, device);
        if (result >= 0 && expected == *(uint64_t *)local_addr)
        {
            zQP_CAS_step2(zqp, new_val, remote_addr, rkey, zqp->time_stamp, offset, device);
        }
        else
        {
            printf("CAS failed, expect %lu, get %lu\n", expected, *(uint64_t *)local_addr);
        }
        return 0;
#endif
    }

    int zQP_CAS_step2(zQP *zqp, uint64_t new_val, void *remote_addr, uint32_t rkey, uint32_t time_stamp, uint64_t offset, int device)
    {
        device = z_resolve_device(zqp, device);
        zQP_requestor *requestor = z_get_active_requestor(zqp, device);
        if (requestor == nullptr)
        {
            return -1;
        }

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
        send_wr.wr.atomic.rkey = rkey;
        send_wr.wr.atomic.compare_add = *(uint64_t *)&entry;
        send_wr.wr.atomic.swap = new_val;
        // printf("CAS: %lu --> %lu\n", *(uint64_t*)entry, new_val);

        struct ibv_sge buffer_sge;
        struct ibv_send_wr buffer_wr;
        zAtomic_buffer *buffer = new zAtomic_buffer();
        buffer->buffer = new_val;
        buffer->finished = 0;
        buffer->target_addr = (uint64_t)(remote_addr);
        buffer->time_stamp = time_stamp;
        uint64_t old_val = *((uint64_t *)(buffer) + 1);
        buffer->finished = 1;
        uint64_t check_val = *((uint64_t *)(buffer) + 1);
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
        buffer_wr.wr.atomic.remote_addr = requestor->server_cmd_msg_ + entry.offset * sizeof(zAtomic_buffer) + sizeof(uint64_t);
        buffer_wr.wr.atomic.rkey = requestor->server_cmd_rkey_[device];
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
                      uint32_t time_stamp, bool unique_cas, int max_depth, int device)
    {
        device = z_resolve_device(zqp, device);
        int entry_index = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;
        ibv_send_wr *copy_wr = &zqp->wr_backup[entry_index];
        ibv_send_wr *p = send_wr;
        ibv_send_wr *q = copy_wr;
        std::set<ibv_send_wr *> wr_set;
        vector<uint64_t> wr_ids;
        int start_index = -1;
        int end_index = 0;
        bool retry = false;
        // deep copy
        zQP_requestor *requestor = z_get_active_requestor(zqp, device);
        if (requestor == nullptr)
        {
            return -1;
        }
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
            if (device != 0)
            {
                for (int i = 0; i < p->num_sge; i++)
                {
                    if(zqp->quick_lkey == 0) {
                        tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                        zqp->m_pd->m_lkey_table.find(a, p->sg_list[i].lkey);
                        q->sg_list[i].lkey = a->second[device];
                        zqp->quick_lkey = q->sg_list[i].lkey;
                    } else {
                        q->sg_list[i].lkey = zqp->quick_lkey;
                    }
                }
                if (q->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
                {
                    if(zqp->quick_rkey == 0) {
                        q->wr.atomic.rkey = zqp->m_rkey_table->at(p->wr.atomic.rkey)[device];
                        zqp->quick_rkey = q->wr.atomic.rkey;
                    } else {
                        q->wr.atomic.rkey = zqp->quick_rkey;
                    }
                }
                else
                {
                    if(zqp->quick_rkey == 0) {
                        q->wr.rdma.rkey = zqp->m_rkey_table->at(p->wr.rdma.rkey)[device];
                        zqp->quick_rkey = q->wr.rdma.rkey;
                    } else {
                        q->wr.rdma.rkey = zqp->quick_rkey;
                    }
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

                q->wr.atomic.swap = *(uint64_t *)&entry;

                struct ibv_sge *buffer_sge = &zqp->sge_[entry_index];
                struct ibv_send_wr *buffer_wr = &zqp->wr_[entry_index];
                zAtomic_buffer *buffer = &zqp->atomic_buffer_[entry_index];
                buffer->buffer = q->wr.atomic.swap;
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
                buffer_wr->next = q->next;
                buffer_wr->opcode = IBV_WR_RDMA_WRITE;
                buffer_wr->send_flags = IBV_SEND_INLINE;
                buffer_wr->wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zAtomic_buffer);
                buffer_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[device];
                q->next = buffer_wr;
                q = q->next;
                // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;
            }
#ifdef LOG
            else if (p->opcode == IBV_WR_SEND || p->opcode == IBV_WR_RDMA_WRITE ||
                     p->opcode == IBV_WR_RDMA_WRITE_WITH_IMM || p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
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
                // zqp->wr_entry_[entry_index].time_stamp = entry->time_stamp;
                // zqp->wr_entry_[entry_index].wr_addr = entry->wr_addr;
                // zqp->wr_entry_[entry_index].finished = 1;
                uint64_t wr_id = *((uint64_t *)(&zqp->wr_entry_[entry_index]) + 1);
                zqp->wr_entry_[entry_index].finished = 1;
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
                log_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[device];
                q->next = log_wr;
                // if( p->send_flags & IBV_SEND_SIGNALED) {
                q->send_flags &= ~IBV_SEND_SIGNALED;
                //     log_wr->send_flags |= IBV_SEND_SIGNALED;
                // }
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
                entry_index = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;
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
        if (ibv_post_send(qp, copy_wr, bad_wr))
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
                z_switch(zqp, device);
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
                    z_switch(zqp, device);
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
                    zqp->entry_start_ = (zqp->entry_start_ + 1) % WR_ENTRY_NUM;
                }
            }
        }
#endif
        // zqp->size_counter_.fetch_add(length);
        return result;
#else
        return z_poll_completion(zqp, &wr_ids, device);
        // return z_poll_completion(zqp, wr_id);
#endif
    }

    int zQP_post_send_async(zQP *zqp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr, bool non_idempotent,
                            uint32_t time_stamp, vector<uint64_t> *wr_ids, bool unique_cas, int max_depth, int wr_id, int device)
    {
        device = z_resolve_device(zqp, device);
                vector<ibv_send_wr*> copy_wrs;
        vector<ibv_sge*> copy_sges;
        vector<zWR_entry*> wr_entries;
        ibv_send_wr *copy_wr = new ibv_send_wr();
        ibv_send_wr *p = send_wr;
        ibv_send_wr *q = copy_wr;
        std::set<ibv_send_wr *> wr_set;
        int start_index = -1;
        int end_index = 0;
        bool retry = false;
        // deep copy
        zQP_requestor *requestor = z_get_active_requestor(zqp, device);
        if (requestor == nullptr)
        {
            return -1;
        }
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
            if (device != 0)
            {
                for (int i = 0; i < p->num_sge; i++)
                {
                    tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                    zqp->m_pd->m_lkey_table.find(a, p->sg_list[i].lkey);
                    q->sg_list[i].lkey = a->second[device];
                }
                if (q->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
                {
                    q->wr.atomic.rkey = zqp->m_rkey_table->at(p->wr.atomic.rkey)[device];
                }
                else
                {
                    q->wr.rdma.rkey = zqp->m_rkey_table->at(p->wr.rdma.rkey)[device];
                }
                // p = p->next;
            }
            if (!unique_cas && q->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
            {
                int entry_index = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;
                zAtomic_entry *entry = new zAtomic_entry();
                entry->time_stamp = time_stamp;
                entry->qp_id = zqp->qp_id_;
                entry->offset = entry_index;

                // struct ibv_send_wr *send_wr = new ibv_send_wr();
                // struct ibv_send_wr *bad_send_wr;
                zqp->wr_entry_[entry_index].time_stamp = time_stamp;
                zqp->wr_entry_[entry_index].wr_addr = (uint64_t)q;
                zqp->wr_entry_[entry_index].finished = 1;
                zqp->wr_entry_[entry_index].reserved = q->wr.atomic.swap;

                q->wr.atomic.swap = *(uint64_t *)entry;

                struct ibv_sge *buffer_sge = new ibv_sge();
                struct ibv_send_wr *buffer_wr = new ibv_send_wr();
                zAtomic_buffer *buffer = new zAtomic_buffer();
                buffer->buffer = q->wr.atomic.swap;
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
                buffer_wr->next = NULL;
                buffer_wr->opcode = IBV_WR_RDMA_WRITE;
                buffer_wr->send_flags = IBV_SEND_INLINE;
                buffer_wr->wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zAtomic_buffer);
                buffer_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[device];
                q->next = buffer_wr;
                q = q->next;
                // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;
            }
            else if (p->opcode == IBV_WR_SEND || p->opcode == IBV_WR_RDMA_WRITE ||
                     p->opcode == IBV_WR_RDMA_WRITE_WITH_IMM || p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
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
                int entry_index = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;
                if (start_index == -1)
                {
                    start_index = entry_index;
                }
                end_index = entry_index;
                zqp->wr_entry_[entry_index].time_stamp = entry->time_stamp;
                zqp->wr_entry_[entry_index].wr_addr = entry->wr_addr;
                zqp->wr_entry_[entry_index].finished = 0;
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
                log_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[device];
                q->next = log_wr;
                q->send_flags &= ~IBV_SEND_SIGNALED;
                q = q->next;
            }
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
        }
        if (retry != non_idempotent)
        {
            std::cerr << "Warning, inconsistent non_idempotent flag" << std::endl;
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
        for (auto k = copy_wr; k != NULL;)
        {
            ibv_send_wr *next = k->next;
            if (k->opcode == IBV_WR_RDMA_READ)
            {
                if (k->sg_list)
                    free(k->sg_list);
                delete k;
            }
            k = next;
        }
        wr_ids->push_back(q->wr_id);
        // q->next = NULL;
        int result;
        for (auto wr : copy_wrs) {
            delete wr;
        }
        for (auto sge : copy_sges) {
            delete sge;
        }
        for (auto entry : wr_entries) {
            delete entry;
        }
        // zqp->size_counter_.fetch_add(length);
        return result;
    }

    int zQP_read_async(zQP *zqp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, uint32_t time_stamp, vector<uint64_t> *wr_ids, int device)
    {
        device = z_resolve_device(zqp, device);
        zQP_requestor *requestor = z_get_active_requestor(zqp, device);
        if (requestor == nullptr)
        {
            return -1;
        }
        struct ibv_sge *sge = new ibv_sge();
        sge->addr = (uint64_t)local_addr;
        sge->length = length;
        if (device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge->lkey = a->second[device];
        }
        else
        {
            sge->lkey = lkey;
        }
        struct ibv_send_wr *send_wr = new ibv_send_wr();
        struct ibv_send_wr *bad_send_wr;

        int entry_index = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;
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
        if (device != 0)
        {
            send_wr->wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[device];
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
                        uint32_t time_stamp, bool use_log, vector<uint64_t> *wr_ids, int device)
    {
        device = z_resolve_device(zqp, device);
        zQP_requestor *requestor = z_get_active_requestor(zqp, device);
        if (requestor == nullptr)
        {
            return -1;
        }
        int entry_index = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;
        struct ibv_sge *sge = &zqp->sge_[entry_index];
        struct ibv_send_wr *send_wr = &zqp->wr_[entry_index];
        struct ibv_send_wr *bad_send_wr;
        struct ibv_sge log_sge;
        struct ibv_send_wr log_wr;

        zWR_entry *entry = &zqp->wr_entry_[entry_index];
        entry->time_stamp = time_stamp;
        entry->wr_addr = (uint64_t)send_wr;
        entry->finished = 1;
        uint64_t wr_id = *((uint64_t *)(&zqp->wr_entry_[entry_index]) + 1);

        zqp->wr_entry_[entry_index].time_stamp = entry->time_stamp;
        zqp->wr_entry_[entry_index].wr_addr = entry->wr_addr;
        zqp->wr_entry_[entry_index].finished = 0;

        if (use_log)
        {
            log_sge.addr = (uint64_t)(&wr_id);
            log_sge.length = sizeof(uint64_t);
            log_sge.lkey = 0;
            log_wr.wr_id = wr_id;
            log_wr.sg_list = &log_sge;
            log_wr.num_sge = 1;
            log_wr.next = NULL;
            log_wr.opcode = IBV_WR_RDMA_WRITE;
            log_wr.send_flags = IBV_SEND_INLINE;
            log_wr.wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zWR_entry) + sizeof(uint64_t);
            log_wr.wr.rdma.rkey = requestor->server_cmd_rkey_[device];
        }

        sge->addr = (uint64_t)local_addr;
        sge->length = length;
        if (device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge->lkey = a->second[device];
        }
        else
        {
            sge->lkey = lkey;
        }
        // if (use_log)
        //     send_wr->wr_id = 0;
        // else
        send_wr->wr_id = wr_id;
        send_wr->sg_list = sge;
        send_wr->num_sge = 1;
        if (use_log)
            send_wr->next = &log_wr;
        else
            send_wr->next = NULL;
        send_wr->opcode = IBV_WR_RDMA_WRITE;
        // if (use_log)
        //     send_wr->send_flags = 0;
        // else
        send_wr->send_flags = IBV_SEND_SIGNALED;
        send_wr->wr.rdma.remote_addr = (uint64_t)remote_addr;
        if (device != 0)
        {
            send_wr->wr.rdma.rkey = zqp->m_rkey_table->at(rkey)[device];
        }
        else
        {
            send_wr->wr.rdma.rkey = rkey;
        }
        ibv_qp *qp = requestor->qp_;
        if (int return_val = ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            // perror("Error, ibv_post_send failed");
            // printf("ibv_post_send return value: %d\n", return_val);
            zqp->wr_entry_[entry_index].finished = 1;
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
                      uint32_t time_stamp, vector<uint64_t> *wr_ids, int device)
    {
        device = z_resolve_device(zqp, device);
        zQP_requestor *requestor = z_get_active_requestor(zqp, device);
        if (requestor == nullptr)
        {
            return -1;
        }
        uint64_t expected = *(uint64_t *)local_addr;

        struct ibv_sge *sge = new ibv_sge();
        sge->addr = (uint64_t)local_addr;
        sge->length = sizeof(uint64_t);
        if (device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            zqp->m_pd->m_lkey_table.find(a, lkey);
            sge->lkey = a->second[device];
        }
        else
        {
            sge->lkey = lkey;
        }
        int entry_index = zqp->entry_end_.fetch_add(1) % WR_ENTRY_NUM;

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
        if (device != 0)
        {
            send_wr->wr.atomic.rkey = zqp->m_rkey_table->at(rkey)[device];
        }
        else
        {
            send_wr->wr.atomic.rkey = rkey;
        }
        send_wr->wr.atomic.compare_add = expected;
        send_wr->wr.atomic.swap = *(uint64_t *)entry;
        // printf("CAS: %lu --> %lu\n", expected, *(uint64_t*)entry);

        struct ibv_sge *buffer_sge = new ibv_sge();
        struct ibv_send_wr *buffer_wr = new ibv_send_wr();
        zAtomic_buffer *buffer = new zAtomic_buffer();
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
        buffer_wr->next = NULL;
        buffer_wr->opcode = IBV_WR_RDMA_WRITE;
        buffer_wr->send_flags = IBV_SEND_INLINE | IBV_SEND_SIGNALED | IBV_SEND_FENCE;
        buffer_wr->wr.rdma.remote_addr = requestor->server_cmd_msg_ + entry_index * sizeof(zAtomic_buffer);
        buffer_wr->wr.rdma.rkey = requestor->server_cmd_rkey_[device];
        send_wr->next = buffer_wr;

        ibv_qp *qp = requestor->qp_;
        int result;
        if (result = ibv_post_send(qp, send_wr, &bad_send_wr))
        {
            zqp->wr_entry_[entry_index].finished = 1;
            std::cerr << "Error, ibv_post_send failed:" << result << std::endl;
            return -1;
        }
        wr_ids->push_back(wr_id);

        // zqp->entry_end_ = (zqp->entry_end_ + 1)%WR_ENTRY_NUM;

        // auto start = TIME_NOW;
        struct ibv_wc wc;
        ibv_cq *cq = requestor->cq_;

        delete entry;
        delete buffer;
        delete buffer_sge;
        delete buffer_wr;
        send_wr->next = NULL;
        return entry_index;
    }

    int zQP_CAS_step2_async(zQP *zqp, uint64_t new_val, void *remote_addr, uint32_t rkey, uint32_t time_stamp, uint64_t offset, int device)
    {
        device = z_resolve_device(zqp, device);
        zQP_requestor *requestor = z_get_active_requestor(zqp, device);
        if (requestor == nullptr)
        {
            return -1;
        }

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
        send_wr->wr.atomic.rkey = rkey;
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
        buffer_wr->wr.atomic.remote_addr = requestor->server_cmd_msg_ + entry->offset * sizeof(zAtomic_buffer) + sizeof(uint64_t);
        buffer_wr->wr.atomic.rkey = requestor->server_cmd_rkey_[device];
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
                            zqp->entry_start_ = (zqp->entry_start_ + 1) % WR_ENTRY_NUM;
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
        return;
    }

    int z_post_send_async(zQP *qp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr, bool non_idempotent,
                          uint32_t time_stamp, vector<uint64_t> *wr_ids, bool unique_cas, int max_depth, int wr_id)
    {
        zRouteSnapshot route;
        if (z_prepare_route(qp, route) < 0)
        {
            return -1;
        }
        int device = route.device;
        if (z_route_uses_rc(qp, route))
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
            int result = zQP_post_send_async(qp, send_wr, bad_wr, non_idempotent, qp->time_stamp, wr_ids, unique_cas, max_depth, wr_id, device);
            if (result == -1)
            {
                if (z_switch(qp, device) < 0)
                {
                    return -1;
                }
                return z_post_send_async(qp, send_wr, bad_wr, non_idempotent, time_stamp, wr_ids, unique_cas, max_depth, wr_id);
            }
            return result;
        }
        else
        {
            int result = zDCQP_send(qp, qp->m_pd->m_requestors[device][0], qp->m_targets[device]->ah, send_wr, qp->m_targets[device]->lid_, qp->m_targets[device]->dct_num_, max_depth);
            return result;
        }
    }

    int z_read_async(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids)
    {
        zRouteSnapshot route;
        if (z_prepare_route(qp, route) < 0)
        {
            return -1;
        }
        uint32_t new_lkey = lkey;
        uint32_t new_rkey = rkey;
        int device = route.device;
        if (z_route_uses_rc(qp, route))
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
            int result = zQP_read_async(qp, local_addr, new_lkey, length, remote_addr, new_rkey, qp->time_stamp, wr_ids, device);
            if (result == -1)
            {
                if (z_switch(qp, device) < 0)
                {
                    return -1;
                }
                return z_read_async(qp, local_addr, lkey, length, remote_addr, rkey, wr_ids);
            }
            return result;
        }
        else
        {
            if (device != 0)
            {
                tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                qp->m_pd->m_lkey_table.find(a, lkey);
                new_lkey = a->second[device];
                new_rkey = qp->m_rkey_table->at(rkey)[device];
            }
            int result = zDCQP_read(qp->m_pd->m_requestors[device][0], qp->m_targets[device]->ah, local_addr, new_lkey, length, remote_addr, new_rkey, qp->m_targets[device]->lid_, qp->m_targets[device]->dct_num_);
            qp->size_counter_.fetch_add(length);
            return result;
        }
    }

    int z_write_async(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey, vector<uint64_t> *wr_ids)
    {
        zRouteSnapshot route;
        if (z_prepare_route(qp, route) < 0)
        {
            return -1;
        }
        uint32_t new_lkey = lkey;
        uint32_t new_rkey = rkey;
        // if(qp->current_device != 0){
        //     tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        //     qp->m_pd->m_lkey_table.find(a, lkey);
        //     new_lkey = a->second[qp->current_device];
        //     new_rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
        // }
        int device = route.device;
        if (z_route_uses_rc(qp, route))
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
#ifdef LOG
            int result = zQP_write_async(qp, local_addr, new_lkey, length, remote_addr, new_rkey, qp->time_stamp, true, wr_ids, device);
#else
            int result = zQP_write_async(qp, local_addr, new_lkey, length, remote_addr, new_rkey, qp->time_stamp, false, wr_ids, device);
#endif
            if (result == -1)
            {
                if (z_switch(qp, device) < 0)
                {
                    return -1;
                }
                return z_write_async(qp, local_addr, lkey, length, remote_addr, rkey, wr_ids);
            }
            return result;
        }
        else
        {
            if (device != 0)
            {
                tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                qp->m_pd->m_lkey_table.find(a, lkey);
                new_lkey = a->second[device];
                new_rkey = qp->m_rkey_table->at(rkey)[device];
            }
            int result = zDCQP_write(qp->m_pd->m_requestors[device][0], qp->m_targets[device]->ah, local_addr, new_lkey, length, remote_addr, new_rkey, qp->m_targets[device]->lid_, qp->m_targets[device]->dct_num_);
            qp->size_counter_.fetch_add(length);
            return result;
        }
    }

    int z_CAS_async(zQP *qp, void *local_addr, uint32_t lkey, uint64_t new_val, void *remote_addr,
                    uint32_t rkey, vector<uint64_t> *wr_ids)
    {
        zRouteSnapshot route;
        if (z_prepare_route(qp, route) < 0)
        {
            return -1;
        }
        uint32_t new_lkey = lkey;
        uint32_t new_rkey = rkey;
        // if(qp->current_device != 0){
        //     tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
        //     qp->m_pd->m_lkey_table.find(a, lkey);
        //     new_lkey = a->second[qp->current_device];
        //     new_rkey = qp->m_rkey_table->at(rkey)[qp->current_device];
        // }
        int device = route.device;
        if (z_route_uses_rc(qp, route))
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
            uint64_t old_val = *(uint64_t *)local_addr;
            int result = zQP_CAS_async(qp, local_addr, new_lkey, new_val, remote_addr, new_rkey,
                                       qp->time_stamp, wr_ids, device);
            if (result == -1)
            {
                if (z_switch(qp, device) < 0)
                {
                    return -1;
                }
                return z_CAS_async(qp, local_addr, lkey, new_val, remote_addr, rkey, wr_ids);
            }
            return 0;
        }
        else
        {
            if (device != 0)
            {
                tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
                qp->m_pd->m_lkey_table.find(a, lkey);
                new_lkey = a->second[device];
                new_rkey = qp->m_rkey_table->at(rkey)[device];
            }
            int result = zDCQP_CAS(qp->m_pd->m_requestors[device][0],
                                   qp->m_targets[device]->ah, local_addr, new_lkey, new_val, remote_addr, new_rkey,
                                   qp->m_targets[device]->lid_, qp->m_targets[device]->dct_num_);
            qp->size_counter_.fetch_add(sizeof(uint64_t));
            return result;
        }
    }

    int z_write(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey)
    {
        zRouteSnapshot route;
        if (z_prepare_route(qp, route) < 0)
        {
            return -1;
        }
        int device = route.device;
        if (device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            qp->m_pd->m_lkey_table.find(a, lkey);
            lkey = a->second[device];
            rkey = qp->m_rkey_table->at(rkey)[device];
        }
        if (z_route_uses_rc(qp, route))
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
#ifdef LOG
            int result = zQP_write(qp, local_addr, lkey, length, remote_addr, rkey, qp->time_stamp, true, device);
#else
            int result = zQP_write(qp, local_addr, lkey, length, remote_addr, rkey, qp->time_stamp, false, device);
#endif
            return result;
        }
        else
        {
            int result = zDCQP_write(qp->m_pd->m_requestors[device][0],
                                     qp->m_targets[device]->ah, local_addr, lkey, length, remote_addr, rkey,
                                     qp->m_targets[device]->lid_, qp->m_targets[device]->dct_num_);
            qp->size_counter_.fetch_add(length);
            return result;
        }
    }

    int z_read(zQP *qp, void *local_addr, uint32_t lkey, uint64_t length, void *remote_addr, uint32_t rkey)
    {
        zRouteSnapshot route;
        if (z_prepare_route(qp, route) < 0)
        {
            return -1;
        }
        int device = route.device;
        if (device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            qp->m_pd->m_lkey_table.find(a, lkey);
            lkey = a->second[device];
            rkey = qp->m_rkey_table->at(rkey)[device];
        }
        if (z_route_uses_rc(qp, route))
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
            return zQP_read(qp, local_addr, lkey, length, remote_addr, rkey, qp->time_stamp, device);
        }
        else
        {
            int result = zDCQP_read(qp->m_pd->m_requestors[device][0],
                                    qp->m_targets[device]->ah, local_addr, lkey, length, remote_addr, rkey,
                                    qp->m_targets[device]->lid_, qp->m_targets[device]->dct_num_);
            qp->size_counter_.fetch_add(length);
            return result;
        }
    }

    int z_CAS(zQP *qp, void *local_addr, uint32_t lkey, uint64_t new_val, void *remote_addr, uint32_t rkey)
    {
        zRouteSnapshot route;
        if (z_prepare_route(qp, route) < 0)
        {
            return -1;
        }
        int device = route.device;
        if (device != 0)
        {
            tbb::concurrent_hash_map<uint32_t, tbb::concurrent_vector<uint32_t>>::accessor a;
            qp->m_pd->m_lkey_table.find(a, lkey);
            lkey = a->second[device];
            rkey = qp->m_rkey_table->at(rkey)[device];
        }
        if (z_route_uses_rc(qp, route))
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
            uint64_t old_val = *(uint64_t *)local_addr;
            return zQP_CAS(qp, local_addr, lkey, new_val, remote_addr, rkey, qp->time_stamp, device);
        }
        else
        {
            int result = zDCQP_CAS(qp->m_pd->m_requestors[device][0], qp->m_targets[device]->ah, local_addr, lkey, new_val, remote_addr, rkey, qp->m_targets[device]->lid_, qp->m_targets[device]->dct_num_);
            qp->size_counter_.fetch_add(sizeof(uint64_t));
            return result;
        }
    }

    int z_post_send(zQP *qp, ibv_send_wr *send_wr, ibv_send_wr **bad_wr, bool non_idempotent,
                    uint32_t time_stamp, bool unique_cas, int max_depth)
    {
        zRouteSnapshot route;
        if (z_prepare_route(qp, route) < 0)
        {
            return -1;
        }
        int device = route.device;
        if (z_route_uses_rc(qp, route))
        {
            qp->time_stamp = (qp->time_stamp + 1) % MAX_REQUESTOR_NUM;
            int result = zQP_post_send(qp, send_wr, bad_wr, non_idempotent, qp->time_stamp, unique_cas, max_depth, device);
            return result;
        }
        else
        {
            int result = zDCQP_send(qp, qp->m_pd->m_requestors[device][0], qp->m_targets[device]->ah, send_wr, qp->m_targets[device]->lid_, qp->m_targets[device]->dct_num_, max_depth);
            return result;
        }
    }

    int z_poll_completion(zQP *qp, uint64_t wr_id, int device)
    {
        device = z_resolve_device(qp, device);
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
            if (TIME_DURATION_US(start, TIME_NOW) > RDMA_TIMEOUT_US)
            {
                // std::cerr << "Error, poll completion timeout" << std::endl;
                z_switch(qp, device);
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
            int start_ = qp->entry_start_;
            int end_ = qp->entry_end_.load() % WR_ENTRY_NUM;
            zWR_header *header = (zWR_header *)&wr_id;
            qp->size_counter_.fetch_add(((ibv_send_wr *)(header->wr_addr))->sg_list->length);
            if (start_ > end_)
                end_ += WR_ENTRY_NUM;
            if (start_ != end_)
            {
                for (int i = start_; i < end_; i++)
                {
                    if (qp->wr_entry_[i % WR_ENTRY_NUM].time_stamp == header->time_stamp &&
                        qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr == header->wr_addr)
                    {
                        // delete ((ibv_send_wr *)qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr)->sg_list;
                        // delete (ibv_send_wr *)qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr;
                        qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr = 0;
                        qp->wr_entry_[i % WR_ENTRY_NUM].finished = 1;
                    }
                    if (qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr == 0 && i % WR_ENTRY_NUM == qp->entry_start_)
                    {
                        qp->entry_start_ = (qp->entry_start_ + 1) % WR_ENTRY_NUM;
                    }
                }
            }
            // a->second.valid = 0;
            qp->m_ep->completed_table.erase(a);
        }
        if (return_val < 0)
        {
            if (error_device != device)
            {
                z_recovery(qp);
            }
            else
            {
                z_switch(qp, device);
                z_recovery(qp);
            }
        }
        return return_val;
    }

    int z_poll_completion(zQP *qp, vector<uint64_t> *wr_ids, int device)
    {
        device = z_resolve_device(qp, device);
        int return_val = 0;
        int error_device = -1;
        while (wr_ids->size() > 0)
        {
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
                    int start_ = qp->entry_start_;
                    int end_ = qp->entry_end_.load() % WR_ENTRY_NUM;
                    if (start_ != end_)
                    {
                        zWR_header *header = (zWR_header *)&id;
                        qp->size_counter_.fetch_add(((ibv_send_wr *)(header->wr_addr))->sg_list->length);
                        if (start_ > end_)
                            end_ += WR_ENTRY_NUM;
                        for (int i = start_; i < end_; i++)
                        {
                            if (qp->wr_entry_[i % WR_ENTRY_NUM].time_stamp == header->time_stamp &&
                                qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr == header->wr_addr)
                            {
                                // delete ((ibv_send_wr *)qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr)->sg_list;
                                // delete (ibv_send_wr *)qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr;
                                qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr = 0;
                                qp->wr_entry_[i % WR_ENTRY_NUM].finished = 1;
                            }
                            if ((qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr == 0 || qp->wr_entry_[i % WR_ENTRY_NUM].finished == 1) && i % WR_ENTRY_NUM == qp->entry_start_)
                            {
                                qp->entry_start_ = (qp->entry_start_ + 1) % WR_ENTRY_NUM;
                            }
                        }
                    }
                    wr_ids->erase(std::remove(wr_ids->begin(), wr_ids->end(), id), wr_ids->end());
                    qp->m_ep->completed_table.erase(a);
                    break;
                }
            }
        }
        if (return_val < 0)
        {
            if (error_device != device)
            {
#ifdef RECOVERY
                z_recovery(qp);
#endif
            }
            else
            {
                z_switch(qp, device);
#ifdef RECOVERY
                z_recovery(qp);
#endif
            }
        }
        return return_val;
    }

    static void zQP_rebuild_rc(zQP *qp)
    {
        while (true)
        {
            int device = -1;
            uint64_t switch_epoch = 0;
            zTarget *target = nullptr;
            {
                std::lock_guard<std::mutex> lock(qp->route_mutex);
                zRouteSnapshot route = z_get_route_snapshot(qp);
                if (route.mode != ZPATH_RC_REBUILDING)
                {
                    qp->rebuild_running.store(false, std::memory_order_release);
                    return;
                }
                device = route.device;
                switch_epoch = route.epoch;
                auto target_it = qp->m_targets.find(device);
                if (target_it != qp->m_targets.end())
                {
                    target = target_it->second;
                }
            }

            int result = -1;
            for (int attempt = 0; attempt < 3; attempt++)
            {
                zQP_requestor *requestor = z_get_active_requestor(qp, device);
                if (requestor != nullptr && requestor->status_ == ZSTATUS_CONNECTED)
                {
                    result = 0;
                    break;
                }
                if (target != nullptr)
                {
                    result = zQP_connect(qp, device, target->ip, target->port);
                }
                if (result == 0)
                {
                    break;
                }
                zRouteSnapshot retry_route = z_get_route_snapshot(qp);
                if (retry_route.device != device || retry_route.epoch != switch_epoch ||
                    retry_route.mode != ZPATH_RC_REBUILDING)
                {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10 << attempt));
            }

            std::lock_guard<std::mutex> lock(qp->route_mutex);
            zRouteSnapshot route = z_get_route_snapshot(qp);
            if (route.device != device || route.epoch != switch_epoch || route.mode != ZPATH_RC_REBUILDING)
            {
                if (route.mode == ZPATH_RC_REBUILDING)
                {
                    continue;
                }
                qp->rebuild_running.store(false, std::memory_order_release);
                return;
            }
            if (result == 0)
            {
                z_publish_route(qp, device, ZPATH_RC_ACTIVE);
                printf("RCQP rebuilt on device %d at time %lu\n", device, TIME_NOW.time_since_epoch().count());
            }
            else
            {
                z_publish_route(qp, device, ZPATH_DC_ACTIVE);
                fprintf(stderr, "RCQP rebuild failed on device %d; continuing with DCQP\n", device);
            }
            qp->rebuild_running.store(false, std::memory_order_release);
            return;
        }
    }

    int z_switch(zQP *qp, int failed_device)
    {
        bool start_rebuild = false;
        std::thread completed_rebuild;
        {
            std::lock_guard<std::mutex> lock(qp->route_mutex);
            zRouteSnapshot route = z_get_route_snapshot(qp);
            if (failed_device >= 0 && failed_device != route.device)
            {
                return 0;
            }
            if (route.mode == ZPATH_FAILED)
            {
                return -1;
            }

            int failed = route.device;
            printf("Start switch on device %d at time %lu\n", failed, TIME_NOW.time_since_epoch().count());
#ifndef NO_ERROR_HANDLE
            qp->m_ep->m_devices[failed]->status.store(ZSTATUS_ERROR);
            zQP_requestor *failed_requestor = z_get_active_requestor(qp, failed);
            if (failed_requestor != nullptr)
            {
                failed_requestor->status_ = ZSTATUS_ERROR;
            }
            qp->failed_device.store(failed, std::memory_order_release);
            qp->failed_requestor.store(failed_requestor, std::memory_order_release);
            if (qp->qp_type == ZQP_RPC)
            {
                z_publish_route(qp, failed, ZPATH_FAILED);
                return -1;
            }

            int recovery_device = -1;
            int device_count = qp->m_ep->m_devices.size();
            for (int offset = 1; offset < device_count; offset++)
            {
                int candidate = (failed + offset) % device_count;
                if (candidate >= MAX_NIC_NUM || qp->m_ep->m_devices[candidate]->status.load() == ZSTATUS_ERROR)
                {
                    continue;
                }
                if (qp->m_targets.find(candidate) == qp->m_targets.end() ||
                    candidate >= static_cast<int>(qp->m_pd->m_requestors.size()) || qp->m_pd->m_requestors[candidate].empty())
                {
                    continue;
                }
                recovery_device = candidate;
                break;
            }

            if (recovery_device < 0)
            {
                z_publish_route(qp, failed, ZPATH_FAILED);
                return -1;
            }

            z_publish_route(qp, recovery_device, ZPATH_RC_REBUILDING);
            if (!qp->rebuild_running.exchange(true, std::memory_order_acq_rel))
            {
                start_rebuild = true;
                if (qp->rebuild_thread_.joinable())
                {
                    completed_rebuild = std::move(qp->rebuild_thread_);
                }
            }
#endif
        }
        if (completed_rebuild.joinable())
        {
            completed_rebuild.join();
        }
        if (start_rebuild)
        {
            std::lock_guard<std::mutex> lock(qp->route_mutex);
            qp->rebuild_thread_ = std::thread(zQP_rebuild_rc, qp);
        }
        return 0;
    }

    int z_recovery(zQP *qp)
    {
        // sleep(1);
        int recovery_device = qp->failed_device.load(std::memory_order_acquire);
        zQP_requestor *recovery_requestor = qp->failed_requestor.load(std::memory_order_acquire);
        if (recovery_device < 0 || recovery_requestor == nullptr)
        {
            return -1;
        }
        // printf("Start recovery on device %d\n", qp->current_device);
        // read recovery log
        int start = qp->entry_start_;
        int end = qp->entry_end_.load() % WR_ENTRY_NUM;
        if (start == end)
        {
            return 0;
        }
        memset(qp->cmd_resp_, 0, sizeof(CmdMsgRespBlock));
        z_read(qp, (void *)qp->cmd_resp_, qp->resp_mr_[0]->lkey, sizeof(CmdMsgRespBlock), (void *)recovery_requestor->server_cmd_msg_, recovery_requestor->server_cmd_rkey_[0]);
        // z_read(qp, (void*)qp->cmd_resp_, qp->resp_mr_[0]->lkey, sizeof(CmdMsgRespBlock), (void*)qp->m_requestors[recovery_device]->server_cmd_msg_, qp->m_requestors[recovery_device]->server_cmd_rkey_[0]);
        zWR_entry *entry = (zWR_entry *)qp->cmd_resp_;

        // for(int i = 0; i < WR_ENTRY_NUM; i++){
        //     printf("Debug: local time_stamp %d, remote time_stamp %d\n", qp->wr_entry_[i%WR_ENTRY_NUM].time_stamp, entry[i].time_stamp);
        // }
        if (start > end)
            end += WR_ENTRY_NUM;
        if (start != end)
        {
            for (int i = start; i < end; i++)
            {
                if (qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr == 0 || qp->wr_entry_[i % WR_ENTRY_NUM].finished == 1)
                {
                    if (i % WR_ENTRY_NUM == qp->entry_start_)
                    {
                        qp->entry_start_ = (qp->entry_start_ + 1) % WR_ENTRY_NUM;
                    }
                    continue;
                }
                struct ibv_send_wr *send_wr = (struct ibv_send_wr *)qp->wr_entry_[i % WR_ENTRY_NUM].wr_addr;
                // if (send_wr->opcode == IBV_WR_ATOMIC_CMP_AND_SWP){
                //     zAtomic_buffer* buffer = (zAtomic_buffer*)(&entry[i%WR_ENTRY_NUM]);
                //     int local_time = qp->wr_entry_[i%WR_ENTRY_NUM].time_stamp;
                //     int remote_time = buffer->time_stamp;
                //     if((local_time > remote_time && local_time - remote_time < 16384) || (local_time < remote_time && remote_time - local_time > 16384)){
                //         // resend CAS
                //         // printf("qp %d resend local timestamp %d, remote timestamp %d, wr_id %lu, opcode %d, addr %lx, length %u\n", qp->qp_id_, local_time, remote_time, send_wr->wr_id, send_wr->opcode, send_wr->sg_list->addr, send_wr->sg_list->length);
                //         z_CAS(qp, (uint64_t*)send_wr->sg_list->addr, send_wr->sg_list->lkey, qp->wr_entry_[i%WR_ENTRY_NUM].reserved, (void*)send_wr->wr.atomic.remote_addr, send_wr->wr.atomic.rkey);
                //         int start_ = qp->entry_start_;
                //         int end_ = qp->entry_end_.load()%WR_ENTRY_NUM;
                //         if(start_ > end_)
                //             end_ += WR_ENTRY_NUM;
                //         if(start_ != end_){
                //             for(int i = start_; i < end_; i++){
                //                 if(qp->wr_entry_[i%WR_ENTRY_NUM].time_stamp == local_time &&
                //                     qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == (uint64_t)send_wr){
                //                     delete ((ibv_send_wr*)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr)->sg_list;
                //                     delete (ibv_send_wr*)qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr;
                //                     qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr = 0;
                //                 }
                //                 if(qp->wr_entry_[i%WR_ENTRY_NUM].wr_addr == 0 && i%WR_ENTRY_NUM == qp->entry_start_){
                //                     qp->entry_start_ = (qp->entry_start_ + 1)%WR_ENTRY_NUM;
                //                 }
                //             }
                //         }
                //         continue;
                //     }
                //     if(buffer->finished == 1) {
                //         continue;
                //     }
                //     int new_val = qp->wr_entry_[i%WR_ENTRY_NUM].reserved;
                //     z_read(qp, (void*)&entry[i%WR_ENTRY_NUM].reserved, qp->resp_mr_[qp->current_device]->lkey, sizeof(uint64_t), (void*)send_wr->wr.atomic.remote_addr, send_wr->wr.atomic.rkey);
                //     if(entry[i%WR_ENTRY_NUM].reserved == send_wr->wr.atomic.swap){
                //         continue;
                //     } else {
                //         *(uint64_t*)send_wr->sg_list->addr = entry[i%WR_ENTRY_NUM].reserved;
                //     }
                // } else {
                int local_time = qp->wr_entry_[i % WR_ENTRY_NUM].time_stamp;
                int remote_time = entry[i % WR_ENTRY_NUM].time_stamp;
#ifndef SEND_TWICE
                if (local_time != remote_time)
                {
#endif
                    // attention: 48bit address to 64bit address
                    z_post_send(qp, send_wr, nullptr, true, local_time, true, 1);
                    qp->size_counter_.fetch_add(send_wr->sg_list->length);
                    // printf("qp %d resend local timestamp %d, remote timestamp %d, local_addr %lx, remote_addr %lx\n", qp->qp_id_, local_time, remote_time, entry[i % WR_ENTRY_NUM].wr_addr, (uint64_t)send_wr);
                    int start_ = qp->entry_start_;
                    int end_ = qp->entry_end_.load() % WR_ENTRY_NUM;
                    if (start_ > end_)
                        end_ += WR_ENTRY_NUM;
                    if (start_ != end_)
                    {
                        for (int j = start_; j < end_; j++)
                        {
                            if (qp->wr_entry_[j % WR_ENTRY_NUM].time_stamp == local_time &&
                                qp->wr_entry_[j % WR_ENTRY_NUM].wr_addr == (uint64_t)send_wr)
                            {
                                // free(((ibv_send_wr *)qp->wr_entry_[j % WR_ENTRY_NUM].wr_addr)->sg_list);
                                // delete (ibv_send_wr *)qp->wr_entry_[j % WR_ENTRY_NUM].wr_addr;
                                qp->wr_entry_[j % WR_ENTRY_NUM].wr_addr = 0;
                                qp->wr_entry_[j % WR_ENTRY_NUM].finished = 1;
                            }
                            if ((qp->wr_entry_[j % WR_ENTRY_NUM].wr_addr == 0 || qp->wr_entry_[j % WR_ENTRY_NUM].finished == 1) && j % WR_ENTRY_NUM == qp->entry_start_)
                            {
                                qp->entry_start_ = (qp->entry_start_ + 1) % WR_ENTRY_NUM;
                            }
                        }
                    }
#ifndef SEND_TWICE
                }
                else if (local_time == remote_time)
                {
                    // printf("finished timestamp %d, remote timestamp %d, wr_id %lu, opcode %d, addr %lx, length %u\n", local_time, remote_time, send_wr->wr_id, send_wr->opcode, send_wr->sg_list->addr, send_wr->sg_list->length);
                    // auto p = send_wr;
                    // while (p != nullptr)
                    // {
                    // if (p->opcode == IBV_WR_ATOMIC_CMP_AND_SWP)
                    // {
                    //     zAtomic_buffer *buffer = (zAtomic_buffer *)(&entry[i % WR_ENTRY_NUM]);
                    //     if (buffer->finished != 1)
                    //     {
                    //         int new_val = qp->wr_entry_[i % WR_ENTRY_NUM].reserved;
                    //         z_read(qp, (void *)&entry[i % WR_ENTRY_NUM].reserved, 
                    //             qp->resp_mr_[qp->current_device]->lkey, sizeof(uint64_t), 
                    //             (void *)p->wr.atomic.remote_addr, p->wr.atomic.rkey);
                    //         if (entry[i % WR_ENTRY_NUM].reserved != p->wr.atomic.swap)
                    //         {
                    //             *(uint64_t *)p->sg_list->addr = entry[i % WR_ENTRY_NUM].reserved;
                    //         }
                    //     }
                    // }
                    //     p = p->next;
                }
                    // }
#endif
            }
        }
        printf("Finish recovery %d at time %lu\n", qp->current_device.load(), TIME_NOW.time_since_epoch().count());
        return 0;
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

    int zQP_listen(zQP_listener *zqp, int nic_index, string ip, string port)
    {
        if (zqp->listeners.find(nic_index) != zqp->listeners.end())
        {
            return -1;
        }
        zQP_responder *qp = new zQP_responder();
        zqp->listeners[nic_index] = qp;

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
        ibv_mr *cache_mr;
        uint64_t cache_addr;
        uint64_t cache_size = (size_t)1024*1024*1024*(6) + (size_t)1024*1024*2500;
        cache_mr = mr_malloc_create(pd, cache_addr,
                                cache_size);
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
                resp_msg->addr = (uint64_t)(cache_mr->addr);
                tbb::concurrent_hash_map<ibv_mr *, tbb::concurrent_vector<ibv_mr *>>::accessor a;
                pd->m_mrs.find(a, cache_mr);
                for (int i = 0; i < a->second.size(); i++)
                {
                    resp_msg->rkey[i] = a->second[i]->rkey;
                }
                resp_msg->status = RES_OK;
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
        ibv_cq *cq = ibv_create_cq(zqp->m_ep->m_devices[nic_index]->context, 1, NULL, comp_channel, 0);
        assert(cq != NULL);
        int result = ibv_req_notify_cq(cq, 0);
        assert(result == 0);
        ibv_qp_init_attr qp_init_attr;
        memset(&qp_init_attr, 0, sizeof(qp_init_attr));
        qp_init_attr.send_cq = cq;
        qp_init_attr.recv_cq = cq;
        qp_init_attr.qp_type = IBV_QPT_RC;
        qp_init_attr.cap.max_send_wr = 1;
        qp_init_attr.cap.max_recv_wr = 1;
        qp_init_attr.cap.max_send_sge = 1;
        qp_init_attr.cap.max_recv_sge = 1;
        qp_init_attr.cap.max_inline_data = 256;
        qp_init_attr.sq_sig_all = 0;
        result = rdma_create_qp(cm_id, zqp->m_pd->m_pds[nic_index], &qp_init_attr);
        assert(result == 0);

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
            ibv_mr *mr = mr_malloc_create(zqp->m_pd, zqp->qp_info[id].addr, sizeof(zAtomic_buffer) * WR_ENTRY_NUM);
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
            memcpy(rep_pdata.ip[i], zqp->m_ep->m_devices[i]->eth_ip.c_str(), zqp->m_ep->m_devices[i]->eth_ip.size());
            memcpy(rep_pdata.port[i], zqp->m_ep->m_devices[i]->port.c_str(), zqp->m_ep->m_devices[i]->port.size());
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
