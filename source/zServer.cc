#include "zQP.h"

using namespace zrdma;
int main(int argc, char** argv) {
    if(argc < 2) {
        printf("Usage: %s <local_config_file>\n", argv[0]);
        return -1;
    }
    string config_file = argv[1];
    zEndpoint *ep = zEP_create(config_file);
    zPD *pd = zPD_create(ep, 1);
    zQP_listener *qp = zQP_listener_create(pd, ep);
    std::vector<std::thread*> listeners;
    for (int i = 0; i < ep->m_device_num; i++) {
        listeners.push_back(new std::thread(&zQP_listen, qp, i,
            std::ref(ep->m_devices[i]->eth_ip), std::ref(ep->m_devices[i]->port)));
    }
    for (auto *t : listeners) {
        t->join();
    }
    return 0;
}
