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
    rkeyTable *table = new rkeyTable();
    zQP_listener *qp = zQP_listener_create(pd, ep);
    std::thread listener_1 = std::thread(&zQP_listen, qp, 0, std::ref(ep->m_devices[0]->eth_ip), std::ref(ep->m_devices[0]->port));
    std::thread listener_2 = std::thread(&zQP_listen, qp, 1, std::ref(ep->m_devices[1]->eth_ip), std::ref(ep->m_devices[1]->port));
    listener_1.join();
    listener_2.join();
    return 0;
}