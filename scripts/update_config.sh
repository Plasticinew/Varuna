#!/bin/bash

sudo mst start

sudo mlxconfig -d /dev/mst/mt4119_pciconf1 -y set KEEP_ETH_LINK_UP_P1=0 KEEP_ETH_LINK_UP_P2=0

