# Motor TPC-C test (Varuna)

Vendored from https://github.com/Plasticinew/Motor_Zep.git and linked against this repo's `libzQP.a`.

Two CloudLab nodes: node-0 is the compute node, node-1 is the memory node. Dual-NIC IPs must match `config/zrdma_*.json`.

```bash
# node-0: build Varuna runtime first
cmake -S /users/X1aoyang/Varuna -B /users/X1aoyang/Varuna/build
cmake --build /users/X1aoyang/Varuna/build -j

# both nodes: build Motor
cd /users/X1aoyang/Varuna/tests/motor
./build.sh          # compute node (also builds server)
./build.sh -s       # memory node only
```

```bash
# node-1
cd /users/X1aoyang/Varuna/tests/motor/build/memory_node/server
./motor_mempool

# node-0 (after MN has loaded tables)
cd /users/X1aoyang/Varuna/tests/motor/build/compute_node/run
./run tpcc 2 2 SI
```

Results: `tests/motor/bench_results/tpcc/result.txt`.
