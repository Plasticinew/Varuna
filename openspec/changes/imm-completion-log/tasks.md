## 1. 数据结构与规则

- [x] 1.1 在 `include/zQP.h` 增加 imm 编码辅助函数、`IMM_RECV_DEPTH`/`IMM_CQ_SIZE`、`zImmRecvConn`，以及 listener 上的连接表与 worker 线程字段
- [x] 1.2 补充中文 Cursor 规则，约束后续改动不得把 CQ 当持久 log、不得在恢复时倒对端 CQ

## 2. 客户端 logged write

- [x] 2.1 修改 `zQP_write`：`use_log` 时改为 `WRITE_WITH_IMM` 并去掉链上的 log WRITE
- [x] 2.2 同样修改 `zQP_write_async`
- [x] 2.3 修改 `zQP_post_send` / `zQP_post_send_async` 中 WRITE 的 log 附加逻辑，改为设置 IMM，而不是再链 inline log WRITE

## 3. 服务端 RQ/CQ 与 worker

- [x] 3.1 `zQP_accept` 对 onesided QP 扩大 CQ/RQ，在 `rdma_accept` 前预投 recv，并登记到 IMM 连接表
- [x] 3.2 实现 `zQP_post_imm_recvs` 与 `zQP_imm_cq_worker`：落 imm 到 completion log 并补 RQ；在 `zQP_listener_create` 启动该线程

## 4. 恢复与验证

- [x] 4.1 确认 `z_recovery()` 仍 RDMA-read 原 log 内存并按 slot 比较 timestamp；CAS 路径不改
- [x] 4.2 编译 `zQP` / `zServer` / `zClient` / `latency_test`，修复新增的编译错误
