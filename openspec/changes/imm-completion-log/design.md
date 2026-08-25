## Context

现有 logged write 在同一 SQ 上链 `数据 WRITE → inline log WRITE`，恢复时 client 用 DCQP/备用 RC 把 `server_cmd_msg_` 读回，按 slot 比较 timestamp。服务端已有 `zQP_flush`（1ms 扫 CAS buffer），但 onesided QP 的 CQ 只有 1、`max_recv_wr` 为 1，且从未 `ibv_post_recv`。

## Goals / Non-Goals

**Goals:**

- 去掉 logged write 的额外 log WRITE，改用 IMM 校验数
- 用常驻 worker 把 imm 落到现有 completion 数组，恢复逻辑尽量不动
- onesided QP 的 RQ/CQ 足以支撑稳态 IMM，且在 accept 前预投 recv

**Non-Goals:**

- 不把 CQ ignore-overrun / 注册 CQE ring 当 log
- 不为 CAS 或 DCQP 临时 write 增加 IMM
- 不改故障检测（heartbeat、CQ error 组合）
- 不把 RPC QP 的 send CQ 交给 IMM worker 抢 poll

## Decisions

1. **imm 编码**  
   主机字节序：`[31:16]=time_stamp`，`[15:0]=slot`；`ibv_send_wr.imm_data` / `ibv_wc.imm_data` 按 verbs 约定用 `htonl`/`ntohl`。写入远端 log 的仍是 `zWR_entry` 第二个 8 字节（与旧 inline log WRITE 同一位置），避免恢复比较改语义。

2. **谁写 log**  
   只有 server worker 写 completion log。Client 不再 RDMA-write log。worker 延迟会造成「数据已执行但 log 尚未可见」，恢复会保守重传；对覆盖写是安全的。

3. **RQ/CQ 深度**  
   `IMM_RECV_DEPTH = 512`，`IMM_CQ_SIZE = 2048`。只对 `ZQP_ONESIDED` 生效。RPC 连接保持小 RQ/CQ，避免和 `zQP_worker`/`worker_write` 抢 CQE。

4. **Worker 形态**  
   独立 `zQP_imm_cq_worker`，不复用 `zQP_flush` 的 1ms sleep。空 CQ 时 `yield`，有完成时批量 poll（最多 64）。连接表用 mutex + vector，accept 时登记。

5. **recv WQE**  
   WRITE_WITH_IMM 不把 payload 放入 recv buffer，投 `num_sge = 0` 的 recv。每消耗一个 CQE 补一个 recv。

6. **覆盖的客户端入口**  
   `zQP_write`、`zQP_write_async`，以及 `zQP_post_send` / `zQP_post_send_async` 里 opcode 为 `RDMA_WRITE` / `RDMA_WRITE_WITH_IMM` 的条目。`IBV_WR_SEND` 仍走旧的链 log WRITE（本变更不依赖 SEND recv）。

7. **备选方案（否决）**  
   - 只在恢复时倒 CQ：稳态 overflow/RNR，且 client 倒不了对端 CQ  
   - 把 CQE ring 注册成 MR：要解析 mlx5 CQE、绕开 ibverbs RQ 记账，DCT 还要再做一遍

## Risks / Trade-offs

- Worker CPU：onesided 服务端多一条忙询线程；换来的是去掉每笔 write 的额外 inline WRITE。
- 空 RQ 仍会在 `rnr_retry=0` 下直接被当成故障；必须保证预投 + 及时补货。
- 多 QP 共用一个 worker 扫描所有 onesided CQ；连接数很大时可能需要后续分片，本版按现有 `MAX_SERVER_CLIENT` 原型规模足够。
