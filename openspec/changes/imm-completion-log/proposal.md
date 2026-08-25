## Why

当前带保护的单边 write 会在数据 WRITE 后再链一次 inline log WRITE，把时间戳写入 responder 的 completion log。这次额外 round 增加约 1µs 延迟。RDMA `WRITE_WITH_IMM` 可以在数据落盘的同时带上 32-bit 立即数，并在接收侧 recv CQ 生成 CQE，适合作为「write 是否已在远端执行」的校验数。

接收侧 CQ 不能当持久日志：不 poll 会 overflow / RNR，poll 掉的 CQE 会消失，且恢复跑在 client、无法 `ibv_poll_cq` 对端 CQ。因此采用推荐方案：稳态 worker 取出 imm、写入现有 completion 数组并补 RQ；恢复仍 RDMA-read 该数组。

## What Changes

- `use_log` 的 RC write 改为 `IBV_WR_RDMA_WRITE_WITH_IMM`，立即数编码时间戳和 log slot，不再链额外 log WRITE。
- 服务端为 onesided QP 扩大 RQ/CQ，连接建立前预投 recv。
- 新增常驻 IMM CQ worker：poll recv CQE、把 imm 写入 `qp_info` 对应的 completion log、按 CQE 数量补 RQ。
- `z_recovery()` 比较逻辑保持「本地 timestamp 对远端 log slot」，只是远端 slot 改由 worker 填写。
- CAS 两阶段 buffer、DCQP 临时路径上的无 log write 不在本变更范围。

## Capabilities

### New Capabilities

- `completion-log`: 用 WRITE_WITH_IMM 校验数填充 responder completion log，并由稳态 worker 维持 RQ/CQ

### Modified Capabilities

## Impact

- `include/zQP.h`：imm 编码、onesided 连接登记、worker 声明
- `source/zQP.cc`：`zQP_write` / `zQP_write_async` / `zQP_post_send*`、`zQP_accept`、listener worker
- 恢复路径 `z_recovery()` 读的仍是 `server_cmd_msg_` 指向的同一块 log 内存
- 不影响 RPC QP 的 `zQP_worker` 发送完成轮询（onesided 与 RPC 的 CQ 用途分开）
