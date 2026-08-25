## Purpose

用单边 write 的立即数作为完成校验，在 responder 侧由稳态 worker 写入 completion log，供故障恢复区分 pre-failure / post-failure write。

## ADDED Requirements

### Requirement: Logged write 携带立即数校验

带 completion log 的 RC 单边 write SHALL 使用 `IBV_WR_RDMA_WRITE_WITH_IMM`，并在立即数中编码该请求的时间戳与本地 log slot。该 write SHALL NOT 再链一条专门写 completion log 的 RDMA WRITE。不带 log 的 write（`use_log == false`）SHALL 保持普通 `IBV_WR_RDMA_WRITE`。

#### Scenario: 同步 logged write 只发一笔数据 WR

- **GIVEN** 调用方走 `zQP_write(..., use_log=true)`
- **WHEN** 运行时向 RC QP `ibv_post_send`
- **THEN** 链头是带 IMM 的数据 write，且 `next == NULL`
- **AND** 立即数（主机字节序）高 16 位为时间戳、低 16 位为 slot

#### Scenario: 未开启 log 的 write 行为不变

- **GIVEN** `use_log == false`（例如 RPC 控制面 write）
- **WHEN** 发送该 write
- **THEN** opcode 仍为 `IBV_WR_RDMA_WRITE`，不消耗 responder 用于 IMM 的校验路径

### Requirement: 服务端稳态处理 recv CQE

服务端 SHALL 在 onesided QP 进入 RTR/RTS 之前预投不少于 `IMM_RECV_DEPTH` 个 recv WQE，并为该 QP 提供足以容纳这些完成的 recv CQ。服务端 SHALL 运行常驻 worker：从 recv CQ 取出 `IBV_WC_RECV_RDMA_WITH_IMM`，把立即数写入该 QP 的 completion log 对应 slot，并按完成数量重新 `ibv_post_recv`。Worker SHALL NOT 只清空 CQ 而不落盘 imm。恢复路径 SHALL NOT 把「现场倒 CQ」当作 completion log 的来源。

#### Scenario: 预投 recv 发生在客户端可发送之前

- **GIVEN** 一次 onesided `rdma_accept`
- **WHEN** 连接对客户端变为 ESTABLISHED
- **THEN** 该 QP 上已经有预投的 recv WQE，首笔 IMM write 不会因为空 RQ 而 RNR

#### Scenario: worker 把 imm 写入可被 RDMA 读的 log

- **GIVEN** responder 收到一笔 logged write 的 IMM CQE，slot 合法
- **WHEN** worker poll 到该 CQE
- **THEN** 该 QP completion log 的对应 slot 时间戳等于立即数中的时间戳
- **AND** worker 再投递至少与本次完成数量相同的 recv WQE

#### Scenario: 禁止把未持久化的 CQ 当恢复依据

- **GIVEN** 主路径故障，client 进入 `z_recovery()`
- **WHEN** 判断 in-flight write 是否已在远端执行
- **THEN** client 通过备用路径 RDMA-read completion log 内存
- **AND** 不调用对端 `ibv_poll_cq`，也不把硬件 CQE ring 当作唯一证据

### Requirement: 恢复比较保持 slot 语义

`z_recovery()` SHALL 继续用本地 `wr_entry_[slot].time_stamp` 与远端 log 同 slot 的时间戳比较：不相等则视为 pre-failure 并重传 write；相等则视为 post-failure 且 MUST NOT 再执行该 write。CAS 条目 SHALL 仍走原有两阶段恢复，不受 IMM write 路径影响。

#### Scenario: 远端 log 命中则不重传 write

- **GIVEN** in-flight write 的本地 timestamp 为 T、slot 为 S
- **AND** 远端 log[S] 的 timestamp 已为 T
- **WHEN** 执行恢复
- **THEN** 该 write 不被再次 `path_write`

#### Scenario: 远端 log 未命中则重传 write

- **GIVEN** in-flight write 的本地 timestamp 为 T、slot 为 S
- **AND** 远端 log[S] 的 timestamp 不是 T
- **WHEN** 执行恢复
- **THEN** 运行时在备用路径重放该 write
