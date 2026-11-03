# DCQP failover pipeline

This note is for the next agent that touches Varuna failover or redraws the Motor bandwidth figure. The steady-state primary path is a different change; read `VARUNA_STEADY_PATH.md` before editing `zQP_post_send_async` on device 0.

CloudLab Utah, experiment `x1aoyang-321905`. Compute is `node-0`, memory is `node-1`. `node-2` and `node-3` have active RDMA and are not part of this figure. The dropped NIC is `ens1f0` / `mlx5_2` on node-0 (25 Gb/s, active MTU 1024, RoCEv2). Do not compare these runs with an older 11 GB/s RC number from a different link.

Both processes must be rebuilt after a `libzQP` change. The DCT shared receive queue lives in the memory node. The probe client is `tests/motor/build-varuna-probe/compute_node/run/run`. The memory node is `tests/motor/build-varuna/memory_node/server/motor_mempool`, started by `tests/motor/perf/start_mn.sh` and copied to node-1 by `run_failover.sh`.

## What the two series mean

`DCQP_FAILOVER` is compiled in. `should_use_dcqp` is true when failover is enabled, `current_device != 0`, a target and a DCI exist, and either `in_recovery` is set or the backup RC is not `ZSTATUS_CONNECTED`. Recovery, including the completion-log read, stays on DCQP. The backup RC is for traffic after `z_recovery` returns.

`VARUNA_DCQP=0` makes `dcqp_failover_enabled` false. `z_switch` then blocks inside `zQP_connect`. That is the `rc_blocking` series. The default leaves DCQP on and starts the backup RC connect in a detached thread. That is `dcqp_background`.

Motor binds one DCI per data QP (`zQP_bind_dci`) and leaves the backup RC unconnected (`tests/motor/thirdparty/zrdma/QP.hpp`). `zPD_create(ep, 1)` only pre-creates slot 0 so the first connect can build an address handle. `framework.cc` sometimes passes `magic_number` (`0xdeadbeefcafebabe`) as the send flags, so `wr_id` stays 0 and bit 3 of the flags is `IBV_SEND_INLINE`.

## Why the first pipelined run died

A synchronous `zDCQP_send` posted a chain and then polled until that completion. Depth never overlapped, and the DC connection was torn down between doorbells.

The first async attempt kept a window of outstanding doorbells but put the log write and the data write in one `ibv_wr_start` / `ibv_wr_complete`. On this ConnectX-4 the chain was accepted and then produced no completion. The log showed `DCQP read ok`, `RECOVERY_STAT ... fail=0`, `DCQP write path=pipeline`, then `Error, dcqp pipeline timeout`, then `framework.cc:184 ... unlock fails during abortion`. The client never printed `TP_PROBE_FILE`. That run's correctness row was `correct=316 incorrect=9 check=-1`. It is not a timeline and it is not the row in `motor_failover_correct.csv`.

The working post path is one signaled work request per doorbell (`dcqp_post_one`). `kDcPipeline` is 128 signaled WRs, about 64 transactions of a log write plus a payload write. `dcqp_post_pipelined` still builds the log-then-data chain so the send queue order is unchanged; `zDCQP_send(..., wait=false)` rings a separate doorbell for each WR. The synchronous log path (`dcqp_post_logged`) is unchanged and still waits.

Every harvested DCI completion decrements `inflight`, including an internal id in `[0xFFFF0000, 0x100000000)`. A tail whose caller left `wr_id` at 0 is posted with an internal id and still occupies a window slot. A synchronous read polls for its own id and returns before `dcqp_account_cqe` sees it. Skipping the decrement for internal ids sticks the window at the cap even after the NIC has completed.

Inline data is used only when the work request asks for it and the length is in `(0, 256]`. A stray `IBV_SEND_INLINE` from `magic_number`-as-flags, posted against `max_inline_data == 0` or a large SGE, wedges the DCI with no completion. The DCT is created with `max_inline_data = 256`. The DCT SRQ has `max_wr = 1024` and `min_rnr_timer = 1`, and 1024 receives of 64 bytes are posted after RTR. A one-entry SRQ serializes every DCI into RNR retries.

Pipeline writes copy a payload into the log slot only when the length is in `(0, 4096]`. Copying every 64 KB doorbell on the post path held a single DCI under half of RC. Transaction values are still copied so a replay does not send whatever the coroutine buffer holds after it is reused.

`z_poll_send_completion` harvests the DCI only while `inflight > 0`. The primary NIC (`current_device == 0`) does not poll that CQ. Motor calls `z_discard_recovered` when a coroutine's pending count hits 0; a recovery completion left queued for that id finishes the next read early.

Backup RC address resolution is split from connect (`ZSTATUS_ROUTE_RESOLVED`). The ACK timeout is set with `rdma_set_option` before `rdma_resolve_route`, because the route copies it. `RC_ACK_TIMEOUT` is 18 (`4.096 µs << 18` = 1.07 s) and `retry_count` is 0, so a lost ACK fails once at that bound.

## How to measure

```bash
cmake --build /users/X1aoyang/Varuna/build --target zQP -j
make -C tests/motor/build-varuna-probe -j run
# Relink and let the script scp motor_mempool when source/zQP.cc changed the DCT.
bash tests/motor/perf/run_failover.sh varuna
VARUNA_FAILOVER_ENV=VARUNA_DCQP=0 bash tests/motor/perf/run_failover.sh varuna
```

The script edits `tpcc_config.json` to `num_warehouse=2` and `attempted_num=8000`, and `cn_config.json` `tp_probe_interval_ms` to 5. It restores both files, brings `ens1f0` back up, and kills `motor_mempool` on exit. Confirm `ibstat mlx5_2` says `State: Active` before the next run. The client command is `./run tpcc 2 2 SI`.

`run_failover.sh` rewrites `motor_failover_correct.csv` from the native, blind, and varuna logs, and rewrites `motor_failover_timeline.csv` from whichever varuna log it just ran. It does not write `motor_timeline_*.csv`. A varuna-only invocation still reparses the old native and blind logs. Copy the probe collapse and the event meta out before the second run, or the first series is overwritten.

Collapse `TP_PROBE_FILE` by keeping the max committed ktps at each `t_ns`. `interval_us` is the gap from the previous tick, in microseconds. `fail_ns` is the first `switch_start` wall time. `recovery_us` is the first successful `RECOVERY_STAT` (`fail=0`) minus that time. `both_us` uses the last such stat. `detect_us` is `fail_ns` minus the last probe tick before it. `rc_ready_us` is the first `rc_ready` event minus `fail_ns`. The plot drops ticks with `interval_us > 200000`.

Redraw with `python3 tests/eval/plot_eval.py --only motor`. The timeline style salt is `motor-timeline-v14`. Bump it only when the plot code changes. `--only motor` also redraws `motor_perf` and `motor_correct`.

## Numbers from 2026-11-02

Probe files: `tp_probe@Motor@2026-11-2@7:26:29.txt` (DCQP) and `tp_probe@Motor@2026-11-2@7:27:3.txt` (RC blocking). Those clocks are UTC. Logs are `tests/motor/perf/logs/failover_dcqp_background.log` and `failover_rc_blocking.log` (gitignored).

Both runs committed about 5.5 K txn/s on the normal 5 ms ticks before the link drop. The NIC timeout is about 2 s, so the last commit sits that far before `switch_start`. The full panel aligns on that last commit.

| Series | recovery | RC ready | commits while the backup RC is down |
| --- | --- | --- | --- |
| `dcqp_background` | 6.846 ms | 2.241 s | 3.65 K txn/s mean, 66% of the 5.52 K txn/s pre-stall mean |
| `rc_blocking` | 1.695 s | 1.692 s | 0 until recovery returns |

DCQP selective recovery reported `correct=513 incorrect=2` on the QP that held the writes, then a second stat of `0/0` on the read-only QP. `TPCC_CHECK pass=1` (`ol_bad=3`; the checker fails `ol_bad` only above 100). The log contains `DCQP write path=pipeline`. RC blocking reported `correct=183` and `correct=72` (`incorrect=1` total) and `TPCC_CHECK pass=1`.

`motor_timeline_compare.csv`, `motor_timeline_dcqp_background.csv`, and `motor_timeline_rc_blocking.csv` are those two runs. The figure is `tests/eval/figures/motor_timeline.png`.

`motor_failover_timeline.csv` is an older single-run collapse (`fail_ns` 1793348158464475528, recovery 9.758 ms). It is what `run_failover.sh` had written before the paired runs. The plot does not read it.

`motor_failover_correct.csv` is a different figure. The Varuna row is the DCQP background run above (`513,2,0,1,6845.657`). Native has no NIC failover, so that row is aborted. Blind is the older `VARUNA_FULL_RETRANSMIT=1` log (`12` correct, `54` incorrect, check failed, recovery 22.8 ms), not a rerun from this day.

## HERD request path, same tree

`tests/herd_rpc` is a separate bench, not a Motor workload. Native posts one 64-byte RC write. Varuna posts one `WRITE_WITH_IMM`; recovery reads the completion log over a DCI and reposts only when the stamp is absent. `tests/herd_rpc/perf/run_herd_eval.sh` uses node-0 as the client (`mlx5_2`, backup `mlx5_3`) and node-1 as the worker (`10.10.1.3:18515`). The perf CSV is 243.644 kops native and 243.486 kops Varuna. `plot_eval.py` reads `tests/herd_rpc/perf/*.csv`. Do not mix those kops with the Motor K txn/s axis.

CMake trees (`tests/motor/build-varuna-probe`, `tests/herd_rpc/build`) and the logs above stay untracked.
