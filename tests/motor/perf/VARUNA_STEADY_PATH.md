# Motor + Varuna steady-state path

This note records why Motor linked to Varuna was about twice as slow as native Motor on TPC-C, and what was changed in the steady-state send path. Failover logging is still compiled in. The change is the hot path that runs while the primary NIC is up.

## Workload

CloudLab Utah, experiment `x1aoyang-321905`. One compute node (`node-0`) and one memory node (`node-1`). No backup replica is placed: `BACKUP_NUM` is larger than the memory-node count.

Both binaries are Release (`-O3 -DNDEBUG`). The NIC active MTU is 1024, and both paths use the last IPv4-mapped RoCEv2 GID. TPC-C, snapshot isolation, 2 coroutines (coroutine 0 polls, one worker runs transactions), 5000 attempts per thread. Thread counts are 2, 4, 8, and 16.

Motor prints one line: attempted ktxn/s, committed ktxn/s, mean of per-thread P50 (µs), mean of per-thread P99 (µs). The CSV next to this file is that line. `plot_motor_varuna.py` redraws the figure from the CSV only. Two consecutive runs of the script produce identical PNG and SVG bytes.

Native rows are from the sweep taken before this change, on the same binary and the same cluster. A later 2-thread native check (4.56 committed ktxn/s, P50 193 µs) matches that row. Varuna rows are the sweep after the change below.

## What was slow

A TPC-C transaction here is about 100 one-sided doorbells. Native P50 is about 190–200 µs, so each doorbell is a few microseconds. Varuna P50 was about 380–410 µs and committed throughput was 53–59% of native.

Posting the caller's work requests with `ibv_post_send`, and nothing else, matched native (2 threads: 4.48 committed ktxn/s, P50 199 µs, against native 4.56 ktxn/s and P50 202 µs on that same check). The gap was not the QP, the MTU, or the round trip.

Two things on the old `zQP_post_send_async` path looked expensive and were not the 2×:

- Every work request was copied with `new` / `malloc` and never freed. The process links tcmalloc, so the copies were cheap next to a round trip. They also leaked.
- Each compare-and-swap was expanded into an inline log write plus three CASes. Those extra messages are pipelined on the same doorbell. Removing them, while the bug below was still present, did not move TPC-C.

The 2× was reclaiming the local request log. The log is a 1024-slot ring (`wr_entry_`). On every completion, `wr_log_complete_by_wr_id` scanned the live window for a slot whose `reserved` field equalled the completion `wr_id`. That match usually failed:

- A CAS slot stores the original swap value in `reserved`, which recovery needs.
- Doorbell entries other than the tail store Motor's `magic_number`, not the coroutine id.
- The completion carries the tail work request's id, which Motor sets to the coroutine id.

Unmatched slots stayed `finished != 1`, so `wr_log_advance` could not move the head. The window sat at about 900 slots. Each completion then walked that window twice. Measured cost was about 9000 cycles per completion, on top of a round trip of about 9300 cycles. Across ~100 doorbells that is the missing ~200 µs.

## What changed

`source/zQP.cc`, steady state only (`current_device == 0`, primary QP connected, the Motor doorbell opcodes). Failover still uses the old post path.

1. **Post from the per-slot backup arrays.** `zQP_post_send_async_fast` copies each user work request into `wr_backup[slot]` / `sge_backup[slot]` and posts that chain. No heap allocation. The copy stays live until the slot is reused, which is what recovery reads.

2. **Remember the slot range for that doorbell.** After a successful `ibv_post_send`, the thread pushes one `ZqpSpan` onto a 256-entry thread-local ring: the `zQP`, the tail `wr_id` that the completion will carry, and the raw `[begin, end)` range of `entry_end_` consumed by this post. `entry_end_` is the monotonic counter; the slot index is `entry_end_ % 1024`.

3. **Reclaim that range on the matching completion.** `wr_log_complete_by_wr_id` pops the oldest span when its `zQP` and `wr_id` match, marks those slots finished, and advances the log head. One RC queue pair completes signaled sends in order, and Motor has one outstanding doorbell per thread in this run, so the oldest span is the one that just completed. A mismatch still falls back to the old window scan.

4. **Leave the CAS as one network CAS.** The local log still stores the original swap in `reserved` so a later retransmit can see it. The steady path does not prepend the inline log write, and does not expand the CAS into the three-CAS chain. That extra write was worth about 10 µs of P50 after the scan was fixed (205 µs with it, 196 µs without, native 193 µs at 2 threads).

5. **Keep `WRITE_WITH_IMM` for writes.** That is the completion log: one message, immediate data packs timestamp and slot, and the memory node records it from the receive completion. Turning immediate data off, after the scan fix, did not beat the version that keeps it.

The memory-node immediate-completion thread also stops calling `sched_yield` on every idle poll, so a burst of `WRITE_WITH_IMM` is less likely to find an empty receive queue. That was not the 2×. A direct `ibv_post_send` of ordinary writes was already as fast as native while that thread was running.

## Result

Committed throughput in ktxn/s, then P50 and P99 in microseconds. "Before" is the Varuna binary from the first sweep. "After" is this path.

| Threads | Native | Varuna before | Varuna after |
| --- | --- | --- | --- |
| 2 | 4.592, 193 µs, 4493 µs | 2.433, 406 µs, 7670 µs | 4.437, 200 µs, 4650 µs |
| 4 | 9.351, 196 µs, 4535 µs | 4.954, 398 µs, 7717 µs | 9.062, 201 µs, 4685 µs |
| 8 | 19.351, 184 µs, 4630 µs | 10.047, 393 µs, 7869 µs | 18.821, 192 µs, 4697 µs |
| 16 | 35.286, 203 µs, 5355 µs | 20.661, 382 µs, 7999 µs | 34.281, 211 µs, 5429 µs |

After the change, committed throughput is 96.6%, 96.9%, 97.3%, and 97.1% of native at 2, 4, 8, and 16 threads. P50 is 3.3%, 2.6%, 4.0%, and 3.9% above native.

Full rows, including attempted throughput, are in `motor_varuna_tpcc.csv`. The figure is `figures/motor_varuna_tpcc.png` and `figures/motor_varuna_tpcc.svg`.

```
png e5f8724fce850f85dbcbe45aa3826448a6b01d585dfc24373ee583b086788eba
svg eb72a9ec60e4964dd781f063c732334685b3c05b32a3c293fc5cce4bd91f4aae
```

## What this does not change

Reads are still copied into a local log slot so a later failover can refill the buffer and inject the application completion. Writes still use `WRITE_WITH_IMM`. The old malloc post path remains for a non-primary device, `unique_cas`, or an opcode the fast path does not recognize. Recovery still walks `wr_entry_` and the saved work-request copy.

The same measurement also needed a few fixes that are not the steady-path change: Release is the default CMake build type, `MOTOR_USE_ZRDMA` selects the native or Varuna data path, native RoCE uses the port's active MTU and RoCEv2 GID, and a version-chain walk that never advanced its cursor now stops instead of spinning at 8 and 16 threads. Those are what make the two columns comparable. They are not the latency gap above.
