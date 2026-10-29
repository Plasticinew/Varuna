# NCCL LL / LL128：论文写作素材

用途：Background 或 Motivation 中说明「AI 集合通信里存在非幂等的单边写」。下面先给可改写的段落，再给边界，避免把 LL 写成训练的主流量。

## 中文

### 可改写进正文的段落

NCCL 的 Low Latency（LL）协议把数据与完成标志打进同一次 8 字节写：低 32 位是数据，高 32 位是 flag。接收方在槽上自旋，直到 flag 等于期望的 step，才使用同一字中的数据。正确性依赖这次 8 字节存储的原子可见性：看见 flag，就认为同一次写里的数据已经有效，中间没有单独的内存栅栏。槽在固定深度的缓冲里循环使用（常见深度为 8）。每次复用，flag 取更大的 step。因此同一地址上先后出现的是不同的 `(data, flag)`，后一次写入会覆盖前一次。

LL128 把这个格式放宽到 128 字节：120 字节数据加 8 字节 flag，带宽效率接近 95%。它不再假设单次 8 字节原子写，而假设这一行按地址顺序变得可见，使 flag 不会早于数据被读到。NCCL 开发者说明，CUDA 编程模型并不保证这种 128 字节顺序，它取决于 GPU、PCIe 和网卡的实现，因此不建议在 NCCL 之外自行仿造 LL128。

LL 的线效率约为 50%，因为每个 8 字节字有一半是 flag。大批量梯度同步通常走 Simple 协议：若干次 RDMA write 传送数据，再跟一次 `WRITE_WITH_IMM` 作为通知。接收方在看到这条通知之后才读数据。Simple 的数据写是覆盖写，同一目的地址重放一次，结果与执行一次相同。LL 和 LL128 不是这种写。NCCL 按消息大小自动选择协议，小消息会落到 LL；`NCCL_PROTO=LL` 可以强制全部使用 LL。SHIFT 据此认为 LL/LL128 很少承担训练的主带宽，并要求在启用其跨网卡切换时由运维关闭这两类协议。

非幂等性出现在槽被复用之后的重放。同一 RC QP 内，硬件用 PSN 抑制重复投递，已经完成的 LL 行不会在原 QP 上执行第二次。跨网卡 failover 使用新的 QP，原网卡上的 PSN 状态并不跟着迁移。此时有三种结果。

- 槽尚未复用。重放写出与原来相同的数据和 flag，接收方至多再看到一次相同的 step，结果与执行一次相同。
- 槽已经复用，接收方正在等待更大的 flag。旧行把 flag 写回较小的 step，新 flag 不再出现，集合通信停在自旋上。
- 接收方已经按新 flag 读过该槽，或仍把槽当作原地缓冲。旧行覆盖新数据。Allreduce 得到错误结果，进程不报错。

多字消息只重放其中一部分时，同一条消息里会混进旧字和新字。SHIFT 的做法是不恢复 LL/LL128，而不是把已完成的行与未完成的行分开。因此 LL 标出了一类单边写：完成标志和有效载荷在同一次写里，盲重传会回退 flag。

### 建议使用的表述

- “NCCL LL packs a 32-bit payload and a 32-bit step flag into one 8-byte store. After the slot is reused, replaying that store rolls the flag back.”
- “Within one RC QP, PSN matching suppresses the duplicate. The duplicate becomes visible when retransmission is posted to a new QP on another NIC.”
- “LL is NCCL’s latency protocol. Bulk gradient exchange uses the Simple protocol, whose data writes are idempotent overwrites followed by a `WRITE_WITH_IMM` notification.”

### 不宜写进论文的表述

- “训练流量主要是 LL。” 大批量 allreduce 默认走 Simple。LL 的价值是协议反例和延迟路径，不是带宽占比。
- “NCCL 在网卡故障时会算错。” 原生 NCCL 在 QP 出错时失败或超时，不会自行换一块网卡重放 LL 行。算错来自换 QP 之后的盲重放，SHIFT 选择直接放弃这类流量。
- “LL128 在所有 GPU 上都是一次原子写。” 公开说明只保证到「依赖硬件的 128 字节顺序」。

### 出处

- NCCL 开发者对线格式的说明：8 字节原子写为 4B 数据 + 4B flag，LL 峰值效率 50%；LL128 为 120B 数据 + 8B flag，峰值效率约 95%，且依赖硬件顺序（NCCL issue #1728，引用 #281 中的实现说明，2025）。
- SHIFT，arXiv:2512.11094（2025-12 投稿，2026-02 修订），Table 1 与 §3.2：LL/LL128 需要字节级顺序和 exactly-once；作者不支持这两类协议，并要求运维在跨网卡切换时关闭它们。同文给出 Simple 为多次 RDMA write 加一次 `WRITE_WITH_IMM`。
- SHIFT 对训练主路径的判断：LL 的带宽效率与大批量训练冲突，LL128 需要特定硬件顺序，因此很少作为训练主协议。文中把 LL 的带宽利用率写成 25%–50%，与 NCCL 开发者给出的 50% 峰值效率并列引用时需要注明各自的出处。

### 本地复现时可以声称的范围

`tests/gpu_semantic` 在 GPU 内存中按 8 字节 LL 布局重放旧行。它说明 flag 回退会使等待新 flag 的读者停住，也会使原地再读看到旧数据。它不是 NCCL 进程，也没有经过 RDMA QP。论文中应写成协议级复现。健康的 `NCCL_PROTO=LL` allreduce 只说明库在无重放时结果正确。

## English

### Paragraphs suitable for the paper

NCCL’s Low Latency (LL) protocol packs the payload and its completion flag into a single 8-byte store: the low 32 bits hold data and the high 32 bits hold a step flag. The receiver spins on the slot until the flag equals the expected step, then consumes the data in that same word. Correctness depends on atomic visibility of the 8-byte store. Observing the flag means the data written by that store is valid; the protocol does not insert a separate memory fence. Slots are reused in a fixed-depth buffer, commonly eight deep, and each reuse carries a larger step. The address therefore holds a different `(data, flag)` pair over time, and a later store overwrites the earlier one.

LL128 widens the unit to 128 bytes, with 120 bytes of data and an 8-byte flag, and reaches roughly 95% of peak payload efficiency. It does not assume an atomic 8-byte store. It assumes that the 128-byte line becomes visible in address order, so the flag is not observed before the data. NCCL developers note that the CUDA programming model does not guarantee this order; it is a property of the GPU, PCIe path, and NIC. Reimplementing LL128 outside NCCL is therefore not portable.

LL itself delivers about half of the line rate because half of every 8-byte word is a flag. Large gradient exchanges use the Simple protocol instead: a sequence of RDMA writes carries the tensor, and a following `WRITE_WITH_IMM` is the notification. The receiver reads the buffer only after that notification completes. Those data writes are overwrites, so repeating one of them leaves the same bytes. LL and LL128 do not have that form. NCCL selects the protocol from the message size, so small messages land on LL, and `NCCL_PROTO=LL` forces the protocol for every message. SHIFT treats LL/LL128 as uncommon on the training bandwidth path and requires operators to disable both before its cross-NIC failover is used.

The operation stops being idempotent once a slot is reused. Inside one RC QP, PSN matching drops a duplicate transmission, so a completed LL line is not applied twice on that QP. Cross-NIC failover posts the retransmission on a new QP, and the original PSN state does not move with it. Three outcomes follow.

- The slot has not been reused. The replay writes the same data and the same flag. The receiver may observe that step again, and the result matches a single execution.
- The slot has been reused and the receiver is waiting for a larger flag. The old line restores a smaller step, the new flag never appears, and the collective spins.
- The receiver has already accepted the new flag, or it still reads the slot in place. The old line replaces the new data. The allreduce result is wrong, and the process does not report an error.

Replaying only some words of a multi-word message mixes old and new words in one buffer. SHIFT refuses LL/LL128 rather than separating completed lines from lines that still need retransmission. LL therefore identifies a one-sided write whose completion flag and payload share one store: blind retransmission rolls the flag backward.

### Claims that stay within the evidence

- NCCL LL packs a 32-bit payload and a 32-bit step flag into one 8-byte store. Replaying that store after the slot is reused rolls the flag back.
- PSN matching hides the duplicate on the original RC QP. The duplicate is applied when the retransmission is posted to a new QP on another NIC.
- LL is NCCL’s latency protocol. Bulk gradient exchange uses Simple, whose data path is an idempotent overwrite plus a `WRITE_WITH_IMM` notification.

### Claims to leave out

- “Training traffic is mostly LL.” Large allreduces use Simple. LL is the latency path and the protocol-level counterexample, not the bandwidth share.
- “A NIC failure makes NCCL compute a wrong result.” Stock NCCL fails or times out when the QP errors. It does not itself move the LL line to another NIC. A wrong result requires a blind replay onto a new QP. SHIFT declines that traffic instead.
- “LL128 is an atomic store on every GPU.” The public description is ordered visibility of a 128-byte line, tied to particular hardware.

### Sources

- NCCL developers, issue #1728 (2025), quoting the implementation note in issue #281: an LL word is an atomic 8-byte store with 4 bytes of data and 4 bytes of flag, at 50% of peak; LL128 is 120 bytes of data plus an 8-byte flag, near 95% of peak, and depends on hardware store order.
- SHIFT, arXiv:2512.11094 (submitted December 2025, revised February 2026), Table 1 and §3.2. LL/LL128 require byte-level order and exactly-once execution. The system does not support either protocol and tells operators to disable them during cross-NIC failover. The same section describes Simple as RDMA writes followed by one `WRITE_WITH_IMM`.
- SHIFT’s deployment claim: LL’s efficiency conflicts with high-bandwidth training, and LL128 needs a specific hardware order, so neither is the common training protocol. SHIFT states LL’s bandwidth utilization as 25–50%. Cite that figure separately from the developers’ 50% peak-efficiency statement.

### What the local reproduction supports

`tests/gpu_semantic` replays an old 8-byte LL line in GPU memory. A reader waiting for the new flag stops, and an in-place reread observes the old data. That binary is not an NCCL process and does not post an RDMA QP. Describe it as a protocol-level reproduction. A healthy `NCCL_PROTO=LL` allreduce shows only that the library is correct when no line is replayed.
