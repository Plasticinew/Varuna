# NVSHMEM 信号与 RDMA atomic：论文写作素材

用途：Background 或 Motivation 中说明「数据写可以重放，但后面的 atomic 通知不能重放」。NVSHMEM 的 ADD 与 DeepEP 的计数器是正文例子；SET 要单独写，避免把所有 signal 都说成非幂等。

## 中文

### 可改写进正文的段落

NVSHMEM 的 put-with-signal 把一次传输分成两段。前一段把连续数据从本地对称堆写到目标 PE；后一段用 `NVSHMEM_SIGNAL_SET` 或 `NVSHMEM_SIGNAL_ADD` 更新目标 PE 上的 `uint64_t` 信号。接收方用 `wait_until` 轮询该信号，条件满足之后才读数据。库保证同一次成功的 put-with-signal 里，信号更新在数据对接收方可见之后发生。这个顺序是这一次调用内部的约定。它并不表示信号变量本身可以执行两次。

`NVSHMEM_SIGNAL_ADD` 把一个 64 位值原子地加到远程信号上。执行两次，信号增加两倍。接收方若以 `signal >= N` 等待第 N 条消息，会在第 N 条数据尚未写完时被唤醒，并读到上一条留在缓冲里的数据。`NVSHMEM_SIGNAL_SET` 是原子存储。把同一个值再写一次，读到的仍是该值；若槽上已经写入了更新的信号，重放旧的 SET 会把信号退回到较早的值，随后的等待会提前满足或不再满足。Counted signaling 按已送达的字节数增加计数器，重复执行会把字节数多计一次。MSCCL++ 的集合通信使用与 NVSHMEM 相同的分工：数据用 RDMA write，通知用 RDMA atomic。SHIFT 将二者都列为「通知 atomic 必须恰好执行一次」。

DeepEP 把这个模式用在跨节点的 expert dispatch 上。GPU 通过 IBGDA 发起单边操作：token 用 put 写入对端缓冲，再以 non-fetching atomic add 推进 per-channel tail，并用同一种 atomic 写入完成计数。作者说明 atomic 在这里同时充当栅栏。IB 规范里，atomic 和 write-with-immediate 可以约束它前面的 write 的可见顺序，普通 write 不能。NVSHMEM 不提供 `WRITE_WITH_IMM`，所以 DeepEP 用 atomic add 来获得这个顺序。低延迟 dispatch 还把 token 数编码进同一个加法：发送方加上 `-num_tokens - 1`，接收方等到计数器非零后取 `-counter - 1`。这条加法执行两次，解码出的 token 数不再是本次发送的数量，tail 也会多前进一段。接收方随后读取从未写入的槽，或提前结束。推理得到错误的 token，训练得到错误的梯度，调用本身不返回错误。

跨网卡重传把这一个调用拆开，产生三种不同的错误。

- 数据写和 atomic 都再次执行。ADD 使信号和 tail 多加一次，等待提前返回，读到的仍是旧 payload。
- 只有 atomic 在新 QP 上执行，对应的数据写没有执行。信号已经满足，数据仍是上一轮的内容。SHIFT 证明在看不到对端网卡状态、且不做额外拷贝时，无法同时保住这种顺序和 exactly-once，因此遇到在途 atomic 就放弃切换并把错误返回应用。
- 数据写已经可见，atomic 没有执行。接收方停在 `wait_until` 上，作业挂起。

UCCL-EP 在不提供 RDMA atomic 的 EFA 上用另一次带 immediate 的小写模拟这条通知，由 CPU 把完成计数加一。计数器仍然只能增加一次。他们也指出 DeepEP 需要 write 加 atomic 两次操作，而 immediate 可以把通知附在同一次写上。这与「用完成见证避免第二次执行 atomic」是同一类约束，但 UCCL-EP 解决的是网卡缺少 atomic 指令，不是跨网卡 failover 之后如何判断 atomic 已经生效。

### 建议使用的表述

- “NVSHMEM put-with-signal delivers the payload with an RDMA write and then updates a 64-bit signal with SET or ADD. ADD is a read-modify-write: executing it twice advances the signal by twice the increment.”
- “The library orders the signal after the data inside one successful put-with-signal. That order does not make a second execution of ADD equivalent to the first.”
- “DeepEP uses a non-fetching atomic add both as a fence for the preceding puts and as the token counter. Replaying the add changes the decoded token count.”
- “SHIFT returns the in-flight atomic to the application instead of retransmitting it on the backup NIC.”

### 不宜写进论文的表述

- “NVSHMEM 的通知是 RDMA CAS。” 公开接口是 `SIGNAL_SET` 与 `SIGNAL_ADD`，DeepEP 使用的是 atomic add。CAS 是事务锁的原语，不要并成同一种指令。
- “所有 NVSHMEM 信号重放都会算错。” 尚未被更新值覆盖的 SET，再写同一个立即数，读到的值不变。非幂等的是 ADD、counted increment，以及旧 SET 覆盖新信号。
- “DeepEP 的 tail 协议代表全部 LLM 推理。” 它是 expert-parallel dispatch/combine 的一条实现。KV block 的字节搬运仍是覆盖写或单边读。
- “SHIFT 已经正确恢复了 NVSHMEM。” SHIFT 对在途 atomic 的处理是向应用报错。

### 出处

- NVIDIA NVSHMEM Signaling Operations：信号对象为 `uint64_t`；`NVSHMEM_SIGNAL_SET` 为原子写，`NVSHMEM_SIGNAL_ADD` 为原子加；put-with-signal 在数据拷贝之后更新信号。Counted signaling 按送达字节数递增。
- SHIFT，arXiv:2512.11094，Table 1 与 §3.2：NVSHMEM 与 MSCCL++ 的数据路径是 RDMA write，通知是 RDMA atomic，atomic 必须恰好执行一次。文中写明，在途 WQE 含有 atomic 时不进行 fallback，而是把错误交给应用。
- MSCCL++，arXiv:2504.09014，ASPLOS 2026。SHIFT 将其通知路径与 NVSHMEM 归为同一类。
- DeepEP，Zhao 等，2025，https://github.com/deepseek-ai/DeepEP 。跨节点低延迟内核使用 IBGDA put 与 `nvshmemi_ibgda_amo_nonfetch_add`。Issue #282、#442（2025-10）说明该 atomic 用来保证前面 write 的可见性，因为 NVSHMEM 不提供 `WRITE_WITH_IMM`。负数计数编码见其 low-latency dispatch 实现。
- UCCL-EP，arXiv:2512.19849（2025-12）：在没有 RDMA atomic 的网卡上用带 immediate 的写和 CPU 侧计数器模拟 DeepEP 的完成通知，并要求该计数只增加一次。

### 本地复现时可以声称的范围

`tests/gpu_semantic` 在 GPU 上执行与 `SIGNAL_ADD` 相同的计数协议：payload 保持 111，atomicAdd 执行两次，等待 `signal >= 2` 的读者被唤醒并读到 111；只加一次时等待不返回；payload 已改为 222 但 atomic 未执行时等待超时。这是协议级复现，不是 NVSHMEM 运行时，也没有 IBGDA。论文中不要写成 DeepEP 端到端实验。

## English

### Paragraphs suitable for the paper

NVSHMEM’s put-with-signal splits one transfer into two parts. The first copies a contiguous buffer from the local symmetric heap to the destination PE. The second updates a `uint64_t` signal on that PE with either `NVSHMEM_SIGNAL_SET` or `NVSHMEM_SIGNAL_ADD`. The receiver polls the signal with `wait_until` and reads the payload only after the predicate holds. On a single successful put-with-signal, the library makes the signal update visible after the data. That order is internal to one call. It does not imply that the signal update is safe to execute twice.

`NVSHMEM_SIGNAL_ADD` atomically adds a 64-bit value to the remote signal. Two executions add twice that value. A receiver waiting for `signal >= N` as the arrival of message N wakes before message N has been written and then reads the payload left by the previous message. `NVSHMEM_SIGNAL_SET` is an atomic store. Writing the same value again leaves that value in place. Replaying an older SET after a newer signal has been stored rolls the signal back, so a later wait fires early or stops firing. Counted signaling increments a counter by the number of bytes delivered, and a repeated increment counts those bytes twice. MSCCL++ uses the same split for collective communication: an RDMA write for the data and an RDMA atomic for the notification. SHIFT lists both libraries as requiring the notification atomic to execute exactly once.

DeepEP uses this pattern for inter-node expert dispatch. The GPU posts one-sided operations through IBGDA. A put writes the token into the peer buffer, and a non-fetching atomic add advances the per-channel tail. The same atomic carries the completion count. The DeepEP authors describe the atomic as a fence: under the InfiniBand rules, an atomic or a write-with-immediate orders the visibility of preceding writes, whereas a plain write does not. NVSHMEM does not offer `WRITE_WITH_IMM`, so DeepEP obtains that order from the atomic add. Its low-latency dispatch also encodes the token count in the addend. The sender adds `-num_tokens - 1`, and the receiver waits until the counter is nonzero and decodes `-counter - 1`. Executing that add twice yields a token count that is no longer the number of tokens sent in this round, and the tail moves an extra step. The receiver then reads a slot that was never written, or it finishes early. Inference emits the wrong token and training applies the wrong gradient, while the call itself returns success.

Cross-NIC retransmission splits the call and produces three distinct failures.

- Both the data write and the atomic run again. ADD advances the signal and the tail a second time, the wait returns early, and the payload still holds the previous bytes.
- Only the atomic runs on the new QP. The signal is already satisfied and the buffer still holds the previous round. SHIFT shows that, without the failed NIC’s receive state and without an extra copy, a failover path cannot keep both this order and exactly-once execution. If an atomic WQE is in flight, SHIFT aborts the failover and returns the error to the application.
- The data write is visible and the atomic never runs. The receiver remains inside `wait_until` and the job stalls.

On EFA, which has no RDMA atomic, UCCL-EP imitates the notification with a short write that carries an immediate, and a CPU proxy increments a completion counter. That counter must still increase once. The authors also observe that DeepEP posts two operations, a write and an atomic, whereas an immediate can attach the notification to the write. This is the same constraint as refusing a second execution of the atomic. UCCL-EP answers a NIC that lacks the atomic instruction. It does not decide, after a cross-NIC failover, whether the atomic has already taken effect.

### Claims that stay within the evidence

- NVSHMEM put-with-signal delivers the payload with an RDMA write and then updates a 64-bit signal with SET or ADD. ADD is a read-modify-write: executing it twice advances the signal by twice the increment.
- The library orders the signal after the data inside one successful put-with-signal. That order does not make a second execution of ADD equivalent to the first.
- DeepEP uses a non-fetching atomic add both as a fence for the preceding puts and as the token counter. Replaying the add changes the decoded token count.
- SHIFT returns an in-flight atomic to the application instead of retransmitting it on the backup NIC.

### Claims to leave out

- “NVSHMEM notifies with an RDMA compare-and-swap.” The documented operators are `SIGNAL_SET` and `SIGNAL_ADD`. DeepEP posts an atomic add. CAS is the transaction-lock primitive; the instructions are different.
- “Every replayed NVSHMEM signal corrupts state.” A repeated SET of a value that has not been overwritten still reads back as that value. The non-idempotent cases are ADD, a counted increment, and an old SET that overwrites a newer signal.
- “DeepEP’s tail protocol represents all LLM inference.” It is one expert-parallel dispatch/combine implementation. Moving a KV-cache block remains an overwrite or a one-sided read.
- “SHIFT recovers NVSHMEM correctly.” SHIFT’s response to an in-flight atomic is to surface an error.

### Sources

- NVIDIA NVSHMEM, Signaling Operations. The signal object is a `uint64_t`. `NVSHMEM_SIGNAL_SET` is an atomic store and `NVSHMEM_SIGNAL_ADD` is an atomic add. Put-with-signal updates the signal after the copy. Counted signaling increments by the number of bytes delivered.
- SHIFT, arXiv:2512.11094, Table 1 and §3.2. NVSHMEM and MSCCL++ send data with RDMA write and notify with an RDMA atomic that must execute exactly once. If an in-flight WQE contains an atomic, SHIFT does not fail over; it returns the error to the application.
- MSCCL++, arXiv:2504.09014, ASPLOS 2026. SHIFT places its notification path in the same class as NVSHMEM.
- DeepEP, Zhao et al., 2025, https://github.com/deepseek-ai/DeepEP. The inter-node low-latency kernels post IBGDA puts and `nvshmemi_ibgda_amo_nonfetch_add`. Issues #282 and #442 (October 2025) state that the atomic orders the preceding writes, because NVSHMEM provides no `WRITE_WITH_IMM`. The negative token-count encoding is in the low-latency dispatch implementation.
- UCCL-EP, arXiv:2512.19849 (December 2025). On NICs without an RDMA atomic, a write with immediate and a CPU-side counter emulate DeepEP’s completion notification, and that counter must increment once.

### What the local reproduction supports

`tests/gpu_semantic` runs the `SIGNAL_ADD` counting rule on a GPU. With the payload left at 111, two atomic adds wake a reader waiting for `signal >= 2`, and that reader still observes 111. One add leaves the wait outstanding. A payload of 222 with no matching atomic also leaves the wait outstanding. This is a protocol-level reproduction, not the NVSHMEM runtime and not IBGDA. Do not describe it as an end-to-end DeepEP run.
