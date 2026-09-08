# Varuna

Varuna 是论文 **“Varuna: Enabling Failure-Type Aware RDMA Failover”** 的研究原型，目标是在 RDMA 链路故障或链路抖动时，实现故障类型感知、语义正确且低开销的快速切换。

论文全文见 [`docs/Varuna.pdf`](docs/Varuna.pdf)。本仓库实现论文中的 RDMA 运行时、故障恢复机制和微基准测试，并在 [`tests/motor`](tests/motor) 中集成 Motor/TPC-C 作为应用层测试（来源：[Motor_Zep](https://github.com/Plasticinew/Motor_Zep.git)）。

仓库当前主要用于论文原型验证和性能实验，尚不是开箱即用的通用 RDMA SDK。示例程序包含实验环境相关的网卡名称、IP 地址和链路启停操作，运行前请先阅读[注意事项](#注意事项)。

## 论文背景

传统多链路 RDMA failover 通常在主路径失败后切换到备用路径，并重传所有 in-flight 请求。论文指出，这种统一重传忽略了请求发生故障时所处的执行阶段：

- **Pre-failure request**：请求尚未在 responder 执行，必须在备用路径上重新发送。
- **Post-failure request**：请求已经在 responder 执行，但 ACK 或返回值在链路故障中丢失，不应盲目重传。

盲目重传 post-failure 请求不仅浪费恢复带宽，还可能重复执行 write、CAS、fetch-and-add 等非幂等操作，破坏远端状态或事务一致性。Varuna 的目标是识别每个 in-flight 请求属于哪一类，只重传 pre-failure 请求，并恢复 post-failure 请求的执行结果。

```text
正常路径：Application -> vQP -> primary RCQP -> Responder
                              |
链路故障：                    +-> standby DCQP
                                      |
                          读取 completion log
                                      |
                    +-----------------+------------------+
                    |                                    |
             pre-failure request                 post-failure request
                  重新发送                         不重复执行/恢复结果
                                      |
                              后台重建 RCQP
```

## 论文核心设计

### 1. Compact Completion Logging

Varuna 在 requester 保存本地 request log，并在 responder 保存 completion log。对于需要保护的请求，运行时在原始操作后追加一个有序的 inline RDMA write，将时间戳和请求标识写入远端 completion log。

故障恢复时，Varuna读取远端 completion log，并与本地尚未完成的 request log 对比：

- 日志不匹配表示原请求没有执行，属于 pre-failure，需要重传；
- 日志匹配表示原请求已执行，属于 post-failure，不应再次执行。

当前代码使用 `zWR_entry`、`wr_entry_`、`entry_start_` 和 `entry_end_` 保存本地请求状态，通过 `zQP_write()`/`zQP_post_send()` 追加远端日志，并在 `z_recovery()` 中拉取和比较日志。代码中的位宽和状态字段与论文示意图存在实现细节差异，应以 `include/zQP.h` 为准。

### 2. Extended Status for CAS

CAS 等原子操作不仅需要判断是否执行，还需要恢复原本应返回给 requester 的结果。论文将 CAS 改造成可追踪的两阶段操作：

1. 将 swap value、目标地址和状态写入 responder 的 CAS buffer，并使用 buffer 地址与 QP ID 生成唯一 UID；
2. CAS 先把 UID 写入目标位置，成功后再将 UID 替换为真正的 swap value，并更新完成状态。

这样，即使 ACK 丢失，恢复过程仍可通过 UID、目标值和 CAS buffer 判断操作是否成功，避免重复执行。代码中的主要对应结构和函数包括 `zAtomic_entry`、`zAtomic_buffer`、`zQP_CAS()`、`zQP_CAS_step2()` 以及服务端的 `zQP_flush()`。

### 3. DCQP-assisted Fast Failover

为每个 RCQP 预建完整备用 RCQP 会显著增加内存占用，而故障后再同步创建 RCQP 又会带来较长停顿。论文使用每块 NIC 上少量、可共享的 DCI/DCT（文中统称 DCQP pool）作为临时数据路径：

1. 正常状态下，虚拟 QP（vQP）映射到专用 RCQP；
2. 主路径故障后，vQP 切换到备用 NIC 的 DCQP；
3. DCQP 承担 completion-log 获取和 pre-failure 请求重传；
4. 后台重建备用路径上的 RCQP，完成后再从 DCQP 切回 RCQP。

代码使用 `zQP` 作为逻辑连接，使用 `current_device` 选择物理路径；`zPD_create()` 为每块 NIC 创建 `zDCQP_requestor`/`zDCQP_responder` 池，`zDCQP_read()`、`zDCQP_write()`、`zDCQP_CAS()` 和 `zDCQP_send()` 提供临时数据路径，`z_switch()` 与 `z_recovery()` 负责切换和恢复。

### 4. Multi-NIC Memory Registration

同一块内存需要在每块 NIC 的 protection domain 中分别注册。Varuna 为一个逻辑 MR 保存各 NIC 对应的 MR、`lkey` 和 `rkey`，切换路径时自动选择当前 NIC 的 key，避免故障时重新注册内存。相关实现位于 `mr_create()`、`mr_malloc_create()`、`zPD::m_mrs`、`zPD::m_lkey_table` 和 `rkeyTable`。

## 论文与代码对应

| 论文概念 | 当前代码 |
| --- | --- |
| Virtual QP（vQP） | `zQP`、`m_requestors`、`m_targets`、`current_device` |
| Request log | `zWR_entry`、`wr_entry_`、`entry_start_`、`entry_end_` |
| Completion log | 服务端为 QP 分配的日志区域、inline log write、`z_recovery()` |
| Extended-status CAS | `zAtomic_entry`、`zAtomic_buffer`、`zQP_CAS*()`、`zQP_flush()` |
| RCQP data path | `zQP_connect()`、`zQP_read/write/CAS()` |
| DCQP pool | `zPD::m_requestors`、`zPD::m_responders`、`zDCQP_*()` |
| Link switching | `z_switch()`、`current_device` |
| In-flight recovery | `z_recovery()`、`z_poll_completion()` |
| Per-NIC MR/key mapping | `m_mrs`、`m_lkey_table`、`rkeyTable` |

## 论文评估结论

以下数字来自论文实验，不代表已在任意环境中运行本仓库即可直接复现：

- 在真实事务负载中，Varuna 的稳态平均延迟开销约为 0.6%–10%。
- 对 4 KiB/64 KiB 批量请求，恢复时间相较统一重传方案降低约 52.2%/64.5%。
- 在 64 KiB 请求实验中，恢复阶段传输的数据量约为统一重传方案的 25.4%。
- 论文中的 CAS 故障注入和事务集成实验报告了 100% 的恢复正确率。
- completion log 约增加 1 µs 操作延迟；批处理和较大 payload 可摊薄该开销。

## 主要功能

- 实现论文提出的 pre-failure/post-failure 请求识别和选择性重传。
- 使用 RDMA CM 建立 RC QP 连接，并使用 mlx5 DCI/DCT 提供临时故障路径。
- 同时管理最多两块 RDMA 网卡，为同一内存区域维护多组 `lkey`/`rkey`。
- 提供同步与异步的 RDMA read、write 和 compare-and-swap（CAS）接口。
- 为非幂等 write/CAS 维护本地请求日志和 responder completion log。
- 使用两阶段、可追踪 CAS 恢复 post-failure 操作结果。
- 通过 RPC QP 在服务端申请并注册远端内存。
- 检测 completion error 或 timeout 后切换设备，并利用日志恢复 in-flight 请求。
- 提供功能测试客户端和延迟/带宽测试程序。

## 目录结构

```text
.
├── CMakeLists.txt          # 顶层 CMake 配置
├── include/
│   └── zQP.h               # 数据结构和公开接口
├── source/
│   ├── zQP.cc              # 连接、内存、数据路径和恢复逻辑
│   ├── ib.cc               # RC QP 状态转换辅助函数
│   ├── zServer.cc          # 双网卡 RDMA 服务端
│   ├── zClient.cc          # 功能与故障切换测试客户端
│   └── latency_test.cc     # 延迟、带宽和恢复测试
├── config/                 # 本地节点、目标节点和网络配置示例
├── docs/
│   └── Varuna.pdf          # 项目对应论文
└── scripts/                # OFED、网卡参数和 HugePage 辅助脚本
```

## 环境要求

建议使用 Linux、支持 RDMA 的 Mellanox/NVIDIA ConnectX 网卡，以及已正确配置的 RoCE 或 InfiniBand 网络。代码使用 C++17，并依赖以下组件：

- CMake 3.2 或更高版本
- GCC/Clang 与 pthread
- RDMA Core：`libibverbs`、`librdmacm`
- mlx5 Direct Verbs：`infiniband/mlx5dv.h` 与 `libmlx5`
- Intel oneTBB
- Boost Property Tree
- gperftools profiler

在 Ubuntu 上可先安装常用的开发包：

```bash
sudo apt update
sudo apt install -y \
  build-essential cmake \
  libibverbs-dev librdmacm-dev \
  libtbb-dev libboost-all-dev \
  libgoogle-perftools-dev
```

具体包名可能随发行版变化。若系统自带的 RDMA Core 或网卡驱动不支持代码使用的 mlx5 DCI/DCT 能力，需要安装与内核、发行版和网卡固件匹配的 MLNX_OFED。`scripts/install_ofed.sh` 固定使用 Ubuntu 20.04 和 MLNX_OFED 5.4，仅应作为旧实验环境参考，不建议在其他系统上直接执行。

可用以下命令检查 RDMA 设备和端口：

```bash
ibv_devices
ibv_devinfo
rdma link show
```

## 构建

```bash
cmake -S . -B build
cmake --build build -j
```

默认构建类型在顶层 `CMakeLists.txt` 中设置为 `Debug`。构建成功后会生成：

```text
build/source/libzQP.a
build/source/zServer
build/source/zClient
build/source/latency_test
```

当前 CMake 直接链接 `ibverbs`、`rdmacm`、`mlx5`、`tbb` 和 `profiler`。如果链接器提示找不到库，请确认对应开发包已安装，并且库目录已加入系统链接器搜索路径。

## 配置

### 本地节点配置

`zServer`、`zClient` 和 `latency_test` 的第一个参数都是本地节点 JSON 配置。示例见 `config/node0.json`：

```json
{
  "node_id": 0,
  "eth_names": ["ens3f0", "ens3f1"],
  "mlx_names": ["mlx5_0", "mlx5_1"],
  "ports": ["1145", "1146"],
  "ips": ["10.10.1.1", "10.10.1.2"]
}
```

字段说明：

| 字段 | 含义 |
| --- | --- |
| `node_id` | 当前节点编号，在连接握手中传递给对端 |
| `eth_names` | Linux 网络接口名称 |
| `mlx_names` | `rdma_get_devices()`/`ibv_devices` 显示的 RDMA 设备名称 |
| `ports` | 每条路径监听或连接使用的 TCP/RDMA CM 端口 |
| `ips` | 每条路径绑定的本地 IP 地址 |

四个数组应按相同顺序一一对应。当前核心数据结构最多支持两块网卡（`MAX_NIC_NUM = 2`），而 `zServer` 会无条件启动两个监听线程，因此服务端配置必须提供两组设备信息。

### 目标节点配置

客户端的第二个配置文件描述要连接的服务端地址。示例见 `config/target.json`：

```json
{
  "target_ports": ["1145"],
  "target_ips": ["10.10.1.3"]
}
```

两个数组应长度一致并按位置对应。普通客户端会遍历全部目标；`latency_test` 当前只使用第一个目标，然后从首次握手返回的信息中取得服务端的第二条路径。

## 运行

至少准备两台可互通的 RDMA 主机，并根据实际网卡名称、RDMA 设备名、IP 和端口分别修改节点配置。

### 1. 启动服务端

```bash
./build/source/zServer config/node1.json
```

服务端会在本地配置中的前两组 IP/端口上启动监听，并保持前台运行。请先确认端口未被占用，且 IP 已配置到对应接口。

### 2. 运行功能测试客户端

```bash
./build/source/zClient config/node0.json config/target.json 1
```

参数依次为：

1. 本地节点配置文件；
2. 目标节点配置文件；
3. 测试线程数。

客户端会创建 RPC QP 和 one-sided QP，在服务端申请 1 KiB 内存，执行 write/read/CAS 校验，并打印吞吐信息。

### 3. 运行延迟与带宽测试

```bash
./build/source/latency_test \
  config/node0.json config/target.json \
  1 write_sync 64 16
```

命令格式：

```text
latency_test <local_config> <target_config> <threads> <test_type> <size> <batch>
```

- `threads`：测试线程数。
- `test_type`：`read_sync`、`read_async`、`read_batch`、`write_sync`、`write_async`、`write_batch`、`cas_sync`、`cas_async` 或 `cas_batch`。
- `size`：单次操作大小，单位为字节。
- `batch`：批处理中的 work request 数量；同步测试也要求提供该参数。

测试会在当前目录生成或更新：

- `bandwidth_log.txt`：按时间采样的带宽数据；
- `result.txt`：汇总结果；
- `latency_test.prof`：gperftools CPU profile。

可使用以下命令查看 profile：

```bash
pprof --text ./build/source/latency_test latency_test.prof
```

## 库接口概览

公开接口定义在 `include/zQP.h` 中，典型调用流程如下：

1. 使用 `zEP_create()` 读取本地配置并打开 RDMA 设备。
2. 使用 `zPD_create()` 为所有设备创建 protection domain 和 DCI/DCT 资源池。
3. 使用 `zQP_create(..., ZQP_RPC)` 创建 RPC QP，通过 `zQP_RPC_Alloc()` 在服务端申请内存。
4. 使用 `zQP_create(..., ZQP_ONESIDED)` 创建数据 QP，并通过 `zQP_connect()` 建立连接。
5. 使用 `mr_malloc_create()` 分配并在所有本地设备上注册内存。
6. 调用 `z_read()`、`z_write()`、`z_CAS()`，或对应的异步接口。

异步接口会把 work request ID 写入调用方提供的 `std::vector<uint64_t>`，之后应调用 `z_poll_completion()` 等待完成。远端分配返回的主 `rkey` 会映射到各条路径实际使用的 `rkey`。

## 辅助脚本

- `scripts/set_2MB_hugepage.sh <page_count>`：设置 2 MiB HugePage 数量，需要 root 权限。当前内存分配实现使用普通匿名 `mmap`，不会自动使用 HugePage。
- `scripts/update_config.sh`：启动 Mellanox Software Tools，并修改指定 PCI 设备的链路保持参数。脚本中的 `/dev/mst/mt4119_pciconf1` 是实验机硬编码值。
- `scripts/install_ofed.sh`：安装固定版本的 MLNX_OFED 和若干旧实验依赖，并修改 GRUB 后重启。执行前必须检查系统版本、下载地址和所有命令。
- `config/00_config.yaml`：Ubuntu Netplan bond 配置示例，不会被程序直接读取。
- `config/bond0.json`：bond 设备配置示例；当前 `zServer` 仍要求两块设备，不能直接使用该单设备配置启动。

## 注意事项

- `zClient` 和 `latency_test` 会通过 `sudo` 执行 `ip link`/`ifconfig`，网卡名和 IP 来自本地 JSON 配置中的 `eth_names` 与 `ips`。
- 两个测试程序都会主动关闭配置中的第一块网卡来模拟链路故障；直接运行可能中断当前机器网络连接。
- 在实际环境运行前，应先修改 `source/zClient.cc` 与 `source/latency_test.cc` 中的网卡控制逻辑，或移除故障注入代码。
- 论文描述的是 DCQP 立即接管、RCQP 后台异步重建的完整设计；默认已启用 `ASYNC_CONNECT`。若要复现预连接备用 RCQP 的 Resend-cache 基线，请在 `latency_test` 中定义 `PRECONNECT_BACKUP`。
- 论文描述了固件/驱动事件、CQ error 和 heartbeat 等组合式故障检测；当前代码主要依赖 completion error、超时和测试程序中的手动断链，没有实现完整的可配置 heartbeat 子系统。
- 论文中的部分 baseline 不在当前仓库内。Motor/TPC-C 应用层测试见 [`tests/motor`](tests/motor)，运行方式见 [`tests/motor/RUN.md`](tests/motor/RUN.md)。
- 配置解析失败目前没有完善的错误报告，错误的 JSON、数组长度不一致或不存在的 RDMA 设备可能导致断言失败或进程退出。
- 项目没有自动化测试，示例程序本身就是当前的集成与性能测试入口。
- 代码和脚本包含针对特定实验集群的参数，建议先在隔离环境中验证，不要直接用于生产系统。

## License

本项目使用 [GNU General Public License v3.0](LICENSE)。
