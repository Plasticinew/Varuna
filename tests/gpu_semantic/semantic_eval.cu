// GPU-side semantic checks for two RDMA patterns used by AI communication:
//   1. NCCL LL: one 8-byte store packs 4B data + 4B flag. Replaying an old
//      line rolls the flag backward.
//   2. NVSHMEM signal: payload write followed by atomicAdd on a 64-bit
//      counter. Replaying the atomic wakes the waiter on a stale payload.
//
// The replay is explicit. Stock NCCL/NVSHMEM will not cross-NIC retransmit;
// this binary shows what that retransmission does to GPU-visible state.

#include <cuda_runtime.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>

struct alignas(8) LLLine {
    uint32_t data;
    uint32_t flag;
};

struct LLObs {
    int status;  // 1 = saw expected flag, 0 = gave up
    uint32_t data;
    uint32_t flag;
};

struct SigObs {
    int status;  // 1 = signal reached expect, 0 = gave up
    unsigned long long signal;
    unsigned long long payload;
};

#define CHECK_CUDA(call)                                                         \
    do {                                                                         \
        cudaError_t err__ = (call);                                             \
        if (err__ != cudaSuccess) {                                              \
            fprintf(stderr, "CUDA error %s:%d: %s\n", __FILE__, __LINE__,       \
                    cudaGetErrorString(err__));                                  \
            return 1;                                                            \
        }                                                                        \
    } while (0)

__device__ __forceinline__ unsigned long long ld_volatile_u64(const void *ptr) {
    unsigned long long v;
    asm volatile("ld.volatile.global.u64 %0, [%1];" : "=l"(v) : "l"(ptr));
    return v;
}

__global__ void wait_ll(const LLLine *line, uint32_t expect, int spins, LLObs *out) {
    LLObs obs;
    obs.status = 0;
    obs.data = 0;
    obs.flag = 0;
    for (int i = 0; i < spins; ++i) {
        unsigned long long raw = ld_volatile_u64(line);
        uint32_t data = static_cast<uint32_t>(raw);
        uint32_t flag = static_cast<uint32_t>(raw >> 32);
        obs.data = data;
        obs.flag = flag;
        if (flag == expect) {
            obs.status = 1;
            break;
        }
    }
    *out = obs;
}

__global__ void reread_ll(const LLLine *line, LLObs *out) {
    unsigned long long raw = ld_volatile_u64(line);
    out->status = 1;
    out->data = static_cast<uint32_t>(raw);
    out->flag = static_cast<uint32_t>(raw >> 32);
}

__global__ void add_signal(unsigned long long *signal, unsigned long long delta) {
    atomicAdd(signal, delta);
}

__global__ void wait_signal(const unsigned long long *signal, const unsigned long long *payload,
                            unsigned long long expect, int spins, SigObs *out) {
    SigObs obs;
    obs.status = 0;
    obs.signal = 0;
    obs.payload = 0;
    for (int i = 0; i < spins; ++i) {
        unsigned long long s = atomicAdd(const_cast<unsigned long long *>(signal), 0ull);
        obs.signal = s;
        if (s >= expect) {
            obs.payload = ld_volatile_u64(payload);
            obs.status = 1;
            break;
        }
    }
    if (!obs.status) {
        obs.payload = ld_volatile_u64(payload);
    }
    *out = obs;
}

static int publish_ll(LLLine *dst, uint32_t data, uint32_t flag) {
    LLLine host;
    host.data = data;
    host.flag = flag;
    CHECK_CUDA(cudaMemcpy(dst, &host, sizeof(host), cudaMemcpyHostToDevice));
    return 0;
}

static int copy_obs_ll(const LLObs *src, LLObs *dst) {
    CHECK_CUDA(cudaMemcpy(dst, src, sizeof(*dst), cudaMemcpyDeviceToHost));
    return 0;
}

static int copy_obs_sig(const SigObs *src, SigObs *dst) {
    CHECK_CUDA(cudaMemcpy(dst, src, sizeof(*dst), cudaMemcpyDeviceToHost));
    return 0;
}

static const int kSpins = 2000000;

static int test_ll_replay() {
    LLLine *line = nullptr;
    LLObs *obs_d = nullptr;
    CHECK_CUDA(cudaMalloc(&line, sizeof(LLLine)));
    CHECK_CUDA(cudaMalloc(&obs_d, sizeof(LLObs)));
    if (publish_ll(line, 3, 3) || publish_ll(line, 1, 1)) {
        return 1;
    }
    wait_ll<<<1, 1>>>(line, 3, kSpins, obs_d);
    CHECK_CUDA(cudaDeviceSynchronize());
    LLObs obs;
    if (copy_obs_ll(obs_d, &obs)) {
        return 1;
    }
    int corrupt = (obs.status == 0 && obs.flag == 1 && obs.data == 1);
    printf("LL_REPLAY status=%s flag=%u data=%u expect_flag=3\n",
           corrupt ? "STALL" : "UNEXPECTED", obs.flag, obs.data);
    cudaFree(line);
    cudaFree(obs_d);
    return corrupt ? 0 : 1;
}

static int test_ll_guard() {
    LLLine *line = nullptr;
    LLObs *obs_d = nullptr;
    CHECK_CUDA(cudaMalloc(&line, sizeof(LLLine)));
    CHECK_CUDA(cudaMalloc(&obs_d, sizeof(LLObs)));
    if (publish_ll(line, 3, 3)) {
        return 1;
    }
    wait_ll<<<1, 1>>>(line, 3, kSpins, obs_d);
    CHECK_CUDA(cudaDeviceSynchronize());
    LLObs obs;
    if (copy_obs_ll(obs_d, &obs)) {
        return 1;
    }
    int ok = (obs.status == 1 && obs.flag == 3 && obs.data == 3);
    printf("LL_GUARD status=%s flag=%u data=%u\n", ok ? "OK" : "UNEXPECTED", obs.flag, obs.data);
    cudaFree(line);
    cudaFree(obs_d);
    return ok ? 0 : 1;
}

// Consumer already accepted flag 2. A later replay of the previous line
// overwrites the slot the consumer still reads in place.
static int test_ll_reread() {
    LLLine *line = nullptr;
    LLObs *obs_d = nullptr;
    CHECK_CUDA(cudaMalloc(&line, sizeof(LLLine)));
    CHECK_CUDA(cudaMalloc(&obs_d, sizeof(LLObs)));
    if (publish_ll(line, 2, 2)) {
        return 1;
    }
    wait_ll<<<1, 1>>>(line, 2, kSpins, obs_d);
    CHECK_CUDA(cudaDeviceSynchronize());
    LLObs first;
    if (copy_obs_ll(obs_d, &first)) {
        return 1;
    }
    if (publish_ll(line, 1, 1)) {
        return 1;
    }
    reread_ll<<<1, 1>>>(line, obs_d);
    CHECK_CUDA(cudaDeviceSynchronize());
    LLObs second;
    if (copy_obs_ll(obs_d, &second)) {
        return 1;
    }
    int corrupt = (first.status == 1 && first.data == 2 && second.data == 1 && second.flag == 1);
    printf("LL_REREAD status=%s first_data=%u second_flag=%u second_data=%u\n",
           corrupt ? "CORRUPT" : "UNEXPECTED", first.data, second.flag, second.data);
    cudaFree(line);
    cudaFree(obs_d);
    return corrupt ? 0 : 1;
}

static int set_u64(unsigned long long *dst, unsigned long long v) {
    CHECK_CUDA(cudaMemcpy(dst, &v, sizeof(v), cudaMemcpyHostToDevice));
    return 0;
}

static int test_signal_replay() {
    unsigned long long *signal = nullptr;
    unsigned long long *payload = nullptr;
    SigObs *obs_d = nullptr;
    CHECK_CUDA(cudaMalloc(&signal, sizeof(unsigned long long)));
    CHECK_CUDA(cudaMalloc(&payload, sizeof(unsigned long long)));
    CHECK_CUDA(cudaMalloc(&obs_d, sizeof(SigObs)));
    if (set_u64(signal, 0) || set_u64(payload, 111)) {
        return 1;
    }
    add_signal<<<1, 1>>>(signal, 1);
    add_signal<<<1, 1>>>(signal, 1);  // duplicate notification, payload unchanged
    CHECK_CUDA(cudaDeviceSynchronize());
    wait_signal<<<1, 1>>>(signal, payload, 2, kSpins, obs_d);
    CHECK_CUDA(cudaDeviceSynchronize());
    SigObs obs;
    if (copy_obs_sig(obs_d, &obs)) {
        return 1;
    }
    int stale = (obs.status == 1 && obs.signal == 2 && obs.payload == 111);
    printf("SIGNAL_REPLAY status=%s signal=%llu payload=%llu\n",
           stale ? "FALSE_WAKE" : "UNEXPECTED", obs.signal, obs.payload);
    cudaFree(signal);
    cudaFree(payload);
    cudaFree(obs_d);
    return stale ? 0 : 1;
}

static int test_signal_guard() {
    unsigned long long *signal = nullptr;
    unsigned long long *payload = nullptr;
    SigObs *obs_d = nullptr;
    CHECK_CUDA(cudaMalloc(&signal, sizeof(unsigned long long)));
    CHECK_CUDA(cudaMalloc(&payload, sizeof(unsigned long long)));
    CHECK_CUDA(cudaMalloc(&obs_d, sizeof(SigObs)));
    if (set_u64(signal, 0) || set_u64(payload, 111)) {
        return 1;
    }
    add_signal<<<1, 1>>>(signal, 1);
    CHECK_CUDA(cudaDeviceSynchronize());
    wait_signal<<<1, 1>>>(signal, payload, 2, kSpins, obs_d);
    CHECK_CUDA(cudaDeviceSynchronize());
    SigObs obs;
    if (copy_obs_sig(obs_d, &obs)) {
        return 1;
    }
    int hold = (obs.status == 0 && obs.signal == 1);
    printf("SIGNAL_GUARD status=%s signal=%llu payload=%llu\n",
           hold ? "HOLD" : "UNEXPECTED", obs.signal, obs.payload);
    cudaFree(signal);
    cudaFree(payload);
    cudaFree(obs_d);
    return hold ? 0 : 1;
}

// Payload of the next message is visible, but the atomic notification is not.
static int test_signal_lost() {
    unsigned long long *signal = nullptr;
    unsigned long long *payload = nullptr;
    SigObs *obs_d = nullptr;
    CHECK_CUDA(cudaMalloc(&signal, sizeof(unsigned long long)));
    CHECK_CUDA(cudaMalloc(&payload, sizeof(unsigned long long)));
    CHECK_CUDA(cudaMalloc(&obs_d, sizeof(SigObs)));
    if (set_u64(signal, 1) || set_u64(payload, 222)) {
        return 1;
    }
    wait_signal<<<1, 1>>>(signal, payload, 2, kSpins, obs_d);
    CHECK_CUDA(cudaDeviceSynchronize());
    SigObs obs;
    if (copy_obs_sig(obs_d, &obs)) {
        return 1;
    }
    int lost = (obs.status == 0 && obs.signal == 1 && obs.payload == 222);
    printf("SIGNAL_LOST status=%s signal=%llu payload=%llu\n",
           lost ? "TIMEOUT" : "UNEXPECTED", obs.signal, obs.payload);
    cudaFree(signal);
    cudaFree(payload);
    cudaFree(obs_d);
    return lost ? 0 : 1;
}

int main() {
    int device = 0;
    CHECK_CUDA(cudaGetDeviceCount(&device));
    if (device < 1) {
        fprintf(stderr, "no CUDA device\n");
        return 1;
    }
    CHECK_CUDA(cudaSetDevice(0));
    cudaDeviceProp prop;
    CHECK_CUDA(cudaGetDeviceProperties(&prop, 0));
    printf("GPU %s cc %d.%d\n", prop.name, prop.major, prop.minor);

    int rc = 0;
    rc |= test_ll_replay();
    rc |= test_ll_guard();
    rc |= test_ll_reread();
    rc |= test_signal_replay();
    rc |= test_signal_guard();
    rc |= test_signal_lost();
    if (rc == 0) {
        printf("SEMANTIC_EVAL PASS\n");
    } else {
        printf("SEMANTIC_EVAL FAIL\n");
    }
    return rc == 0 ? 0 : 1;
}
