// HERD RPC request path, adapted to Varuna's logged WRITE.
//
// Wire layout and poll rule are the ones in efficient/rdma_bench:
//   mica/mica.h      mica_op is a 64-byte slot: 16-byte key, opcode, len, value
//   herd/main.h      HERD_OP_GET/PUT = MICA opcode + 10
//   herd/worker.c    a slot is a new request iff opcode >= HERD_OP_GET;
//                    the worker then subtracts 10, which both selects the
//                    MICA operation and disarms the slot.
// The client delivers that slot with one RC WRITE. rdma_bench posts it on a
// UC QP. Here the same bytes go through the WRITE that zQP_write logs.
//
// Replay rule is z_recovery() in source/zQP.cc: a logged WRITE is posted
// again only when the local timestamp differs from the responder's
// completion-log timestamp (or when full retransmission is forced). A match
// means the WRITE already landed, so the slot is left alone.
// The UD SEND that carries the HERD response is not part of this path.

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <unordered_map>

namespace {

constexpr uint8_t kMicaOpGet = 111;
constexpr uint8_t kMicaOpPut = 112;
constexpr uint8_t kHerdMicaOffset = 10;
constexpr uint8_t kHerdOpGet = kMicaOpGet + kHerdMicaOffset;
constexpr uint8_t kHerdOpPut = kMicaOpPut + kHerdMicaOffset;
constexpr int kMicaMaxValue = 64 - (16 + 1 + 1);

struct MicaKey {
    uint64_t unused;
    uint32_t bkt;
    uint16_t server;
    uint16_t tag;
};
static_assert(sizeof(MicaKey) == 16, "HERD key is 16 bytes");

struct HerdRequest {
    MicaKey key;
    uint8_t opcode;
    uint8_t val_len;
    uint8_t value[kMicaMaxValue];
};
static_assert(sizeof(HerdRequest) == 64, "rdma_bench mica_op is one cache line");
static_assert(offsetof(HerdRequest, opcode) == 16, "worker polls opcode at byte 16");

struct Store {
    std::unordered_map<uint32_t, std::string> values;
    int puts = 0;
    int gets = 0;
};

HerdRequest make_put(uint32_t key, const char *value) {
    HerdRequest req{};
    req.key.bkt = key;
    req.opcode = kHerdOpPut;
    req.val_len = static_cast<uint8_t>(std::strlen(value));
    std::memcpy(req.value, value, req.val_len);
    return req;
}

HerdRequest make_get(uint32_t key) {
    HerdRequest req{};
    req.key.bkt = key;
    req.opcode = kHerdOpGet;
    return req;
}

// herd/worker.c: opcode >= HERD_OP_GET, then opcode -= HERD_MICA_OFFSET.
bool herd_poll(HerdRequest *slot, Store *store) {
    if (slot->opcode < kHerdOpGet) {
        return false;
    }
    slot->opcode = static_cast<uint8_t>(slot->opcode - kHerdMicaOffset);
    if (slot->opcode == kMicaOpPut) {
        store->values[slot->key.bkt] =
            std::string(reinterpret_cast<char *>(slot->value), slot->val_len);
        store->puts++;
        return true;
    }
    if (slot->opcode == kMicaOpGet) {
        store->gets++;
        return true;
    }
    return false;
}

// source/zQP.cc z_recovery(): WRITE is rewritten iff the timestamps differ,
// unless full retransmission is on. READ is always redone; HERD's request
// is a WRITE, so that branch does not apply.
bool varuna_rewrites_write(uint16_t local_time, uint16_t remote_time, bool full) {
    return full || local_time != remote_time;
}

void failover_write(HerdRequest *slot, const HerdRequest &posted,
                    uint16_t local_time, uint16_t remote_time, bool full) {
    if (varuna_rewrites_write(local_time, remote_time, full)) {
        *slot = posted;
    }
}

int g_failed = 0;

void expect(bool cond, const char *name) {
    if (cond) {
        std::printf("PASS %s\n", name);
        return;
    }
    std::printf("FAIL %s\n", name);
    g_failed++;
}

std::string stored(const Store &store, uint32_t key) {
    auto it = store.values.find(key);
    return it == store.values.end() ? std::string() : it->second;
}

void test_one_put_disarms_the_slot() {
    Store store;
    HerdRequest slot = make_put(7, "v1");
    expect(herd_poll(&slot, &store), "first poll accepts HERD_OP_PUT");
    expect(store.puts == 1 && stored(store, 7) == "v1", "PUT applied once");
    expect(slot.opcode == kMicaOpPut, "opcode disarmed to MICA_OP_PUT");
    expect(!herd_poll(&slot, &store) && store.puts == 1, "second poll sees no request");
}

void test_blind_replay_runs_the_put_again() {
    Store store;
    HerdRequest posted = make_put(7, "v1");
    HerdRequest slot = posted;
    herd_poll(&slot, &store);

    slot = posted;
    expect(herd_poll(&slot, &store), "replayed WRITE is a new request");
    expect(store.puts == 2 && stored(store, 7) == "v1", "same PUT executed twice");
}

void test_replay_after_reuse_rolls_the_value_back() {
    Store store;
    HerdRequest first = make_put(7, "v1");
    HerdRequest slot = first;
    herd_poll(&slot, &store);

    HerdRequest second = make_put(7, "v2");
    slot = second;
    herd_poll(&slot, &store);
    expect(stored(store, 7) == "v2" && store.puts == 2, "newer PUT occupies the slot");

    slot = first;
    herd_poll(&slot, &store);
    expect(store.puts == 3 && stored(store, 7) == "v1", "old WRITE rolls the key back");
}

void test_varuna_skips_a_landed_write() {
    Store store;
    HerdRequest first = make_put(7, "v1");
    HerdRequest slot = first;
    herd_poll(&slot, &store);
    HerdRequest second = make_put(7, "v2");
    slot = second;
    herd_poll(&slot, &store);

    constexpr uint16_t kStamp = 4;
    failover_write(&slot, first, kStamp, kStamp, false);
    expect(slot.opcode == kMicaOpPut, "matched log does not restore HERD_OP_PUT");
    expect(!herd_poll(&slot, &store), "worker does not run the skipped WRITE");
    expect(store.puts == 2 && stored(store, 7) == "v2", "newer value stays");
}

void test_varuna_replays_a_lost_write() {
    Store store;
    HerdRequest posted = make_put(7, "v1");
    HerdRequest slot{};
    constexpr uint16_t kLocal = 4;
    constexpr uint16_t kRemote = 0;
    failover_write(&slot, posted, kLocal, kRemote, false);
    expect(herd_poll(&slot, &store), "unmatched log rewrites the slot");
    expect(store.puts == 1 && stored(store, 7) == "v1", "lost PUT runs once");
}

void test_full_retransmit_rewrites_even_when_the_log_matches() {
    Store store;
    HerdRequest posted = make_put(7, "v1");
    HerdRequest slot = posted;
    herd_poll(&slot, &store);
    failover_write(&slot, posted, 4, 4, true);
    expect(herd_poll(&slot, &store) && store.puts == 2,
           "full retransmission posts the landed WRITE again");
}

void test_get_replay_counts_a_second_execution() {
    Store store;
    HerdRequest put = make_put(7, "v1");
    HerdRequest slot = put;
    herd_poll(&slot, &store);
    HerdRequest get = make_get(7);
    slot = get;
    herd_poll(&slot, &store);
    slot = get;
    herd_poll(&slot, &store);
    expect(store.gets == 2 && stored(store, 7) == "v1",
           "replayed GET runs twice and leaves the value");
}

}  // namespace

int main() {
    test_one_put_disarms_the_slot();
    test_blind_replay_runs_the_put_again();
    test_replay_after_reuse_rolls_the_value_back();
    test_varuna_skips_a_landed_write();
    test_varuna_replays_a_lost_write();
    test_full_retransmit_rewrites_even_when_the_log_matches();
    test_get_replay_counts_a_second_execution();
    if (g_failed != 0) {
        std::printf("%d failed\n", g_failed);
        return 1;
    }
    std::printf("herd rpc request path: all checks passed\n");
    return 0;
}
