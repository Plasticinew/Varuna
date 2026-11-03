#!/bin/bash
# Measure HERD request-path throughput, recovery outcomes, and the
# Varuna failover timeline. Writes CSV files next to this script.
#
#   ./run_herd_eval.sh
#
# Node-0 is the client (mlx5_2, backup mlx5_3). Node-1 is the worker.

set -euo pipefail

ROOT=/users/X1aoyang/Varuna
BIN=${ROOT}/tests/herd_rpc/build/herd_rpc_bench
OUT=${ROOT}/tests/herd_rpc/perf
LOG=${OUT}/logs
IP=10.10.1.3
PORT=18515
DEV=mlx5_2
BACKUP=mlx5_3

mkdir -p "${LOG}"
cmake --build "${ROOT}/tests/herd_rpc/build" -j"$(nproc)" --target herd_rpc_bench

start_server() {
  local backup_dev=$1
  ssh -o BatchMode=yes node-1 'pkill -x herd_rpc_bench >/dev/null 2>&1 || true; sleep 0.3; rm -f /tmp/herd_rpc_bench'
  scp -q "${BIN}" node-1:/tmp/herd_rpc_bench
  ssh -o BatchMode=yes node-1 "chmod +x /tmp/herd_rpc_bench; ulimit -l unlimited; nohup /tmp/herd_rpc_bench server ${IP} ${PORT} ${DEV} ${backup_dev} >/tmp/herd_bench_server.log 2>&1 </dev/null &"
  local i
  for i in $(seq 1 30); do
    if ssh -o BatchMode=yes node-1 'grep -q listening /tmp/herd_bench_server.log'; then
      return 0
    fi
    sleep 0.2
  done
  ssh -o BatchMode=yes node-1 'cat /tmp/herd_bench_server.log' >&2 || true
  return 1
}

stop_server() {
  ssh -o BatchMode=yes node-1 'pkill -x herd_rpc_bench >/dev/null 2>&1 || true' || true
}
trap stop_server EXIT

ulimit -l unlimited
start_server "${DEV}"
"${BIN}" perf "${IP}" "${PORT}" "${DEV}" 1.0 | tee "${LOG}/perf.log"
"${BIN}" correct "${IP}" "${PORT}" "${DEV}" 40 | tee "${LOG}/correct.log"
stop_server
start_server "${BACKUP}"
"${BIN}" timeline "${IP}" "${PORT}" "${DEV}" 20 8 "${BACKUP}" | tee "${LOG}/timeline.log"
stop_server

python3 - "${OUT}" <<'PY'
import pathlib, re, sys
out = pathlib.Path(sys.argv[1])
logs = out / "logs"

perf = ["system,kops,p50_us,p99_us"]
for line in (logs / "perf.log").read_text().splitlines():
    m = re.search(r"PERF system=(\S+) .* kops=([0-9.]+) p50_ns=(\d+) p99_ns=(\d+)", line)
    if m:
        perf.append(f"{m.group(1)},{m.group(2)},{int(m.group(3))/1000:.3f},{int(m.group(4))/1000:.3f}")
(out / "herd_perf.csv").write_text("\n".join(perf) + "\n")

correct = ["system,trials,correct,duplicate,lost,mismatch"]
for line in (logs / "correct.log").read_text().splitlines():
    m = re.search(
        r"CORRECT system=(\S+) trials=(\d+) correct=(\d+) duplicate=(\d+) lost=(\d+) mismatch=(\d+)",
        line,
    )
    if m:
        correct.append(",".join(m.groups()))
(out / "herd_correct.csv").write_text("\n".join(correct) + "\n")

rows = ["t_us,kops"]
meta = None
for line in (logs / "timeline.log").read_text().splitlines():
    m = re.search(r"TIMELINE_META bucket_us=(\d+) fail_us=(\d+) recovery_us=([0-9.]+)", line)
    if m:
        meta = m.groups()
    m = re.search(r"TIMELINE_ROW t_us=(\d+) kops=([0-9.]+)", line)
    if m:
        rows.append(f"{m.group(1)},{m.group(2)}")
if meta is None:
    raise SystemExit("timeline meta missing")
(out / "herd_timeline.csv").write_text("\n".join(rows) + "\n")
(out / "herd_timeline_meta.csv").write_text(
    "bucket_us,fail_us,recovery_us\n" + ",".join(meta) + "\n"
)
print(f"wrote {out}/herd_perf.csv {out}/herd_correct.csv {out}/herd_timeline.csv")
PY
