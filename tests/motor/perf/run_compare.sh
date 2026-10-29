#!/bin/bash
# Run TPC-C on Motor native (rlib) and Motor+Varuna (zQP), append one CSV row
# per configuration. The plot script reads that CSV and does not rerun this.
#
#   ./run_compare.sh
#   THREADS="2 4" ATTEMPTED_NUM=200 ./run_compare.sh
#
# Layout: node-0 compute (this host), node-1 memory. One memory node, no
# backup replica (BACKUP_NUM is larger than the memory-node count, so Motor
# skips backup placement). Both builds are Release.

set -euo pipefail

ROOT=/users/X1aoyang/Varuna
MOTOR=${ROOT}/tests/motor
MN_HOST=${MN_HOST:-node-1}
BENCH=${BENCH:-tpcc}
ISO=${ISO:-SI}
CORO=${CORO:-2}
ATTEMPTED_NUM=${ATTEMPTED_NUM:-5000}
THREADS=${THREADS:-"2 4 8 16"}
CSV=${CSV:-${MOTOR}/perf/motor_varuna_tpcc.csv}
LOG_DIR=${MOTOR}/perf/logs
META_PORT=12347

mkdir -p "${LOG_DIR}"

if [[ ! -f ${CSV} ]]; then
  printf '%s\n' 'system,benchmark,isolation,threads,coroutines,attempted_num,attempted_ktps,committed_ktps,p50_us,p99_us' > "${CSV}"
fi

sync_sources() {
  rsync -a \
    --exclude build \
    --exclude build-native \
    --exclude build-varuna \
    --exclude bench_results \
    --exclude 'perf/logs' \
    --exclude 'perf/figures' \
    --exclude 'perf/motor_varuna_tpcc.csv' \
    "${ROOT}/" "${MN_HOST}:${ROOT}/"
}

build_tree() {
  local host=$1
  local cmd
  cmd=$(cat <<'EOF'
set -euo pipefail
ROOT=/users/X1aoyang/Varuna
cmake -S "${ROOT}" -B "${ROOT}/build" -DCMAKE_BUILD_TYPE=Release
cmake --build "${ROOT}/build" -j"$(nproc)"
cmake -S "${ROOT}/tests/motor" -B "${ROOT}/tests/motor/build-native" \
  -DCMAKE_BUILD_TYPE=Release -DMOTOR_USE_ZRDMA=OFF -DCMAKE_CXX_FLAGS="-DRDMA_LOG_LEVEL=2"
cmake --build "${ROOT}/tests/motor/build-native" -j"$(nproc)"
cmake -S "${ROOT}/tests/motor" -B "${ROOT}/tests/motor/build-varuna" \
  -DCMAKE_BUILD_TYPE=Release -DMOTOR_USE_ZRDMA=ON -DCMAKE_CXX_FLAGS="-DRDMA_LOG_LEVEL=2"
cmake --build "${ROOT}/tests/motor/build-varuna" -j"$(nproc)"
EOF
)
  if [[ ${host} == local ]]; then
    bash -lc "${cmd}"
  else
    ssh -o BatchMode=yes "${host}" "bash -lc $(printf '%q' "${cmd}")"
  fi
}

stop_memory_node() {
  ssh -o BatchMode=yes "${MN_HOST}" 'if [[ -f /tmp/motor_mempool.pid ]]; then kill "$(cat /tmp/motor_mempool.pid)" >/dev/null 2>&1 || true; rm -f /tmp/motor_mempool.pid; fi; pkill -x motor_mempool >/dev/null 2>&1 || true'
}

start_memory_node() {
  local variant=$1
  stop_memory_node
  scp -q "${MOTOR}/perf/start_mn.sh" "${MN_HOST}:${MOTOR}/perf/start_mn.sh"
  ssh -o BatchMode=yes "${MN_HOST}" "bash ${MOTOR}/perf/start_mn.sh ${variant}"
  local i
  for i in $(seq 1 300); do
    if ssh -o BatchMode=yes "${MN_HOST}" "ss -ltn | grep -q ':${META_PORT} '"; then
      return 0
    fi
    if ! ssh -o BatchMode=yes "${MN_HOST}" 'kill -0 "$(cat /tmp/motor_mempool.pid)" >/dev/null 2>&1'; then
      echo "memory node exited early" >&2
      ssh -o BatchMode=yes "${MN_HOST}" 'tail -n 80 /tmp/motor_mempool.log' >&2 || true
      return 1
    fi
    sleep 2
  done
  echo "timed out waiting for memory node on port ${META_PORT}" >&2
  ssh -o BatchMode=yes "${MN_HOST}" 'tail -n 80 /tmp/motor_mempool.log' >&2 || true
  return 1
}

append_result() {
  local system=$1
  local threads=$2
  local log_file=$3
  python3 - "${CSV}" "${system}" "${BENCH}" "${ISO}" "${threads}" "${CORO}" "${ATTEMPTED_NUM}" "${log_file}" <<'PY'
import re, sys
csv_path, system, bench, iso, threads, coros, attempted, log_path = sys.argv[1:]
ansi = re.compile(r"\x1b\[[0-9;]*m")
text = [ansi.sub("", line) for line in open(log_path, errors="replace").read().splitlines()]
pat = re.compile(r"^Motor\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s*$")
hits = [pat.match(line) for line in text]
hits = [hit for hit in hits if hit]
if len(hits) != 1:
    sys.exit(f"{log_path}: expected 1 Motor result line, found {len(hits)}")
attempted_ktps, committed_ktps, p50, p99 = hits[0].groups()
with open(csv_path, "a", encoding="utf-8", newline="\n") as handle:
    handle.write(
        f"{system},{bench},{iso},{threads},{coros},{attempted},"
        f"{attempted_ktps},{committed_ktps},{p50},{p99}\n"
    )
print(f"{system} threads={threads} committed_ktps={committed_ktps} p50_us={p50} p99_us={p99}")
PY
}

run_client() {
  local variant=$1
  local system=$2
  local threads=$3
  local log_file="${LOG_DIR}/${system}_t${threads}_c${CORO}.log"
  local run_dir="${MOTOR}/build-${variant}/compute_node/run"
  echo "client ${system} threads=${threads}"
  set +e
  (
    cd "${run_dir}"
    ulimit -l unlimited
    timeout 180 stdbuf -oL -eL ./run "${BENCH}" "${threads}" "${CORO}" "${ISO}"
  ) > "${log_file}" 2>&1
  local rc=$?
  set -e
  if [[ ${rc} -ne 0 ]]; then
    echo "client failed rc=${rc}, log ${log_file}" >&2
    tail -n 60 "${log_file}" >&2 || true
    return 1
  fi
  append_result "${system}" "${threads}" "${log_file}"
}

if [[ ${SKIP_BUILD:-0} != 1 ]]; then
  echo "sync and build"
  sync_sources
  build_tree local
  build_tree "${MN_HOST}"
fi

python3 - "${MOTOR}/config/tpcc_config.json" "${ATTEMPTED_NUM}" <<'PY'
import pathlib, re, sys
path, attempted = sys.argv[1:]
file = pathlib.Path(path)
text = file.read_text()
updated, n = re.subn(r'("attempted_num"\s*:\s*)\d+', rf'\g<1>{attempted}', text, count=1)
if n != 1:
    sys.exit("failed to set attempted_num")
file.write_text(updated)
PY
# The memory node reads the same workload file.
scp -q "${MOTOR}/config/tpcc_config.json" "${MN_HOST}:${MOTOR}/config/tpcc_config.json"

specs=("native motor_native" "varuna motor_varuna")
if [[ -n ${VARIANTS:-} ]]; then
  specs=()
  for variant_name in ${VARIANTS}; do
    case "${variant_name}" in
      native) specs+=("native motor_native") ;;
      varuna) specs+=("varuna motor_varuna") ;;
      *) echo "unknown variant ${variant_name}" >&2; exit 1 ;;
    esac
  done
fi
for spec in "${specs[@]}"; do
  variant=${spec%% *}
  system=${spec##* }
  for threads in ${THREADS}; do
    # The memory node serves one compute connection. Restart it for every
    # thread count so each point loads a fresh database.
    echo "start memory node ${variant} for ${threads} threads"
    start_memory_node "${variant}"
    run_client "${variant}" "${system}" "${threads}"
  done
  stop_memory_node
done

echo "csv ${CSV}"
