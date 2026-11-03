#!/bin/bash
# Fail the client data NIC during TPC-C and record recovery outcomes.
#
# Varuna selective recovery and blind retransmission (VARUNA_FULL_RETRANSMIT=1)
# use the probe-enabled Varuna client. Native Motor has no NIC failover.
# The timeline is the selective run.
#
#   ./run_failover.sh
#
# Restores ens1f0 and the TPC-C config on the way out. Does not touch the
# management NIC.

set -euo pipefail

ROOT=/users/X1aoyang/Varuna
MOTOR=${ROOT}/tests/motor
OUT=${MOTOR}/perf
LOG=${OUT}/logs
MN_HOST=node-1
XMIT=/sys/class/infiniband/mlx5_2/ports/1/counters/port_xmit_data
TPCC=${MOTOR}/config/tpcc_config.json
CN=${MOTOR}/config/cn_config.json
BAK=$(mktemp -d)

mkdir -p "${LOG}"
cp "${TPCC}" "${BAK}/tpcc_config.json"
cp "${CN}" "${BAK}/cn_config.json"

restore_all() {
  sudo ip link set ens1f0 up >/dev/null 2>&1 || true
  local i
  for i in $(seq 1 40); do
    if ibstat mlx5_2 2>/dev/null | grep -q "State: Active"; then
      break
    fi
    sleep 0.25
  done
  cp "${BAK}/tpcc_config.json" "${TPCC}"
  cp "${BAK}/cn_config.json" "${CN}"
  scp -q "${TPCC}" "${CN}" "${MN_HOST}:${MOTOR}/config/" || true
  ssh -o BatchMode=yes "${MN_HOST}" 'if [[ -f /tmp/motor_mempool.pid ]]; then kill "$(cat /tmp/motor_mempool.pid)" >/dev/null 2>&1 || true; fi; pkill -x motor_mempool >/dev/null 2>&1 || true' || true
  rm -rf "${BAK}"
}
trap restore_all EXIT

python3 - "${TPCC}" "${CN}" <<'PY'
import pathlib, re, sys
tpcc, cn = sys.argv[1:]
file = pathlib.Path(tpcc)
text = file.read_text()
text2, n1 = re.subn(r'("num_warehouse"\s*:\s*)\d+', r'\g<1>2', text, count=1)
text3, n2 = re.subn(r'("attempted_num"\s*:\s*)\d+', r'\g<1>8000', text2, count=1)
if n1 != 1 or n2 != 1:
    raise SystemExit("failed to edit tpcc config")
file.write_text(text3)
file = pathlib.Path(cn)
text = file.read_text()
text2, n = re.subn(r'("tp_probe_interval_ms"\s*:\s*)\d+', r'\g<1>5', text, count=1)
if n != 1:
    raise SystemExit("failed to edit probe interval")
file.write_text(text2)
PY

scp -q "${MOTOR}/build-varuna/memory_node/server/motor_mempool" \
  "${MN_HOST}:${MOTOR}/build-varuna/memory_node/server/motor_mempool"
scp -q "${MOTOR}/build-native/memory_node/server/motor_mempool" \
  "${MN_HOST}:${MOTOR}/build-native/memory_node/server/motor_mempool"
scp -q "${TPCC}" "${CN}" "${MN_HOST}:${MOTOR}/config/"
scp -q "${MOTOR}/perf/start_mn.sh" "${MN_HOST}:${MOTOR}/perf/start_mn.sh"

stop_mn() {
  ssh -o BatchMode=yes "${MN_HOST}" 'if [[ -f /tmp/motor_mempool.pid ]]; then kill "$(cat /tmp/motor_mempool.pid)" >/dev/null 2>&1 || true; rm -f /tmp/motor_mempool.pid; fi; pkill -x motor_mempool >/dev/null 2>&1 || true'
}

start_mn() {
  local variant=$1
  stop_mn
  ssh -o BatchMode=yes "${MN_HOST}" "bash ${MOTOR}/perf/start_mn.sh ${variant}"
  local i
  for i in $(seq 1 300); do
    if ssh -o BatchMode=yes "${MN_HOST}" "ss -ltn | grep -q ':12347 '"; then
      return 0
    fi
    if ! ssh -o BatchMode=yes "${MN_HOST}" 'kill -0 "$(cat /tmp/motor_mempool.pid)" >/dev/null 2>&1'; then
      echo "memory node exited" >&2
      ssh -o BatchMode=yes "${MN_HOST}" 'tail -n 40 /tmp/motor_mempool.log' >&2 || true
      return 1
    fi
    sleep 1
  done
  echo "memory node timeout" >&2
  return 1
}

link_up() {
  sudo ip link set ens1f0 up
  local i
  for i in $(seq 1 40); do
    if ibstat mlx5_2 | grep -q "State: Active"; then
      return 0
    fi
    sleep 0.25
  done
  echo "ens1f0 did not return" >&2
  return 1
}

run_case() {
  local name=$1
  local variant=$2
  local bin=$3
  local extra=$4
  local wait_s=$5
  local log=${LOG}/failover_${name}.log
  echo "===== ${name} ====="
  if ss -ltn | grep -q ':12345 '; then
    pkill -f './run tpcc' >/dev/null 2>&1 || true
    sleep 0.5
  fi
  link_up
  start_mn "${variant}"
  (
    cd "$(dirname "${bin}")"
    ulimit -l unlimited
    # shellcheck disable=SC2086
    env ${extra} stdbuf -oL -eL ./run tpcc 2 2 SI
  ) > "${log}" 2>&1 &
  local pid=$!
  local saw=0
  local i
  for i in $(seq 1 400); do
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      echo "${name} exited before traffic" >&2
      tail -n 40 "${log}" >&2 || true
      stop_mn
      return 1
    fi
    if grep -q "Executing" "${log}"; then
      saw=1
      break
    fi
    sleep 0.25
  done
  if (( saw == 0 )); then
    echo "${name} did not reach Executing" >&2
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" || true
    stop_mn
    return 1
  fi
  local base now
  base=$(cat "${XMIT}")
  saw=0
  for i in $(seq 1 80); do
    if ! kill -0 "${pid}" >/dev/null 2>&1; then
      echo "${name} exited during warmup" >&2
      tail -n 40 "${log}" >&2 || true
      stop_mn
      return 1
    fi
    now=$(cat "${XMIT}")
    if (( now > base + 100000 )); then
      saw=1
      break
    fi
    sleep 0.1
  done
  if (( saw == 0 )); then
    echo "${name} never sent on mlx5_2" >&2
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" || true
    stop_mn
    return 1
  fi
  sleep 0.5
  echo "drop ens1f0"
  sudo ip link set ens1f0 down
  local ticks=0
  local limit=$((wait_s * 2))
  while kill -0 "${pid}" >/dev/null 2>&1; do
    sleep 0.5
    ticks=$((ticks + 1))
    if (( ticks >= limit )); then
      echo "${name} still running after ${wait_s}s, stopping it"
      kill "${pid}" >/dev/null 2>&1 || true
      sleep 1
      kill -9 "${pid}" >/dev/null 2>&1 || true
      pkill -9 -f './run tpcc' >/dev/null 2>&1 || true
      break
    fi
  done
  wait "${pid}" || true
  pkill -9 -f './run tpcc' >/dev/null 2>&1 || true
  link_up
  stop_mn
  echo "log ${log}"
}

only=${1:-all}
if [[ "${only}" == all || "${only}" == varuna ]]; then
  run_case varuna varuna "${MOTOR}/build-varuna-probe/compute_node/run/run" \
    "${VARUNA_FAILOVER_ENV:-}" "${VARUNA_FAILOVER_WAIT:-150}"
fi
if [[ "${only}" == all || "${only}" == blind ]]; then
  run_case blind varuna "${MOTOR}/build-varuna-probe/compute_node/run/run" "VARUNA_FULL_RETRANSMIT=1" 150
fi
if [[ "${only}" == all || "${only}" == native ]]; then
  run_case native native "${MOTOR}/build-native/compute_node/run/run" "" 40
fi

python3 - "${LOG}" "${OUT}" "${MOTOR}" <<'PY'
import pathlib, re, sys
log_dir, out_dir, motor = map(pathlib.Path, sys.argv[1:])

def parse(name):
    text = (log_dir / f"failover_{name}.log").read_text(errors="replace")
    plain = re.sub(r"\x1b\[[0-9;]*m", "", text)
    correct = incorrect = 0
    fail = 0
    walls = []
    switches = []
    for line in plain.splitlines():
        m = re.search(
            r"RECOVERY_STAT mode=\S+ correct=(\d+) incorrect=(\d+) .* wall_ns=(\d+) fail=(\d+)",
            line,
        )
        if m:
            if m.group(4) == "0":
                correct += int(m.group(1))
                incorrect += int(m.group(2))
                walls.append(int(m.group(3)))
            else:
                fail += 1
        m = re.search(r"EVENT .* wall_ns=(\d+) name=switch_start", line)
        if m:
            switches.append(int(m.group(1)))
    finished = re.search(r"^Motor\s+\S+", plain, re.M) is not None
    check = re.search(r"TPCC_CHECK pass=(\d+)", plain)
    return {
        "correct": correct,
        "incorrect": incorrect,
        "fail": fail,
        "finished": finished,
        "check": int(check.group(1)) if check else -1,
        "switches": switches,
        "walls": walls,
        "text": text,
    }

rows = ["system,correct,incorrect,aborted,consistency_pass,recovery_us"]
parsed = {}
for name, system in (("native", "motor_native"), ("blind", "motor_blind"), ("varuna", "motor_varuna")):
    item = parse(name)
    parsed[name] = item
    decided = item["correct"] + item["incorrect"]
    if name == "native" or decided == 0:
        aborted = 1
        correct = incorrect = 0
    else:
        aborted = 0
        correct = item["correct"]
        incorrect = item["incorrect"]
    # First successful RECOVERY_STAT. That is the QP holding the writes and
    # CAS. A later stat is the read-only QP and is not the recovery time.
    if item["switches"] and item["walls"]:
        recovery_us = (min(item["walls"]) - min(item["switches"])) / 1000.0
    else:
        recovery_us = 0
    rows.append(
        f"{system},{correct},{incorrect},{aborted},{item['check']},{recovery_us:.3f}"
    )
    print(
        f"{system} correct={correct} incorrect={incorrect} aborted={aborted} "
        f"check={item['check']} finished={item['finished']} recovery_us={recovery_us:.3f}"
    )
(out_dir / "motor_failover_correct.csv").write_text("\n".join(rows) + "\n")

varuna = parsed["varuna"]
if not varuna["switches"]:
    raise SystemExit("varuna run did not switch NICs")
fail_ns = min(varuna["switches"])
probe = None
for line in varuna["text"].splitlines():
    if line.startswith("TP_PROBE_FILE "):
        probe = pathlib.Path(line.split(None, 1)[1])
if probe is None or not probe.is_absolute():
    run_dir = motor / "build-varuna-probe" / "compute_node" / "run"
    if probe is not None:
        probe = (run_dir / probe).resolve()
if probe is None or not probe.is_file():
    raise SystemExit(f"probe file missing: {probe}")
# Collapse gap-fill rows that repeat one timestamp with a zero. A real tick
# keeps the summed throughput. interval_us is the gap since the previous tick;
# a multi-second gap is the blocked QP, and that tick's rate averages the stall.
best = {}
order = []
for line in probe.read_text().splitlines()[1:]:
    if not line.strip():
        continue
    parts = line.split()
    t_ns = int(parts[0])
    ktps = float(parts[1])
    if t_ns not in best:
        order.append(t_ns)
        best[t_ns] = ktps
    elif ktps > best[t_ns]:
        best[t_ns] = ktps
body = ["t_ns,committed_ktps,interval_us"]
prev = None
for t_ns in order:
    interval_us = 0.0 if prev is None else (t_ns - prev) / 1000.0
    body.append(f"{t_ns},{best[t_ns]:.6f},{interval_us:.3f}")
    prev = t_ns
(out_dir / "motor_failover_timeline.csv").write_text("\n".join(body) + "\n")
recovery_us = (min(varuna["walls"]) - fail_ns) / 1000.0 if varuna["walls"] else 0
both_us = (max(varuna["walls"]) - fail_ns) / 1000.0 if varuna["walls"] else 0
last_before = max((t for t in order if t < fail_ns), default=fail_ns)
detect_us = (fail_ns - last_before) / 1000.0
(out_dir / "motor_failover_timeline_meta.csv").write_text(
    "fail_ns,recovery_us,both_us,detect_us\n"
    f"{fail_ns},{recovery_us:.3f},{both_us:.3f},{detect_us:.3f}\n"
)
print(
    f"probe {probe} samples {len(body)-1} "
    f"recovery_us {recovery_us:.3f} both_us {both_us:.3f} detect_us {detect_us:.3f}"
)
PY
