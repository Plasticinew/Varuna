#!/bin/bash
# Start motor_mempool in a detached session. Stdin stays open on a fifo so the
# post-run command loop does not observe EOF and unregister the RDMA heap.
set -euo pipefail

variant=${1:?variant native or varuna}
cd "/users/X1aoyang/Varuna/tests/motor/build-${variant}/memory_node/server"
ulimit -l unlimited

if [[ -f /tmp/motor_mempool.pid ]]; then
  kill "$(cat /tmp/motor_mempool.pid)" >/dev/null 2>&1 || true
  rm -f /tmp/motor_mempool.pid
fi
pkill -x motor_mempool >/dev/null 2>&1 || true
sleep 0.3

rm -f /tmp/motor.stdin
mkfifo /tmp/motor.stdin
setsid bash -c 'exec 3<>/tmp/motor.stdin; ./motor_mempool </tmp/motor.stdin >/tmp/motor_mempool.log 2>&1 & echo $! >/tmp/motor_mempool.pid; wait' \
  </dev/null >/dev/null 2>&1 &

for _ in 1 2 3 4 5 6 7 8 9 10; do
  if [[ -s /tmp/motor_mempool.pid ]]; then
    exit 0
  fi
  sleep 0.2
done
echo "memory node pid file missing" >&2
exit 1
