#!/usr/bin/env bash
# One-shot GPU checks for NCCL LL flag rollback and NVSHMEM-style signal replay.
#
# On a GPU machine, from anywhere:
#   bash tests/gpu_semantic/run.sh
#
# Requires: nvidia-smi, nvcc.
# Optional: python3 + torch with NCCL, for a healthy LL allreduce across local GPUs.
# Link flap is not part of this script. Stock NCCL aborts on a dead QP instead of
# replaying an LL line; semantic_eval replays that line on purpose.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD="${ROOT}/build"
mkdir -p "${BUILD}"

if ! command -v nvidia-smi >/dev/null 2>&1; then
    echo "nvidia-smi not found. Run this on a machine with a GPU driver." >&2
    exit 1
fi

if ! command -v nvcc >/dev/null 2>&1; then
    if [[ -x /usr/local/cuda/bin/nvcc ]]; then
        export PATH="/usr/local/cuda/bin:${PATH}"
    else
        echo "nvcc not found. Load the CUDA toolkit first." >&2
        exit 1
    fi
fi

NGPU="$(nvidia-smi -L | wc -l | tr -d ' ')"
CAP="$(nvidia-smi --query-gpu=compute_cap --format=csv,noheader | head -n 1 | tr -d '.')"
echo "gpus=${NGPU} arch=sm_${CAP}"

nvcc -O2 -std=c++14 -arch="sm_${CAP}" "${ROOT}/semantic_eval.cu" -o "${BUILD}/semantic_eval"
"${BUILD}/semantic_eval"

NCCL_STATUS="SKIP"
if [[ "${NGPU}" -lt 2 ]]; then
    echo "NCCL_LL status=SKIP reason=need_at_least_2_gpus"
elif ! python3 -c "import torch" >/dev/null 2>&1; then
    echo "NCCL_LL status=SKIP reason=python_torch_missing"
else
    export NCCL_PROTO=LL
    export NCCL_DEBUG="${NCCL_DEBUG:-WARN}"
    set +e
    if command -v torchrun >/dev/null 2>&1; then
        torchrun --standalone --nproc_per_node="${NGPU}" "${ROOT}/nccl_ll_smoke.py"
    else
        python3 -m torch.distributed.run --standalone --nproc_per_node="${NGPU}" "${ROOT}/nccl_ll_smoke.py"
    fi
    NCCL_RC=$?
    set -e
    if [[ "${NCCL_RC}" -eq 0 ]]; then
        NCCL_STATUS="OK"
    else
        NCCL_STATUS="FAIL"
        echo "NCCL_LL status=FAIL rc=${NCCL_RC}" >&2
        exit "${NCCL_RC}"
    fi
fi

echo "RUN_DONE semantic=PASS nccl_ll=${NCCL_STATUS}"
