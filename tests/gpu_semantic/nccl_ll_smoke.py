#!/usr/bin/env python3
"""Healthy NCCL allreduce forced onto the LL protocol.

This does not inject retransmission. It checks that a GPU box can run LL
and that the numerical result matches. The corruption cases are in
semantic_eval.cu.
"""

import os

os.environ.setdefault("NCCL_PROTO", "LL")

import torch
import torch.distributed as dist


def main():
    dist.init_process_group("nccl")
    rank = dist.get_rank()
    world = dist.get_world_size()
    torch.cuda.set_device(rank % torch.cuda.device_count())
    x = torch.full((1 << 16,), float(rank + 1), device="cuda")
    dist.all_reduce(x, op=dist.ReduceOp.SUM)
    expect = world * (world + 1) / 2.0
    ok = torch.allclose(x, torch.full_like(x, expect))
    dist.barrier()
    if rank == 0:
        print(
            "NCCL_LL status=%s world=%d proto=%s"
            % ("OK" if ok.item() else "MISMATCH", world, os.environ.get("NCCL_PROTO"))
        )
    dist.destroy_process_group()
    if not ok.item():
        raise SystemExit(1)


if __name__ == "__main__":
    main()
