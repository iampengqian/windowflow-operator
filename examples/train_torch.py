#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""CPU PyTorch/DDP example over the tiny text windows in kind-storage.yaml.

Run in training Pods with a mounted cache, reader RBAC and matching reader IDs.
This tests training integration, not video decoding or Megatron TP/PP. For pure
DDP use torchrun and --ddp; each process uses its own dp<rank> reader identity.
"""

from __future__ import annotations

import argparse
from datetime import timedelta
from pathlib import Path

import torch
from torch.utils.data import Dataset, DistributedSampler

from windowflow import TorchEpochFactory, WindowClient, WindowRunner


class TinyTextDataset(Dataset):
    def __init__(self, root: Path):
        self.root = root
        self.files = sorted(root.glob("**/*.txt"))
        if not self.files:
            raise ValueError("No tiny .txt samples found")
        if any(path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root) for path in self.files):
            raise ValueError("Unsafe sample path")

    def __len__(self):
        return len(self.files)

    def __getitem__(self, index):
        # This demo's sample files are a few bytes. A real VideoDataset should
        # decode clips and return owned tensors without surviving file handles.
        with self.files[index].open("rb") as stream:
            content = stream.read(4097)
        if not content or len(content) > 4096:
            raise ValueError("This demo expects nonempty text samples <= 4 KiB")
        value = sum(content) / (255.0 * len(content))
        return torch.tensor([value]), torch.tensor([value * 0.5])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--mount", default="/cache")
    parser.add_argument("--reader", default="dp0", help="single-process reader; --ddp uses dp<rank>")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--epochs-per-window", type=int, default=2)
    parser.add_argument("--ddp", action="store_true")
    args = parser.parse_args()
    if args.ddp:
        torch.distributed.init_process_group("gloo", timeout=timedelta(minutes=3))
    try:
        rank = torch.distributed.get_rank() if args.ddp else 0
        world_size = torch.distributed.get_world_size() if args.ddp else 1
        torch.manual_seed(42)
        model = torch.nn.Linear(1, 1)
        if args.ddp:
            model = torch.nn.parallel.DistributedDataParallel(model)
        optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

        def make_sampler(dataset, context):
            sampler = DistributedSampler(dataset, num_replicas=world_size, rank=rank, seed=42)
            sampler.set_epoch(context.handle.window_index * args.epochs_per_window + context.epoch)
            return sampler

        client = WindowClient(args.plan, args.namespace, f"dp{rank}" if args.ddp else args.reader, args.mount)
        factory = TorchEpochFactory(
            lambda context: TinyTextDataset(context.handle.root),
            batch_size=1, num_workers=args.workers, seed=42,
            sampler_factory=make_sampler,
            multiprocessing_context="spawn" if args.workers else None,
        )
        runner = WindowRunner(client, factory, epochs_per_window=args.epochs_per_window)

        def train_batch(batch, context):
            inputs, targets = batch
            optimizer.zero_grad()
            loss = torch.nn.functional.mse_loss(model(inputs), targets)
            loss.backward()
            optimizer.step()
            print(f"rank={rank} window={context.handle.window_index} epoch={context.epoch} loss={loss.item():.6f}", flush=True)

        runner.run(train_batch)
    finally:
        if args.ddp:
            torch.distributed.destroy_process_group()


if __name__ == "__main__":
    main()
