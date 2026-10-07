# SPDX-License-Identifier: Apache-2.0
"""Real CPU DDP training; the lease API is a process-local fake, not Kubernetes."""

from __future__ import annotations

import json
import multiprocessing
from pathlib import Path
from queue import Empty
import tempfile
import time
from types import SimpleNamespace
import unittest

from windowflow import TorchEpochFactory, WindowHandle, WindowRunner, generate_schedule

try:
    import torch
except ImportError:
    torch = None


class FileDataset:
    def __init__(self, root, on_close):
        self.root, self.on_close = root, on_close

    def __len__(self):
        return 4

    def __getitem__(self, index):
        with (self.root / "samples.json").open() as stream:
            value = json.load(stream)[index]
        return torch.tensor([value], dtype=torch.float32), torch.tensor([value * 0.5], dtype=torch.float32)

    def close(self):
        self.on_close()


def _train_rank(rank, root, rendezvous, schedule, results):
    from datetime import timedelta
    import torch.distributed as dist
    from torch.nn.parallel import DistributedDataParallel
    from torch.utils.data import DistributedSampler

    torch.set_num_threads(1)
    dist.init_process_group("gloo", init_method=rendezvous, rank=rank, world_size=2, timeout=timedelta(seconds=45))
    try:
        events = {"acquired": [], "released": [], "closed": {}, "steps": 0}

        class Client:
            def get_status(self):
                return SimpleNamespace(start_window=0, window_count=len(schedule["entries"]), phase="Running")

            def acquire(self, index):
                entry = schedule["entries"][index]
                events["acquired"].append(entry["windowId"])
                return WindowHandle("ddp-plan", index, entry["windowId"], f"w{index:06d}", Path(root) / str(index))

            def release(self, handle):
                if events["closed"].get(handle.window_index) != 2:
                    raise AssertionError("release before both epochs' Dataset resources closed")
                events["released"].append(handle.window_index)

        def dataset_factory(context):
            index = context.handle.window_index

            def closed():
                events["closed"][index] = events["closed"].get(index, 0) + 1

            return FileDataset(context.handle.root, closed)

        def sampler_factory(dataset, context):
            sampler = DistributedSampler(dataset, num_replicas=2, rank=rank, seed=13)
            sampler.set_epoch(context.handle.window_index * 2 + context.epoch)
            return sampler

        torch.manual_seed(42)
        model = DistributedDataParallel(torch.nn.Linear(1, 1))
        initial = [p.detach().item() for p in model.parameters()]
        optimizer = torch.optim.SGD(model.parameters(), lr=0.02)
        factory = TorchEpochFactory(dataset_factory, batch_size=1, num_workers=0, seed=13, sampler_factory=sampler_factory)
        runner = WindowRunner(Client(), factory, epochs_per_window=2)

        def train_batch(batch, context):
            inputs, targets = batch
            optimizer.zero_grad()
            loss = torch.nn.functional.mse_loss(model(inputs), targets)
            if not torch.isfinite(loss):
                raise AssertionError("nonfinite training loss")
            loss.backward()
            optimizer.step()
            events["steps"] += 1

        runner.run(train_batch)
        results.put({"rank": rank, "state": runner.state, "events": events,
                     "weights": [p.detach().item() for p in model.parameters()], "initial": initial})
    finally:
        dist.destroy_process_group()


@unittest.skipIf(torch is None, "optional torch dependency not installed")
class DDPWindowTests(unittest.TestCase):
    def test_two_ranks_train_three_cycles_and_release_after_two_epochs(self):
        if not torch.distributed.is_available() or not torch.distributed.is_gloo_available():
            self.skipTest("CPU Gloo backend is unavailable")
        catalog = {"schemaVersion": "windowflow.catalog/v1", "windows": [
            {"id": f"source-{i}", "source": f"windows/{i}", "expectedBytes": 20} for i in range(3)
        ]}
        schedule = generate_schedule(catalog, seed=42, num_cycles=3, shuffle_windows=True)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            for entry in schedule["entries"]:
                directory = root / str(entry["ordinal"])
                directory.mkdir()
                (directory / "samples.json").write_text(json.dumps([0.1, 0.2, 0.3, 0.4]))
            context = multiprocessing.get_context("spawn")
            results = context.Queue()
            processes = [context.Process(target=_train_rank, args=(rank, str(root), (root / "rendezvous").as_uri(), schedule, results)) for rank in range(2)]
            try:
                for process in processes:
                    process.start()
                deadline = time.monotonic() + 120
                for process in processes:
                    process.join(max(0, deadline - time.monotonic()))
                self.assertTrue(all(not p.is_alive() for p in processes), "DDP children timed out")
                self.assertEqual([p.exitcode for p in processes], [0, 0])
                try:
                    reports = sorted([results.get(timeout=5), results.get(timeout=5)], key=lambda result: result["rank"])
                except Empty:
                    self.fail("DDP child did not report training results")
                for report in reports:
                    self.assertEqual(report["state"], "completed")
                    self.assertEqual(report["events"]["acquired"], [e["windowId"] for e in schedule["entries"]])
                    self.assertEqual(report["events"]["released"], list(range(9)))
                    self.assertEqual(sum(report["events"]["closed"].values()), 18)
                    self.assertEqual(report["events"]["steps"], 36)
                    self.assertNotEqual(report["weights"], report["initial"])
                self.assertEqual(reports[0]["weights"], reports[1]["weights"])
            finally:
                for process in processes:
                    if process.pid is not None and process.is_alive():
                        process.terminate()
                        process.join(5)
                        if process.is_alive():
                            process.kill()
                            process.join(5)
                results.close()
                results.join_thread()
