# SPDX-License-Identifier: Apache-2.0
"""Real CPU DataLoader lifecycle tests; optional torch and no Kubernetes cluster."""

from __future__ import annotations

import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest

from windowflow.client import WindowFlowError, WindowHandle, WindowValidationError
from windowflow.pytorch import TorchEpochFactory
from windowflow.runner import WindowContext, WindowRunner

try:
    import torch
    from torch.utils.data import Dataset, IterableDataset
except ImportError:
    torch = None
    Dataset = IterableDataset = object


class IndexedDataset(Dataset):
    """Defined at module level so macOS/spawn workers can deserialize it."""

    def __init__(self, count=8, *, stop_at=None, close_error=False):
        self.count = count
        self.stop_at = stop_at
        self.close_error = close_error
        self.reads = []
        self.close_count = 0

    def __len__(self):
        return self.count

    def __getitem__(self, index):
        self.reads.append(index)
        if index == self.stop_at:
            raise StopIteration("bad Dataset: this is not sampler exhaustion")
        return {"index": index, "pid": os.getpid()}

    def close(self):
        self.close_count += 1
        if self.close_error:
            raise RuntimeError("decoder close failed")


class EndlessIterableDataset(IterableDataset):
    def __iter__(self):
        while True:
            yield 0


class FakeClient:
    """Lease fake checks state and captures child processes at actual release."""

    def __init__(self, *, windows=1, start_window=0):
        self.status = SimpleNamespace(window_count=windows, start_window=start_window, phase="Running")
        self.acquired = []
        self.released = []
        self.release_active_pids = []
        self.on_release = None

    def get_status(self):
        return self.status

    def acquire(self, index):
        if index in self.acquired:
            raise AssertionError("A window must only be acquired once")
        self.acquired.append(index)
        return WindowHandle("test-plan", index, f"window-{index}", f"w{index:06d}", Path("/cache") / str(index))

    def release(self, handle):
        if handle.window_index in self.released:
            raise AssertionError("A window must only be released once")
        self.release_active_pids.append({p.pid for p in multiprocessing.active_children()})
        if self.on_release is not None:
            self.on_release(handle)
        self.released.append(handle.window_index)


class OptionalTorchImportTests(unittest.TestCase):
    def test_sdk_and_adapter_import_without_site_packages_or_torch(self):
        sdk_root = Path(__file__).resolve().parents[1]
        environment = dict(os.environ, PYTHONPATH=str(sdk_root))
        result = subprocess.run(
            [sys.executable, "-S", "-c",
             "import sys; import windowflow; import windowflow.runner; "
             "import windowflow.pytorch; assert 'torch' not in sys.modules"],
            env=environment, text=True, capture_output=True, timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


@unittest.skipIf(torch is None, "optional PyTorch is not installed")
class TorchEpochFactoryTests(unittest.TestCase):
    def make_runner(self, dataset, *, client=None, **factory_options):
        client = client if client is not None else FakeClient()
        runner = WindowRunner(client, TorchEpochFactory(lambda context: dataset, **factory_options))
        self.addCleanup(runner.close)
        return runner, client

    def test_single_process_exhaustion_closes_dataset_before_release(self):
        dataset = IndexedDataset(5)
        runner, client = self.make_runner(dataset, batch_size=2)
        client.on_release = lambda handle: self.assertEqual(dataset.close_count, 1)
        batches = list(runner)
        self.assertEqual([i for batch in batches for i in batch["index"].tolist()], list(range(5)))
        self.assertTrue(all(batch["index"].device.type == "cpu" for batch in batches))
        self.assertEqual({pid for batch in batches for pid in batch["pid"].tolist()}, {os.getpid()})
        self.assertEqual(client.acquired, [0])
        self.assertEqual(client.released, [0])
        self.assertEqual(dataset.close_count, 1)
        self.assertEqual(runner.state, "completed")

    def test_spawn_workers_exit_before_release_without_private_worker_api(self):
        dataset = IndexedDataset(12)
        runner, client = self.make_runner(
            dataset, batch_size=2, num_workers=2, multiprocessing_context="spawn",
        )
        batches = list(runner)
        worker_pids = {pid for batch in batches for pid in batch["pid"].tolist()}
        self.assertEqual(len(worker_pids), 2)
        self.assertNotIn(os.getpid(), worker_pids)
        self.assertFalse(worker_pids & client.release_active_pids[0])
        self.assertFalse(worker_pids & {p.pid for p in multiprocessing.active_children()})
        self.assertEqual([i for batch in batches for i in batch["index"].tolist()], list(range(12)))
        self.assertEqual(dataset.close_count, 1)
        self.assertEqual(client.released, [0])

    def test_recreates_and_closes_each_epoch_but_releases_once_per_window(self):
        datasets = []
        contexts = []
        client = FakeClient(windows=2)

        def create(context):
            contexts.append((context.handle.window_index, context.epoch))
            dataset = IndexedDataset(2)
            datasets.append(dataset)
            return dataset

        runner = WindowRunner(client, TorchEpochFactory(create), epochs_per_window=2)
        self.addCleanup(runner.close)
        list(runner)
        self.assertEqual(contexts, [(0, 0), (0, 1), (1, 0), (1, 1)])
        self.assertEqual([dataset.close_count for dataset in datasets], [1, 1, 1, 1])
        self.assertEqual(client.acquired, [0, 1])
        self.assertEqual(client.released, [0, 1])

    def test_shuffle_repeats_with_same_seed_context_and_preserves_model_rng(self):
        orders = []
        # Include spawn: constructing worker base seeds must use the private
        # generator, not advance the model's global CPU generator.
        for workers in (0, 2):
            with self.subTest(num_workers=workers):
                options = {"multiprocessing_context": "spawn"} if workers else {}
                for _ in range(2):
                    torch.manual_seed(9182)
                    before = torch.get_rng_state().clone()
                    runner, client = self.make_runner(
                        IndexedDataset(16), batch_size=4, shuffle=True, seed=271,
                        num_workers=workers, **options,
                    )
                    orders.append([i for batch in runner for i in batch["index"].tolist()])
                    self.assertTrue(torch.equal(before, torch.get_rng_state()))
                    self.assertEqual(client.released, [0])
        self.assertTrue(all(order == orders[0] for order in orders))
        self.assertEqual(sorted(orders[0]), list(range(16)))

    def test_sampler_factory_controls_order_and_receives_epoch_context(self):
        calls = []

        def sampler(dataset, context):
            calls.append((len(dataset), context.handle.window_index, context.epoch))
            return [5, 2, 4]

        runner, client = self.make_runner(IndexedDataset(6), batch_size=2, sampler_factory=sampler)
        indices = [i for batch in runner for i in batch["index"].tolist()]
        self.assertEqual(indices, [5, 2, 4])
        self.assertEqual(calls, [(6, 0, 0)])
        self.assertEqual(client.released, [0])

    def test_drop_last_respects_loader_length_and_releases_after_real_exhaustion(self):
        dataset = IndexedDataset(5)
        runner, client = self.make_runner(dataset, batch_size=2, drop_last=True)
        self.assertEqual([i for batch in runner for i in batch["index"].tolist()], [0, 1, 2, 3])
        self.assertEqual(dataset.reads, [0, 1, 2, 3])
        self.assertEqual(client.released, [0])

    def test_dataset_stop_iteration_is_failure_and_never_releases(self):
        for workers in (0, 2):
            with self.subTest(num_workers=workers):
                options = {"multiprocessing_context": "spawn"} if workers else {}
                dataset = IndexedDataset(5, stop_at=1)
                runner, client = self.make_runner(dataset, num_workers=workers, **options)
                self.assertEqual(next(runner)["index"].tolist(), [0])
                with self.assertRaises(WindowFlowError):
                    list(runner)
                self.assertEqual(runner.state, "failed")
                self.assertEqual(client.released, [])
                runner.close()
                self.assertEqual(client.released, [])
                self.assertEqual(dataset.close_count, 1)

    def test_close_does_not_scan_remaining_dataset_or_release(self):
        dataset = IndexedDataset(10**12)
        runner, client = self.make_runner(dataset, batch_size=2)
        self.assertEqual(next(runner)["index"].tolist(), [0, 1])
        runner.close()
        runner.close()
        self.assertEqual(dataset.reads, [0, 1])
        self.assertEqual(dataset.close_count, 1)
        self.assertEqual(client.released, [])
        self.assertEqual(runner.state, "stopped")
        with self.assertRaises(WindowFlowError):
            next(runner)

    def test_fixed_step_finish_exhausts_workers_and_releases(self):
        runner, client = self.make_runner(
            IndexedDataset(4), batch_size=2, num_workers=2, multiprocessing_context="spawn",
        )
        batches = [next(runner), next(runner)]
        worker_pids = {pid for batch in batches for pid in batch["pid"].tolist()}
        self.assertEqual(client.released, [])
        runner.finish()
        runner.finish()
        self.assertEqual(client.released, [0])
        self.assertFalse(worker_pids & client.release_active_pids[0])
        self.assertEqual(runner.state, "completed")

    def test_finish_retains_peeked_batch_and_does_not_close_dataset(self):
        dataset = IndexedDataset(3)
        runner, client = self.make_runner(dataset)
        self.assertEqual(next(runner)["index"].tolist(), [0])
        with self.assertRaises(WindowFlowError):
            runner.finish()
        with self.assertRaises(WindowFlowError):
            runner.finish()
        self.assertEqual(dataset.reads, [0, 1])
        self.assertEqual(dataset.close_count, 0)
        self.assertEqual(client.released, [])
        self.assertEqual(next(runner)["index"].tolist(), [1])
        self.assertEqual(next(runner)["index"].tolist(), [2])
        runner.finish()
        self.assertEqual(dataset.close_count, 1)
        self.assertEqual(client.released, [0])

    def test_dataset_close_error_keeps_lease_held(self):
        dataset = IndexedDataset(1, close_error=True)
        runner, client = self.make_runner(dataset)
        try:
            with self.assertRaisesRegex(RuntimeError, "decoder close failed"):
                list(runner)
            self.assertEqual(runner.state, "failed")
            self.assertEqual(client.released, [])
        finally:
            # This test concerns failed success-drain, not intentionally failing
            # the best-effort abort registered with unittest cleanup as well.
            dataset.close_error = False

    def test_callback_exception_aborts_without_reading_next_batch_or_releasing(self):
        dataset = IndexedDataset(100)
        runner, client = self.make_runner(dataset)

        def train(batch, context):
            raise RuntimeError("training failed")

        with self.assertRaisesRegex(RuntimeError, "training failed"):
            runner.run(train)
        self.assertEqual(dataset.reads, [0])
        self.assertEqual(dataset.close_count, 1)
        self.assertEqual(client.released, [])
        self.assertEqual(runner.state, "failed")

    def test_empty_dataset_and_empty_sampler_keep_lease_and_close_dataset(self):
        for count, sampler in ((0, None), (3, lambda dataset, context: [])):
            with self.subTest(dataset_size=count):
                dataset = IndexedDataset(count)
                runner, client = self.make_runner(dataset, sampler_factory=sampler)
                with self.assertRaises(WindowValidationError):
                    next(runner)
                self.assertEqual(client.released, [])
                self.assertEqual(dataset.close_count, 1)

    def test_rejects_iterable_dataset_and_shuffle_with_custom_sampler(self):
        runner, client = self.make_runner(EndlessIterableDataset())
        with self.assertRaises(WindowValidationError):
            next(runner)
        self.assertEqual(client.released, [])
        with self.assertRaises(WindowValidationError):
            TorchEpochFactory(lambda context: IndexedDataset(), shuffle=True,
                              sampler_factory=lambda dataset, context: [0])


if __name__ == "__main__":
    unittest.main()
