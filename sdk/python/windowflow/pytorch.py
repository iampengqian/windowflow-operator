# SPDX-License-Identifier: Apache-2.0
"""Optional PyTorch factory. Importing this module does not import torch."""

from __future__ import annotations

import hashlib
from typing import Any, Callable

from .client import WindowValidationError
from .runner import WindowContext, WindowEpoch


class _TorchBatches:
    def __init__(self, loader: Any, dataset: Any) -> None:
        self.loader = loader
        self.dataset = dataset
        self.iterator = iter(loader)
        self.expected_batches = len(loader)
        self.yielded_batches = 0
        self.exhausted = False

    def __iter__(self):
        return self

    def __next__(self):
        if self.iterator is None:
            raise StopIteration
        try:
            batch = next(self.iterator)
        except StopIteration:
            if self.yielded_batches != self.expected_batches:
                raise WindowValidationError("DataLoader stopped before its declared batch count; lease remains held")
            self.exhausted = True
            raise
        self.yielded_batches += 1
        if self.yielded_batches > self.expected_batches:
            raise WindowValidationError("DataLoader exceeded its declared batch count; lease remains held")
        return batch

    def drain(self) -> None:
        if not self.exhausted:
            raise WindowValidationError("PyTorch loader has not been exhausted; lease remains held")
        # Nonpersistent DataLoader workers terminate on normal exhaustion.
        # A main-process decoder must also be closed before permitting release.
        close = getattr(self.dataset, "close", None)
        if close is not None:
            close()
        self.iterator = self.loader = self.dataset = None

    def abort(self) -> None:
        # Drop owned references to allow PyTorch's iterator cleanup. There is no
        # public DataLoader.close/join API, so this is NOT a fencing guarantee.
        # In particular, never exhaust all remaining video data just to cancel.
        self.iterator = self.loader = None
        dataset, self.dataset = self.dataset, None
        close = getattr(dataset, "close", None)
        if close is not None:
            close()


class TorchEpochFactory:
    """Build a fresh finite map-style Dataset/DataLoader per window-local epoch.

    ``dataset_factory(context)`` must return fully materialized CPU values, never
    open readers, lazy decoders or file-backed tensors. ``sampler_factory`` takes
    (dataset, context) and owns DP sharding and set_epoch; TP/PP topology stays in
    the training platform. Set shuffle=False when supplying a sampler factory.
    Workers are always nonpersistent. Persistent-worker hot swapping and async
    datasets are outside this adapter's completion contract.
    """

    def __init__(
        self,
        dataset_factory: Callable[[WindowContext], Any],
        *,
        batch_size: int = 1,
        num_workers: int = 0,
        shuffle: bool = False,
        drop_last: bool = False,
        seed: int = 0,
        sampler_factory: Callable[[Any, WindowContext], Any] | None = None,
        collate_fn: Callable | None = None,
        multiprocessing_context: str | None = None,
    ) -> None:
        if not callable(dataset_factory):
            raise WindowValidationError("dataset_factory must be callable")
        for name, value, minimum in (("batch_size", batch_size, 1), ("num_workers", num_workers, 0), ("seed", seed, 0)):
            if type(value) is not int or value < minimum:
                raise WindowValidationError(f"{name} must be an integer >= {minimum}")
        if type(shuffle) is not bool or type(drop_last) is not bool:
            raise WindowValidationError("shuffle and drop_last must be booleans")
        if sampler_factory is not None and (not callable(sampler_factory) or shuffle):
            raise WindowValidationError("sampler_factory must be callable and requires shuffle=False")
        if collate_fn is not None and not callable(collate_fn):
            raise WindowValidationError("collate_fn must be callable or None")
        if multiprocessing_context is not None and (num_workers == 0 or multiprocessing_context not in ("spawn", "fork", "forkserver")):
            raise WindowValidationError("multiprocessing_context requires workers and a supported start method")
        self.dataset_factory = dataset_factory
        self.batch_size = batch_size
        self.num_workers = num_workers
        self.shuffle = shuffle
        self.drop_last = drop_last
        self.seed = seed
        self.sampler_factory = sampler_factory
        self.collate_fn = collate_fn
        self.multiprocessing_context = multiprocessing_context

    def __call__(self, context: WindowContext) -> WindowEpoch:
        try:
            import torch
            from torch.utils.data import DataLoader, IterableDataset
        except ImportError as exc:
            raise ImportError("Install windowflow[torch] or use a custom WindowEpoch factory") from exc
        dataset = self.dataset_factory(context)
        try:
            if isinstance(dataset, IterableDataset) or not hasattr(dataset, "__getitem__") or len(dataset) < 1:
                raise WindowValidationError("A nonempty finite map-style Dataset is required")
            sampler = None if self.sampler_factory is None else self.sampler_factory(dataset, context)
            # Do not consume the model's CPU RNG when constructing loaders.
            identity = f"windowflow-torch/v1:{self.seed}:{context.handle.window_index}:{context.epoch}"
            loader_seed = int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big")
            options = {} if self.multiprocessing_context is None else {"multiprocessing_context": self.multiprocessing_context}
            loader = DataLoader(
                dataset, batch_size=self.batch_size, num_workers=self.num_workers,
                shuffle=self.shuffle, sampler=sampler, drop_last=self.drop_last,
                collate_fn=self.collate_fn, persistent_workers=False,
                generator=torch.Generator().manual_seed(loader_seed), **options,
            )
            if len(loader) == 0:
                raise WindowValidationError("Batch/sampler settings produce an empty epoch")
            batches = _TorchBatches(loader, dataset)
        except BaseException:
            close = getattr(dataset, "close", None)
            if close is not None:
                close()
            raise
        return WindowEpoch(batches, batches.drain, batches.abort)
