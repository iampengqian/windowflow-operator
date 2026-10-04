# SPDX-License-Identifier: Apache-2.0
"""Integration SKETCH for a finite, deterministic map-style video Dataset.

This is not a tested Megatron/PPU adapter. Adapt to the pinned Megatron fork and
existing TP/PP data-reader topology. Install WindowFlow's SDK and PyTorch in the
training image. Existing get_batch/model/optimizer code need not know windows.

Only actual data-reader processes construct this iterator. Give each a UNIQUE
reader_id in WindowPlan.spec.readers, including PP/virtual-PP readers if present.
TP replicas may use the same DP rank for sample assignment but must not share a
WindowLease reader identity. Preserve the training platform's existing broadcast
and reader placement; this module does not decide which stages need data.

The application checkpoint's consumed_train_samples is the sole resume offset.
A NEW plan attempt must set spec.startWindow to checkpoint_position(...)[0].
Do not ALSO skip the offset in an outer Megatron sampler or StatefulDataLoader. Dataset
augmentation state/exact reproducibility, packed samples, variable batch size,
Megatron rerun semantics, and elastic DP resizing require additional integration.
"""

from __future__ import annotations

from typing import Callable, Iterator, Sequence

from windowflow import WindowClient, WindowHandle


def checkpoint_position(consumed_samples: int, window_sample_counts: Sequence[int]) -> tuple[int, int]:
    """Translate a GLOBAL sample count into (window ordinal, global offset)."""
    if type(consumed_samples) is not int or consumed_samples < 0:
        raise ValueError("consumed_samples must be a nonnegative integer")
    if any(type(count) is not int or count <= 0 for count in window_sample_counts):
        raise ValueError("window_sample_counts must contain positive integers")
    remaining = consumed_samples
    for index, count in enumerate(window_sample_counts):
        if remaining < count:
            return index, remaining
        remaining -= count
    if remaining:
        raise ValueError("checkpoint offset is beyond the immutable window schedule")
    return len(window_sample_counts), 0


def continuous_window_batches(
    client: WindowClient,
    dataset_factory: Callable[[WindowHandle], object],
    *,
    window_sample_counts: Sequence[int],
    consumed_train_samples: int,
    micro_batch_size: int,
    global_batch_size: int,
    num_workers: int = 4,
    collate_fn=None,
) -> Iterator:
    """Yield uninterrupted batches from successive finite window DataLoaders.

    dataset_factory returns an immutable indexed Dataset of exactly the declared
    size, using window.root. Its indices must follow the SAME deterministic
    window-local order on every reader of the same DP replica; any global IDs
    must map consistently back to original samples. It must return fully decoded
    CPU tensors/values, never a lazy file/video reader. Do not add another shuffle
    that accesses files outside the acquired window.

    Pass the resulting iterator through the platform's Megatron external-loader
    hook. Most versions expose dataloader_type='external'; verify your fork.
    There are no collectives/barriers in this adapter, including at boundaries.
    """
    import torch
    from megatron.core import parallel_state
    from megatron.training.datasets.data_samplers import MegatronPretrainingSampler

    # IMPORTANT: DP topology, not torch.distributed.get_rank()/get_world_size().
    dp_rank = parallel_state.get_data_parallel_rank()
    dp_size = parallel_state.get_data_parallel_world_size()
    if micro_batch_size <= 0 or global_batch_size <= 0 or num_workers < 0:
        raise ValueError("invalid batch sizes or worker count")
    if global_batch_size % (micro_batch_size * dp_size):
        raise ValueError("this sketch requires fixed global batch alignment")
    if any(count % global_batch_size for count in window_sample_counts):
        raise ValueError("each window sample budget must align to a complete global batch")
    if consumed_train_samples % global_batch_size:
        raise ValueError("checkpoint must be at a complete training-step boundary")
    start_window, global_offset = checkpoint_position(consumed_train_samples, window_sample_counts)
    for ordinal in range(start_window, len(window_sample_counts)):
        handle = client.acquire(ordinal)
        dataset = dataset_factory(handle)
        if len(dataset) != window_sample_counts[ordinal]:
            raise ValueError("dataset length differs from the checkpointed schedule; lease remains held")
        sampler = MegatronPretrainingSampler(
            total_samples=len(dataset), consumed_samples=global_offset,
            micro_batch_size=micro_batch_size,
            data_parallel_rank=dp_rank, data_parallel_size=dp_size, drop_last=True,
        )
        # Dedicated generator avoids consuming the model's CPU RNG on each
        # DataLoader construction. Exact transform RNG restore is not implemented.
        generator = torch.Generator().manual_seed(ordinal)
        loader = torch.utils.data.DataLoader(
            dataset, batch_sampler=sampler, num_workers=num_workers,
            persistent_workers=False, collate_fn=collate_fn, generator=generator,
        )
        # On normal exhaustion PyTorch shuts down these nonpersistent workers.
        # Any exception/GeneratorExit bypasses release and leaves the lease held.
        for batch in loader:
            yield batch
        close = getattr(dataset, "close", None)
        if close is not None:
            close()  # Main-process readers too (e.g. num_workers == 0).
        del loader, dataset
        client.release(handle)
        global_offset = 0
