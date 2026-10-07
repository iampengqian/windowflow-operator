#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Read finite window-local epochs with CPU workers and WindowRunner.

Run in a Pod with the cache PVC mounted at /data and WindowLease RBAC. This is a
storage lifecycle demonstration, not a model trainer. Recovery's --start-window
must come from an application/model checkpoint, not from lease/cache status.
"""

from __future__ import annotations

import argparse
import hashlib
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path, PurePosixPath

from windowflow import WindowClient, WindowContext, WindowEpoch, WindowRunner


def read_file(path: str) -> tuple[int, str]:
    """Fully materialize reads and close the handle within the worker."""
    total = 0
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            total += len(block)
            digest.update(block)
    return total, digest.hexdigest()


def file_epoch(
    context: WindowContext, *, pattern: str, workers: int, reader_id: str, epochs_per_window: int,
) -> WindowEpoch:
    """Materialize hash results and join all subprocesses before yielding them.

    This intentionally demonstrates storage lifetime, not overlap with model
    execution. No batch contains a lazy reader or a file-backed view.
    """
    window = context.handle
    if (not pattern or PurePosixPath(pattern).is_absolute() or "\\" in pattern
            or any(char in pattern for char in "\x00\r\n") or ".." in pattern.split("/")):
        raise ValueError("--pattern must be relative and cannot contain parent traversal")
    files = sorted(window.root.glob(pattern))
    if not files:
        raise RuntimeError(f"No files matched {pattern!r} in {window.root}; lease remains held")
    for path in files:
        candidate = window.root
        for component in path.relative_to(window.root).parts:
            candidate /= component
            if candidate.is_symlink():
                raise RuntimeError(f"Symlink in sample path: {path}; lease remains held")
        if not path.is_file() or not path.resolve().is_relative_to(window.root):
            raise RuntimeError(f"Unsafe or non-regular sample path: {path}; lease remains held")

    pool_finished = False
    # A factory exception exits the pool context and propagates to the runner;
    # it cannot authorize release. Even successful shutdown alone is not release.
    with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("spawn")) as pool:
        results = list(pool.map(read_file, map(str, files)))
    pool_finished = True  # __exit__ waited for all worker processes to finish.

    def drain() -> None:
        if not pool_finished:
            raise RuntimeError("Reader process pool has not finished; lease remains held")
        print(
            f"reader={reader_id} window={window.window_id} ordinal={window.window_index} "
            f"epoch={context.epoch + 1}/{epochs_per_window} files={len(results)} "
            f"bytes={sum(size for size, _ in results)} pool=closed",
            flush=True,
        )

    return WindowEpoch(batches=results, drain=drain)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--reader", default="dp0")
    parser.add_argument("--mount", type=Path, default=Path("/data"))
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--epochs-per-window", type=int, default=1, help="Complete local file-reading passes before release")
    parser.add_argument("--start-window", type=int, default=0)
    parser.add_argument("--pattern", default="**/*.txt", help="Demo sample files, relative to each window root")
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be positive")
    if args.epochs_per_window < 1:
        parser.error("--epochs-per-window must be positive")
    client = WindowClient(args.plan, args.namespace, args.reader, args.mount)
    runner = WindowRunner(
        client,
        lambda context: file_epoch(
            context, pattern=args.pattern, workers=args.workers,
            reader_id=args.reader, epochs_per_window=args.epochs_per_window,
        ),
        epochs_per_window=args.epochs_per_window,
        start_window=args.start_window,
    )

    def consume_hash(result: tuple[int, str], context: WindowContext) -> None:
        # The CPU demonstration has already hashed each file in its subprocess.
        # A real trainer would synchronously consume fully materialized batches.
        size, digest = result
        if size < 0 or len(digest) != 64:
            raise RuntimeError(f"Invalid file result in {context.handle.window_id}; lease remains held")

    # run() normally exhausts every epoch, calls its drain, and releases only
    # after the last epoch. Exceptions/cancellation never force lease release.
    runner.run(consume_hash)


if __name__ == "__main__":
    main()
