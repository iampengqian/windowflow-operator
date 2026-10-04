#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Read each ready window with CPU workers, then explicitly release it.

Run in a Pod with the cache PVC mounted at /data and WindowLease RBAC. This is a
storage lifecycle demonstration, not a model trainer. Recovery's --start-window
must come from an application/model checkpoint, not from lease/cache status.
"""

from __future__ import annotations

import argparse
import hashlib
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from windowflow import WindowClient


def read_file(path: str) -> tuple[int, str]:
    """Fully materialize reads and close the handle within the worker."""
    total = 0
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            total += len(block)
            digest.update(block)
    return total, digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", required=True)
    parser.add_argument("--namespace", default="default")
    parser.add_argument("--reader", default="dp0")
    parser.add_argument("--mount", type=Path, default=Path("/data"))
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--start-window", type=int, default=0)
    parser.add_argument("--pattern", default="**/*.txt", help="Demo sample files, relative to each window root")
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be positive")
    client = WindowClient(args.plan, args.namespace, args.reader, args.mount)
    for window in client.iter_windows(args.start_window):
        files = sorted(window.root.glob(args.pattern))
        if not files:
            raise RuntimeError(f"No files matched {args.pattern!r} in {window.root}; lease remains held")
        for path in files:
            if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(window.root):
                raise RuntimeError(f"Unsafe or non-regular sample path: {path}; lease remains held")
        # On the normal path, consume ALL results, then join ALL processes. Video
        # decoders must likewise close their underlying files before completion.
        with ProcessPoolExecutor(max_workers=args.workers, mp_context=multiprocessing.get_context("spawn")) as pool:
            results = list(pool.map(read_file, map(str, files)))
        print(f"{window.window_id}: read {len(results)} files, {sum(size for size, _ in results)} bytes", flush=True)
        # No finally-release: an error or cancellation must keep the generation
        # protected until the application/operator recovery procedure verifies it.
        client.release(window)


if __name__ == "__main__":
    main()
