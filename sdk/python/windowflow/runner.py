# SPDX-License-Identifier: Apache-2.0
"""Training-side window lifecycle, independent of PyTorch and model topology."""

from __future__ import annotations

from dataclasses import dataclass
from functools import wraps
import threading
from typing import Any, Callable, Iterable, Iterator

from .client import PlanFailedError, WindowClient, WindowFlowError, WindowHandle, WindowValidationError, _index


@dataclass(frozen=True)
class WindowContext:
    handle: WindowHandle
    epoch: int  # Zero-based epoch within this window, not a full-dataset epoch.


@dataclass(frozen=True)
class WindowEpoch:
    """One finite epoch and its storage-I/O completion contract.

    ``drain`` must return only after ALL workers, decoders and async reads covered
    by this reader have stopped using the window. It is called after exhaustion.
    ``abort`` is optional best-effort resource cleanup; it NEVER permits release.
    Batches must own their data, not contain lazy readers or file-backed views.
    """

    batches: Iterable[Any]
    drain: Callable[[], None]
    abort: Callable[[], None] | None = None


_EMPTY = object()


def _guarded(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        if not self._guard.acquire(blocking=False):
            raise WindowFlowError("Runner operations cannot be concurrent or reentrant")
        try:
            identity = threading.get_ident()
            if self._owner_thread is not None and self._owner_thread != identity:
                raise WindowFlowError("A runner belongs to one reader thread")
            if self._in_callback:
                raise WindowFlowError("Training callbacks cannot advance, finish or close their runner")
            self._owner_thread = identity
            return method(self, *args, **kwargs)
        finally:
            self._guard.release()
    return call


class WindowRunner(Iterator[Any]):
    """Yield continuous batches from finite epochs over an immutable plan.

    Factories run in the reader's main process, once per window-local epoch.
    There are no distributed collectives and no per-batch Kubernetes requests.
    Prefer ``run(train_batch)`` for callback-based trainers. External iterators
    must normally exhaust the runner, or explicitly ``finish()`` after their
    final planned batch. ``close()``/exception/early break never release data.

    This version supports full epochs only. It does not implement a global
    sample budget, sampler state, or checkpoint progress; yielded != trained.
    """

    def __init__(
        self,
        client: WindowClient,
        epoch_factory: Callable[[WindowContext], WindowEpoch],
        *,
        epochs_per_window: int = 1,
        start_window: int = 0,
    ) -> None:
        if type(epochs_per_window) is not int or epochs_per_window < 1:
            raise WindowValidationError("epochs_per_window must be a positive integer")
        if not callable(epoch_factory):
            raise WindowValidationError("epoch_factory must be callable")
        self.client = client
        self.epoch_factory = epoch_factory
        self.epochs_per_window = epochs_per_window
        self.start_window = _index(start_window, "start window")
        self._window_index = self.start_window
        self._window_count: int | None = None
        self._epoch_index = 0
        self._handle: WindowHandle | None = None
        self._context: WindowContext | None = None
        self._epoch: WindowEpoch | None = None
        self._iterator: Iterator[Any] | None = None
        self._buffered: Any = _EMPTY
        self._epoch_batches = 0
        self._state = "new"
        self._guard = threading.Lock()
        self._owner_thread: int | None = None
        self._in_callback = False
        self._running = False

    @property
    def state(self) -> str:
        """new, active, completed, stopped or failed; not checkpoint progress."""
        return self._state

    @property
    def context(self) -> WindowContext | None:
        return self._context

    def __iter__(self) -> WindowRunner:
        return self

    def _start(self) -> None:
        status = self.client.get_status()
        if status.phase == "Failed":
            raise PlanFailedError("Cannot run a failed plan")
        if not status.start_window <= self.start_window <= status.window_count:
            raise WindowValidationError("start_window is outside this attempt's schedule")
        self._window_count = status.window_count
        self._state = "active"

    def _open_epoch(self) -> None:
        if self._handle is None:
            self._handle = self.client.acquire(self._window_index)
        self._context = WindowContext(self._handle, self._epoch_index)
        try:
            epoch = self.epoch_factory(self._context)
        except StopIteration as exc:
            raise WindowFlowError("epoch_factory raised StopIteration; lease remains held") from exc
        if not isinstance(epoch, WindowEpoch) or not callable(epoch.drain):
            raise WindowValidationError("epoch_factory must return WindowEpoch with a drain callback")
        if epoch.abort is not None and not callable(epoch.abort):
            raise WindowValidationError("WindowEpoch.abort must be callable or None")
        self._epoch = epoch
        self._iterator = iter(epoch.batches)
        self._epoch_batches = 0

    def _complete_epoch(self) -> None:
        if self._epoch_batches == 0:
            raise WindowValidationError("Empty training epoch; lease remains held")
        assert self._epoch is not None and self._handle is not None
        # A failure at any point retains the lease, including drain or API errors.
        self._epoch.drain()
        self._epoch = None
        self._iterator = None
        self._epoch_index += 1
        if self._epoch_index == self.epochs_per_window:
            self.client.release(self._handle)
            self._handle = None
            self._context = None
            self._window_index += 1
            self._epoch_index = 0

    @_guarded
    def __next__(self) -> Any:
        if self._state == "completed":
            raise StopIteration
        if self._state in ("stopped", "failed"):
            raise WindowFlowError(f"Runner is {self._state}; held leases require explicit recovery")
        try:
            if self._state == "new":
                self._start()
            while self._window_index < self._window_count:
                if self._iterator is None:
                    self._open_epoch()
                if self._buffered is not _EMPTY:
                    batch, self._buffered = self._buffered, _EMPTY
                else:
                    try:
                        batch = next(self._iterator)
                    except StopIteration:
                        self._complete_epoch()
                        continue
                self._epoch_batches += 1
                return batch
            self._state = "completed"
        except StopIteration as exc:
            self._state = "failed"
            raise WindowFlowError("Unexpected StopIteration in lifecycle callback; lease remains held") from exc
        except BaseException:
            self._state = "failed"
            raise
        raise StopIteration

    @_guarded
    def finish(self) -> None:
        """Confirm normal completion after a fixed-step trainer's final batch.

        Only valid in the final epoch of the final window. Check for exhaustion
        without silently discarding an untrained batch. If another batch exists,
        retain it for the next ``next()`` and keep the lease held. The caller is
        responsible for confirming previously yielded batches finished training.
        """
        if self._state == "completed":
            return
        if (self._state != "active" or self._window_index != self._window_count - 1
                or self._epoch_index != self.epochs_per_window - 1 or self._iterator is None):
            raise WindowFlowError("finish() requires the final window's final epoch; no lease released")
        if self._buffered is not _EMPTY:
            raise WindowFlowError("Unconsumed batch remains; continue iteration or close without release")
        try:
            try:
                self._buffered = next(self._iterator)
            except StopIteration:
                self._complete_epoch()
                self._state = "completed"
                return
        except StopIteration as exc:
            self._state = "failed"
            raise WindowFlowError("Unexpected StopIteration in lifecycle callback; lease remains held") from exc
        except BaseException:
            self._state = "failed"
            raise
        raise WindowFlowError("Unconsumed batch remains; continue iteration or close without release")

    @_guarded
    def close(self) -> None:
        """Stop consumption and attempt cleanup, always retaining the active lease.

        Cleanup is not distributed fencing. Even successful cleanup does not
        prove other processes or escaped lazy batches have stopped reading.
        """
        if self._state in ("completed", "stopped"):
            return
        if self._state != "failed":
            self._state = "stopped"
        epoch, self._epoch = self._epoch, None
        self._iterator = None
        self._buffered = _EMPTY
        if epoch is not None and epoch.abort is not None:
            epoch.abort()

    def run(self, train_batch: Callable[[Any, WindowContext], None]) -> None:
        """Run a synchronous training callback and normally exhaust all windows."""
        if not callable(train_batch):
            raise WindowValidationError("train_batch must be callable")
        if self._running or self._guard.locked() or self._in_callback:
            raise WindowFlowError("run() cannot be concurrent or reentrant")
        if self._owner_thread is not None and self._owner_thread != threading.get_ident():
            raise WindowFlowError("A runner belongs to one reader thread")
        self._owner_thread = threading.get_ident()
        self._running = True
        try:
            for batch in self:
                self._in_callback = True
                try:
                    train_batch(batch, self._context)
                finally:
                    self._in_callback = False
        except BaseException as exc:
            self._state = "failed"
            try:
                self.close()
            except BaseException as cleanup:
                raise WindowFlowError(
                    f"Training/iteration failed and abort cleanup also failed ({type(cleanup).__name__}); "
                    "lease remains held"
                ) from exc
            if isinstance(exc, StopIteration):
                raise WindowFlowError("Training callback raised StopIteration; lease remains held") from exc
            raise
        finally:
            self._running = False
