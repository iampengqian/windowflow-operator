# SPDX-License-Identifier: Apache-2.0
"""Read-only plan observations and explicitly released Kubernetes window leases.

Kubernetes is an optional dependency so the state machine can be tested with a
dependency-injected CustomObjectsApi. The observer performs no filesystem I/O;
the lease client checks mounted paths but never copies data or runs distributed
collectives. Instances are intended for one process; do not share a lease client
with DataLoader workers.
"""

from __future__ import annotations

import hashlib
import math
import re
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterator

GROUP = "data.windowflow.io"
VERSION = "v1alpha1"
PLANS = "windowplans"
LEASES = "windowleases"
_TOKEN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")


class WindowFlowError(RuntimeError):
    """Base SDK error. None of these errors imply that a lease was released."""


class WindowValidationError(WindowFlowError, ValueError):
    """Invalid local argument or malformed Kubernetes object."""


class WindowTimeoutError(WindowFlowError, TimeoutError):
    """The bounded wait ended; the reader must still treat its lease as held."""


class PlanReplacedError(WindowFlowError):
    """The pinned plan disappeared or its name now identifies a different UID."""


class PlanFailedError(WindowFlowError):
    """The operator froze the plan after an error."""


class LeaseConflictError(WindowFlowError):
    """A deterministic lease name identifies unexpected content."""


class LeaseReleasedError(WindowFlowError):
    """A released lease cannot be acquired again within the same attempt."""


@dataclass(frozen=True)
class WindowHandle:
    """An immutable acquired generation; ``root`` is an absolute pathlib.Path."""

    plan_uid: str
    window_index: int
    window_id: str
    generation: str
    root: Path


@dataclass(frozen=True)
class WindowSlotStatus:
    """A read-only slot observation, not a lease or permission to read files."""

    index: int
    window_index: int
    window_id: str
    generation: str
    phase: str
    relative_path: str
    reserved_bytes: int
    stage_job: str
    clean_job: str


@dataclass(frozen=True)
class WindowPlanStatus:
    """An immutable point-in-time observation; readiness is not a reservation.

    ``completed_windows`` counts reclaimed windows in this attempt, not model
    training progress. No per-reader state or transfer percentage is inferred.
    """

    plan_name: str
    namespace: str
    plan_uid: str
    phase: str
    message: str
    start_window: int
    window_count: int
    slot_count: int
    next_window: int
    completed_windows: int
    observed_generation: int
    slots: tuple[WindowSlotStatus, ...]


def _index(value: Any, name: str) -> int:
    # bool is an int subclass, but is not a valid API index.
    if type(value) is not int or value < 0:
        raise WindowValidationError(f"{name} must be a nonnegative integer")
    return value


def _token(value: Any, name: str) -> str:
    if not isinstance(value, str) or not _TOKEN.fullmatch(value):
        raise WindowValidationError(f"{name} must be a nonempty identifier without path separators")
    return value


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise WindowValidationError(f"{name} must be an object")
    return value


def _string(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise WindowValidationError(f"{name} must be a string")
    return value


def _relative_path(uid: str, generation: str, relative: Any) -> str:
    """Validate API path identity without touching a local filesystem."""
    if not isinstance(relative, str) or "\\" in relative or "\x00" in relative:
        raise WindowValidationError("slot relativePath must be a safe POSIX relative path")
    parts = relative.split("/")
    if PurePosixPath(relative).is_absolute() or any(part in ("", ".", "..") for part in parts):
        raise WindowValidationError("slot relativePath contains an absolute path or traversal")
    if relative != f"windowflow/{uid}/{generation}":
        raise WindowValidationError("slot relativePath does not match the plan UID and generation")
    return relative


def lease_name(plan_uid: str, reader_id: str, window_index: int, generation: str) -> str:
    """Return the exact v0.1 lease name shared with the controller."""
    _token(plan_uid, "plan UID")
    _token(reader_id, "reader ID")
    _index(window_index, "window index")
    _token(generation, "generation")
    identity = f"{plan_uid}\n{reader_id}\n{window_index}\n{generation}"
    return "wl-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:40]


class WindowObserver:
    """Observe an immutable plan using only GET permission on WindowPlans.

    No reader identity, cache mount, storage I/O, or lease API permission is
    required. A Ready observation never reserves a window: training readers must
    independently acquire their leases through ``WindowClient.acquire``.

    ``api`` may be a kubernetes.client.CustomObjectsApi or an object exposing the
    corresponding GET method. When omitted, in-cluster credentials
    are loaded, with kubeconfig fallback only for configuration unavailability.
    API authorization/validation errors are propagated immediately, not retried.

    ``timeout`` bounds polling, excluding an individual API
    call's transport latency. Kubernetes transport timeouts are also configured
    on each request. Timeout, process death, and iterator abandonment never
    automatically release a window.
    """

    def __init__(
        self,
        plan_name: str,
        namespace: str,
        timeout: float = 3600,
        poll_interval: float = 1,
        api: Any = None,
    ) -> None:
        self.plan_name = _token(plan_name, "plan name")
        self.namespace = _token(namespace, "namespace")
        for name, value in (("timeout", timeout), ("poll_interval", poll_interval)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise WindowValidationError(f"{name} must be a positive finite number")
        self.timeout = float(timeout)
        self.poll_interval = float(poll_interval)
        self._plan_uid: str | None = None
        if api is None:
            try:
                from kubernetes import client, config
                from kubernetes.config.config_exception import ConfigException
            except ImportError as exc:
                raise ImportError("Install windowflow[kubernetes] or supply api=CustomObjectsApi") from exc
            try:
                config.load_incluster_config()
            except ConfigException:
                config.load_kube_config()
            api = client.CustomObjectsApi()
        self.api = api

    def _request_options(self, deadline: float) -> dict[str, Any]:
        remaining = max(0.001, deadline - time.monotonic())
        return {"_request_timeout": min(30.0, remaining)}

    def _get(self, plural: str, name: str, deadline: float) -> dict[str, Any]:
        return self.api.get_namespaced_custom_object(
            group=GROUP, version=VERSION, namespace=self.namespace,
            plural=plural, name=name, **self._request_options(deadline),
        )

    def _pause(self, deadline: float, operation: str) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise WindowTimeoutError(f"Timed out {operation}; no lease was automatically released")
        time.sleep(min(self.poll_interval, remaining))

    def _read_plan(self, deadline: float) -> dict[str, Any]:
        """Shared object validation; Failed plans remain available to observers."""
        try:
            plan = _mapping(self._get(PLANS, self.plan_name, deadline), "WindowPlan")
        except Exception as exc:
            if getattr(exc, "status", None) == 404 and self._plan_uid is not None:
                raise PlanReplacedError("The pinned WindowPlan no longer exists") from exc
            raise
        metadata = _mapping(plan.get("metadata"), "WindowPlan metadata")
        uid = _token(metadata.get("uid"), "plan UID")
        if metadata.get("name") != self.plan_name:
            raise WindowValidationError("WindowPlan metadata name does not match the request")
        if metadata.get("namespace", self.namespace) != self.namespace:
            raise WindowValidationError("WindowPlan namespace does not match the request")
        if self._plan_uid is None:
            self._plan_uid = uid
        elif self._plan_uid != uid:
            raise PlanReplacedError("The WindowPlan name now identifies a different UID; start a new client for a new attempt")
        if metadata.get("deletionTimestamp") is not None:
            raise PlanReplacedError("The pinned WindowPlan is being deleted")
        spec = _mapping(plan.get("spec"), "WindowPlan spec")
        readers = spec.get("readers")
        if not isinstance(readers, list) or not readers:
            raise WindowValidationError("WindowPlan readers must be a nonempty list")
        for reader in readers:
            _token(reader, "plan reader ID")
        if len(set(readers)) != len(readers):
            raise WindowValidationError("WindowPlan readers must be unique")
        windows = spec.get("windows")
        if not isinstance(windows, list) or not windows:
            raise WindowValidationError("WindowPlan windows must be a nonempty list")
        ids = [_token(_mapping(window, "window").get("id"), "window ID") for window in windows]
        if len(set(ids)) != len(ids):
            raise WindowValidationError("WindowPlan window IDs must be unique")
        start = _index(spec.get("startWindow", 0), "start window")
        if start >= len(windows):
            raise WindowValidationError("WindowPlan startWindow is outside the plan")
        slots_count = _index(spec.get("slots"), "slot count")
        if not 2 <= slots_count <= 64:
            raise WindowValidationError("WindowPlan needs 2..64 slots")
        status = _mapping(plan.get("status", {}), "WindowPlan status")
        phase = status.get("phase", "Pending")
        if phase not in ("Pending", "Running", "Completed", "Failed"):
            raise WindowValidationError("Unknown WindowPlan phase")
        _string(status.get("message", ""), "WindowPlan status message")
        next_window = _index(status.get("nextWindow", 0), "next window")
        if next_window > len(windows):
            raise WindowValidationError("WindowPlan nextWindow is outside the plan")
        completed = _index(status.get("completedWindows", 0), "completed windows")
        if completed > len(windows) - start:
            raise WindowValidationError("WindowPlan completedWindows exceeds this attempt's window count")
        _index(status.get("observedGeneration", 0), "observed generation")
        slots = status.get("slots", [])
        if not isinstance(slots, list):
            raise WindowValidationError("WindowPlan status slots must be a list")
        seen_slots: set[int] = set()
        seen_windows: set[int] = set()
        for slot in slots:
            slot = _mapping(slot, "slot")
            slot_index = _index(slot.get("index"), "slot index")
            index = _index(slot.get("windowIndex"), "slot window index")
            if slot_index >= slots_count or slot_index in seen_slots:
                raise WindowValidationError("Invalid or duplicate slot index")
            if index < start or index >= len(windows) or index in seen_windows:
                raise WindowValidationError("Invalid or duplicate slot window index")
            seen_slots.add(slot_index)
            seen_windows.add(index)
            if slot.get("windowId") != ids[index]:
                raise WindowValidationError("Slot window ID does not match the immutable plan")
            if slot.get("generation") != f"w{index:06d}":
                raise WindowValidationError("Slot generation does not match the window ordinal")
            if slot.get("phase") not in ("Loading", "Ready", "Reclaiming", "Failed"):
                raise WindowValidationError("Unknown slot phase")
            _relative_path(uid, slot["generation"], slot.get("relativePath"))
            _index(slot.get("reservedBytes", 0), "slot reserved bytes")
            for field in ("stageJob", "cleanJob"):
                value = _string(slot.get(field, ""), f"slot {field}")
                if value:
                    _token(value, f"slot {field}")
        return plan

    def _status_snapshot(self, plan: dict[str, Any]) -> WindowPlanStatus:
        metadata, spec, status = plan["metadata"], plan["spec"], plan.get("status", {})
        return WindowPlanStatus(
            plan_name=metadata["name"], namespace=metadata.get("namespace", self.namespace),
            plan_uid=metadata["uid"], phase=status.get("phase", "Pending"),
            message=status.get("message", ""), start_window=spec.get("startWindow", 0),
            window_count=len(spec["windows"]), slot_count=spec["slots"],
            next_window=status.get("nextWindow", 0), completed_windows=status.get("completedWindows", 0),
            observed_generation=status.get("observedGeneration", 0),
            slots=tuple(WindowSlotStatus(
                index=slot["index"], window_index=slot["windowIndex"], window_id=slot["windowId"],
                generation=slot["generation"], phase=slot["phase"], relative_path=slot["relativePath"],
                reserved_bytes=slot.get("reservedBytes", 0), stage_job=slot.get("stageJob", ""),
                clean_job=slot.get("cleanJob", ""),
            ) for slot in status.get("slots", [])),
        )

    def get_status(self) -> WindowPlanStatus:
        """Return one validated, immutable snapshot, including a Failed plan.

        This performs exactly one WindowPlan GET. Malformed, deleting, or
        replaced plans still raise; it does not list readers' leases or retry
        API authorization errors. Missing status means Pending with no slots.
        """
        return self._status_snapshot(self._read_plan(time.monotonic() + self.timeout))

    def wait_ready(self, window_index: int = 0) -> WindowPlanStatus:
        """Wait for a window's Ready slot and return a read-only plan snapshot.

        This creates no lease and returns no acquired handle or local path.
        Readiness can change immediately after this call. A failed, completed,
        reclaiming, skipped, or already reclaimed window is never waited on
        indefinitely. This wait never advances training or releases data.
        """
        index = _index(window_index, "window index")
        deadline = time.monotonic() + self.timeout
        while True:
            if time.monotonic() >= deadline:
                raise WindowTimeoutError(f"Timed out waiting for window {index} readiness; no lease was created or released")
            plan = self._read_plan(deadline)
            snapshot = self._status_snapshot(plan)
            if index < snapshot.start_window:
                raise WindowValidationError("window index precedes this attempt's startWindow")
            if index >= snapshot.window_count:
                raise WindowValidationError("window index is outside the plan")
            if snapshot.phase == "Failed":
                raise PlanFailedError(f"The operator marked the WindowPlan Failed: {snapshot.message}")
            if snapshot.phase == "Completed":
                raise WindowFlowError("The plan has completed; windows are no longer available")
            slot = next((s for s in snapshot.slots if s.window_index == index), None)
            if slot is not None:
                if slot.phase == "Ready":
                    return snapshot
                if slot.phase == "Failed":
                    raise PlanFailedError(f"Window {index} is Failed; inspect the plan and worker Job")
                if slot.phase == "Reclaiming":
                    raise WindowFlowError(f"Window {index} is Reclaiming; it cannot become Ready again")
            elif index < snapshot.next_window:
                raise WindowFlowError(f"Window {index} has already left the cache; it cannot become Ready again")
            self._pause(deadline, f"waiting for window {index} readiness")


class WindowClient(WindowObserver):
    """Acquire and explicitly release windows for one reader and mounted cache.

    Inherits the lease-free ``get_status`` and ``wait_ready`` observer methods.
    Only ``acquire`` returns a usable WindowHandle and records a reader lease.
    Timeout, process death, and iterator abandonment never release a window.
    """

    def __init__(
        self,
        plan_name: str,
        namespace: str,
        reader_id: str,
        mount_path: str | Path,
        timeout: float = 3600,
        poll_interval: float = 1,
        api: Any = None,
    ) -> None:
        self.reader_id = _token(reader_id, "reader ID")
        self.mount_path = Path(mount_path).expanduser().resolve()
        self._released: set[tuple[str, int, str]] = set()
        self._acquired: dict[tuple[str, int, str], WindowHandle] = {}
        super().__init__(plan_name, namespace, timeout, poll_interval, api)

    def _root(self, uid: str, generation: str, relative: Any) -> Path:
        relative = _relative_path(uid, generation, relative)
        candidate = self.mount_path
        for part in relative.split("/"):
            candidate /= part
            if candidate.is_symlink():
                raise WindowValidationError("slot path contains a symlink")
        resolved = candidate.resolve()
        if not resolved.is_relative_to(self.mount_path) or resolved == self.mount_path:
            raise WindowValidationError("slot path escapes the mounted cache")
        return resolved

    def _plan(self, deadline: float) -> dict[str, Any]:
        plan = self._read_plan(deadline)
        if self.reader_id not in plan["spec"]["readers"]:
            raise WindowValidationError(f"Reader {self.reader_id!r} is not a member of this plan")
        status = plan.get("status", {})
        if status.get("phase") == "Failed":
            raise PlanFailedError("The operator marked the WindowPlan Failed; inspect its status before recovery")
        for slot in status.get("slots", []):
            self._root(plan["metadata"]["uid"], slot["generation"], slot["relativePath"])
        return plan

    def _lease_spec(self, handle: WindowHandle) -> dict[str, Any]:
        return {
            "planName": self.plan_name,
            "planUID": handle.plan_uid,
            "readerID": self.reader_id,
            "windowIndex": handle.window_index,
            "generation": handle.generation,
            "released": False,
        }

    def _validate_lease(self, lease: Any, handle: WindowHandle) -> dict[str, Any]:
        lease = _mapping(lease, "WindowLease")
        metadata = _mapping(lease.get("metadata"), "WindowLease metadata")
        expected_name = lease_name(handle.plan_uid, self.reader_id, handle.window_index, handle.generation)
        if metadata.get("name") != expected_name or metadata.get("namespace", self.namespace) != self.namespace:
            raise LeaseConflictError("WindowLease metadata does not match this reader")
        actual = _mapping(lease.get("spec"), "WindowLease spec")
        for key, expected in self._lease_spec(handle).items():
            if key != "released" and (actual.get(key) != expected or type(actual.get(key)) is not type(expected)):
                raise LeaseConflictError(f"WindowLease {key} does not match the acquired tuple")
        if type(actual.get("released")) is not bool:
            raise LeaseConflictError("WindowLease released must be boolean")
        return lease

    def acquire(self, window_index: int) -> WindowHandle:
        """Wait for READY, create/verify this reader's lease, and return its root.

        Existing unreleased leases may be reattached after a reader restart.
        Existing released leases are irreversible and require a new plan attempt.
        """
        index = _index(window_index, "window index")
        deadline = time.monotonic() + self.timeout
        while True:
            plan = self._plan(deadline)
            if index < _index(plan["spec"].get("startWindow", 0), "start window"):
                raise WindowValidationError("window index precedes this attempt's startWindow")
            if index >= len(plan["spec"]["windows"]):
                raise WindowValidationError("window index is outside the plan")
            uid = plan["metadata"]["uid"]
            generation = f"w{index:06d}"
            key = (uid, index, generation)
            if key in self._released:
                raise LeaseReleasedError("This reader has already released this generation")
            status = plan.get("status", {})
            slot = next((s for s in status.get("slots", []) if s["windowIndex"] == index), None)
            if slot is None or slot["phase"] != "Ready":
                # A released lease may have outlived a reclaimed slot (or this
                # client may be a restarted process), so check before waiting.
                probe = WindowHandle(uid, index, plan["spec"]["windows"][index]["id"], generation,
                                     self._root(uid, generation, f"windowflow/{uid}/{generation}"))
                try:
                    existing = self._validate_lease(self._get(LEASES, lease_name(uid, self.reader_id, index, generation), deadline), probe)
                except Exception as exc:
                    if getattr(exc, "status", None) != 404:
                        raise
                else:
                    if existing["spec"]["released"]:
                        self._released.add(key)
                        raise LeaseReleasedError("This reader's lease was already released")
                if status.get("phase") == "Completed":
                    raise WindowFlowError("The plan has completed; windows cannot be acquired again")
                if slot is not None and slot["phase"] in ("Failed", "Reclaiming"):
                    raise WindowFlowError(f"Window is {slot['phase']}; it cannot be acquired")
                self._pause(deadline, f"waiting for window {index}")
                continue
            handle = WindowHandle(uid, index, slot["windowId"], generation,
                                  self._root(uid, generation, slot["relativePath"]))
            name = lease_name(uid, self.reader_id, index, generation)
            body = {
                "apiVersion": f"{GROUP}/{VERSION}", "kind": "WindowLease",
                "metadata": {
                    "name": name, "namespace": self.namespace,
                    "ownerReferences": [{"apiVersion": f"{GROUP}/{VERSION}", "kind": "WindowPlan",
                                         "name": self.plan_name, "uid": uid}],
                },
                "spec": self._lease_spec(handle),
            }
            try:
                lease = self.api.create_namespaced_custom_object(
                    group=GROUP, version=VERSION, namespace=self.namespace,
                    plural=LEASES, body=body, **self._request_options(deadline),
                )
            except Exception as exc:
                if getattr(exc, "status", None) != 409:
                    raise
                lease = self._get(LEASES, name, deadline)
            lease = self._validate_lease(lease, handle)
            if lease["spec"]["released"]:
                self._released.add(key)
                raise LeaseReleasedError("This reader's lease was already released; it cannot be acquired again")
            # Catch replacement/deletion during create. A failure here retains
            # the old lease: uncertainty is never permission to reclaim data.
            current = self._plan(deadline)
            current_slot = next((s for s in current.get("status", {}).get("slots", []) if s["windowIndex"] == index), None)
            if current_slot is None or current_slot["phase"] != "Ready" or current_slot["generation"] != generation:
                raise WindowFlowError("The acquired generation stopped being Ready; lease remains held")
            self._acquired[key] = handle
            return handle

    def release(self, handle: WindowHandle) -> None:
        """Mark the lease released after ALL this reader's storage I/O has ended.

        Idempotent for the same acquired handle. Do not put this call in an
        unconditional ``finally`` block: outstanding workers/decoders might still
        be reading even after the training loop raises or is cancelled.
        """
        if not isinstance(handle, WindowHandle):
            raise WindowValidationError("release requires an acquired WindowHandle")
        key = (handle.plan_uid, handle.window_index, handle.generation)
        if self._acquired.get(key) != handle:
            raise WindowValidationError("This handle was not acquired by this client; acquire the unreleased lease first")
        deadline = time.monotonic() + self.timeout
        # Always check identity, even on an idempotent repeat.
        self._plan(deadline)
        if handle.plan_uid != self._plan_uid:
            raise PlanReplacedError("Handle belongs to a different plan UID")
        expected_root = self._root(handle.plan_uid, handle.generation,
                                   f"windowflow/{handle.plan_uid}/{handle.generation}")
        if handle.root != expected_root:
            raise WindowValidationError("Handle root does not match the owned generation")
        name = lease_name(handle.plan_uid, self.reader_id, handle.window_index, handle.generation)
        while True:
            lease = self._validate_lease(self._get(LEASES, name, deadline), handle)
            if lease["spec"]["released"]:
                self._released.add(key)
                return
            rv = lease["metadata"].get("resourceVersion")
            if not isinstance(rv, str) or not rv:
                raise LeaseConflictError("WindowLease resourceVersion is missing")
            try:
                updated = self.api.patch_namespaced_custom_object(
                    group=GROUP, version=VERSION, namespace=self.namespace,
                    plural=LEASES, name=name,
                    body={"metadata": {"resourceVersion": rv}, "spec": {"released": True}},
                    **self._request_options(deadline),
                )
            except Exception as exc:
                if getattr(exc, "status", None) != 409:
                    raise
                self._pause(deadline, "updating a conflicting lease")
                self._plan(deadline)
                continue
            updated = self._validate_lease(updated, handle)
            if not updated["spec"]["released"]:
                raise LeaseConflictError("The API did not acknowledge the lease release")
            self._released.add(key)
            return

    def iter_windows(self, start_index: int = 0) -> Iterator[WindowHandle]:
        """Acquire windows in order; the caller must explicitly release each one.

        ``start_index`` must come from the application's checkpoint, never from
        the operator's cache status. Abandoning this generator keeps leases held.
        """
        start = _index(start_index, "start index")
        plan = self._plan(time.monotonic() + self.timeout)
        count = len(plan["spec"]["windows"])
        if start > count:
            raise WindowValidationError("start index is outside the plan")
        for index in range(start, count):
            yield self.acquire(index)
