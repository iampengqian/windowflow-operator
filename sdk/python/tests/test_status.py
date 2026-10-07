# SPDX-License-Identifier: Apache-2.0
"""Read-only plan observation; the API fake exposes no lease or write methods."""

from __future__ import annotations

import copy
import dataclasses
import unittest
from pathlib import Path
from unittest.mock import patch

from windowflow.client import (
    PlanFailedError,
    PlanReplacedError,
    WindowClient,
    WindowFlowError,
    WindowHandle,
    WindowObserver,
    WindowPlanStatus,
    WindowSlotStatus,
    WindowTimeoutError,
    WindowValidationError,
)


class ApiError(Exception):
    def __init__(self, status):
        self.status = status
        super().__init__(f"HTTP {status}")


def plan(*, uid="plan-uid", slot_phase="Loading", window_index=0):
    generation = f"w{window_index:06d}"
    return {
        "metadata": {"name": "training", "namespace": "default", "uid": uid, "generation": 1},
        "spec": {
            "slots": 2, "readers": ["dp0", "dp1"],
            "windows": [{"id": f"window-{i}"} for i in range(3)],
        },
        "status": {
            "phase": "Running", "message": "preparing data", "observedGeneration": 1,
            "nextWindow": window_index + 1, "completedWindows": 0,
            "slots": [{
                "index": window_index % 2, "windowIndex": window_index,
                "windowId": f"window-{window_index}", "generation": generation,
                "phase": slot_phase, "relativePath": f"windowflow/{uid}/{generation}",
                "reservedBytes": 500, "stageJob": "stage-job", "cleanJob": "",
            }],
        },
    }


class ReadOnlyApi:
    """Return the same dict deliberately, to catch mutable snapshot leakage."""

    def __init__(self, resource=None):
        self.plan = plan() if resource is None else resource
        self.calls = []
        self.failure = None

    def get_namespaced_custom_object(self, **kwargs):
        self.calls.append(kwargs)
        if kwargs["plural"] != "windowplans":
            raise AssertionError("Observers must only read WindowPlans")
        if self.failure is not None:
            raise ApiError(self.failure)
        if self.plan is None:
            raise ApiError(404)
        return self.plan


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class WindowObserverTests(unittest.TestCase):
    def setUp(self):
        self.api = ReadOnlyApi()
        self.observer = self.make_observer()

    def make_observer(self, **kwargs):
        return WindowObserver("training", "default", api=self.api,
                              timeout=kwargs.pop("timeout", 0.1),
                              poll_interval=kwargs.pop("poll_interval", 0.01), **kwargs)

    def test_get_status_requires_neither_reader_nor_mount_and_only_gets_plan(self):
        with patch.object(Path, "resolve", side_effect=AssertionError("No filesystem access")), \
             patch.object(Path, "is_symlink", side_effect=AssertionError("No filesystem access")):
            observer = self.make_observer()
            snapshot = observer.get_status()
        self.assertFalse(hasattr(observer, "reader_id"))
        self.assertFalse(hasattr(observer, "mount_path"))
        self.assertFalse(hasattr(observer, "acquire"))
        self.assertFalse(hasattr(observer, "release"))
        self.assertIsInstance(snapshot, WindowPlanStatus)
        self.assertEqual((snapshot.plan_uid, snapshot.phase, snapshot.message),
                         ("plan-uid", "Running", "preparing data"))
        self.assertEqual((snapshot.start_window, snapshot.window_count, snapshot.slot_count), (0, 3, 2))
        self.assertEqual((snapshot.next_window, snapshot.completed_windows, snapshot.observed_generation), (1, 0, 1))
        self.assertIsInstance(snapshot.slots, tuple)
        self.assertIsInstance(snapshot.slots[0], WindowSlotStatus)
        self.assertEqual(snapshot.slots[0].reserved_bytes, 500)
        self.assertEqual(snapshot.slots[0].stage_job, "stage-job")
        self.assertEqual(len(self.api.calls), 1)
        call = self.api.calls[0]
        self.assertEqual((call["group"], call["version"], call["namespace"], call["name"]),
                         ("data.windowflow.io", "v1alpha1", "default", "training"))
        self.assertGreater(call["_request_timeout"], 0)

    def test_snapshots_are_immutable_and_independent_of_api_and_later_reads(self):
        first = self.observer.get_status()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            first.message = "changed"
        with self.assertRaises(dataclasses.FrozenInstanceError):
            first.slots[0].phase = "Ready"
        self.api.plan["status"]["message"] = "loaded"
        self.api.plan["status"]["slots"][0]["phase"] = "Ready"
        second = self.observer.get_status()
        self.assertEqual((first.message, first.slots[0].phase), ("preparing data", "Loading"))
        self.assertEqual((second.message, second.slots[0].phase), ("loaded", "Ready"))
        self.api.plan["status"]["slots"].clear()
        self.assertEqual(len(first.slots), 1)
        self.assertEqual(len(second.slots), 1)

    def test_absent_status_is_pending_and_missing_namespace_uses_request(self):
        del self.api.plan["status"]
        del self.api.plan["metadata"]["namespace"]
        status = self.observer.get_status()
        self.assertEqual(status.namespace, "default")
        self.assertEqual(status.phase, "Pending")
        self.assertEqual(status.slots, ())
        self.assertEqual(status.message, "")
        self.assertEqual(status.next_window, 0)

    def test_failed_plan_is_readable_but_wait_ready_raises_immediately(self):
        self.api.plan["status"]["phase"] = "Failed"
        self.api.plan["status"]["message"] = "Worker import failed"
        snapshot = self.observer.get_status()
        self.assertEqual(snapshot.phase, "Failed")
        self.assertEqual(snapshot.message, "Worker import failed")
        with patch("windowflow.client.time.sleep", side_effect=AssertionError("Do not wait on failure")):
            with self.assertRaisesRegex(PlanFailedError, "Worker import failed"):
                self.observer.wait_ready()
        self.assertEqual(len(self.api.calls), 2)

    def test_wait_ready_polls_only_plans_and_returns_snapshot_not_handle(self):
        def ready(_):
            self.assertEqual(len(self.api.calls), 1)
            self.api.plan["status"]["slots"][0]["phase"] = "Ready"

        with patch("windowflow.client.time.sleep", side_effect=ready):
            result = self.observer.wait_ready()
        self.assertIsInstance(result, WindowPlanStatus)
        self.assertNotIsInstance(result, WindowHandle)
        self.assertFalse(hasattr(result, "root"))
        self.assertEqual(result.slots[0].phase, "Ready")
        self.assertEqual(len(self.api.calls), 2)

    def test_waits_for_requested_window_not_another_ready_slot(self):
        self.api.plan["status"]["slots"][0]["phase"] = "Ready"

        def next_ready(_):
            self.api.plan = plan(slot_phase="Ready", window_index=1)

        with patch("windowflow.client.time.sleep", side_effect=next_ready):
            result = self.observer.wait_ready(1)
        self.assertEqual(result.slots[0].window_index, 1)
        self.assertEqual(len(self.api.calls), 2)

    def test_wait_timeout_is_bounded_and_does_not_poll_after_deadline(self):
        clock = Clock()
        with patch("windowflow.client.time.monotonic", side_effect=clock.monotonic), \
             patch("windowflow.client.time.sleep", side_effect=clock.sleep):
            with self.assertRaises(WindowTimeoutError):
                self.make_observer(timeout=0.025, poll_interval=0.01).wait_ready()
        self.assertAlmostEqual(clock.now, 0.025)
        self.assertEqual(len(self.api.calls), 3)

    def test_uid_is_shared_between_get_status_and_wait_ready(self):
        self.observer.get_status()
        self.api.plan = plan(uid="replacement", slot_phase="Ready")
        with self.assertRaises(PlanReplacedError):
            self.observer.wait_ready()
        with self.assertRaises(PlanReplacedError):
            self.observer.get_status()
        # Explicit construction is required for a genuinely new attempt.
        self.assertEqual(self.make_observer().wait_ready().plan_uid, "replacement")

    def test_replacement_during_poll_is_rejected(self):
        with patch("windowflow.client.time.sleep", side_effect=lambda _: setattr(self.api, "plan", plan(uid="new"))):
            with self.assertRaises(PlanReplacedError):
                self.observer.wait_ready()
        self.assertEqual(len(self.api.calls), 2)

    def test_deleted_or_deleting_pinned_plan_is_not_waited_on(self):
        self.observer.get_status()
        self.api.plan = None
        with self.assertRaises(PlanReplacedError):
            self.observer.wait_ready()
        self.api.plan = plan()
        self.api.plan["metadata"]["deletionTimestamp"] = "2026-10-06T00:00:00Z"
        with self.assertRaises(PlanReplacedError):
            self.observer.get_status()
        with self.assertRaises(PlanReplacedError):
            self.make_observer().wait_ready()

    def test_initial_not_found_and_rbac_errors_are_not_retried(self):
        for code in (401, 403, 404, 422):
            for operation in ("get_status", "wait_ready"):
                with self.subTest(code=code, operation=operation):
                    self.api.calls.clear()
                    self.api.failure = code
                    with self.assertRaises(ApiError) as error:
                        getattr(self.make_observer(), operation)()
                    self.assertEqual(error.exception.status, code)
                    self.assertEqual(len(self.api.calls), 1)

    def test_terminal_slots_and_completed_plans_are_not_waited_on(self):
        for phase, error in (("Failed", PlanFailedError), ("Reclaiming", WindowFlowError)):
            with self.subTest(phase=phase):
                self.api.plan = plan(slot_phase=phase)
                with patch("windowflow.client.time.sleep", side_effect=AssertionError("Do not poll")):
                    with self.assertRaises(error):
                        self.make_observer().wait_ready()
        for slots in ([], plan(slot_phase="Ready")["status"]["slots"]):
            self.api.plan = plan()
            self.api.plan["status"].update(phase="Completed", slots=slots)
            self.assertEqual(self.make_observer().get_status().phase, "Completed")
            with self.assertRaisesRegex(WindowFlowError, "completed"):
                self.make_observer().wait_ready()

    def test_reclaimed_window_does_not_wait_even_while_plan_runs(self):
        self.api.plan["status"].update(slots=[], nextWindow=1)
        with self.assertRaisesRegex(WindowFlowError, "left the cache"):
            self.observer.wait_ready(0)
        self.assertEqual(len(self.api.calls), 1)

    def test_skipped_and_out_of_range_indices_are_rejected(self):
        for index in (True, False, -1, "0", 0.0, 3):
            with self.subTest(index=index), self.assertRaises(WindowValidationError):
                self.make_observer().wait_ready(index)
        self.api.plan = plan(slot_phase="Ready", window_index=1)
        self.api.plan["spec"]["startWindow"] = 1
        with self.assertRaisesRegex(WindowValidationError, "precedes"):
            self.make_observer().wait_ready(0)
        snapshot = self.make_observer().wait_ready(1)
        self.assertEqual(snapshot.start_window, 1)
        self.assertEqual(snapshot.window_count, 3)

    def test_malformed_status_fields_fail_validation(self):
        corruptions = (
            ("phase", "Unknown"), ("message", {}), ("nextWindow", True),
            ("nextWindow", -1), ("nextWindow", 4), ("completedWindows", "1"),
            ("completedWindows", -1), ("completedWindows", 4),
            ("observedGeneration", False), ("slots", {}),
        )
        for field, value in corruptions:
            with self.subTest(field=field, value=value):
                self.api.plan = plan()
                self.api.plan["status"][field] = value
                with self.assertRaises(WindowValidationError):
                    self.make_observer().get_status()

    def test_malformed_slot_paths_and_values_are_rejected_without_filesystem_access(self):
        corruptions = (
            ("index", True), ("index", 2), ("windowIndex", -1), ("windowIndex", 3),
            ("windowId", "wrong"), ("generation", "w000001"), ("phase", "Unknown"),
            ("relativePath", "../../elsewhere"), ("relativePath", "windowflow/other/w000000"),
            ("relativePath", "/windowflow/plan-uid/w000000"),
            ("relativePath", "windowflow\\plan-uid\\w000000"),
            ("reservedBytes", -1), ("reservedBytes", True), ("stageJob", []),
            ("stageJob", "../bad"), ("cleanJob", None),
        )
        for field, value in corruptions:
            with self.subTest(field=field, value=value):
                self.api.plan = plan()
                self.api.plan["status"]["slots"][0][field] = value
                with self.assertRaises(WindowValidationError):
                    self.make_observer().get_status()
        self.api.plan = plan()
        self.api.plan["status"]["slots"].append(copy.deepcopy(self.api.plan["status"]["slots"][0]))
        with self.assertRaises(WindowValidationError):
            self.make_observer().get_status()

    def test_malformed_plan_identity_and_spec_are_rejected(self):
        mutations = (
            ("metadata", "uid", "../escape"), ("metadata", "name", "other"),
            ("metadata", "namespace", "other"), ("spec", "readers", []),
            ("spec", "readers", ["dp0", "dp0"]), ("spec", "readers", ["../bad"]),
            ("spec", "windows", []), ("spec", "windows", [{"id": "same"}, {"id": "same"}]),
            ("spec", "slots", True), ("spec", "slots", 1), ("spec", "slots", 65),
            ("spec", "startWindow", True), ("spec", "startWindow", -1), ("spec", "startWindow", 3),
        )
        for section, field, value in mutations:
            with self.subTest(section=section, field=field, value=value):
                self.api.plan = plan()
                self.api.plan[section][field] = value
                with self.assertRaises(WindowValidationError):
                    self.make_observer().get_status()

    def test_invalid_observer_parameters_fail_before_api_access(self):
        for kwargs in ({"timeout": 0}, {"timeout": float("inf")}, {"timeout": True},
                       {"poll_interval": 0}, {"poll_interval": float("nan")}):
            with self.subTest(kwargs=kwargs), self.assertRaises(WindowValidationError):
                self.make_observer(**kwargs)
        self.assertEqual(self.api.calls, [])

    def test_client_inherits_read_only_methods_without_acquiring_or_mount_checks(self):
        client = WindowClient("training", "default", "dp0", "/not-mounted", api=self.api)
        self.assertIsInstance(client, WindowObserver)
        self.api.plan["status"]["slots"][0]["phase"] = "Ready"
        with patch.object(client, "_root", side_effect=AssertionError("Status must not inspect mount")):
            snapshot = client.wait_ready()
            self.assertEqual(client.get_status().plan_uid, snapshot.plan_uid)
        with self.assertRaises(WindowValidationError):
            client.release(snapshot)
        self.assertEqual(client._acquired, {})
        self.assertEqual(client._released, set())
        self.assertEqual(len(self.api.calls), 2)
        self.api.plan["status"]["phase"] = "Failed"
        self.assertEqual(client.get_status().phase, "Failed")


if __name__ == "__main__":
    unittest.main()
