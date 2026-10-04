# SPDX-License-Identifier: Apache-2.0
"""The API fake checks optimistic writes; no cluster or kubernetes package needed."""

from __future__ import annotations

import copy
import dataclasses
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from windowflow import (
    LeaseConflictError,
    LeaseReleasedError,
    PlanFailedError,
    PlanReplacedError,
    WindowClient,
    WindowTimeoutError,
    WindowValidationError,
    lease_name,
)


class ApiError(Exception):
    def __init__(self, status):
        self.status = status
        super().__init__(f"HTTP {status}")


def plan(uid="plan-uid", phase="Ready", index=0):
    generation = f"w{index:06d}"
    return {
        "metadata": {"name": "training", "namespace": "default", "uid": uid, "generation": 1},
        "spec": {"slots": 2, "readers": ["dp0", "dp1"],
                 "windows": [{"id": f"window-{i}"} for i in range(3)]},
        "status": {"phase": "Running", "observedGeneration": 1, "slots": [{
            "index": index % 2, "windowIndex": index, "windowId": f"window-{index}",
            "generation": generation, "phase": phase,
            "relativePath": f"windowflow/{uid}/{generation}",
        }]},
    }


class FakeApi:
    def __init__(self, resource=None):
        self.plan = resource or plan()
        self.leases = {}
        self.creates = 0
        self.patches = 0
        self.gets = 0
        self.on_create = None
        self.on_patch = None
        self.fail_get = None
        self.fail_create = None
        self.fail_patch = None

    def get_namespaced_custom_object(self, *, plural, name, **kwargs):
        self.gets += 1
        if self.fail_get:
            raise ApiError(self.fail_get)
        if plural == "windowplans":
            if self.plan is None:
                raise ApiError(404)
            return copy.deepcopy(self.plan)
        if name not in self.leases:
            raise ApiError(404)
        return copy.deepcopy(self.leases[name])

    def create_namespaced_custom_object(self, *, body, **kwargs):
        self.creates += 1
        if self.fail_create:
            raise ApiError(self.fail_create)
        name = body["metadata"]["name"]
        if name in self.leases:
            raise ApiError(409)
        resource = copy.deepcopy(body)
        resource["metadata"]["resourceVersion"] = "1"
        self.leases[name] = resource
        if self.on_create:
            self.on_create(self)
        return copy.deepcopy(resource)

    def patch_namespaced_custom_object(self, *, name, body, **kwargs):
        self.patches += 1
        if self.fail_patch:
            raise ApiError(self.fail_patch)
        if self.on_patch:
            self.on_patch(self)
        lease = self.leases[name]
        if body["metadata"]["resourceVersion"] != lease["metadata"]["resourceVersion"]:
            raise ApiError(409)
        assert body["spec"] == {"released": True}
        lease["spec"]["released"] = True
        lease["metadata"]["resourceVersion"] = str(int(lease["metadata"]["resourceVersion"]) + 1)
        return copy.deepcopy(lease)


class WindowClientTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.api = FakeApi()
        self.client = self.make_client()

    def make_client(self, **kwargs):
        return WindowClient("training", "default", "dp0", self.root, api=self.api,
                            timeout=kwargs.pop("timeout", 0.05), poll_interval=0.001, **kwargs)

    def only_lease(self):
        self.assertEqual(len(self.api.leases), 1)
        return next(iter(self.api.leases.values()))

    def test_acquire_pins_exact_generation_and_release_is_idempotent(self):
        handle = self.client.acquire(0)
        self.assertEqual(handle.root, self.root / "windowflow/plan-uid/w000000")
        self.assertEqual(handle.window_id, "window-0")
        self.assertFalse(self.only_lease()["spec"]["released"])
        with self.assertRaises(dataclasses.FrozenInstanceError):
            handle.generation = "w000001"
        self.client.release(handle)
        self.client.release(handle)
        self.assertEqual(self.api.patches, 1)
        self.assertTrue(self.only_lease()["spec"]["released"])

    def test_lease_name_matches_language_independent_contract(self):
        expected = "wl-" + hashlib.sha256(b"plan-uid\ndp0\n0\nw000000").hexdigest()[:40]
        self.assertEqual(lease_name("plan-uid", "dp0", 0, "w000000"), expected)

    def test_unreleased_lease_can_be_reattached_after_reader_restart(self):
        original = self.client.acquire(0)
        restarted = self.make_client()
        recovered = restarted.acquire(0)
        self.assertEqual(original, recovered)
        restarted.release(recovered)
        self.assertTrue(self.only_lease()["spec"]["released"])

    def test_release_requires_this_clients_acquired_handle(self):
        handle = self.client.acquire(0)
        other = self.make_client()
        with self.assertRaises(WindowValidationError):
            other.release(handle)
        with self.assertRaises(WindowValidationError):
            self.client.release(dataclasses.replace(handle, root=self.root / "elsewhere"))
        self.assertEqual(self.api.patches, 0)

    def test_released_lease_cannot_be_reacquired_even_after_slot_reuse(self):
        handle = self.client.acquire(0)
        self.client.release(handle)
        self.api.plan = plan(index=2)  # Slot A now contains a different generation.
        with self.assertRaises(LeaseReleasedError):
            self.make_client().acquire(0)
        self.api.leases.clear()  # Local tombstone is also irreversible.
        with self.assertRaises(LeaseReleasedError):
            self.client.acquire(0)
        self.assertEqual(self.api.creates, 1)

    def test_conflicting_lease_must_match_entire_tuple(self):
        self.client.acquire(0)
        for field, value in (("planUID", "old-uid"), ("readerID", "dp1"),
                             ("windowIndex", True), ("generation", "w000001"),
                             ("planName", "other-plan")):
            with self.subTest(field=field):
                lease = self.only_lease()
                original = lease["spec"][field]
                lease["spec"][field] = value
                with self.assertRaises(LeaseConflictError):
                    self.make_client().acquire(0)
                lease["spec"][field] = original
        self.assertEqual(self.api.patches, 0)

    def test_released_or_malformed_conflict_never_acquires(self):
        self.client.acquire(0)
        self.only_lease()["spec"]["released"] = True
        with self.assertRaises(LeaseReleasedError):
            self.make_client().acquire(0)
        self.only_lease()["spec"]["released"] = "false"
        with self.assertRaises(LeaseConflictError):
            self.make_client().acquire(0)

    def test_release_recovers_resource_version_conflict(self):
        handle = self.client.acquire(0)

        def competing_update(api):
            self.only_lease()["metadata"]["resourceVersion"] = "2"
            api.on_patch = None

        self.api.on_patch = competing_update
        self.client.release(handle)
        self.assertEqual(self.api.patches, 2)
        self.assertTrue(self.only_lease()["spec"]["released"])

    def test_another_releaser_is_idempotent_after_conflict(self):
        handle = self.client.acquire(0)

        def competing_release(api):
            self.only_lease()["metadata"]["resourceVersion"] = "2"
            self.only_lease()["spec"]["released"] = True
            api.on_patch = None

        self.api.on_patch = competing_release
        self.client.release(handle)
        self.assertEqual(self.api.patches, 1)

    def test_replacement_during_acquire_keeps_old_lease_held(self):
        self.api.on_create = lambda api: setattr(api, "plan", plan(uid="replacement"))
        with self.assertRaises(PlanReplacedError):
            self.client.acquire(0)
        self.assertFalse(self.only_lease()["spec"]["released"])
        self.assertEqual(self.api.patches, 0)

    def test_replacement_or_deletion_before_release_never_patches(self):
        handle = self.client.acquire(0)
        self.api.plan = plan(uid="replacement")
        with self.assertRaises(PlanReplacedError):
            self.client.release(handle)
        self.api.plan = None
        with self.assertRaises(PlanReplacedError):
            self.client.release(handle)
        self.assertEqual(self.api.patches, 0)

    def test_replacement_during_poll_never_switches_uid(self):
        self.api.plan = plan(phase="Loading")
        with patch("windowflow.client.time.sleep", side_effect=lambda _: setattr(self.api, "plan", plan(uid="replacement"))):
            with self.assertRaises(PlanReplacedError):
                self.client.acquire(0)
        self.assertEqual(self.api.creates, 0)

    def test_waits_for_ready_without_creating_early_lease(self):
        self.api.plan = plan(phase="Loading")

        def ready(_):
            self.assertEqual(self.api.creates, 0)
            self.api.plan = plan()

        with patch("windowflow.client.time.sleep", side_effect=ready):
            self.client.acquire(0)
        self.assertEqual(self.api.creates, 1)

    def test_timeout_does_not_release_or_create_for_unready_window(self):
        self.api.plan = plan(phase="Loading")
        with self.assertRaises(WindowTimeoutError):
            self.make_client(timeout=0.003).acquire(0)
        self.assertEqual(self.api.creates, 0)
        self.assertEqual(self.api.patches, 0)

    def test_failed_plan_never_grants_lease(self):
        self.api.plan["status"]["phase"] = "Failed"
        with self.assertRaises(PlanFailedError):
            self.client.acquire(0)
        self.assertEqual(self.api.creates, 0)

    def test_no_automatic_release_when_generator_is_abandoned(self):
        iterator = self.client.iter_windows()
        next(iterator)
        iterator.close()
        self.assertFalse(self.only_lease()["spec"]["released"])
        self.assertEqual(self.api.patches, 0)

    def test_skipped_checkpoint_window_is_rejected_before_waiting(self):
        self.api.plan["spec"]["startWindow"] = 1
        with self.assertRaises(WindowValidationError):
            self.client.acquire(0)
        self.assertEqual(self.api.creates, 0)

    def test_unknown_reader_is_rejected_before_create(self):
        self.api.plan["spec"]["readers"] = ["dp1"]
        with self.assertRaises(WindowValidationError):
            self.client.acquire(0)
        self.assertEqual(self.api.creates, 0)

    def test_boolean_negative_and_out_of_range_indices_rejected(self):
        for index in (True, -1, 3, "0"):
            with self.subTest(index=index), self.assertRaises(WindowValidationError):
                self.client.acquire(index)
        self.assertEqual(self.api.creates, 0)

    def test_slot_paths_cannot_escape_or_alias_generation(self):
        for relative in ("/tmp/outside", "../outside", "windowflow/plan-uid/../w000000",
                         "windowflow/other-uid/w000000", "windowflow/plan-uid/w000001",
                         "windowflow//plan-uid/w000000", "windowflow\\plan-uid\\w000000"):
            with self.subTest(path=relative):
                self.api.plan["status"]["slots"][0]["relativePath"] = relative
                with self.assertRaises(WindowValidationError):
                    self.make_client().acquire(0)
        self.assertEqual(self.api.creates, 0)

    def test_symlink_in_owned_path_is_rejected(self):
        (self.root / "outside").mkdir()
        (self.root / "windowflow").symlink_to(self.root / "outside", target_is_directory=True)
        with self.assertRaises(WindowValidationError):
            self.client.acquire(0)
        self.assertEqual(self.api.creates, 0)

    def test_invalid_slot_identity_and_duplicate_assignment_rejected(self):
        original = plan()
        corruptions = (("windowIndex", -1), ("windowIndex", True), ("index", 2),
                       ("generation", "w000001"), ("windowId", "another"), ("phase", "Unknown"))
        for field, value in corruptions:
            with self.subTest(field=field, value=value):
                self.api.plan = copy.deepcopy(original)
                self.api.plan["status"]["slots"][0][field] = value
                with self.assertRaises(WindowValidationError):
                    self.make_client().acquire(0)
        self.api.plan = copy.deepcopy(original)
        self.api.plan["status"]["slots"].append(copy.deepcopy(original["status"]["slots"][0]))
        with self.assertRaises(WindowValidationError):
            self.make_client().acquire(0)
        self.assertEqual(self.api.creates, 0)

    def test_rbac_and_validation_errors_are_not_poll_retried(self):
        for status in (401, 403, 422):
            with self.subTest(status=status):
                api = FakeApi()
                api.fail_create = status
                client = WindowClient("training", "default", "dp0", self.root, api=api)
                with self.assertRaises(ApiError):
                    client.acquire(0)
                self.assertEqual(api.creates, 1)
                self.assertEqual(api.patches, 0)

    def test_release_rbac_error_never_auto_releases(self):
        handle = self.client.acquire(0)
        self.api.fail_patch = 403
        with self.assertRaises(ApiError):
            self.client.release(handle)
        self.assertEqual(self.api.patches, 1)
        self.assertFalse(self.only_lease()["spec"]["released"])

    def test_repeated_release_conflicts_have_bounded_wait(self):
        client = self.make_client(timeout=0.003)
        handle = client.acquire(0)
        self.api.fail_patch = 409
        with self.assertRaises(WindowTimeoutError):
            client.release(handle)
        self.assertFalse(self.only_lease()["spec"]["released"])


if __name__ == "__main__":
    unittest.main()
