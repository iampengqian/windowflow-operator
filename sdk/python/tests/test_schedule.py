# SPDX-License-Identifier: Apache-2.0
"""Pure schedule and local CLI tests; no Kubernetes dependency or storage data."""

from __future__ import annotations

import contextlib
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from windowflow.plan_cli import _write_outputs, main
from windowflow.schedule import (
    CATALOG_ANNOTATION,
    GENERATOR_ANNOTATION,
    SCHEDULE_ANNOTATION,
    ScheduleValidationError,
    build_plan,
    generate_schedule,
    validate_schedule,
)


def catalog():
    return {
        "schemaVersion": "windowflow.catalog/v1", "contentVersion": "dataset-v1",
        "windows": [{"id": name, "source": "windows/" + name, "expectedBytes": i + 1, "sampleCount": 10 * (i + 1)} for i, name in enumerate(["alpha", "bravo", "charlie", "delta"])],
    }


def template():
    return {
        "apiVersion": "data.windowflow.io/v1alpha1", "kind": "WindowPlan",
        "metadata": {"name": "generated", "namespace": "default", "annotations": {"example.org/note": "keep"}},
        "spec": {"pvcName": "cache", "sourcePVC": "source", "backend": "local", "slots": 2,
                 "capacityBytes": 7, "readers": ["dp0", "dp1"], "workerImage": "example.org/windowflow:test"},
    }


def cpfs_template():
    result = template()
    del result["spec"]["sourcePVC"]
    result["spec"]["backend"] = "cpfs-dataflow"
    result["spec"]["dataFlow"] = {
        "region": "cn-wulanchabu", "fileSystemId": "bmcpfs-demo", "dataFlowId": "df-demo",
        "fileSystemPath": "/linked/", "pvcPath": "/linked/cache/", "credentialsSecret": "aliyun-dataflow",
    }
    return result


class ScheduleTests(unittest.TestCase):
    def test_known_shuffle_vector_and_digests(self):
        schedule = generate_schedule(catalog(), seed=42, num_cycles=3, shuffle_windows=True)
        self.assertEqual([e["catalogId"] for e in schedule["entries"]], [
            "bravo", "delta", "alpha", "charlie", "delta", "alpha", "charlie", "bravo", "bravo", "delta", "alpha", "charlie",
        ])
        self.assertEqual(schedule["catalogDigest"], "sha256:28d9f8fa08a7e7a61300c22d0f9cd8fe429d51fb9a4ae2bddc2bfc9510a30ca7")
        self.assertEqual(schedule["scheduleDigest"], "sha256:f745258e59ddda2621555794091c806b0bb050224f3a79880b3b9d80f7d4126a")
        body = {k: v for k, v in schedule.items() if k != "scheduleDigest"}
        digest = hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("ascii")).hexdigest()
        self.assertEqual(schedule["scheduleDigest"], "sha256:" + digest)

    def test_cycles_preserve_unshuffled_order_and_unique_ids(self):
        source = catalog()
        source["windows"][0]["contentVersion"] = "video-snapshot-1"
        before = copy.deepcopy(source)
        schedule = generate_schedule(source, seed=4, num_cycles=2)
        entries = schedule["entries"]
        self.assertEqual([e["catalogId"] for e in entries], ["alpha", "bravo", "charlie", "delta"] * 2)
        self.assertEqual([e["cycle"] for e in entries], [0] * 4 + [1] * 4)
        self.assertEqual(len({e["windowId"] for e in entries}), 8)
        self.assertEqual(entries[4]["source"], entries[0]["source"])
        self.assertEqual(entries[4]["sampleCount"], 10)
        self.assertEqual(entries[4]["contentVersion"], "video-snapshot-1")
        self.assertEqual(source, before)
        source["windows"][0]["source"] = "changed"
        self.assertEqual(schedule["catalog"], before)

    def test_reproducible_across_mapping_order_and_process_hash_seed(self):
        value = catalog()
        reordered = {"windows": value["windows"], "contentVersion": value["contentVersion"], "schemaVersion": value["schemaVersion"]}
        expected = generate_schedule(value, seed=42, num_cycles=3, shuffle_windows=True)
        self.assertEqual(expected, generate_schedule(reordered, seed=42, num_cycles=3, shuffle_windows=True))
        script = "import json,sys; from windowflow.schedule import generate_schedule; print(generate_schedule(json.load(sys.stdin),seed=42,num_cycles=3,shuffle_windows=True)['scheduleDigest'])"
        for hash_seed in ("0", "random"):
            result = subprocess.run([sys.executable, "-c", script], input=json.dumps(value), text=True, capture_output=True,
                                    env={**os.environ, "PYTHONHASHSEED": hash_seed}, check=True)
            self.assertEqual(result.stdout.strip(), expected["scheduleDigest"])

    def test_seed_and_content_version_are_bound_to_digest(self):
        first = generate_schedule(catalog(), seed=1, shuffle_windows=True)
        second = generate_schedule(catalog(), seed=2, shuffle_windows=True)
        self.assertNotEqual(first["scheduleDigest"], second["scheduleDigest"])
        source = catalog()
        source["contentVersion"] = "dataset-v2"
        changed = generate_schedule(source, seed=1, shuffle_windows=True)
        self.assertNotEqual(first["catalogDigest"], changed["catalogDigest"])
        self.assertNotEqual(first["scheduleDigest"], changed["scheduleDigest"])

    def test_sidecar_tampering_even_with_recomputed_digest_is_rejected(self):
        schedule = generate_schedule(catalog())
        for mutate in (
            lambda s: s["entries"].reverse(),
            lambda s: s["entries"][0].update(source="other"),
            lambda s: s["entries"][0].update(ordinal=False),
            lambda s: s.update(generatorVersion="future"),
            lambda s: s.update(extra="ignored"),
            lambda s: s.update(scheduleDigest="sha256:wrong"),
        ):
            bad = copy.deepcopy(schedule)
            mutate(bad)
            with self.assertRaises(ScheduleValidationError):
                validate_schedule(bad)
        bad = copy.deepcopy(schedule)
        bad["entries"].reverse()
        body = {k: v for k, v in bad.items() if k != "scheduleDigest"}
        bad["scheduleDigest"] = "sha256:" + hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        with self.assertRaises(ScheduleValidationError):
            validate_schedule(bad)
        verified = validate_schedule(schedule)
        verified["entries"].clear()
        self.assertEqual(len(schedule["entries"]), 4)

    def test_maximum_expansion_and_identifier_length(self):
        source = {"schemaVersion": "windowflow.catalog/v1", "windows": [{"id": "a" * 128, "source": "data", "expectedBytes": 0}]}
        schedule = generate_schedule(source, num_cycles=1024)
        self.assertEqual(len(schedule["entries"]), 1024)
        self.assertTrue(all(len(e["windowId"]) == 128 for e in schedule["entries"]))
        with self.assertRaisesRegex(ScheduleValidationError, "exceeds"):
            generate_schedule(catalog(), num_cycles=257)

    def test_invalid_parameters(self):
        for kwargs in ({"seed": True}, {"seed": -1}, {"seed": 2**63}, {"seed": 1.0},
                       {"num_cycles": 0}, {"num_cycles": False}, {"num_cycles": 1025},
                       {"shuffle_windows": 1}, {"shuffle_windows": "false"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ScheduleValidationError):
                generate_schedule(catalog(), **kwargs)

    def test_invalid_catalog_values_and_unknown_fields(self):
        bad_values = [
            ("id", "../bad"), ("id", "x" * 129), ("id", True),
            ("expectedBytes", -1), ("expectedBytes", True), ("expectedBytes", 2**63),
            ("expectedBytes", 1.5), ("sampleCount", 0), ("sampleCount", False),
            ("contentVersion", ""), ("contentVersion", "\ud800"), ("expectedByte", 4),
        ]
        for key, value in bad_values:
            source = catalog()
            source["windows"][0][key] = value
            with self.subTest(key=key, value=repr(value)), self.assertRaises(ScheduleValidationError):
                generate_schedule(source)
        for source in ({"schemaVersion": "wrong", "windows": []}, {"schemaVersion": "windowflow.catalog/v1", "windows": []},
                       {"schemaVersion": "windowflow.catalog/v1", "windows": {}, "extra": 1}):
            with self.assertRaises(ScheduleValidationError):
                generate_schedule(source)
        source = catalog()
        source["windows"][1]["id"] = "alpha"
        with self.assertRaisesRegex(ScheduleValidationError, "duplicate"):
            generate_schedule(source)

    def test_unsafe_relative_sources_are_rejected(self):
        for value in ("", ".", "..", "../data", "/data", "data/", "data//part", "data/./part", "data/../part",
                      "data\\part", "data\x00x", "data\nx", "data/.windowflow-owner", "\ud800"):
            source = catalog()
            source["windows"][0]["source"] = value
            with self.subTest(value=repr(value)), self.assertRaises(ScheduleValidationError):
                generate_schedule(source)

    def test_plan_binding_preserves_template_and_excludes_training_metadata_from_crd(self):
        original = template()
        before = copy.deepcopy(original)
        schedule = generate_schedule(catalog(), num_cycles=2)
        plan = build_plan(original, schedule)
        self.assertEqual(original, before)
        self.assertEqual(len(plan["spec"]["windows"]), 8)
        self.assertEqual(set(plan["spec"]["windows"][0]), {"id", "source", "expectedBytes"})
        self.assertEqual(plan["metadata"]["annotations"][SCHEDULE_ANNOTATION], schedule["scheduleDigest"])
        self.assertEqual(plan["metadata"]["annotations"]["example.org/note"], "keep")
        plan["spec"]["readers"].clear()
        self.assertEqual(original, before)

    def test_template_owned_fields_are_never_overwritten(self):
        for key in (SCHEDULE_ANNOTATION, CATALOG_ANNOTATION, GENERATOR_ANNOTATION):
            value = template()
            value["metadata"]["annotations"][key] = "prior"
            with self.assertRaisesRegex(ScheduleValidationError, "overwrite"):
                build_plan(value, generate_schedule(catalog()))
        value = template()
        value["spec"]["windows"] = []
        with self.assertRaisesRegex(ScheduleValidationError, "overwrite"):
            build_plan(value, generate_schedule(catalog()))
        for field in ("uid", "resourceVersion"):
            value = template()
            value["metadata"][field] = "live-object"
            with self.assertRaises(ScheduleValidationError):
                build_plan(value, generate_schedule(catalog()))

    def test_template_limits_and_conflicting_backend_fields(self):
        for key, invalid in (("slots", 1), ("slots", True), ("capacityBytes", 3), ("capacityBytes", 2**63),
                             ("maxConcurrentLoads", 3), ("maxConcurrentLoads", 0), ("startWindow", 4),
                             ("jobTimeoutSeconds", 59), ("readers", ["dp0", "dp0"]), ("sourcePVC", "cache"),
                             ("dataFlow", {}), ("capacityByte", 7), ("backend", "unknown")):
            value = template()
            value["spec"][key] = invalid
            with self.subTest(key=key), self.assertRaises(ScheduleValidationError):
                build_plan(value, generate_schedule(catalog()))
        value = template()
        value["spec"]["capacityBytes"] = 4  # Largest window fits; two-at-once residency is not mandatory.
        value["spec"]["startWindow"] = 3
        self.assertEqual(build_plan(value, generate_schedule(catalog()))["spec"]["startWindow"], 3)

    def test_cpfs_path_mapping_and_limits(self):
        source = catalog()
        self.assertEqual(build_plan(cpfs_template(), generate_schedule(source))["spec"]["backend"], "cpfs-dataflow")
        for key, bad in (("pvcPath", "/linked-other/cache/"), ("pvcPath", "/linked/../cache"),
                         ("fileSystemPath", "relative"), ("fileSystemPath", "/linked//"),
                         ("region", "cn/region"), ("fileSystemId", "wrong"), ("credentialsSecret", "UPPER")):
            value = cpfs_template()
            value["spec"]["dataFlow"][key] = bad
            with self.subTest(key=key), self.assertRaises(ScheduleValidationError):
                build_plan(value, generate_schedule(source))
        value = cpfs_template()
        value["spec"]["dataFlow"]["fileSystemPath"] = "/"
        build_plan(value, generate_schedule(source))
        source["windows"][0]["source"] = "视频" * 180
        with self.assertRaisesRegex(ScheduleValidationError, "source exceeds"):
            build_plan(cpfs_template(), generate_schedule(source))


class CLITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.catalog = self.root / "catalog.json"
        self.template = self.root / "template.json"
        self.output = self.root / "plan.json"
        self.sidecar = self.root / "schedule.json"
        self.catalog.write_text(json.dumps(catalog()), encoding="utf-8")
        self.template.write_text(json.dumps(template()), encoding="utf-8")

    def args(self):
        return ["--catalog", str(self.catalog), "--template", str(self.template), "--output", str(self.output),
                "--schedule-output", str(self.sidecar), "--seed", "42", "--num-cycles", "3", "--shuffle-windows"]

    def run_cli(self, args=None, fails=False):
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            if fails:
                with self.assertRaises(SystemExit) as error:
                    main(args or self.args())
                self.assertEqual(error.exception.code, 2)
            else:
                self.assertEqual(main(args or self.args()), 0)

    def test_cli_writes_verified_plan_and_sidecar(self):
        self.run_cli()
        schedule = validate_schedule(json.loads(self.sidecar.read_text()))
        plan = json.loads(self.output.read_text())
        self.assertEqual(plan, build_plan(template(), schedule))
        self.assertEqual(len(plan["spec"]["windows"]), 12)

    def test_outputs_never_replace_inputs_existing_files_or_each_other(self):
        original = self.catalog.read_bytes()
        self.output = self.catalog
        self.run_cli(fails=True)
        self.assertEqual(self.catalog.read_bytes(), original)
        self.assertFalse(self.sidecar.exists())
        self.output = self.root / "output.json"
        self.sidecar = self.output
        self.run_cli(fails=True)
        self.assertFalse(self.output.exists())
        self.sidecar = self.root / "schedule.json"
        self.sidecar.write_text("existing")
        self.run_cli(fails=True)
        self.assertFalse(self.output.exists())
        self.assertEqual(self.sidecar.read_text(), "existing")

    def test_symlink_hardlink_and_path_alias_outputs_are_rejected(self):
        for kind in ("symlink", "hardlink", "dangling"):
            if kind == "symlink":
                self.output.symlink_to(self.catalog)
            elif kind == "hardlink":
                os.link(self.catalog, self.output)
            else:
                self.output.symlink_to(self.root / "missing")
            self.run_cli(fails=True)
            self.assertTrue(self.output.exists() or self.output.is_symlink())
            self.assertFalse(self.sidecar.exists())
            self.output.unlink()

    def test_duplicate_keys_nonfinite_and_invalid_plan_leave_no_outputs(self):
        for contents in ('{"windows":[],"windows":[]}', '{"schemaVersion":NaN}', '{"schemaVersion":Infinity}'):
            self.catalog.write_text(contents)
            self.run_cli(fails=True)
            self.assertFalse(self.output.exists())
            self.assertFalse(self.sidecar.exists())
        self.catalog.write_text(json.dumps(catalog()))
        value = template()
        value["spec"]["windows"] = []
        self.template.write_text(json.dumps(value))
        self.run_cli(fails=True)
        self.assertFalse(self.output.exists())

    def test_second_write_failure_rolls_back_only_this_calls_new_file(self):
        self.sidecar.write_text("keep me")
        with self.assertRaises(FileExistsError):
            _write_outputs([(self.output, {"one": 1}), (self.sidecar, {"two": 2})])
        self.assertFalse(self.output.exists())
        self.assertEqual(self.sidecar.read_text(), "keep me")

    def test_cli_module_entry_point_needs_no_kubernetes(self):
        result = subprocess.run([sys.executable, "-m", "windowflow.plan_cli", *self.args()], capture_output=True, text=True, check=True)
        self.assertIn("Generated 12 window visits", result.stdout)


if __name__ == "__main__":
    unittest.main()
