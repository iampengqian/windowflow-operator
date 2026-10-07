# SPDX-License-Identifier: Apache-2.0
"""Pure, deterministic schedules for existing immutable directory windows.

This module does not inventory, rearrange, or verify source data and performs no
Kubernetes calls. A contentVersion is a caller-supplied identity, not a content
hash computed by WindowFlow. Repeat cycles create distinct window generations;
they do not promise that data stays resident between repeated visits.

Canonical digests use SHA-256 of ASCII JSON with sorted keys, compact separators,
and no NaN values. With shuffling enabled, each cycle sorts catalog entries by
SHA-256 of canonical [GENERATOR_VERSION, seed, zero_based_cycle, catalog_id],
then by catalog ID as a collision tie-breaker. No Python PRNG is used.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Mapping
from typing import Any

CATALOG_SCHEMA = "windowflow.catalog/v1"
SCHEDULE_SCHEMA = "windowflow.schedule/v1"
GENERATOR_VERSION = "sha256-sort-v1"
MAX_WINDOWS = 1024
MAX_INT64 = (1 << 63) - 1
SCHEDULE_ANNOTATION = "data.windowflow.io/schedule-digest"
CATALOG_ANNOTATION = "data.windowflow.io/catalog-digest"
GENERATOR_ANNOTATION = "data.windowflow.io/schedule-generator"
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\Z")
_REGION = re.compile(r"[a-z0-9-]+\Z")


class ScheduleValidationError(ValueError):
    """The supplied catalog, schedule, or fresh-plan template is invalid."""


def _object(value: Any, name: str, allowed: set[str], required: set[str]) -> dict:
    if not isinstance(value, Mapping):
        raise ScheduleValidationError(f"{name} must be an object")
    if any(not isinstance(key, str) for key in value):
        raise ScheduleValidationError(f"{name} keys must be strings")
    unknown, missing = set(value) - allowed, required - set(value)
    if unknown or missing:
        raise ScheduleValidationError(f"{name}: unknown fields {sorted(unknown)}, missing fields {sorted(missing)}")
    return dict(value)


def _integer(value: Any, name: str, minimum: int = 0, maximum: int = MAX_INT64) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ScheduleValidationError(f"{name} must be an integer in [{minimum}, {maximum}]")
    return value


def _text(value: Any, name: str, maximum: int = 1024) -> str:
    if not isinstance(value, str) or not value or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ScheduleValidationError(f"{name} must be nonempty text without control characters")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ScheduleValidationError(f"{name} must be valid UTF-8") from exc
    if len(encoded) > maximum:
        raise ScheduleValidationError(f"{name} exceeds {maximum} UTF-8 bytes")
    return value


def _identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise ScheduleValidationError(f"{name} must be a safe identifier of 1–128 ASCII characters")
    return value


def _directory(value: Any, name: str, *, absolute: bool = False) -> str:
    value = _text(value, name, maximum=4096)
    if "\\" in value or value.startswith("/") != absolute:
        raise ScheduleValidationError(f"{name} must be an absolute POSIX directory" if absolute else f"{name} must be a relative POSIX directory")
    trimmed = value.removesuffix("/") if absolute else value
    components = trimmed.removeprefix("/").split("/")
    if absolute and value == "/":
        return value
    if any(part in ("", ".", "..") or (not absolute and part.startswith(".windowflow-")) for part in components):
        raise ScheduleValidationError(f"{name} contains a dot, empty, or reserved component")
    return value


def _canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(",", ":"), allow_nan=False).encode("ascii")
    except (TypeError, ValueError, RecursionError) as exc:
        raise ScheduleValidationError("value is not canonical JSON") from exc


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value)).hexdigest()


def _catalog(catalog: Mapping[str, Any]) -> dict:
    result = _object(catalog, "catalog", {"schemaVersion", "contentVersion", "windows"}, {"schemaVersion", "windows"})
    if result["schemaVersion"] != CATALOG_SCHEMA:
        raise ScheduleValidationError(f"catalog.schemaVersion must be {CATALOG_SCHEMA!r}")
    if "contentVersion" in result:
        _text(result["contentVersion"], "catalog.contentVersion")
    windows = result["windows"]
    if not isinstance(windows, list) or not 1 <= len(windows) <= MAX_WINDOWS:
        raise ScheduleValidationError(f"catalog.windows must be a list of 1–{MAX_WINDOWS} windows")
    normalized, ids = [], set()
    for index, window in enumerate(windows):
        name = f"catalog.windows[{index}]"
        entry = _object(window, name, {"id", "source", "expectedBytes", "sampleCount", "contentVersion"}, {"id", "source", "expectedBytes"})
        identifier = _identifier(entry["id"], f"{name}.id")
        if identifier in ids:
            raise ScheduleValidationError(f"duplicate catalog window ID {identifier!r}")
        ids.add(identifier)
        _directory(entry["source"], f"{name}.source")
        _integer(entry["expectedBytes"], f"{name}.expectedBytes")
        if "sampleCount" in entry:
            _integer(entry["sampleCount"], f"{name}.sampleCount", 1)
        if "contentVersion" in entry:
            _text(entry["contentVersion"], f"{name}.contentVersion")
        normalized.append(entry)
    result["windows"] = normalized
    return copy.deepcopy(result)


def generate_schedule(
    catalog: Mapping[str, Any], *, seed: int = 0, num_cycles: int = 1, shuffle_windows: bool = False,
) -> dict[str, Any]:
    """Expand a validated catalog into at most 1024 deterministic visits.

    Catalog order is preserved when shuffle_windows=False. Each visit has a
    distinct CRD windowId, even if its source appeared in a previous cycle.
    sampleCount describes one catalog visit; within-window epochs are a separate
    training policy. No timestamp or runtime-specific randomness is recorded.
    """
    normalized = _catalog(catalog)
    _integer(seed, "seed")
    _integer(num_cycles, "num_cycles", 1, MAX_WINDOWS)
    if type(shuffle_windows) is not bool:
        raise ScheduleValidationError("shuffle_windows must be a boolean")
    if len(normalized["windows"]) * num_cycles > MAX_WINDOWS:
        raise ScheduleValidationError(f"expanded schedule exceeds the CRD limit of {MAX_WINDOWS} windows")
    entries = []
    for cycle in range(num_cycles):
        windows = normalized["windows"]
        if shuffle_windows:
            windows = sorted(windows, key=lambda window: (
                hashlib.sha256(_canonical([GENERATOR_VERSION, seed, cycle, window["id"]])).digest(),
                window["id"],
            ))
        for window in windows:
            ordinal = len(entries)
            # Ordinal guarantees uniqueness; the suffix is only for readability.
            entry = {
                "ordinal": ordinal, "cycle": cycle, "catalogId": window["id"],
                "windowId": f"w{ordinal:06d}-{window['id'][:120]}",
                "source": window["source"], "expectedBytes": window["expectedBytes"],
            }
            for key in ("sampleCount", "contentVersion"):
                if key in window:
                    entry[key] = window[key]
            entries.append(entry)
    schedule = {
        "schemaVersion": SCHEDULE_SCHEMA, "generatorVersion": GENERATOR_VERSION,
        "catalog": normalized, "catalogDigest": _digest(normalized),
        "seed": seed, "numCycles": num_cycles, "shuffleWindows": shuffle_windows,
        "entries": entries,
    }
    schedule["scheduleDigest"] = _digest(schedule)
    return schedule


def validate_schedule(schedule: Mapping[str, Any]) -> dict[str, Any]:
    """Regenerate and verify a sidecar, including order, identities, and digests.

    Returns a detached verified copy. The digest detects changes; it does not
    authenticate a publisher or verify objects in storage.
    """
    value = _object(schedule, "schedule", {
        "schemaVersion", "generatorVersion", "catalog", "catalogDigest", "seed",
        "numCycles", "shuffleWindows", "entries", "scheduleDigest",
    }, {"schemaVersion", "generatorVersion", "catalog", "catalogDigest", "seed", "numCycles", "shuffleWindows", "entries", "scheduleDigest"})
    if value["schemaVersion"] != SCHEDULE_SCHEMA or value["generatorVersion"] != GENERATOR_VERSION:
        raise ScheduleValidationError("unsupported schedule schema or generator version")
    regenerated = generate_schedule(value["catalog"], seed=value["seed"], num_cycles=value["numCycles"], shuffle_windows=value["shuffleWindows"])
    if _canonical(value) != _canonical(regenerated):
        raise ScheduleValidationError("schedule order, content, or digest differs from deterministic regeneration")
    return regenerated


def _dns(value: Any, name: str, *, label: bool = False) -> str:
    maximum = 63 if label else 253
    if not isinstance(value, str) or len(value) > maximum or not value:
        raise ScheduleValidationError(f"{name} must be a DNS {'label' if label else 'subdomain'}")
    parts = [value] if label else value.split(".")
    if any(len(part) > 63 or not _DNS_LABEL.fullmatch(part) for part in parts):
        raise ScheduleValidationError(f"{name} must be a DNS {'label' if label else 'subdomain'}")
    return value


def _string_map(value: Any, name: str) -> dict:
    if not isinstance(value, Mapping) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in value.items()):
        raise ScheduleValidationError(f"{name} must map strings to strings")
    return dict(value)


def build_plan(template: Mapping[str, Any], schedule: Mapping[str, Any]) -> dict[str, Any]:
    """Fill a fresh WindowPlan template without replacing caller-owned fields.

    The template must omit spec.windows and the three schedule annotations.
    Metadata supports name, namespace, labels, and annotations; an exported live
    CR with status, UID, resourceVersion, etc. is deliberately not a template.
    Validates scheduling capacity, but never checks physical free space.
    """
    verified = validate_schedule(schedule)
    plan = _object(template, "template", {"apiVersion", "kind", "metadata", "spec"}, {"apiVersion", "kind", "metadata", "spec"})
    if plan["apiVersion"] != "data.windowflow.io/v1alpha1" or plan["kind"] != "WindowPlan":
        raise ScheduleValidationError("template must be a data.windowflow.io/v1alpha1 WindowPlan")
    metadata = _object(plan["metadata"], "metadata", {"name", "namespace", "labels", "annotations"}, {"name"})
    _dns(metadata["name"], "metadata.name")
    if "namespace" in metadata:
        _dns(metadata["namespace"], "metadata.namespace", label=True)
    if "labels" in metadata:
        metadata["labels"] = _string_map(metadata["labels"], "metadata.labels")
    annotations = _string_map(metadata.get("annotations", {}), "metadata.annotations")
    reserved = {SCHEDULE_ANNOTATION, CATALOG_ANNOTATION, GENERATOR_ANNOTATION}
    if reserved.intersection(annotations):
        raise ScheduleValidationError("template contains reserved schedule annotations; refusing to overwrite")
    fields = {"pvcName", "slots", "capacityBytes", "readers", "workerImage", "backend", "maxConcurrentLoads", "sourcePVC", "dataFlow", "jobTimeoutSeconds", "startWindow"}
    if isinstance(plan["spec"], Mapping) and "windows" in plan["spec"]:
        raise ScheduleValidationError("template.spec.windows must be absent; refusing to overwrite")
    spec = _object(plan["spec"], "spec", fields, {"pvcName", "slots", "capacityBytes", "readers", "workerImage", "backend"})
    _dns(spec["pvcName"], "spec.pvcName")
    _text(spec["workerImage"], "spec.workerImage")
    _integer(spec["slots"], "spec.slots", 2, 64)
    capacity = _integer(spec["capacityBytes"], "spec.capacityBytes", 1)
    if "maxConcurrentLoads" in spec:
        _integer(spec["maxConcurrentLoads"], "spec.maxConcurrentLoads", 1, spec["slots"])
    if "jobTimeoutSeconds" in spec:
        _integer(spec["jobTimeoutSeconds"], "spec.jobTimeoutSeconds", 60, 2592000)
    if "startWindow" in spec:
        _integer(spec["startWindow"], "spec.startWindow", 0, len(verified["entries"]) - 1)
    readers = spec["readers"]
    if not isinstance(readers, list) or not 1 <= len(readers) <= 4096:
        raise ScheduleValidationError("spec.readers must be a list of 1–4096 reader IDs")
    for reader in readers:
        _identifier(reader, "reader ID")
    if len(readers) != len(set(readers)):
        raise ScheduleValidationError("spec.readers must be unique")
    if spec["backend"] == "local":
        _dns(spec.get("sourcePVC"), "spec.sourcePVC")
        if spec["sourcePVC"] == spec["pvcName"] or "dataFlow" in spec:
            raise ScheduleValidationError("local backend needs a distinct sourcePVC and no dataFlow")
    elif spec["backend"] == "cpfs-dataflow":
        if "sourcePVC" in spec:
            raise ScheduleValidationError("cpfs-dataflow must not specify sourcePVC")
        dataflow = _object(spec.get("dataFlow"), "spec.dataFlow", {
            "region", "fileSystemId", "dataFlowId", "fileSystemPath", "pvcPath", "credentialsSecret",
        }, {"region", "fileSystemId", "dataFlowId", "fileSystemPath", "pvcPath", "credentialsSecret"})
        region = _text(dataflow["region"], "dataFlow.region", 128)
        if not _REGION.fullmatch(region):
            raise ScheduleValidationError("dataFlow.region must contain lowercase letters, digits, or hyphens")
        for key, prefix in (("fileSystemId", "bmcpfs-"), ("dataFlowId", "df-")):
            if not _identifier(dataflow[key], f"dataFlow.{key}").startswith(prefix):
                raise ScheduleValidationError(f"dataFlow.{key} must start with {prefix!r}")
        _dns(dataflow["credentialsSecret"], "dataFlow.credentialsSecret")
        linked = _directory(dataflow["fileSystemPath"], "dataFlow.fileSystemPath", absolute=True).rstrip("/")
        pvc = _directory(dataflow["pvcPath"], "dataFlow.pvcPath", absolute=True).rstrip("/")
        if linked and pvc != linked and not pvc.startswith(linked + "/"):
            raise ScheduleValidationError("dataFlow.pvcPath must be inside fileSystemPath")
        # Kubernetes assigns a 36-byte UUID; generation w000000 is seven bytes.
        suffix = pvc[len(linked):] + "/windowflow/" + "0" * 36 + "/w000000/"
        if len(suffix.encode("utf-8")) > 1023:
            raise ScheduleValidationError("generated CPFS destination exceeds the API directory limit")
        for entry in verified["entries"]:
            if len(entry["source"].encode("utf-8")) + 2 > 1023:
                raise ScheduleValidationError("CPFS source exceeds the API directory limit")
        spec["dataFlow"] = dataflow
    else:
        raise ScheduleValidationError("spec.backend must be local or cpfs-dataflow")
    if any(entry["expectedBytes"] > capacity for entry in verified["entries"]):
        raise ScheduleValidationError("a window exceeds spec.capacityBytes; it could never be staged")
    spec["windows"] = [{"id": e["windowId"], "source": e["source"], "expectedBytes": e["expectedBytes"]} for e in verified["entries"]]
    annotations.update({SCHEDULE_ANNOTATION: verified["scheduleDigest"], CATALOG_ANNOTATION: verified["catalogDigest"], GENERATOR_ANNOTATION: GENERATOR_VERSION})
    metadata["annotations"] = annotations
    plan["metadata"], plan["spec"] = metadata, spec
    return copy.deepcopy(plan)
