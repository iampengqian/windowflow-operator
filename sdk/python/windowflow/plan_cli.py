# SPDX-License-Identifier: Apache-2.0
"""Generate JSON WindowPlan and schedule sidecar files without cluster access."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Sequence

from .schedule import ScheduleValidationError, build_plan, generate_schedule


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ScheduleValidationError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _bad_constant(value):
    raise ScheduleValidationError(f"nonfinite JSON number {value!r} is not allowed")


def _load(path: Path):
    with path.open(encoding="utf-8") as stream:
        return json.load(stream, object_pairs_hook=_pairs, parse_constant=_bad_constant)


def _paths(catalog: str, template: str, output: str, sidecar: str) -> list[Path]:
    paths = [Path(value).expanduser().resolve() for value in (catalog, template, output, sidecar)]
    if len(set(paths)) != 4:
        raise ScheduleValidationError("catalog, template, output, and schedule-output must be distinct paths")
    for first, second in ((paths[0], paths[1]),):
        if first.exists() and second.exists() and first.samefile(second):
            raise ScheduleValidationError("catalog and template must not refer to the same file")
    for path in paths[:2]:
        if not path.is_file():
            raise ScheduleValidationError(f"input is not a regular file: {path}")
    for original, path in zip((output, sidecar), paths[2:]):
        if Path(original).expanduser().is_symlink() or path.exists():
            raise ScheduleValidationError(f"output already exists; refusing to overwrite: {original}")
        if not path.parent.is_dir():
            raise ScheduleValidationError(f"output parent directory does not exist: {path.parent}")
    return paths


def _write_outputs(outputs: list[tuple[Path, dict]]) -> None:
    # Validate/serialize everything before creating either file. Exclusive
    # creation never overwrites existing data. Roll back this invocation's files
    # on ordinary errors; two files are not a crash-atomic transaction.
    encoded = [(path, json.dumps(value, indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n") for path, value in outputs]
    created = []
    try:
        for path, value in encoded:
            with path.open("x", encoding="utf-8", newline="\n") as stream:
                created.append((path, os.fstat(stream.fileno())))
                stream.write(value)
                stream.flush()
                os.fsync(stream.fileno())
    except BaseException:
        for path, original in created:
            try:
                current = path.lstat()
                if (current.st_dev, current.st_ino) == (original.st_dev, original.st_ino):
                    path.unlink()
            except FileNotFoundError:
                pass
        raise


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a deterministic finite WindowPlan and execution sidecar from existing immutable directory windows. JSON only; no data copying or Kubernetes access.",
        epilog="The template must omit spec.windows and reserved schedule annotations. Outputs must not exist and cannot alias inputs. The sidecar records order/digests, not model checkpoints or source-content verification.",
    )
    parser.add_argument("--catalog", required=True, help="JSON catalog with schemaVersion=windowflow.catalog/v1 and windows")
    parser.add_argument("--template", required=True, help="fresh WindowPlan JSON containing metadata and remaining spec fields")
    parser.add_argument("--output", required=True, help="new output plan JSON (accepted by kubectl apply -f)")
    parser.add_argument("--schedule-output", required=True, help="new execution sidecar JSON; retain alongside checkpoints")
    parser.add_argument("--seed", type=int, default=0, help="nonnegative integer seed, at most 2^63-1 (default: 0)")
    parser.add_argument("--num-cycles", type=int, default=1, help="finite full-catalog passes; expanded plan is limited to 1024 visits")
    parser.add_argument("--shuffle-windows", action="store_true", help="deterministically order each cycle by SHA-256 keys")
    args = parser.parse_args(argv)
    try:
        catalog_path, template_path, plan_path, schedule_path = _paths(args.catalog, args.template, args.output, args.schedule_output)
        schedule = generate_schedule(_load(catalog_path), seed=args.seed, num_cycles=args.num_cycles, shuffle_windows=args.shuffle_windows)
        plan = build_plan(_load(template_path), schedule)
        _write_outputs([(plan_path, plan), (schedule_path, schedule)])
    except (OSError, ValueError, UnicodeError) as exc:
        parser.error(str(exc))
    print(f"Generated {len(schedule['entries'])} window visits; {schedule['scheduleDigest']}")
    print(f"Plan: {plan_path}\nSchedule: {schedule_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
