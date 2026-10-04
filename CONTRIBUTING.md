# Contributing

WindowFlow is an experimental reference implementation. Contributions should make its safety and failure behavior easier to verify, without claiming validation that has not been performed.

## Development setup

Use Go 1.26, Python 3.10 or newer, and a local Kubernetes toolchain appropriate to the change. Start by reading [CONTRACT.md](CONTRACT.md) and [docs/architecture.md](docs/architecture.md).

```sh
make test
make manifests
make build
pip install ./sdk/python
```

Use a disposable dataset/PVC for worker or cluster tests. Do not point a development cleanup experiment at a production cache. Cloud testing is optional for contributors; explain clearly when only fake APIs or local storage were exercised.

## Changes and tests

Keep changes focused. Read the relevant code and reproduce a defect when practical before modifying it. Preserve these invariants:

- A stale or unknown lease never authorizes deletion.
- Every declared reader must release the matching window generation.
- Missing readers, timeouts, and exceptions do not imply release.
- A failed worker keeps its reservation and does not trigger automatic cleanup.
- Cleanup is confined to an ownership-verified generation directory.
- PVC ownership is never stolen on a timeout; source data is never deleted.
- Secrets are never committed or logged.

Add meaningful tests for changed behavior, especially identity confusion, partial work, concurrent releases, path validation, and source/ownership failures. Do not replace storage safety assertions with broad exception handling or forced retries.

If changing API types, regenerate manifests with `make manifests` and review the generated diff. Check that documentation, examples, and SDK assumptions agree with the final API. `make test` runs Go and Python tests; to focus on the SDK, run `make test-python` or `PYTHONPATH=sdk/python python3 -m unittest discover -s sdk/python/tests -v`.

## Pull requests

Describe the concrete problem and resulting behavior. Include:

- A concise summary and any API or operational compatibility impact.
- Exact test commands actually run, with their outcomes.
- A clear distinction between unit/fake-API tests, local worker tests, Kubernetes tests, and live provider tests.
- Any unavailable checks or remaining limitations.

For CPFS testing, record region/configuration, image or commit identity, and sanitized task results. Never include access keys, security tokens, kubeconfigs, or private dataset listings. Do not call a change production-ready because unit tests pass.

Do not silently add a new dependency on model-specific distributed collectives to the SDK. The training framework owns its DP/TP/PP synchronization and checkpoint semantics.

## Reporting issues

Use the bug report or feature request template. Include a minimal, sanitized reproduction and the smallest relevant plan. For suspected vulnerabilities or unsafe deletion paths, use [SECURITY.md](SECURITY.md) instead of publishing exploitable details in a public issue.

Contributions are made under the project's [Apache License 2.0](LICENSE).
