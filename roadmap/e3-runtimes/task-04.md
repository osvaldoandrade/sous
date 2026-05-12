# Runtime parity test harness

**Parent epic:** #22
**Phase:** Next
**Estimated size:** M

## Problem
The runtime-parity invariant in `docs/02-requirements.md` requires that the CLI and the server, and now every adapter (`cs-js`, `cs-python`, `cs-wasm`), expose identical host APIs and return identical response shapes for the same input. Today there is no shared fixture suite enforcing that — each runtime ships its own unit tests in isolation (`internal/runtime/runner_test.go`).

## Proposed solution
- Create a fixture corpus under `test/parity/` containing one folder per scenario (`hello`, `kv-roundtrip`, `codeq-publish`, `http-fetch-allowed`, `http-fetch-denied`, `timeout`, `memory-cap`, `log-emission`, `private-ip-block`), each with `event.json`, `expected.json`, and a `bundles/{js,py,wasm}/` subdir containing equivalent guest code + manifest.
- Add a Go test harness `internal/runtime/paritytest` that, for every scenario × every adapter, builds the bundle, runs `Runner.Execute` with the same `event`, and asserts: identical `statusCode`/`headers`/`body`, identical log ordering and levels, identical `cserrors.Code`, and timing within a tolerance window.
- Wire the harness into `Makefile` (`make parity`) and `.github/workflows/` so CI fails when any runtime drifts; publish a coverage report listing supported scenarios per adapter.
- Provide a `cs parity` CLI subcommand (`cmd/cs-cli`) so authors can run the same suite locally before publishing a new adapter version, reinforcing the "CLI ≡ server" invariant.

## Acceptance criteria
- [ ] ≥9 scenarios live under `test/parity/` with bundles for all three runtimes; each scenario produces byte-identical normalized output across adapters.
- [ ] CI job `parity-matrix` runs the suite on every PR and posts a per-runtime pass/fail table as a check summary.
- [ ] Negative scenarios prove that egress denial, private-IP block, fs/process gating, timeout, and memory-cap behavior emit the same typed `cserrors.Code` regardless of runtime, matching `docs/15-security.md` and `docs/21-errors.md`.
- [ ] `make parity` runs locally without Docker and finishes under 90s on a developer laptop; `cs parity` runs the same suite against the local runner.
- [ ] `docs/17-testing.md` gains a "Runtime parity harness" section documenting how to add a new scenario and a new adapter.
- [ ] Harness is reusable by future runtimes (e.g. a hypothetical `cs-deno`) without rewriting fixtures.

## Dependencies & risks
- Depends on Task E3.03 to know the canonical `runtime` discriminator and on Task E3.01 / E3.02 for the adapters under test.
- Non-determinism risk in fixtures (timestamps, map iteration order); mitigation: a normalization step strips/sorts before comparison, fixtures avoid wall-clock unless the scenario is timing-focused.
- Build-time cost for compiling WASM fixtures in CI; mitigation: prebuild and commit `*.wasm` artifacts under `test/parity/bundles/<scenario>/wasm/` with a `make regen-parity` target.

## Out of scope
- Performance benchmarking (separate epic / harness).
- Fuzzing of host APIs (future security epic).
- Cross-version parity (e.g. cs-js v1 vs v2) — initial harness pins one version per adapter.
