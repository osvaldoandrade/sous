# `cs-python` runtime adapter

**Parent epic:** #22
**Phase:** Next
**Estimated size:** L

## Problem
Agent-generated code is increasingly Python-first, but `code-sous` only ships the Goja-based `cs-js` runtime today. We need a Python runtime that exposes the same `cs.*` host API surface as `cs-js` and preserves the no-fs / no-process / deny-egress-by-default invariants from `docs/15-security.md` and `docs/02-requirements.md`.

## Proposed solution
- Add a new adapter package `internal/runtime/python` (alongside today's Goja-based `internal/runtime/runner.go`) that embeds a sandboxed CPython interpreter (e.g. CPython via `cgo` with a restricted builtins shim, or RustPython/Pyodide-WASI depending on parity benchmarks); the adapter must implement the same `Runner.Execute` contract as the JS runtime, returning an `ExecutionOutput` with the same fields.
- Introduce a `RuntimeAdapter` interface in `internal/runtime/providers.go` so `cmd/cs-invoker-pool/main.go` can dispatch on `manifest.runtime` and instantiate either the JS adapter or the Python adapter; bundles with `runtime: cs-python` must boot a Python isolate, others remain on Goja.
- Map host APIs 1:1: `cs.log.info/warn/error`, `cs.kv.get/set/del`, `cs.codeq.publish`, `cs.http.fetch` — all enforcing the same capability allowlists (KV prefixes, codeQ topic prefixes, HTTP host allowlist, private-IP block) inside the Python adapter, sharing the existing `KVProvider`, `CodeQProvider`, and HTTP client wiring.
- Enforce wall-time and memory budgets at the interpreter level: cancel via `ctx.Done()` plus interpreter-level interrupt; cap memory through `RLIMIT_AS` (or wazero memory page caps if the WASI route is chosen) so a runaway Python activation matches `cs-js` semantics in `docs/08-runtime-cs-js.md`.

## Acceptance criteria
- [ ] `manifest.json` with `"runtime": "cs-python"` and `"entry": "function.py"` boots a Python isolate in both `cs-invoker-pool` and the `cs` CLI local runner (parity invariant from `docs/02-requirements.md`).
- [ ] `cs.log.*`, `cs.kv.*`, `cs.codeq.publish`, and `cs.http.fetch` exist in Python with identical argument shapes and return shapes to the JS runtime, validated by shared fixtures.
- [ ] Filesystem access (`open`, `os`, `pathlib`), process spawn (`subprocess`, `os.system`, `os.fork`), raw sockets, and dynamic native-module loading (`ctypes`, `cffi`) are blocked at import time; egress is denied unless `http.allowHosts` is set; private-IP block from `docs/15-security.md` is enforced.
- [ ] Wall-time and memory caps from manifest `limits` terminate the activation deterministically and return the same `cserrors.Code` taxonomy as the JS runtime.
- [ ] `docs/08-runtime-cs-js.md` is split or supplemented with a sibling `docs/08b-runtime-cs-python.md` describing the Python contract, and `docs/20-config-reference.md` lists any new invoker-pool flags.
- [ ] CI builds and runs Python-runtime tests on Linux amd64/arm64.

## Dependencies & risks
- Depends on Task E3.03 (manifest schema) for the `runtime` discriminator, and on the parity harness in Task E3.04 for cross-runtime assertions.
- Embedding CPython adds a non-trivial cgo dependency; mitigation: gate behind a build tag (`+build cs_python`) and ship a separate `cs-invoker-pool` image variant during incubation.
- Sandboxing CPython is historically hard (gadgets via `__builtins__`, `gc`, `sys`); mitigation: run inside a WASI sandbox (Pyodide on Wasmtime) so the OS-level guarantees come from WASM, not from Python introspection.
- Performance regression vs. Goja on cold start; mitigation: warm-pool reuse keyed by version sha in `cs-invoker-pool`.

## Out of scope
- Python package management or `pip install` at publish time (no build step invariant — see `docs/02-requirements.md`).
- Async I/O frameworks (`asyncio` event loops beyond the handler `await`).
- Multi-file Python bundles with relative imports beyond `function.py` (deferred to a later epic).
