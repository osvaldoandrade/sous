# Epic: Runtime expansion — Python + WASM

**Phase:** Next (2–5 months)
**Theme:** Make `code-sous` polyglot while preserving runtime parity and the security capability model.

## Why
Agents generating function code today reach for Python and Rust at least as often as JavaScript, and ecosystem coverage is the single largest blocker reported by early `cs` CLI users. Adding `cs-python` and `cs-wasm` adapters expands the surface of tasks agents can close without leaving `code-sous`, while preserving the no-build-step and capability-allowlist invariants in `docs/02-requirements.md` and `docs/15-security.md`. WASM in particular gives us a cleaner sandbox primitive that any future runtime (Go, Rust, Zig, TinyGo) can target without re-implementing isolation.

## Scope
- Embed a sandboxed Python interpreter and expose the `cs.*` host API at parity with `cs-js`.
- Embed a WASM runtime (Wasmtime or wazero) and expose the same host API over WASI hostcalls.
- Extend the function manifest schema to declare `runtime`, pin runtime version, and request capabilities.
- Ship a shared parity test harness so every adapter is held to the same behavioral contract, in CI and locally via `cs parity`.

## Outcomes / success metrics
- Three adapters (`cs-js`, `cs-python`, `cs-wasm`) pass ≥9 parity scenarios with byte-identical normalized output.
- Manifest schema v2 ships with a documented v1→v2 migration and zero regressions for existing `cs-js` functions.
- CI parity matrix is green on every PR; new adapters can be added without modifying fixtures.
- At least one published runtime version pinned per adapter (`python3.12`, `wasi-preview1`, `node20-goja`).
- Internal/early adopter telemetry shows ≥20% of new functions published with a non-JS runtime within one quarter of GA.

## Tasks
- [ ] #11 — `cs-python` runtime adapter
- [ ] #13 — `cs-wasm` runtime adapter
- [ ] #18 — Manifest changes for runtime selection at publish time
- [ ] #20 — Runtime parity test harness

## Non-goals
- Package/dependency management at publish time (no build step invariant; tracked under the Packaging epic in `docs/18-roadmap.md`).
- Signed tenant-key bundles (also Packaging).
- WASM Component Model / WIT bindings.
- GPU acceleration or large-model inference inside user code.
- New trigger types (cron, codeQ subscriptions) — covered by the Triggers epic.
