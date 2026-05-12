# `cs-wasm` runtime adapter

**Parent epic:** #22
**Phase:** Next
**Estimated size:** L

## Problem
Compiled languages (Rust, Go, AssemblyScript, Zig, TinyGo) are common in agent toolchains, and a WASM runtime is the cleanest substrate for capability-based sandboxing. `code-sous` needs a `cs-wasm` adapter so any language that targets WASI can run inside `cs-invoker-pool` with the same host API surface as `cs-js`.

## Proposed solution
- Add `internal/runtime/wasm` integrating Wasmtime via `github.com/bytecodealliance/wasmtime-go` (or `github.com/tetratelabs/wazero` for a pure-Go option); decision documented in an ADR, default chosen by benchmark on `cs-invoker-pool` workloads.
- Implement a `wasi_snapshot_preview1` adapter that exposes ONLY the `cs.*` host calls; all other WASI imports (`fd_*`, `path_*`, `sock_*`, `proc_*`) are stubbed to return `EACCES` so the no-fs / no-process / no-egress invariants from `docs/15-security.md` hold by construction.
- Map host APIs to hostcalls: `cs_log_info/warn/error`, `cs_kv_get/set/del`, `cs_codeq_publish`, `cs_http_fetch`, with all data passed via linear-memory pointer+length and JSON encoding; provide a thin SDK header (`cs.h`, `cs.rs`, `cs.ts`) under `spec/wasm/` so guest authors can compile against a stable ABI.
- Apply capability restrictions at `Module.Instantiate` time: enforce `limits.memoryMb` via Wasmtime `MemoryLimits`, enforce `limits.timeoutMs` via fuel or epoch-based interruption, refuse instantiation if the module imports anything outside the published host-call surface.

## Acceptance criteria
- [ ] A `manifest.json` with `"runtime": "cs-wasm"` and `"entry": "function.wasm"` runs inside both `cs-invoker-pool` and the `cs` CLI; the same `event` JSON produces the same response shape as the JS reference fixture.
- [ ] All `cs.*` host APIs are reachable from a Rust+`cs-rs` guest and an AssemblyScript guest, exercised by parity fixtures from Task E3.04.
- [ ] Module instantiation fails closed when a guest imports `fd_write`, `path_open`, `sock_send`, `proc_exit`, or any non-`cs_*` symbol; failure surfaces as a typed `cserrors.Code` (`runtime/forbidden_import`).
- [ ] Fuel/epoch interrupt fires within 50 ms of `deadline_ms`; memory cap denies `memory.grow` past `limits.memoryMb` and terminates with a typed error.
- [ ] `docs/08-runtime-cs-js.md` siblings receive `docs/08c-runtime-cs-wasm.md` describing the ABI, and `spec/` contains the host-call IDL.
- [ ] CI runs WASM-runtime tests on Linux amd64 and arm64.

## Dependencies & risks
- Depends on Task E3.03 (manifest `runtime` discriminator) and Task E3.04 (parity harness).
- Wasmtime cgo dep adds build complexity; mitigation: keep wazero as fallback and choose at build-time via tag.
- WASI surface is broader than what users need; mitigation: explicit allowlist (not denylist) of host imports.
- Cold-start latency for large `.wasm` modules; mitigation: enable Wasmtime module caching keyed on bundle `sha256` from `docs/15-security.md`.

## Out of scope
- WASM Component Model / WIT bindings (target the post-v0.2 epic).
- GPU / SIMD acceleration beyond the runtime defaults.
- WASI Preview 2 networking (`wasi-sockets`) — egress remains through `cs.http.fetch` only.
