# Runtime: cs-wasm

This document defines the WebAssembly runtime contract. `cs-wasm` runs any
language that compiles to a `wasm32-unknown-unknown` or `wasm32-wasi` target
inside the same sandbox model as cs-js, with one substitution: the
publisher ships a pre-compiled `.wasm` module instead of a JavaScript file.

The adapter lives in `internal/runtime/wasm` and embeds the
[wazero](https://github.com/tetratelabs/wazero) pure-Go runtime — no CGo, no
external Wasmtime build dependency.

## Runtime shape

A published function version contains two UTF-8 files:

- `module.wasm`  — the compiled WebAssembly module
- `manifest.json`

The system stores them as a canonical tar bundle. The runtime loads the
bundle from KVRocks and instantiates it in a fresh wazero `Runtime` for
every activation. The runtime is torn down when `Execute` returns; nothing
persists across activations.

## Why an isolate exists

User code runs in the same cluster as platform services. The platform must
treat user code as untrusted. The wasm isolate is even narrower than the JS
isolate because:

- the guest has no access to the host OS (no `wasi_snapshot_preview1` is
  registered),
- the only imports it can resolve live in the `env` module's cs.* allowlist,
- linear memory growth is bounded by `limits.memoryMb`,
- wall time is bounded by `limits.timeoutMs` via wazero's
  `CloseOnContextDone` plumbing.

Modules that try to import `fd_write`, `path_open`, `sock_*`, `proc_*`, or
any non-cs_* symbol fail to instantiate with `CS_RUNTIME_CAPABILITY_DENIED`.

## Guest ABI

The runtime calls a single export, `handle`, after performing any optional
`_initialize` / `_start` setup. The guest must export:

```text
memory                          ;; linear memory
cs_alloc(size: i32) -> i32      ;; returns a guest-owned pointer
cs_free(ptr: i32, size: i32)    ;; optional; called after responses are read
handle(req_ptr: i32, req_len: i32) -> i64
```

`handle` takes a pair of i32 arguments describing the offset and length of
a UTF-8 JSON document inside linear memory. The document has shape:

```json
{
  "event": <publisher-supplied JSON>,
  "ctx": {
    "activation_id": "uuid",
    "deadline_ms": 1730000003000,
    "tenant": "t_abc123",
    "namespace": "payments",
    "function": "reconcile",
    "ref":     { "alias": "prod", "version": 17 },
    "trigger": { "type": "http" },
    "principal": { "sub": "user:123", "roles": ["role:app"] }
  }
}
```

The handler returns an i64 that packs `(ptr << 32) | len` for a buffer
inside linear memory containing the response. The response may be either:

- a full FunctionResponse envelope (`{"statusCode", "headers", "body",
  "isBase64Encoded"}`), or
- a raw string, in which case the host wraps it as a `200 OK` with the
  string as `body`.

A return value of `0` is equivalent to `{"statusCode":200,"body":""}`.

## Host calls

Every host call lives under the `env` module. The shape mirrors the cs-js
host API so a parity fixture written for cs-js compiles unchanged for
cs-wasm at the protocol layer.

| Import (in `env`)      | Signature                                  | Returns                  |
| ---------------------- | ------------------------------------------ | ------------------------ |
| `cs_log_write`         | `(level: i32, msg_ptr: i32, msg_len: i32)` | `i32 status`             |
| `cs_kv_get`            | `(key_ptr: i32, key_len: i32)`             | `i64 packed (ptr, len)`  |
| `cs_kv_set`            | `(kp, kl, vp, vl, ttl_seconds)`            | `i32 status`             |
| `cs_kv_del`            | `(key_ptr, key_len)`                       | `i32 status`             |
| `cs_codeq_publish`     | `(topic_ptr, topic_len, payload_ptr, payload_len)` | `i32 status`     |
| `cs_http_fetch`        | `(req_ptr, req_len)`                       | `i64 packed (ptr, len)`  |

### Status codes

`i32 status` is one of:

| Value | Meaning                                          |
| ----- | ------------------------------------------------ |
|  0    | `StatusOK`                                       |
| -1    | `StatusErrInvalidArg` — bad pointer/length       |
| -2    | `StatusErrCapDenied` — capability gate refused   |
| -3    | `StatusErrHostFailure` — provider error          |
| -4    | `StatusErrNotFound`  — reserved for future use   |

### Packed (ptr, len) return

`i64` returns pack a guest-owned buffer: high 32 bits = pointer, low 32 bits
= length. A return of `0` means "no payload" (either no data was produced
or the call was denied). Output-bearing host calls invoke the guest's
`cs_alloc(size)` to acquire the destination buffer before copying.

### Log levels

`cs_log_write` treats `level=0` as info, `1` as warn, `2` as error.

### HTTP request envelope

`cs_http_fetch` reads a JSON document with shape:

```json
{
  "url": "https://api.example.com/foo",
  "method": "POST",
  "headers": { "x-trace": "abc" },
  "body": "...",
  "isBase64Encoded": false,
  "timeoutMs": 1500
}
```

When `isBase64Encoded` is `true`, `body` is base64-decoded before the
request is sent. The response envelope follows the cs-js shape verbatim:

```json
{
  "status": 200,
  "headers": { "content-type": "text/plain" },
  "body": "base64...",
  "isBase64Encoded": true
}
```

## Manifest schema

```json
{
  "schema": "cs.function.script.v1",
  "runtime": "cs-wasm",
  "runtimeVersion": "wasi-preview1",
  "entry": "module.wasm",
  "handler": "default",
  "limits": { "timeoutMs": 3000, "memoryMb": 64, "maxConcurrency": 1 },
  "capabilities": {
    "kv": { "prefixes": ["ctr:"], "ops": ["get","set"] },
    "codeq": { "publishTopics": ["jobs.*"] },
    "http": { "allowHosts": ["api.example.com"], "timeoutMs": 1500 }
  }
}
```

The control plane validates the manifest at publish time and at execution
time. The `runtime` field must be `cs-wasm` for the wasm adapter to pick
up the bundle. Imports (`manifest.imports`) are JS-only and ignored when
the runtime is `cs-wasm`; wasm modules ship their dependencies inline at
compile time.

## Capability model

The cs-wasm runtime is allowlist-first:

- Host imports: a guest may only import the six `cs_*` host functions
  listed above from the `env` module. Any other import (including a
  memory import, a global import, or a table import) fails to instantiate
  with `CS_RUNTIME_CAPABILITY_DENIED`.
- KV access: `cs_kv_*` consult `manifest.capabilities.kv.{prefixes, ops}`
  and surface denials via `StatusErrCapDenied`.
- codeQ publish: `cs_codeq_publish` consults
  `manifest.capabilities.codeq.publishTopics` (suffix `*` wildcards
  supported).
- HTTP fetch: `cs_http_fetch` consults `manifest.capabilities.http.allowHosts`
  and additionally refuses RFC1918 / loopback hosts unless the runner is
  configured for local-only test runs.

## Limits

### Wall time

The invoker sets `deadline_ms = now_ms + timeoutMs`. wazero's
`CloseOnContextDone` runtime config cancels in-flight wasm execution when
the context deadline elapses; the host returns
`CS_RUNTIME_TIMEOUT`.

### Memory

`limits.memoryMb` upper-bounds the guest's linear memory. Wazero exposes
the page count to host callbacks; modules that grow past the cap fail
with a wazero memory error which surfaces as
`CS_RUNTIME_EXCEPTION`.

### Concurrency

Per-version concurrency is enforced by `cs-invoker-pool`, the same way as
cs-js. The adapter itself does not synchronise across activations.

## Local runner parity

The `cs` CLI embeds the wasm adapter behind the same constructor used by
`cs-invoker-pool`, so a fixture invoked locally and remotely produces
byte-identical results.

## Reserved values and forward-compatibility

- `runtimeVersion` is recommended to be `wasi-preview1` for wasi-targeted
  builds and `wasm32` for `unknown-unknown` builds.
- WASI Preview 2 (`wasi-sockets`, component model) is not in scope for
  this milestone; it will land as `cs-wasm@2` without changing the v1
  manifest contract.
