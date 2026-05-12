# Runtime: cs-python

This document defines the Python runtime contract. `cs-python` runs
user code authored in Python 3, with the same canonical manifest +
bundle layout as `cs-js` and `cs-wasm`, in an isolated subprocess
managed by the cs-invoker-pool.

The v0.1 adapter lives in `internal/runtime/python` and shells out to
the operator-installed `python3` binary on the host. The richer
embedded-CPython adapter — needed for in-process capability gates
matching cs-js — is tracked as a follow-up issue. See the **Deferred
features** section below.

## Runtime shape

A published function version contains two UTF-8 files:

- `function.py`   — the publisher's Python module
- `manifest.json`

The system stores them as a canonical tar bundle and addresses the tar
bundle by `sha256`. The invoker loads the bundle from KVRocks and
spawns a fresh `python3` child for every activation; the child is
torn down when `Execute` returns and nothing persists across
activations.

## Why a subprocess (v0.1)

CPython is hard to embed in Go: linking `libpython` requires CGo and a
matching system-installed development package, and going through a
WASI build of CPython (Pyodide) trades a CGo dependency for a 50 MB
WASM module that has to bootstrap an interpreter on every cold start.

The v0.1 cs-python adapter trades that complexity for a simple
subprocess model:

- zero new `go.mod` dependencies,
- works on every machine that has `python3` on `PATH`,
- isolates user code in its own OS process (kernel-enforced address
  space separation, kernel-enforced wall-time kill via SIGKILL on
  context timeout),
- ships a working cs-python end-to-end today rather than gating the
  whole runtime on the embedded build.

The embedded adapter follow-up replaces the runner body without
touching the registry wiring, the manifest contract, or any client
code.

## Handler contract

`function.py` is loaded as the script's `__main__` module and runs
top-to-bottom. The runtime hands the request payload on stdin and
reads the response from stdout.

```python
import json, sys

req = json.load(sys.stdin)
event = req["event"]
ctx   = req["ctx"]

# user logic
print(json.dumps({
    "statusCode": 200,
    "headers": {"x-cs-runtime": "cs-python"},
    "body": "hello " + event["name"],
}))
```

The handler may print either:

- a full FunctionResponse envelope (`{statusCode, headers, body,
  isBase64Encoded}`), or
- a bare JSON value, in which case the host wraps it as `200 OK` with
  the JSON-encoded value as `body`.

An empty stdout is equivalent to `{"statusCode":200,"body":""}`.

The runtime takes only the **last non-empty line** of stdout as the
response envelope, so progress prints earlier in the run do not need
to be suppressed.

## Context contract

`ctx` is a JSON object with stable fields matching `cs-js` and
`cs-wasm`:

```json
{
  "activation_id": "uuid",
  "deadline_ms":   1730000003000,
  "tenant":        "t_abc123",
  "namespace":     "payments",
  "function":      "reconcile",
  "ref":           { "alias": "prod", "version": 17 },
  "trigger":       { "type": "http" },
  "principal":     { "sub": "user:123", "roles": ["role:app"] }
}
```

## Logging

Every line the child writes to stderr is captured as an activation
log entry, mirroring `cs.log.*` in cs-js. The bootstrap script
installs a `cs_log(level, message)` helper in `builtins` so user code
can write:

```python
cs_log("info", "starting up")
cs_log("warn", "halfway through")
```

without an explicit import. Plain `sys.stderr.write(...)` or
`print(..., file=sys.stderr)` also surface as log lines.

## Capability gates (v0.1 best-effort)

Before user code runs, the runtime installs a `sys.addaudithook` that
raises `PermissionError("CS_RUNTIME_CAPABILITY_DENIED: ...")` when the
child invokes any of:

- `os.system`
- `subprocess.Popen`
- `os.exec*` / `os.fork` / `os.posix_spawn*`
- raw `socket.socket` constructors and `socket.bind` / `socket.connect`
- `ctypes` library loads (`dlopen`, `LoadLibrary`, `addressof`)

The denial maps to the cs-js / cs-wasm equivalent error code
`CS_RUNTIME_CAPABILITY_DENIED` so policy violations look the same
across runtimes.

The `python3` invocation also uses `-I -S` so user-site packages and
`PYTHON*` environment variables are ignored. Process env is scrubbed
to a minimal whitelist (`PATH`, `TMPDIR`, locale, `CS_RUNTIME`) and
the invoker-pool's secret env (E6.01) is **never** exposed via process
env in v0.1.

## Security caveats

The audit hook lives **inside the child Python interpreter**. A
determined adversary can usually defeat an in-process hook through
native-module loading or by rebinding `sys.audit`; the hook is
defence-in-depth, not a hermetic sandbox. The production gate ships
with the embedded adapter follow-up, where capability decisions live
on the Go side of an FFI boundary that the guest cannot inspect.

Operators who need stronger isolation in v0.1 should:

- run `cs-invoker-pool` under a seccomp profile that denies the same
  syscalls the audit hook denies in-process,
- pin the `python3` binary path (drop user-writable shadowing) and
  audit any third-party `.pth` files,
- consider per-activation containers (e.g. gVisor, Firecracker) for
  high-risk tenants.

These hardening steps will remain useful even after the embedded
adapter lands; the runtime contract documents the policy, but defence
in depth at the OS layer is always a force multiplier.

## Wall time and memory caps

- **Wall time** is enforced by `context.WithTimeout(ctx,
  manifest.limits.timeoutMs)`. When the deadline fires, the runner
  cancels the context and `exec.CommandContext` sends SIGKILL to the
  child. The activation returns `status: "timeout"` with
  `CS_RUNTIME_TIMEOUT`.
- **Memory** is bounded by the OS (the child inherits the
  invoker-pool's cgroup limits). The v0.1 adapter does not yet apply
  an `RLIMIT_AS` per activation; doing so reliably across Linux and
  macOS hosts is part of the embedded-adapter follow-up.

## Error mapping

| Outcome                                  | `ExecutionOutput.Status` | `ResolvedCode`                    |
| ---------------------------------------- | ------------------------ | --------------------------------- |
| Successful FunctionResponse              | `success`                | `""`                              |
| Deadline / wall-time timeout             | `timeout`                | `CS_RUNTIME_TIMEOUT`              |
| `python3` not on PATH                    | `error`                  | `CS_RUNTIME_EXCEPTION`            |
| Bundle missing `function.py`             | `error`                  | `CS_VALIDATION_MANIFEST`          |
| Manifest invalid                         | `error`                  | `CS_VALIDATION_MANIFEST`          |
| Capability-denied (audit hook)           | `error`                  | `CS_RUNTIME_CAPABILITY_DENIED`    |
| Any other non-zero exit                  | `error`                  | `CS_RUNTIME_EXCEPTION`            |
| stdout is not a JSON FunctionResponse    | `error`                  | `CS_RUNTIME_EXCEPTION`            |

The error message in the activation record is the **tail** of stderr
(capped at `maxErrorBytes`), so operators see a real Python traceback
rather than a bare exit status.

## Result truncation

The cs-python adapter applies the same `maxResultBytes`,
`maxErrorBytes`, and `maxLogBytes` caps as the cs-js / cs-wasm
adapters. When stdout exceeds the result cap or stderr exceeds the log
cap, the runtime sets `ExecutionOutput.Truncated = true`; the invoker
stamps the documented `X-CS-Truncated` header on synchronous
responses (see `docs/26-capacity-and-limits.md`).

## Deferred features (follow-up adapter)

The v0.1 subprocess MVP intentionally omits the richer `cs.*` host
surface present in cs-js. The follow-up issue tracks:

- **cs.kv.get / cs.kv.set / cs.kv.del** — Python helpers that talk to
  the cs-invoker-pool KV provider, gated on the manifest's
  `capabilities.kv` allowlist.
- **cs.codeq.publish** — Python helpers that publish to the codeQ
  broker, gated on `capabilities.codeq.publishTopics`.
- **cs.http.fetch** — egress shim with per-tenant allowlist (E6.02)
  and private-IP block, plus `X-CS-Parent-Activation` injection for
  call-tree observability.
- **cs.env.get / cs.env.list** — secret injection (E6.01) without
  routing material through process env.
- **In-process capability gates** — replace the `sys.addaudithook`
  defence-in-depth with FFI-boundary enforcement that user code
  cannot inspect or rebind.
- **Cold-start latency** — embed CPython directly so an activation
  does not pay the cost of forking, loading the Python startup files,
  and parsing the bootstrap on every invocation.

When that adapter lands, the manifest contract and the
`runtime.Executor` interface stay the same; only the implementation
body of `internal/runtime/python.Runner.Execute` is replaced.

## See also

- [`08-runtime-cs-js.md`](08-runtime-cs-js.md) — the cs-js runtime
  contract and registry seam.
- [`08b-runtime-cs-wasm.md`](08b-runtime-cs-wasm.md) — the cs-wasm
  adapter and the WASM host ABI.
- [`15-security.md`](15-security.md) — the platform-wide threat model
  and the network egress / private-IP rules.
- [`02-requirements.md`](02-requirements.md) — the no-fs /
  no-process / deny-egress-by-default invariants every runtime adapter
  must satisfy.
