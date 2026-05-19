# Adding a runtime

A runtime in Sous is an implementation of the runtime registry contract declared in `internal/runtime/registry.go`.

It is the component that, given a tar bundle and an invocation envelope, evaluates the user's code and returns an `ExecutionOutput`.
The control plane resolves a runtime by name.
The manifest's `runtime` field is matched against a process-wide registry.
Adding a runtime to Sous therefore means registering an adapter with that registry and implementing the execution contract.

This page is the contract every new adapter must meet.
It exists because the platform makes a hard promise to callers.
The capability model, the egress posture, the timeout budget, the deterministic-mode constraints, and the parity guarantees are identical across runtimes.

A new adapter that meets those constraints is a candidate for inclusion.
An adapter that bends them — even slightly, for a "convenient" deviation — is not.

## The three existing adapters

The three existing adapters are the reference for what "in-tree" looks like.

**cs-js** at `internal/runtime/runner.go` is the in-tree JS adapter.
It is implemented directly on `*Runner` and uses Goja for in-process JS evaluation.
It is the implicit default for an empty `runtime` field.
The dispatch helper short-circuits to it before consulting the registry.

**cs-python** at `internal/runtime/python/runner.go` is the `python3` subprocess adapter.
It landed under E3.01.
It marshals the invocation envelope over stdin and stdout and parses a JSON response.
Registration happens through `internal/runtime/python/register.go`.

**cs-wasm** at `internal/runtime/wasm/runner.go` is the wazero-backed wasm adapter.
It landed under E3.02.
It validates imports against a stable ABI allowlist (`internal/runtime/wasm/abi.go`) and executes the bundle in-process.

All three implement the same `Executor` interface.
All three are registered through an `init()` function in their package.
All three pass the same parity fixtures.

Read them before you start.

## The registry contract

The registry is small and lives in one file: `internal/runtime/registry.go`.

There are exactly two interfaces an adapter cares about.

### `Handler`

```go
type Handler interface {
    Name() string
}
```

`Name()` returns the canonical slug — `cs-js`, `cs-python`, `cs-wasm`.

The control plane uses this slug to reject publishes whose manifest names a runtime nobody has installed in the binary.
A handler that only implements `Name()` (a "slot") is enough for the control plane to accept the publish.
It cannot execute anything, however.

### `Executor`

```go
type Executor interface {
    Handler
    Execute(ctx context.Context, bundleBytes []byte, request api.InvocationRequest) ExecutionOutput
}
```

`Execute` is the entire execution surface.

It receives the raw tar bundle and the invocation request.
It returns an `ExecutionOutput` value defined in `internal/runtime/runner.go`:

```go
type ExecutionOutput struct {
    Status       string
    Result       *api.FunctionResponse
    Error        *api.InvocationError
    Logs         []string
    Truncated    bool
    DurationMS   int64
    ResolvedCode cserrors.Code
}
```

A new adapter implements `Name()` and `Execute()` on its own concrete type.
That is the full interface obligation.

Note that the older verb triad (`LoadBundle` / `Invoke` / `Cleanup`) from an earlier design is no longer the contract.
The surface is intentionally a single `Execute` call that owns its own lifecycle from bundle to result.

## How registration works

Adapters register themselves at process start through an `init()` function.

The pattern is the same across all three reference adapters.
The cs-python registration in `internal/runtime/python/register.go` is the shortest example:

```go
package python

import rt "github.com/osvaldoandrade/sous/internal/runtime"

func init() {
    rt.DefaultRegistry.Register(NewRunner(nil, nil, 0, 0, 0))
}
```

`DefaultRegistry` is the process-wide registry.

`Register` is last-write-wins.
A real adapter registered later overrides the slot stub that `registry.go`'s own `init()` installs for `cs-js` and `cs-wasm`.
The slot stubs exist so the control plane accepts publishes for those runtimes even in unit-test binaries that have not imported the concrete adapter package.

The net effect: importing the adapter package is enough to wire the runtime in.

Binaries that want the runtime add a blank import.
For example: `_ "github.com/osvaldoandrade/sous/internal/runtime/python"`.

Binaries that do not want it (test binaries, the control plane's own publish path) get a slot stub.
The stub surfaces `CS_RUNTIME_UNSUPPORTED` if a caller actually tries to execute.

When an `Execute` call lands in the cs-js `*Runner.Execute` path with a non-cs-js runtime name, `selectRunner` consults `DefaultRegistry`.
It type-asserts the handler to `Executor` and delegates.
A slot stub fails that type assertion and the call returns `CS_RUNTIME_UNSUPPORTED`.

There is no other dispatch path.

## What the platform requires

The interface surface is small, but the platform's behavioural contract is not.

A new runtime must meet every constraint below.
The reference adapters meet them today.
The parity harness locks them in across the matrix.

### Capability model

User code reaches the outside world only through host APIs that the runtime explicitly binds.

cs-js exposes `cs.kv`, `cs.codeq`, `cs.http`, `cs.cadence`, `cs.env`, and `cs.log` — nothing else.

A new runtime must expose the same surface.
The names must match.
The arguments must match.
The error shapes must match.
The capability gates (manifest `kv.allowKeys`, `codeq.allowTopics`, `http.allowHosts`, etc.) must be honoured identically.

The capability model is documented in [Concepts: Capabilities and Isolation](Concepts-Capabilities-and-Isolation).

If your runtime exposes a host API that another runtime does not, you have widened the platform contract.
That is a design change, not an adapter.

### Isolation

User code does not get unrestricted access to the host.

The cs-js adapter runs in a Goja isolate with no filesystem, no process spawn, no host network, and no native modules.
Egress goes through the bound `cs.http` helper that enforces the manifest allowlist and the private-IP block.

The cs-wasm adapter runs in wazero with a validated import list.
The list admits only the ABI under `internal/runtime/wasm/abi.go`.

The cs-python adapter runs CPython as a subprocess in a scratch directory with a curated environment (`childEnv` in `runner.go`).
It cannot reach the parent's secrets, sockets, or environment.

A new runtime must enforce an equivalent posture.
No host syscalls without a capability check.
No implicit egress.
No shared filesystem with the parent.

### Wall-time and memory caps

Every `Execute` call carries a `deadline_ms` in the invocation envelope.
The call runs inside a `context.Context` that the invoker cancels on deadline.

The adapter must propagate cancellation to the user code.
It must return a typed error when wall time is exceeded (`CSTimeout`).

Memory caps are enforced by the runtime layer.
Goja handles it for cs-js.
Wazero memory limits handle it for cs-wasm.
OS-level limits handle it for cs-python.

Any new runtime must offer a comparable knob.

### Result and log truncation

`ExecutionOutput.Truncated` reports whether the runtime had to drop bytes from the result or the log stream.

The byte budgets (`maxResultBytes`, `maxErrorBytes`, `maxLogBytes`) are passed into `NewRunner`.
They must be respected exactly.

The cs-js `boundedBuffer` (and its python and wasm counterparts) are the reference implementation.
Copy the pattern.

### Deterministic mode for workflow handlers

When a function is invoked as a workflow handler, the runtime must reject non-deterministic constructs.
That covers random, wall-clock time, and network calls outside the workflow API.

The publish-time static linter under `internal/cadence/determinism` catches the obvious cases for cs-js.
Per-runtime checks layer on top.

A new runtime that wants to host workflow handlers must implement determinism at the language level.
See E8 and the cs-js implementation in `internal/cadence/workflow/executor.go`.

### Parity harness

Every registered runtime is automatically dispatched against every fixture under `test/parity/fixtures/` by the suite in `internal/runtime/parity`.

A new adapter joins the matrix the moment its `init()` registers.
If any fixture diverges, the build fails.

New runtimes must pass the full fixture set unmodified.
Adding a new fixture is fine.
Subsetting an existing one is not.

See [Testing](Testing) for the fixture format.

## The bar for inclusion

Sous deliberately ships one authoritative adapter per language.

cs-js is the authoritative JavaScript runtime.
cs-python is the authoritative Python runtime.
cs-wasm is the authoritative WebAssembly runtime.

A new adapter is a candidate for inclusion only if it covers a language or execution surface that no existing adapter covers.

That bar exists for a concrete reason.

Every runtime in the tree carries an ongoing parity tax.
Every fixture, every host API, every capability check has to be reimplemented and kept in sync.
Two adapters for the same language means two parity surfaces to maintain.

The platform is happier with one reference adapter per language and a clear extension story (host APIs, dependency bundles, secret backends) for the cases that motivate a flavoured variant.

### Practical examples

A "cs-python-fast" adapter that uses Pyodide instead of CPython is not a separate adapter.
It is a configuration of the cs-python adapter, gated behind a manifest flag or a deploy option.

A "cs-js-bun" adapter that swaps Goja for Bun would need to replace cs-js, not run alongside it.

Proposals that fit those shapes should open an `epic`-labelled issue first.
The design conversation usually decides whether the variant is an internal switch or a wholesale replacement before any code lands.

## A checklist for a new adapter

A new in-tree adapter is ready for review when every box below is ticked.

- A new package under `internal/runtime/<name>/` with a `runner.go` that defines a concrete type with `Name()` and `Execute()` methods.
- A `register.go` in the same package whose `init()` calls `runtime.DefaultRegistry.Register(NewRunner(...))`.
- A runtime slug added to `internal/api` (the `RuntimeCS<Name>` constant) so manifest parsing accepts it.
- Capability bindings that match the cs-js surface byte-for-byte: `cs.kv`, `cs.codeq`, `cs.http` (with allowlist and private-IP block), `cs.cadence`, `cs.env`, `cs.log`.
- Wall-time and memory caps wired to the same `NewRunner` knobs the reference adapters use.
- Result, error, and log truncation that honour `maxResultBytes`, `maxErrorBytes`, `maxLogBytes` and set `ExecutionOutput.Truncated`.
- A unit test next to `runner.go` covering at least the happy path, the capability-denied path, the timeout path, and the bundle-error path.
- A blank import in the binaries that should host the runtime (typically `cmd/cs-invoker-pool/main.go`).
- A green run of `make test-parity` with the new adapter active in the matrix.
- A roadmap task issue under E3 (or its successor epic) tracking the work, with the standard `E{epic}.{task}: {summary}` commit prefix on every PR.

The contract is narrow on purpose.
Meet it, pass the harness, and the new runtime is indistinguishable from the in-tree set from a caller's perspective.

That is the point.
