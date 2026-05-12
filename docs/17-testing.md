# Testing

This file defines test layers and acceptance gates.

## Unit tests

- Manifest parsing and validation.
- KVRocks key builders.
- Tikti authorization evaluator.
- codeQ envelope marshal and unmarshal.
- HTTP event mapping.

## Integration tests

### Local docker-compose

The repo provides `docker-compose.yml` for:

- KVRocks
- codeQ
- Tikti mock
- code-flow mock (Cadence API subset)
- code-sous services

### Test cases

- Publish a version and invoke via API.
- Invoke via HTTP gateway and validate response mapping.
- Create a schedule and validate tick publishes.
- Register WorkerBinding and validate ActivityTask flow.

## End-to-end tests

- Run `cs` CLI against a real cluster.
- Create function, publish, set alias.
- Invoke by alias and read activation logs.
- Run schedule for 60 ticks and verify activation count.
- Run cadence activity and verify completion.

## Fuzz tests

- Fuzz HTTP headers and query parsing.
- Fuzz manifest JSON fields.
- Fuzz sandbox return types.

## Chaos tests

- Kill invoker pods during execution.
- Kill poller pods during inflight tasks.
- Inject codeQ delays and drops.

## Runtime parity harness

The runtime-parity invariant in `docs/02-requirements.md` says the CLI,
the server, and every adapter (`cs-js`, `cs-python`, `cs-wasm`) must
expose identical host APIs and return identical response shapes for the
same input. The harness under `internal/runtime/parity` enforces that
invariant by loading a shared fixture corpus and dispatching each
fixture against every runtime registered with
`runtime.DefaultRegistry`.

### Layout

- `internal/runtime/parity/parity.go` — `Suite` orchestrator and the
  `Executor` interface adapters plug into.
- `internal/runtime/parity/fixture.go` — JSON `Fixture` loader.
- `internal/runtime/parity/assertions.go` — permissive deep-compare
  with normalisation for timing, header casing, and JSON int/float
  drift.
- `test/parity/fixtures/*.json` — the corpus. Each fixture declares its
  manifest, per-runtime guest code, trigger, event, and the golden
  shape the runtime must produce.

### Running the suite

```bash
make test-parity            # runs the full matrix
go test ./internal/runtime/parity/... -run TestSuiteMatrix -v
```

Subtests are keyed `cs-js/simple-echo`, `cs-js/kv-roundtrip`, etc., so
`go test -run TestSuiteMatrix/cs-js/simple-echo` narrows down to a
single matrix cell.

### Adding a new scenario

1. Drop a JSON file under `test/parity/fixtures/`. Use the existing
   fixtures as a template; the keys are documented on the `Fixture`
   struct in `internal/runtime/parity/fixture.go`.
2. Provide a `files` entry for every runtime the scenario should cover.
   A missing entry produces a *skip*, not a failure, so cs-js fixtures
   land today and cs-python / cs-wasm fixtures can follow without
   blocking the suite.
3. Keep guest code deterministic — no wall-clock reads, no random IDs,
   no sleeps. Timing-dependent scenarios (timeout) use the explicit
   `deadlineOffsetMs` field instead.

### Adding a new adapter

1. Register a `Handler` with `runtime.DefaultRegistry` in the adapter's
   `init()`. From that moment, every fixture that has a `files` entry
   for the new slug is dispatched against it automatically; the suite
   reads `runtime.DefaultRegistry.Names()` at run time.
2. Implement the `Executor` interface (`Execute(ctx, bundleBytes,
   request) ExecutionOutput`) if the adapter cannot reuse the default
   in-process runner. Inject it via `Suite.NewRunner`.
3. Port the cs-js fixtures by adding a per-runtime `files` entry; the
   golden output should already match because the contract is
   runtime-agnostic.

### Normalisation rules

- `responseShape` is matched key-by-key; only the keys named in the
  fixture are compared.
- Numeric values are normalised through JSON round-trip so `200` and
  `200.0` compare equal.
- `logSubstrings` asserts *presence*, not order or exclusivity, so a
  runtime that adds debug noise on top of the contract still passes.
- `headers` is matched key-by-key with case-sensitive values; cross-
  runtime case folding is the gateway layer's problem.
- `resolvedCode` locks the typed `cserrors.Code`, which is the parity
  guarantee for negative scenarios (capability denied, timeout, import
  not found, ...).
