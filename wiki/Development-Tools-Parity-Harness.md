# Development Tools: Parity Harness

SOUS makes a strong claim about its multi-runtime story: a function authored in `cs-js` is replaceable with an equivalent `cs-python` or `cs-wasm` function with identical observable behavior. Given the same input and the same KV state, every runtime must return the same response body, emit the same typed errors, write the same log lines in the same order, and produce the same KV side effects. The parity guarantee is what makes the runtime choice a publish-time concern — authors pick the language that fits the workload, and the platform handles the rest.

The parity harness, introduced in roadmap task E3.04, is the mechanism that enforces this guarantee in CI. It is a shared fixture set, a small Go driver, and a matrix runner. The fixtures live in `test/parity/fixtures/`; the driver lives in `internal/runtime/parity/`; the runner is `go test ./internal/runtime/parity/...` (or `make test-parity`). Every fixture is dispatched against every runtime registered with `runtime.DefaultRegistry`. A fixture that passes for `cs-js` but drifts on `cs-python` fails the build. Drift across runtimes is the only behavior the harness exists to catch.

The harness is not a replacement for unit tests, integration tests, or the local CLI's `cs fn test`. Unit tests cover the internals of each adapter; integration tests cover the full publish-and-invoke cycle against a running cluster; the local CLI covers the developer inner loop. The parity harness sits orthogonal to all of these — it is the single integration point where the platform's runtime invariant is mechanically enforced. Authors who write a new fixture think about parity; reviewers who land a new adapter think about parity; CI gates merges on parity. Everything else is in the service of catching the parity drift earlier or later, but the parity harness is where the contract is written down.

## The shared fixture format

A parity fixture is a single JSON document. The schema is defined by the `Fixture` struct in `internal/runtime/parity/fixture.go`. It carries five fields the harness needs to construct an `api.InvocationRequest` and one field that captures the golden output.

The `name` field is the fixture identifier. The harness uses it as the Go subtest name (`go test -run TestSuiteMatrix/cs-js/simple-echo` filters to one cell) and as the sort key for deterministic iteration order. Names should be short, dash-separated, and unique across the corpus.

The `description` field is a human-readable one-liner. It appears in failure messages and in the eventual per-runtime parity report.

The `manifest` field is the publish-time `api.FunctionManifest`. The harness overwrites `manifest.runtime` with the matrix cell's runtime slug, so fixture authors leave it as `cs-js` (or omit it). Everything else — the entry, the handler, the limits, the capabilities — is preserved verbatim. A fixture that wants to test capability enforcement sets `capabilities.kv.prefixes` and `capabilities.kv.ops` to the minimal allowlist; a fixture that wants to test timeout enforcement sets `limits.timeoutMs` to a small number and supplies a `deadlineOffsetMs` of `-1`.

The `files` field is the per-runtime guest code. Its type is `map[runtime]map[path]content` — the outer key is a runtime slug (`cs-js`, `cs-python`, `cs-wasm`), the inner key is the bundle-relative path, and the value is the file content as a string. A fixture that supplies only a `cs-js` entry is skipped for `cs-python` and `cs-wasm` cells. This forward-compatibility is intentional: the `cs-js` corpus lands first, then each new adapter (`cs-python`, `cs-wasm`) is exercised by adding the equivalent guest code to existing fixtures rather than by rewriting the corpus.

The `trigger` and `event` fields populate `api.InvocationRequest.Trigger` and `api.InvocationRequest.Event`. Most fixtures use the default `{"type": "api"}` trigger and a JSON event the handler echoes. Timing fixtures use `deadlineOffsetMs` to express deadlines relative to "now" at run time — zero means "manifest TimeoutMS from now," and negative values express deadlines that have already elapsed.

The `expected` field is the golden output. Its schema is the `Expected` struct in `internal/runtime/parity/fixture.go`. All of its fields are optional and only the keys explicitly set are compared, so a fixture that asserts on `statusCode` and `body` does not need to enumerate every header. The fields the assertion engine understands are `status` (one of `success`, `error`, `timeout`), `resolvedCode` (the cserrors slug, e.g. `CS_RUNTIME_CAPABILITY_DENIED`), `responseShape` (a partial map of response keys), `logSubstrings` (each substring must appear somewhere in the captured logs), `errorSubstrings` (each must appear in the error message), and `headers` (each key/value must match the response headers, case-insensitively).

A complete fixture from `test/parity/fixtures/simple-echo.json` looks like this:

```json
{
  "name": "simple-echo",
  "description": "Returns the event verbatim with a deterministic header.",
  "manifest": {
    "schema": "cs.function.script.v1",
    "runtime": "cs-js",
    "entry": "function.js",
    "handler": "default",
    "limits": {"timeoutMs": 2000, "memoryMb": 64, "maxConcurrency": 1},
    "capabilities": {
      "kv":    {"prefixes": ["ctr:"], "ops": ["get", "set", "del"]},
      "codeq": {"publishTopics": ["jobs.*"]},
      "http":  {"allowHosts": ["example.com"], "timeoutMs": 1500}
    }
  },
  "files": {
    "cs-js": {
      "function.js": "export default async function handle(event, ctx) { return { statusCode: 200, headers: {\"x-parity\":\"1\"}, body: JSON.stringify(event) } }"
    }
  },
  "trigger": {"type": "api", "source": {}},
  "event": {"hello": "world"},
  "expected": {
    "status": "success",
    "responseShape": {
      "statusCode": 200,
      "body": "{\"hello\":\"world\"}"
    },
    "headers": {"x-parity": "1"}
  }
}
```

## The shared assertion model

The comparison rules live in `internal/runtime/parity/assertions.go`. They are deliberately permissive on the things runtimes legitimately differ on (timing, log slice headers, header casing) and strict on the things the parity invariant cares about (response body, error code, log content, KV side effects).

The assertion engine compares the runner's `runtime.ExecutionOutput` against the fixture's `Expected` field by field. The `status` check is exact: `success` matches `success`, nothing else. The `resolvedCode` check is exact when set. The `responseShape` check is a partial deep-equal — every key in the expected map must appear in the response with the same value, but the response may carry extra keys. The `logSubstrings` and `errorSubstrings` checks are substring matches against the captured logs and error message, in iteration order. The `headers` check normalizes keys to lowercase before comparing.

Drift is reported as a list of `Mismatch` records, each carrying the field name, the expected value, and the actual value. The harness prints one `t.Errorf` per mismatch so a CI run surfaces the full diff instead of one-shotting on the first miss. The matrix subtest naming (`TestSuiteMatrix/<runtime>/<fixture>`) makes it trivial to filter down to a single failing cell when investigating.

## Adding a new fixture

Adding a fixture is a three-step operation. The first step is to write the JSON document under `test/parity/fixtures/<name>.json`. The schema is enforced by `LoadFixture` in `fixture.go` — a missing `name`, a missing `files` map, or a malformed JSON file fails the load with a path-prefixed error so the offending file is obvious in CI logs.

The second step is to author the guest code. The minimum is a `cs-js` entry in the `files` map; the corpus accepts `cs-js`-only fixtures and skips them silently for runtimes that have not yet been ported. The recommended practice for new fixtures is to add the `cs-python` and `cs-wasm` equivalents in the same PR — every fixture that lands as `cs-js`-only is a parity coverage gap that someone has to backfill later.

The third step is to verify locally. Run `make test-parity` from the repo root. The Go test driver loads the corpus, dispatches every fixture against every registered runtime, and prints the per-cell result. A fixture that lands cleanly produces N green subtests (one per runtime). A fixture that drifts produces a `parity drift` line per affected cell with the field-level mismatches enumerated underneath.

The corpus today (the JSON files under `test/parity/fixtures/`) covers the load-bearing parity scenarios: `simple-echo` for the happy path, `kv-roundtrip` for KV state, `log-emission` for log capture, `capability-denied` for typed-error parity, and `timeout` for deadline parity. Future fixtures should follow the same naming convention — short kebab-case slugs that describe what the fixture asserts.

## Running locally

The canonical way to run the harness is the Makefile target:

```
make test-parity
```

This expands to `go test ./internal/runtime/parity/...` with the default verbosity. To filter to a single runtime cell, use Go's standard `-run` flag:

```
go test ./internal/runtime/parity/... -run TestSuiteMatrix/cs-js
```

To filter to a single fixture across all runtimes:

```
go test ./internal/runtime/parity/... -run TestSuiteMatrix/.*/simple-echo
```

To run with verbose output (one line per cell, including skips):

```
go test -v ./internal/runtime/parity/...
```

The harness loads every fixture and dispatches every registered runtime on every run — there is no `-tag` gate or `-short` skip. The full matrix runs in under a second on a developer laptop because each cell is an in-process `runtime.NewRunner` call; the only matrix cell that pays subprocess cost is `cs-python`, and `cs-python` fixtures only execute when `python3` is present on PATH.

## How runtimes opt in

A new runtime adapter joins the parity matrix by registering itself with `runtime.DefaultRegistry` in its package `init()`. The harness reads `runtime.DefaultRegistry.Names()` at run time, so adapters auto-join without touching the parity package or the fixture corpus. The forward-compatibility test `TestSuiteSkipsRuntimesWithoutFiles` in `internal/runtime/parity/parity_test.go` documents the auto-join contract: adding a new runtime to `DefaultRegistry` produces skips for fixtures that have not been ported yet, not failures. The intent is that adapter teams can land the runtime and the parity wiring in the same PR; backfilling per-runtime guest code into the corpus is then a follow-up PR that converts skips into green cells.

The Executor surface the harness depends on is `internal/runtime/parity/parity.go`'s `Executor` interface — a single `Execute(ctx, bundleBytes, request)` method that mirrors `runtime.Runner.Execute`. Adapter teams that need to swap the runner (for example, to point a `cs-python` cell at a recording KV instead of the in-memory default) inject a `NewRunner func(runtimeName string) Executor` into the `Suite` struct. The default executor returns the in-process `runtime.NewRunner`, which today is sufficient because the cs-js, cs-python, and cs-wasm adapters all register on `runtime.DefaultRegistry` and the runner routes by `manifest.Runtime`.

## Reading a parity drift

When the harness reports drift, the failure shape is intentionally easy to read. A `parity drift: runtime=cs-python fixture=kv-roundtrip` line identifies the matrix cell. Underneath, each `t.Errorf` line is one `Mismatch` record — a field name, the expected value, the actual value. The author scans the list, identifies the offending field, and works backward into the adapter or the fixture.

A drift in the `response.body` field usually points at a serialization difference. The cs-js adapter encodes JSON one way; the cs-python adapter encodes it another. Most of these surface as key-ordering or trailing-whitespace differences and are resolved by tightening the encoder in the adapter under test, not by widening the assertion.

A drift in the `logs` field usually points at a log-format difference. The cs-js adapter formats log lines with one prefix; the cs-python adapter formats them with another. The remediation is to canonicalize the prefix at the adapter boundary so the harness sees the same byte sequence regardless of which runtime produced it.

A drift in the `error.code` field is the most serious. It means the two adapters surface different typed errors for the same condition — for example, cs-js returning `CS_RUNTIME_CAPABILITY_DENIED` and cs-python returning `CS_RUNTIME_INTERNAL`. The fix is always in the adapter, never in the fixture; the typed error model is the user-facing contract, and an adapter that fails to honor it is broken.

A drift in the `headers` field is usually a casing difference (`X-Parity` vs `x-parity`). The harness already normalizes casing, so a real drift here is a genuine header difference. The remediation is to align the adapter, typically by following the cs-js precedent because cs-js was the reference adapter when the contract was written down.

## When to widen an assertion

Most drifts are bugs to fix. Occasionally a drift represents a legitimate runtime-specific tolerance — a timing field that is necessarily microseconds different across adapters, a debug-log line that one adapter emits and another does not. The harness handles these by leaving the field out of the `Expected` struct entirely. If a fixture does not assert on `durationMs`, the harness does not check it.

For a more nuanced tolerance — "the body must match modulo trailing whitespace" — the harness does not currently support custom comparators. The expectation is that the adapter normalizes the output before it leaves the runtime, so the harness sees byte-identical output. Adding a custom-comparator hook would let authors paper over real bugs; the design choice is to keep the assertion strict and force the adapter to converge.

The exception is timing fixtures. The `timeout` fixture exercises deadline behavior; the exact `durationMs` of a timed-out invocation legitimately differs across runtimes because the cancellation mechanism is adapter-specific. The fixture asserts on `status: "timeout"` and `error.code` but not on `durationMs`. This is the model — assert on the things the contract guarantees, leave the things the contract does not guarantee out of the assertion entirely.

## The corpus today

The fixtures under `test/parity/fixtures/` cover the load-bearing parity scenarios for v0.1. `simple-echo` is the canonical happy path — the function returns the event verbatim with a deterministic header. `kv-roundtrip` exercises the KV host API across `Get`, `Set`, and `Del` with prefix validation. `log-emission` checks that `cs.log.info` produces an identical log line in every runtime. `capability-denied` checks that a function trying to access a KV key outside its prefix allowlist surfaces the same typed error in every runtime. `timeout` checks that a handler that exceeds `manifest.limits.timeoutMs` returns `status: "timeout"` in every runtime.

The corpus grows as the platform grows. Every new host API — `cs.codeq.publish`, `cs.http.fetch`, `cs.workflow.*`, `cs.secrets.get` — needs a parity fixture before the adapters can claim it as supported. Every new error code — `CS_RUNTIME_QUOTA_EXCEEDED`, `CS_RUNTIME_NET_HOST_DENIED`, `CS_WORKFLOW_NON_DETERMINISTIC` — needs a parity fixture that exercises it. The corpus is the executable inventory of what the platform's runtime contract actually covers; gaps in the corpus are gaps in the contract.

The maintenance burden of the corpus is small because the fixtures are JSON and the harness reads them deterministically. Adding a fixture is a single new file under `test/parity/fixtures/<name>.json`. Removing a fixture is a single `git rm`. The Go driver does not change — it discovers new fixtures on the next test run and dispatches them across every registered runtime. The cost of expanding parity coverage is the cost of writing the per-runtime guest code, which is typically a few dozen lines per fixture per runtime.

## Relationship to the rest of the testing pyramid

The parity harness lives at the integration tier of the testing pyramid. Below it sit the per-adapter unit tests in `internal/runtime/js/`, `internal/runtime/python/`, and `internal/runtime/wasm/`. Above it sit the end-to-end tests in `test/integration/` that exercise the full publish-and-invoke cycle against a running cluster. Each tier catches a different class of regression:

The per-adapter unit tests catch regressions in a single adapter. They run fast (milliseconds), they cover the adapter's internal correctness, and they fail localized to the adapter under test.

The parity harness catches regressions in the cross-adapter contract. It runs fast (sub-second), it covers the platform's runtime invariant, and it fails localized to the matrix cell that drifted.

The integration tests catch regressions in the full path. They run slow (seconds), they cover the platform's external behavior, and they fail in the way the user would experience the failure.

A change that touches an adapter typically updates the per-adapter unit tests first, runs the parity harness to confirm the change is contract-compatible, and then waits for the integration tier in CI to confirm the change survives the full deployment path. The three tiers are complementary; skipping any of them shifts the cost of catching the regression to later in the pipeline.

## A note on cs-python and cs-wasm coverage

The corpus today is cs-js-complete. Every fixture has a cs-js entry in `files`. The cs-python and cs-wasm coverage is partial: some fixtures have a cs-python entry, some have a cs-wasm entry, some have neither. The forward-compatibility test `TestSuiteSkipsRuntimesWithoutFiles` documents the behavior: a fixture without a per-runtime entry is skipped for that runtime, not failed.

Skipping is a known-coverage gap, not a passing cell. The eventual goal is for every fixture to have a complete per-runtime files map so the matrix has no skips. The path there is incremental — each PR that touches the corpus is expected to backfill at least one per-runtime entry, and each PR that lands a new adapter is expected to convert the corpus's `cs-js`-only fixtures into multi-runtime fixtures.

The discipline matters because skips are silent. A cell that is skipped does not produce a failing CI signal; it produces a green test that quietly excludes the runtime from the contract. The roadmap tracks the cs-python and cs-wasm coverage gaps as explicit tasks (E3.02 and E3.03 respectively); the corpus is the source of truth for what those tasks still owe.

## Failure attribution

When the parity harness fails in CI, the responsible party depends on the shape of the failure. A drift on a `cs-js` cell is the cs-js adapter team's problem — the cs-js path is the reference implementation, so a cs-js drift means the adapter changed behavior in a way the contract did not anticipate. A drift on a `cs-python` or `cs-wasm` cell is that adapter team's problem — the cs-js cell is unchanged, so the new runtime is what diverged.

A drift on every cell of the same fixture is the fixture's problem — the corpus is asserting on something the platform does not actually guarantee, and the fix is to either widen the assertion (by removing the offending field from the `Expected` struct) or to tighten the contract (by adding a normalizer to every adapter). The latter is the more disciplined fix because it preserves the parity invariant; the former is the pragmatic escape hatch when the assertion was wrong in the first place.

A drift on a fixture that no other fixture drifts on is usually a real bug. A drift on every fixture is usually a regression in the harness itself — typically a comparison function that became too strict or a load order that introduced a dependency. The harness has its own tests in `internal/runtime/parity/parity_test.go` that catch most of these; a corpus-wide drift that the harness's own tests did not catch is a signal that the harness coverage itself needs to grow.

## The bundle building path

Every cell of the matrix builds its own bundle. The harness does not pre-build a bundle and reuse it across runtimes — the manifest's `runtime` field must match the cell, so the bundle is regenerated per cell. The `buildFixtureBundle` function in `parity.go` does the work: it picks the per-runtime files map from `fx.Files[runtime]`, overwrites `manifest.Runtime` with the cell's slug, marshals the manifest, merges it into the files map, and hands the result to `bundle.BuildCanonical`. The output is the canonical tar bytes that the runner sees.

This per-cell rebuild matters for the parity invariant. A platform that lets the manifest's runtime drift from the actual adapter on the receiving end would silently violate the contract. The harness's per-cell rebuild forces the manifest to be honest, and `bundle.BuildCanonical` produces the same byte-identical tar layout that the publish path produces, so the runner sees exactly the same input it would see in production.

The deterministic InvocationRequest that the harness constructs in `buildRequest` is another piece of the same discipline. The request carries fixed `ActivationID`, `RequestID`, `Tenant`, `Namespace`, `Ref`, `Principal`, and `DeadlineMS` values derived from the fixture name and the manifest's `TimeoutMS`. There is no hidden dependency on `uuid.New()` or `time.Now()` — every value the runner observes is reproducible across runs and across machines.

## Per-runtime files maps

The `files` field on a fixture is the key place where per-runtime variation is allowed. The fixture author writes the cs-js handler, the cs-python handler, and the cs-wasm module separately; the harness picks the right one per cell. The shape lets fixtures express scenarios that have legitimate per-language idioms without forcing every adapter to look like cs-js.

A cs-js handler returns `{ statusCode, headers, body }` from an async function. A cs-python handler returns the same shape from a `def handle(event, ctx)` callable. A cs-wasm module exports the same shape through the cs-wasm ABI. The fixture's `expected` field asserts on the shape every runtime is expected to produce, so the handlers must converge on the response even though their source code differs.

Per-runtime files maps also let fixtures express runtime-specific edge cases. A fixture that exercises the cs-python `__import__` boundary has a cs-python entry but no cs-js entry. The harness skips the cs-js cell with `SupportsRuntime("cs-js") == false`, runs the cs-python cell, and asserts on the result. This is the same auto-skip mechanism that lets the corpus accommodate runtimes at different maturity levels — the discipline cuts both ways.

## Authoring a parity-equivalent handler

When an author writes the per-runtime guest code for a new fixture, the goal is byte-equivalent observable behavior. The cs-js handler and the cs-python handler should differ only in the language idioms — the response shape, the error codes, the log lines, and the KV side effects must converge.

The recommended approach is to write the cs-js handler first because cs-js is the reference adapter and its behavior is the most-tested. Then port the handler to cs-python by translating each operation one-to-one: a JSON response becomes a `dict`, an async function becomes a `def`, a `cs.kv.set` call uses the same key and value bytes. Then port the handler to cs-wasm by compiling a Rust or AssemblyScript source through the cs-wasm ABI. At each step, run `make test-parity` and compare the cells; converge until every cell is green.

The fixture's `expected` field is the contract. The handler authors are free to use any language idioms internally — `await Promise.all` in cs-js, `asyncio.gather` in cs-python, manual continuation passing in cs-wasm — as long as the observable output matches. The discipline is not "write the same code three times"; the discipline is "write three handlers that produce the same observable output."

This is also where the harness's strict comparison pays off. A subtle drift — a trailing newline in the body, a different log prefix, a case difference in a header — that would be invisible in production is loud in the harness. The harness amplifies small drifts into clear failures, and the small drifts are caught at the source rather than discovered after a runtime migration.

## Future expansions

The v0.1 harness covers happy paths, error paths, KV behavior, log behavior, and timeout behavior. The roadmap identifies several axes the harness will grow along.

Capability enforcement parity beyond KV is the next logical step. The current `capability-denied` fixture exercises KV prefix enforcement; an equivalent fixture for codeQ topic enforcement and one for HTTP allowHost enforcement would round out the host-API contract.

Workflow parity is a separate dimension that the harness does not currently exercise. Workflow handlers are non-trivial to compare across runtimes because the replay model interacts with the per-runtime async machinery. The plan is to add a `cadence.kind == "workflow"` mode to the fixture format and to compare not just the final response but the entire decision sequence.

Bundle-layout parity is a more speculative axis. The harness currently assumes the canonical bundle shape (`function.js`, `manifest.json`, `deps/*.js`); a future expansion would exercise per-runtime bundle layouts (`function.py` for cs-python, `module.wasm` for cs-wasm) and confirm that the control plane's `cs.function.script.v1` schema admits each correctly.

None of these are blocked on the harness itself. The fixture format already supports per-runtime variation, the assertion model is permissive enough to widen, and the matrix driver is already runtime-agnostic. The work is in writing the new fixtures and confirming the adapters honor the contract — the harness is the lever, the corpus is the surface.

## CI integration

The parity harness runs on every PR through the standard Go test workflow. The `test-parity` Makefile target is exposed as its own job so CI can surface parity regressions independently of the broader unit-test suite. A failing cell blocks the merge until the drift is resolved — either by fixing the adapter, by fixing the fixture, or (rarely) by widening the assertion to admit a legitimate runtime-specific tolerance.

The contract the harness enforces is the same contract the platform users see: a function written in `cs-js` and rewritten in `cs-python` must return the same response, emit the same logs, write the same KV keys, and surface the same typed errors. The parity harness is the executable spec of that contract. Failing builds are how the spec stays honest.
