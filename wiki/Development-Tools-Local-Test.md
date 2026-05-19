# Development Tools: Local Test

The local test command is the canonical inner loop for SOUS function authors. When a developer types `cs fn test`, the CLI loads the function from the current directory and executes it through the same runtime adapter that the platform uses in production: goja for `cs-js`, a `python3 -I -S` subprocess for `cs-python`, and wazero for `cs-wasm`. No cluster is contacted, no draft is uploaded, and no activation is recorded in KVRocks. The author edits the handler, runs `cs fn test`, reads the result, and iterates — every cycle goes through the same bundle path, the same manifest validation, and the same host API surface the published version will hit.

The command lives in `cmd/cs-cli/main.go` under the `fnTest` function, dispatched from the `fn test` case of `handleFunction`. It is intentionally short. It reads `function.js` and `manifest.json` from the bundle path, hands them to `bundle.BuildCanonical` so the developer exercises the exact tar layout the control plane will store, then constructs a single `runtime.NewRunner` against an in-process `runtime.NewMemoryKV` and `runtime.NopCodeQ`. The runner executes one `api.InvocationRequest` and prints the resulting `runtime.ExecutionOutput` as indented JSON. The exit code is zero when the runner's `Status` field is `success`, non-zero otherwise.

This design preserves the property that gives SOUS its agent-friendliness: the local result is the cluster result. The same bundle bytes hash to the same `sha256`. The same manifest is parsed by the same validator. The same host API surface (`cs.log`, `cs.kv`, `cs.codeq`, `cs.http`) is exposed by the same isolate. A handler that passes `cs fn test` locally and then fails when invoked through `cs fn invoke` indicates a real difference — a missing capability, a host network policy, a tenant quota — not a runtime drift.

The command is the first thing an author runs after `cs fn init`. It is the last thing they run before `cs fn draft upload`. Every loop in between is `edit handler → cs fn test → read JSON → repeat`. The time budget per loop is sub-second on the cs-js path because the runner is in-process and the bundle build is deterministic. The cs-python path pays the subprocess startup cost, which lands in the low tens of milliseconds. The cs-wasm path is in-process again because wazero is a pure-Go runtime. None of these costs are visible to the author; the JSON output is on screen before they have moved their hands off the keyboard.

The runner is not a mock. It is the same `runtime.NewRunner` constructor that backs `cs-invoker-pool`, instantiated with the same `runtime.MemoryKV` test double the parity harness uses, and threaded through the same `runtime.Registry` lookup that selects the adapter from `manifest.Runtime`. Authors who introspect the `cs-cli` source see this directly: the call chain is `fnTest → bundle.BuildCanonical → runtime.NewRunner.Execute → runtime.DefaultRegistry.Lookup(manifest.Runtime).Execute`. The CLI is a wrapper; the runtime is the same code path the production invoker pool walks.

The decision to keep the runner in-process for cs-js and cs-wasm is deliberate. Both adapters are pure-Go libraries — goja for cs-js, wazero for cs-wasm — and both can be embedded into the CLI binary without extra system dependencies. The cs-python adapter is necessarily a subprocess because the cs-python guest needs a real Python interpreter; the adapter spawns `python3 -I -S`, pipes the bundle and request over stdin, and reads the response from stdout. The protocol is the same whether the spawning process is the CLI or the production invoker pool. This is the underlying reason for the parity guarantee: the same adapter binary, the same wire protocol, the same fault handling, regardless of caller.

## Project layout

A SOUS function project is a directory with two required files and a conventional layout for ancillary test inputs:

```
reconcile/
  function.js           # required: handler source
  manifest.json         # required: schema, runtime, limits, capabilities
  fixtures/
    happy.json          # event JSON
    empty.json
    malformed.json
  README.md
```

The `function.js` and `manifest.json` filenames are part of the canonical bundle contract enforced by `internal/bundle`. They are non-negotiable. A bundle that swaps `function.js` for `index.js` will not load, and the local CLI surfaces that as a file-not-found error before the runner is invoked.

The `fixtures/` directory is a convention adopted across the SOUS examples. The CLI does not parse this directory or auto-discover anything inside it; the convention exists so that `cs fn test --event fixtures/happy.json` is a stable idiom across projects. Authors who prefer a flat layout can keep `event.json` next to the handler. The CLI does not care where the event file lives.

For multi-runtime projects, the convention is to keep separate top-level files: `function.js` for the cs-js variant, `function.py` for the cs-python variant, and `module.wasm` for the cs-wasm variant. The manifest's `runtime` field selects which one the bundle uses. The local CLI reads `function.js` today and is being extended to follow the same logic — until then, multi-runtime projects swap the manifest before running `cs fn test`.

## Running a single fixture

The minimal invocation reads the handler from the current directory and passes an empty event:

```
cs fn test
```

The output is a JSON document with the runner's full `ExecutionOutput`: status, response shape, logs, error envelope, and timing. A successful run looks like this:

```json
{
  "status": "success",
  "response": {
    "statusCode": 200,
    "headers": {"x-parity": "1"},
    "body": "{}"
  },
  "logs": [],
  "durationMs": 4
}
```

To drive the handler with a real event, point `--event` at a JSON file:

```
cs fn test --event fixtures/happy.json
```

The CLI reads the file, parses it as JSON, and assigns it to `api.InvocationRequest.Event`. The handler receives it as the first argument (`event` for cs-js, `event` keyword in the cs-python boot script, the imported `event` binding in cs-wasm). A non-JSON file or a malformed payload surfaces as a parse error before the runner is invoked — the developer sees the error from `json.Unmarshal`, not a downstream type panic from inside the handler.

To run the handler from a different directory, use `--path`:

```
cs fn test --path ./services/reconcile --event ./fixtures/happy.json
```

This is the common pattern for monorepos that keep multiple functions under a shared top-level directory. The `--path` argument points at the bundle root, which is the directory containing `function.js` and `manifest.json`. The `--event` argument is resolved against the current working directory, not the bundle root, so authors can keep shared fixture files at the monorepo top level.

## Writing fixtures

A fixture is a single JSON document representing the `Event` field of `api.InvocationRequest`. The shape is whatever the handler expects:

```json
{
  "kind": "order.created",
  "order_id": "ord_01HZ",
  "amount_cents": 12500,
  "currency": "USD"
}
```

Authors typically keep three categories of fixture beside the handler.

A *happy path* fixture covers the canonical input the function was designed for. It is the first fixture written, the one referenced in the README, and the one CI runs on every PR.

*Edge cases* exercise empty maps, missing optional keys, and boundary numeric values. They are the fixtures that catch the off-by-one errors and the unhandled-null branches that production traffic eventually surfaces. Authors add them whenever they fix a bug — the fixture goes in beside the handler change, so the regression cannot recur.

*Adversarial inputs* test that the function refuses malformed payloads with a clean error, not a 500. They are the fixtures that exercise the validation layer: missing required fields, wrong types, oversized strings, deeply nested structures. The function should reject them with a typed `4xx` response, not crash the runtime.

Each fixture lives under `fixtures/` with a descriptive name and is invoked via `cs fn test --event fixtures/<name>.json`. The naming convention is freeform; common patterns are `happy.json`, `empty.json`, `malformed-missing-id.json`, `malformed-bad-currency.json`.

Fixtures intentionally do not carry expected outputs. `cs fn test` is a runner, not a comparator. To assert on output shape across runtimes, authors graduate the fixture to the parity harness, which does enforce a golden result. See [Parity Harness](Development-Tools-Parity-Harness) for the format.

## Asserting on outputs

The CLI is a thin wrapper. It prints the full `ExecutionOutput` and exits 0 on success or non-zero on error. Authors who want a richer assertion harness on top of `cs fn test` have two patterns to choose from.

The first pattern is shell-based. Pipe the CLI output through `jq` and compare the relevant field. This is the idiomatic approach for one-off smoke tests and for the templated `test.sh` scripts that ship with `cs fn init`:

```
result=$(cs fn test --event fixtures/happy.json)
status=$(echo "$result" | jq -r .response.statusCode)
if [ "$status" != "200" ]; then
  echo "expected 200, got $status" >&2
  exit 1
fi
```

The advantage of the shell pattern is that the assertion is co-located with the fixture and runs in any environment that has `jq` and a shell. The disadvantage is that the assertion is opaque to Go's tooling — there is no `go test` integration, no per-fixture timing, no per-fixture failure aggregation. For one-off smoke tests this is fine; for anything load-bearing it is the wrong tool.

The second pattern is to write a Go test that calls the same `runtime.NewRunner` API. This is the approach the parity harness uses (see `internal/runtime/parity/parity.go`) and is recommended for any function with non-trivial assertion surface. The advantage is that the assertion lives in source control next to the handler, runs under `go test`, and produces a single failure line that points at the offending field. The disadvantage is that the assertion is Go-only, which is a barrier for non-Go contributors. Most teams resolve this by keeping the shell smoke tests for quick local iteration and the Go tests for the merge-blocking assertions in CI.

## Mock secrets in dev mode

The `cs fn test` command runs the function with `Principal.Sub = "cli"` and `Principal.Roles = ["role:app"]`. The KV provider is an in-process `runtime.NewMemoryKV`, which means any keys the function writes during the test live only for the duration of the process and are discarded when the runner returns. This is the dev-mode default: no real KVRocks, no real codeQ, no real HTTP egress.

When the handler needs a secret to exercise a code path, authors prime the in-memory KV before the test by adding a small bootstrap snippet to the top of `function.js` guarded by `process.env.CS_LOCAL`. The runner sets `CS_RUNTIME` and similar dev hints; secrets are not injected through the CLI today. The recommended pattern is to keep the secret-handling branch behind a feature flag that the local fixture sets:

```json
{
  "secrets": {
    "stripe_api_key": "sk_test_local_only"
  },
  "kind": "order.created"
}
```

The handler reads `event.secrets.stripe_api_key` when present and falls back to `cs.secrets.get` otherwise. The local run uses the inline value; the published version uses the platform-issued credential. This split keeps real credentials out of fixture files and out of the CLI exit log.

The mock-secret pattern is a workaround. The roadmap eventually surfaces a `--secrets-file` argument on `cs fn test` that loads a sidecar JSON file outside the bundle. Until then, the fixture-inline pattern is the supported approach. Authors are expected to keep the inline values bounded — never commit a real production secret to a fixture file, even one named `local-only`.

## Mock KV state

The `runtime.NewMemoryKV` provider that backs `cs fn test` is a simple `map[string][]byte` with the same `Get/Set/Del` surface as the production KVRocks adapter. It enforces the same key-prefix allowlist the manifest declares and the same operation allowlist the manifest declares. A function that tries to read a key outside its prefix allowlist will get the same typed `CS_RUNTIME_CAPABILITY_DENIED` error it would get in production.

Fixtures that depend on pre-existing KV state — counters, idempotency tokens, cached lookups — supply that state through the event itself, then have the handler write it via `cs.kv.set` before reading it back. The result is a single self-contained invocation that does not depend on test order:

```json
{
  "_setup": {
    "ctr:reconcile:lock": "owner:cli-test"
  },
  "kind": "order.created"
}
```

The handler reads `event._setup` at the top of the function and primes the KV before running the real logic. The `_setup` field is a convention; the CLI does not interpret it. Authors who do not want to mix setup with payload can use a wrapper handler that takes a `{setup, event}` envelope and forwards the inner `event` to the production handler.

For multi-step flows that genuinely require KV state to persist across invocations, authors write a Go test against `runtime.NewRunner` directly. The CLI is deliberately a single-shot tool: each `cs fn test` invocation starts with an empty KV and discards it on exit. This is a feature, not a limitation — the deterministic empty-state start is what makes the fixture results reproducible across machines.

## Exit codes for CI integration

The CLI returns one of four exit codes the way `wiki/CLI.md` documents:

- `0` — the runner returned `Status == "success"`.
- `1` — client error: bad arguments, missing files, malformed fixture JSON.
- `2` — server error: reserved for control-plane commands that contact the API; not used by `cs fn test`.
- `3` — runtime error: the runner returned a non-success status (`error`, `timeout`, or a typed `InvocationError`).

CI pipelines can rely on exit code 0 as a green signal. Smoke tests that want to assert on the response body should not parse stdout — they should pipe the JSON through `jq` and check the specific field. The CLI never writes structured assertions to stderr; stderr is reserved for harness errors (file-not-found, JSON parse failure) that the developer needs to see before they look at the JSON output.

A typical CI block runs the same handler against every fixture under `fixtures/`:

```
set -e
for fx in fixtures/*.json; do
  echo "::group::$fx"
  cs fn test --event "$fx"
  echo "::endgroup::"
done
```

The loop short-circuits on the first non-zero exit, which is the common case for catching a regression. Pipelines that want to surface every failure (rather than the first) drop `set -e` and accumulate exit codes manually.

For matrix CI runs that exercise the function across multiple runtimes, the convention is to set `manifest.runtime` per matrix cell and re-run the same loop. The fixtures stay the same, the handler files change per runtime, and the exit code is the green/red signal the pipeline reports. The richer cross-runtime assertion (same response, same logs, same KV writes) is the job of the parity harness, not the local CLI.

## Failure modes and how to read them

The local CLI surfaces three classes of failure. Each has a distinct exit code and a distinct shape on stdout. Authors who learn to read the difference at a glance save themselves the most common debugging-cycle waste.

A *harness failure* is the CLI's own problem: a missing `function.js`, a malformed manifest, a fixture that does not parse as JSON, or an argument the CLI does not understand. These exit with code 1 and write a single error line to stderr (`error: open function.js: no such file or directory`). They are fixed by correcting the working directory, the file name, or the command-line arguments. They do not indicate anything about the handler.

A *runtime failure* is the adapter's problem: a syntax error in the handler, an unhandled exception, a panic from inside the isolate, or a typed error returned by the handler itself. These exit with code 3 and write the full `ExecutionOutput` JSON to stdout. The `status` field is `error`, the `error.type` field describes the failure class, and the `error.message` field carries the underlying message. Authors investigate by reading the JSON, locating the line number in the message, and editing the handler.

A *timeout failure* is the deadline's problem: the handler took longer than `manifest.limits.timeoutMs` to return. These exit with code 3 and write the `ExecutionOutput` JSON with `status: "timeout"`. The fix is either to raise the manifest timeout (if the slow path is legitimate) or to lower the wall-clock cost of the handler (if the slow path is a bug). Authors who want to exercise the timeout path explicitly set a small `timeoutMs` in the manifest and confirm the JSON output carries `status: "timeout"` on the slow fixture.

The other status values the runner can emit — `success`, `error`, `timeout` — are documented in `internal/runtime/types.go`. Authors who want to assert on a specific status in CI should match the field exactly; the strings are stable and will not change without a major version bump.

## Iterating on the handler

The recommended inner loop is `edit → save → cs fn test → read JSON`. Each step is fast enough that the author can keep both terminals visible: the editor on the left, the shell on the right, the JSON output replaced on each invocation. The CLI does not have a watch mode today; authors who want one wrap `cs fn test` in `entr` or `watchexec`:

```
echo function.js manifest.json fixtures/happy.json | entr -c cs fn test --event fixtures/happy.json
```

This re-runs the test whenever any of the three files changes. The output is on screen within a second of the save. The pattern is the same as the rest of the SOUS toolchain: small composable commands, no built-in watcher, lean on Unix conventions for the developer experience.

For handlers that exercise multiple fixtures, authors typically chain them in a small shell wrapper:

```
for fx in fixtures/*.json; do
  printf "\n--- %s ---\n" "$fx"
  cs fn test --event "$fx" || echo "FAILED: $fx"
done
```

The wrapper runs every fixture, prints a banner before each, and notes failures inline. It is the local equivalent of the CI block above, but without the `set -e` short-circuit — authors usually want to see every fixture's output, not just the first failure.

## What the CLI does not do

The CLI does not validate the bundle against the publish-time policy. It does not check that `invoke_http_roles` is non-empty. It does not check that the function name is unique within the namespace. It does not check that the manifest's runtime is one the cluster supports. All of those checks are the control plane's job, and the local CLI deliberately skips them so authors can iterate without a deployment cycle.

The CLI does not cache. Every invocation reads `function.js` and `manifest.json` from disk, builds the canonical bundle from scratch, and constructs a fresh `runtime.Runner`. There is no daemon, no warm pool, no incremental compile. The decision is intentional: caching introduces a class of "works locally, fails in CI" bugs that are catastrophically expensive to debug, and the cost of a cold start is sub-second on every supported runtime.

The CLI does not exercise concurrency. The runner is single-threaded; the `maxConcurrency` field in the manifest is honored at the platform level by `cs-invoker-pool`, not by the local CLI. Authors who want to test concurrent behavior — race conditions, lock contention, KV write conflicts — write Go tests against `runtime.NewRunner` directly and drive them with `t.Parallel`.

The CLI does not write logs to disk. The runner captures logs in-memory and writes them as part of the `ExecutionOutput` JSON. There is no `cs-invoker-pool` log shipper running locally, no Loki/Tempo equivalent, no persistent activation record. Authors who want to inspect logs across multiple invocations save the JSON output explicitly:

```
cs fn test --event fixtures/happy.json | tee logs/happy-$(date +%s).json
```

These are intentional limits. The local CLI is a fast iteration tool, not a production simulator. Production-fidelity testing is the job of an integration suite running against a real cluster, where the HTTP gateway, the scheduler, the Cadence poller, and the persistence layer are all in the loop. The local CLI's value is that it is one command, one second, and one piece of JSON — the smallest possible unit of feedback the author can act on.

## When local test is not enough

The CLI runs the handler against an in-process runner with a memory KV and a no-op codeQ. It does not exercise the HTTP gateway, the scheduler, or the Cadence poller. A handler that passes `cs fn test` is correct under the runtime invariant — same bundle, same manifest, same host API — but the deployment path still has to validate authorization roles (`invoke_http_roles`, `invoke_schedule_roles`, `invoke_cadence_roles`), quota envelopes, and trigger-specific mapping. Authors who need to exercise the full path use `cs fn draft upload` followed by `cs fn invoke` against a dev cluster.

For cross-runtime correctness — the assertion that a `cs-js` handler and its `cs-python` equivalent agree on every observable byte — graduate the fixture to the [Parity Harness](Development-Tools-Parity-Harness). The local test command is the first line of defense; the parity harness is what catches drift between adapters.

For workflow handlers — code authored against `cs.workflow.*` and published with `manifest.cadence.kind == "workflow"` — the local CLI runs the handler once, top to bottom. It does not exercise the replay model, the DecisionTask loop, or the activity completion flow. Authors who want to catch non-deterministic code paths before publish run the [Determinism Linter](Development-Tools-Determinism-Linter) alongside `cs fn test`. The linter takes milliseconds and catches the most common replay-breaking mistakes (`Date.now`, `Math.random`, `fetch`) before they reach the cluster.
