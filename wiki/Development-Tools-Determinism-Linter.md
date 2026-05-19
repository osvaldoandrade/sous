# Development Tools: Determinism Linter

Cadence workflows are durable because they are replayed. Every time a worker picks up a new DecisionTask for a running workflow, the host reconstructs the workflow's decision history from the persisted event log and feeds it back through the workflow handler from the beginning. The handler does not know it is replaying — it sees the same events in the same order and produces the same sequence of decisions. The framework recognises decisions it has already recorded and skips re-applying them; new decisions are appended to the history. This is the mechanism that lets workflows survive worker restarts, crashes, and rolling deploys without losing state.

The replay model imposes one absolute requirement on the workflow handler: the same input must always produce the same sequence of decisions. Any call that observes state outside the decision history — wall-clock time, system entropy, the network, the filesystem, the host event loop — breaks the invariant. The handler that sees a different `Date.now()` on replay schedules a different timer; the timer ID drifts; Cadence detects the non-deterministic history and the workflow run fails with `CS_WORKFLOW_NON_DETERMINISTIC`. The failure surfaces hours or days after the offending publish, when a real worker happens to replay the broken handler.

The determinism linter, introduced in roadmap task E8.03, is the publish-time static analyzer that catches these violations before the bundle is accepted. It lives in `internal/cadence/determinism/`. `cs-control` runs `ScanWorkflow` against the JS source files of any bundle whose manifest declares `cadence.kind == "workflow"` and rejects the publish with `CS_WORKFLOW_NON_DETERMINISTIC` when violations are found. The same scanner is exposed through the CLI as `cs fn check-determinism --path <dir>` so authors can validate locally before they publish.

The linter is a contract enforcer, not a code reviewer. It does not understand the workflow's intent, does not propose remediations beyond the per-pattern message, and does not look at any code outside the bundle's JS sources. Its job is to catch the small, finite set of banned APIs that experience has shown to break replay; the bigger problem of "is this workflow well-designed" is left to human review. The narrow scope is the source of the linter's value — a publish-time check that takes milliseconds and never falsely passes a Date.now call is more useful than a long-running heuristic that flags 5% of clean workflows.

## Which APIs are banned

The banned-API table is in `internal/cadence/determinism/scan.go`, in the `bannedPatterns` slice. The v0.1 list covers the canonical sources of non-determinism in a JavaScript workflow handler:

`Date.now` reads the wall clock. Two replays of the same handler observe different values, so any branch that compares `Date.now()` to a stored timestamp drifts on replay. The remediation is `cs.workflow.now()`, which returns the deterministic workflow time recorded in the decision history.

`Math.random` reads the system entropy pool. The same argument applies: every replay observes a different number. The remediation is `cs.workflow.sideEffect`, which records the value into history on the first run and replays the recorded value on every subsequent decision.

`new Date()` with no arguments reads the wall clock, exactly like `Date.now()`. The regex is anchored to the no-arg form (`new Date(\s*)`); `new Date(history.startedAtMs)` is deterministic when the argument comes from history and is not flagged.

`setTimeout`, `setInterval`, and `setImmediate` schedule callbacks against the host event loop. Workflows must be synchronous between awaits — the decision history is what records "wait one hour" or "wait until X". The remediation for timers is `cs.workflow.sleep`; for recurrence, model the loop explicitly with `cs.workflow.continueAsNew`.

`fetch` performs unmediated network I/O. Two replays of the same handler will see different responses (or the same response with a different `Date` header), and the network call is not recorded in history. The remediation is to delegate the I/O to an Activity that the workflow calls through `cs.workflow.executeActivity` — the activity result is what lands in history, and the workflow handler observes the same recorded result on every replay.

`performance.now` reads the monotonic clock. Same problem as `Date.now`, same remediation.

`crypto.getRandomValues` reads system entropy. Same problem as `Math.random`, same remediation: wrap the entropy read in `cs.workflow.sideEffect` so the value is recorded into history.

The patterns are pre-compiled regular expressions, not AST queries. A full JS AST walk via goja would catch a handful of additional false negatives (`globalThis.Date.now` aliased through a binding, for example) but would pay parser cost on every publish. The trade-off documented in `scan.go`'s package comment is intentional: regex scanning is cheap, the false-negative rate of a careful pattern set is low, and authors who genuinely need a banned API behind a deterministic wrapper have the per-line escape hatch described below.

## How the linter is invoked

The linter runs in two places. The first is automatic — `cs-control` invokes `ScanWorkflow` on every publish of a bundle whose manifest declares `cadence.kind == "workflow"`. Violations produce an error envelope with `code: CS_WORKFLOW_NON_DETERMINISTIC`, the file path, the 1-indexed (line, column) pair, the banned pattern name, and the remediation hint. The publish is rejected — no version record is created, no draft is consumed, the operator sees the diagnostic and fixes the handler.

The second is on-demand, through the CLI:

```
cs fn check-determinism --path ./reconcile-workflow
```

The command reads `function.js` plus every file under `deps/*.js` from the bundle path, passes them through `ScanWorkflow`, and prints one line per violation in `file:line:col: pattern — message` format. The exit code is 0 when the bundle is clean and 3 when violations are found, which is the same `runtime error` code the rest of the CLI uses. Running the check before `cs fn publish` is the recommended local workflow — the linter takes milliseconds, and a clean local check is a near-guarantee that the publish-time check will also pass.

The CLI surface mirrors the rest of `cmd/cs-cli/main.go`: the command is a `flag.FlagSet` with a `--path` argument that defaults to `.`, the bundle is assembled the same way `cs fn test` assembles it, and the diagnostic format is the GNU-standard `file:line:col` form that editors and CI annotators parse natively. The same `ScanWorkflow` function backs both surfaces, which means the local check and the publish-time check cannot drift.

## What scanning covers

The scanner walks the bundle's file map and applies every banned pattern to every JS source it finds. `function.js` is always scanned. Files under `deps/` that end in `.js` are also scanned — a workflow that pulls in a banned API through a frozen dependency is just as broken as one that calls it directly, so the linter follows the bundle's transitive closure.

Files outside the JS source set are skipped. `manifest.json` is not scanned (the regex would happily match the substring `Date.now` inside an unrelated comment). `lib/` and other non-`deps/` directories are not scanned. Non-`.js` files under `deps/` (README, TypeScript sources, source maps) are not scanned. The filter is conservative: anything that will not execute under the runtime is out of scope.

The scan is deterministic. File names are sorted alphabetically before iteration; violations within a file are sorted by (line, column, pattern name). Repeated runs of `ScanWorkflow` over the same input produce byte-identical output, which makes the publish-time diagnostic easy to diff against a local run.

## How false positives are handled

The linter has one escape hatch: the `cs-determinism-allow` line comment. Any line containing that substring suppresses every violation on the same line. The marker is line-scoped, so an opt-out on line 42 does not silence line 43 — reviewers can audit each exception independently.

```js
const correlationId = Math.random().toString(36); // cs-determinism-allow recorded via sideEffect downstream
```

The marker is intentionally heavyweight. The substring is long enough that it cannot appear by accident, the comment is on the same line as the violation so a code-review diff makes the opt-out visible, and the keyword `allow` makes the intent explicit. The expectation is that operators review each marker at PR time and document the justification in the comment — "recorded via sideEffect downstream", "history-derived input", or a similar one-liner that explains why the determinism rule is safe to bypass here.

Markers should be rare. A workflow that needs more than a handful of opt-outs is almost certainly fighting the replay model and is better refactored to push the non-deterministic logic into an Activity. The escape hatch exists for the cases where the determinism is established by code the linter cannot statically see — typically because the banned API is wrapped in `cs.workflow.sideEffect` two lines down.

## What to do instead

Every banned pattern has a deterministic replacement that the runtime provides. The replacements share a single idea: anything that is not in the decision history must be recorded into history before it is read.

For wall-clock and monotonic time, use `cs.workflow.now()`. The host records the time at the moment of the first call and replays the recorded value on every subsequent decision.

For entropy and unique identifiers, use `cs.workflow.sideEffect(fn)`. The host runs `fn` once, records the return value into history, and replays the recorded value on every replay. The common pattern is `await cs.workflow.sideEffect(() => crypto.randomUUID())` — the call to `crypto.randomUUID` happens exactly once per workflow run, and every replay sees the same UUID.

For timers and sleep, use `cs.workflow.sleep(ms)` or `cs.workflow.timer(ms)`. These schedule decisions against the workflow timeline rather than the host event loop, so the sleep duration is part of the recorded history.

For network and disk I/O, call an Activity. Activities are the platform's unit of non-deterministic work — they run on a separate worker, their inputs and outputs are recorded into history, and the workflow handler observes the activity result deterministically on every replay. The `cs.workflow.executeActivity(name, input)` call returns the activity result; the workflow handler then operates on that result. The full pattern is documented in [Event Sources: Cadence Workflows](Event-Sources-Cadence-Workflows).

A workflow handler that follows these conventions has no need for the escape hatch. It reads time through `cs.workflow.now`, gets fresh IDs through `cs.workflow.sideEffect`, schedules waits through `cs.workflow.sleep`, and pushes I/O to Activities. The determinism linter then has nothing to flag, the publish goes through clean, and the workflow replays correctly on every DecisionTask.

## Static analysis scope

The determinism linter is a static analysis. It catches the patterns it knows about — the entries in the banned table — and nothing else. It does not, and cannot, catch every source of non-determinism. A handler that reads from a global mutable variable, that depends on JavaScript's `Map` iteration order in a way that varies across engine versions, or that hashes a JSON object whose key order is not stable will all replay non-deterministically and the linter will not flag it. The runtime replayer is the safety net for everything the static scan misses; the linter's job is to catch the mechanical mistakes early, not to prove the workflow correct.

## Diagnostic format

When the linter fails a publish, the error envelope the control plane returns includes one `violations` array per file with one entry per banned-API call site. Each entry carries `file`, `line`, `column`, `pattern`, and `message`. The shape matches the `Violation` struct in `scan.go`:

```json
{
  "code": "CS_WORKFLOW_NON_DETERMINISTIC",
  "violations": [
    {
      "file": "function.js",
      "line": 14,
      "column": 22,
      "pattern": "Date.now",
      "message": "Date.now() reads the wall clock; use cs.workflow.now() inside workflows."
    },
    {
      "file": "deps/utils.js",
      "line": 7,
      "column": 12,
      "pattern": "Math.random",
      "message": "Math.random() is nondeterministic; use cs.workflow.sideEffect for entropy."
    }
  ]
}
```

The diagnostic is intentionally structured so that editor integrations, CI annotators, and the publishing agent can all parse it. The `file:line:column` triple is the GNU-standard error location; the `pattern` slug is a stable identifier the author can search for; the `message` is the human-readable remediation hint.

The CLI surfaces the same diagnostic in a shorter form. Each violation prints as `file:line:col: pattern — message` on stdout, and the process exits with code 3. Authors who pipe the output into an editor's quick-fix window get the locations underlined automatically.

When zero violations are found, the CLI prints `ok: no violations` and exits 0. The publish-time check is silent on success — the bundle simply lands as a new version. Authors who want to confirm the workflow check ran can grep the control-plane logs for the `workflow.determinism.scan` event, which records the file count and the violation count at INFO level.

## Examples of clean and dirty workflows

A clean workflow handler that the linter accepts:

```js
export default async function (ctx) {
  const now = await cs.workflow.now();
  const id = await cs.workflow.sideEffect(() => crypto.randomUUID());

  const order = await cs.workflow.executeActivity("LoadOrder", { id: ctx.event.orderId });
  if (order.status === "pending") {
    await cs.workflow.sleep(60_000);
    await cs.workflow.executeActivity("ReminderEmail", { id: order.id, requestId: id });
  }

  return { statusCode: 200, body: JSON.stringify({ workflowId: id, completedAt: now }) };
}
```

Every non-deterministic operation is routed through a `cs.workflow.*` helper. Time goes through `cs.workflow.now`. Entropy goes through `cs.workflow.sideEffect`. Waiting goes through `cs.workflow.sleep`. I/O goes through `cs.workflow.executeActivity`. The linter sees zero violations.

A dirty workflow handler that the linter rejects:

```js
export default async function (ctx) {
  const startedAt = Date.now();
  const id = Math.random().toString(36);

  const response = await fetch(`https://orders.internal/${ctx.event.orderId}`);
  const order = await response.json();

  if (order.status === "pending") {
    setTimeout(() => console.log("waiting"), 60_000);
  }

  return { statusCode: 200, body: JSON.stringify({ id, startedAt, orderId: order.id }) };
}
```

The linter reports four violations on this handler: `Date.now` on line 2, `Math.random` on line 3, `fetch` on line 5, and `setTimeout` on line 9. Each violation carries the remediation hint pointing at the appropriate `cs.workflow.*` helper. The author rewrites the handler to match the clean example above, re-runs the check, and the publish proceeds.

## Linter performance

The scan is O(patterns × bytes) per file. With nine banned patterns and a typical workflow JS source of a few kilobytes, the scan takes microseconds. A bundle with twenty deps files runs to a millisecond at most. The cost is invisible on the publish hot path.

The performance budget is intentionally generous because the linter is on the critical path of every workflow publish. A future version that adds AST-based checks for the harder cases (aliased globals, dynamic property access) will pay parser cost, which is two orders of magnitude more expensive. The team has explicitly chosen to keep the v0.1 linter regex-based and accept the residual false-negative rate — the runtime replayer is the safety net that catches everything the static scan misses.

The scan is also memory-light. It allocates a single `[]int` for the newline index per file and a per-pattern slice of match offsets; the per-file working set is proportional to the source size and is freed when the function returns. Even a pathological bundle with hundreds of deps files stays under a megabyte of resident memory during the scan.

## Integration with the publish flow

When a bundle is uploaded as a draft, the control plane parses the manifest and inspects the `cadence` field. A manifest without a `cadence` block is treated as a non-workflow function; the determinism linter does not run. A manifest with `cadence.kind == "activity"` is a Cadence activity; the linter does not run because activities are non-deterministic by design — they exist precisely to wrap the I/O that workflows cannot perform.

A manifest with `cadence.kind == "workflow"` triggers the scan. The control plane extracts the bundle, walks the file map, and calls `ScanWorkflow(files)`. A non-empty result fails the draft upload with `CS_WORKFLOW_NON_DETERMINISTIC` and the violations array; the draft is not stored. An empty result lets the draft upload succeed; the bundle is persisted, and the author proceeds to `cs fn publish` as normal.

The publish step does not re-run the linter. The check is run once, at draft upload time, because the bundle is immutable from that point forward. Re-running the linter at publish would be redundant and would only catch a control-plane bug — the bundle bytes have not changed.

This design has a subtle implication. A draft that passes the linter under one version of the linter would pass under every subsequent version that only adds new banned patterns. A draft that passes today and then has its scanner widened (a new banned pattern is added) is not retroactively rejected. The control plane's policy is forward-only: existing drafts are grandfathered against scanner additions; new drafts pay the full current check. The discipline is acceptable because runtime replay still catches anything the static check misses, and forcing every existing draft to re-scan on every linter update would create a cliff where unrelated bundles fail to publish.

## Linter coverage and limitations

The linter scans JavaScript sources. The cs-python and cs-wasm runtimes do not currently support workflows; when they do, the linter will need a per-language banned-API table. The expectation is that the patterns are language-portable in spirit (Date.now, Math.random, fetch all exist in cs-python under different names) but the regex set will be different per language. The codebase is structured to make adding `ScanPythonWorkflow` and `ScanWasmWorkflow` straightforward — each is a new function next to `ScanWorkflow` with its own banned-pattern table.

The linter does not catch indirection through dynamic property access. A handler that writes `const d = global["Date"]["now"]()` evades the regex. The team has accepted this gap because the alternative — full AST analysis — is disproportionate to the threat. Workflow authors who write the dynamic-access pattern are deliberately bypassing the check, and the runtime replayer will reject the resulting non-determinism the first time the handler is replayed.

The linter does not catch indirection through user functions. A handler that imports a util module which itself calls `Date.now()` will produce a violation against the util module's file, not against the handler. This is the right behavior — the violation needs to be fixed where it occurs — but it does mean authors occasionally see the lint failure on a file they did not edit. The remediation is the same: replace the banned call with the `cs.workflow.*` helper or push the call into an Activity.

The linter does not catch type-system shenanigans. A handler that destructures `const { now } = Date` and calls `now()` would, in principle, evade a naïve check; the pattern `Date.now` does not appear in `now()`. The v0.1 regex set anchors `Date\.now\b` so a literal `Date.now` is the only form caught. Authors who write the destructured form are deliberately circumventing the check; the runtime replayer is the backstop, and code review is the human gate.

## Operator review at code-review time

The `cs-determinism-allow` escape hatch is the only way to suppress a violation without changing the handler. Its design assumes a human reviewer will look at every marker at PR time. The substring is long and specific (`cs-determinism-allow`) so the marker is searchable across the corpus, the line-scoped semantics mean each marker has to be justified independently, and the comment style is conventional (`// cs-determinism-allow <justification>`) so reviewers know what to look for.

The recommended PR-review checklist is short. Search the diff for `cs-determinism-allow`. For each match, read the comment that follows. Confirm the justification corresponds to an actual deterministic wrapper — typically `cs.workflow.sideEffect` two lines down or a history-derived input three lines up. If the justification is missing or ambiguous, the marker is rejected and the author rewrites the code to avoid the banned API outright.

In practice, well-written workflows accumulate zero markers. The host APIs cover the legitimate needs (time, entropy, sleep, I/O) and the linter's patterns target the JS standard library's non-deterministic surface specifically. An author who finds themselves reaching for the marker more than once or twice per workflow is usually missing a `cs.workflow.*` helper that does the right thing — and the right response is to ask the platform team rather than to multiply markers.

## CI integration

The standard Go test workflow exercises the linter through `internal/cadence/determinism/scan_test.go`. The tests cover the banned-pattern table, the line-scoped escape hatch, the deps directory traversal, and the deterministic output ordering. A PR that adds a new banned pattern is expected to add a corresponding test case; a PR that changes the escape-hatch semantics is expected to update the test coverage accordingly.

The control plane's tests cover the integration: a publish of a non-workflow bundle skips the linter, a publish of a workflow with violations returns the typed error, a publish of a clean workflow proceeds. The integration tests are in `cmd/cs-control/`; they exercise the full draft-upload-and-publish path against an in-process control plane.

The CLI's `cs fn check-determinism` command is wired into the same `cmd/cs-cli/` test surface that covers `cs fn test` and `cs fn invoke`. The smoke test asserts on the exit code and on the diagnostic format. A regression that changed the format would surface as a CI failure on the CLI's own tests.

## The relationship to the workflow runtime

The linter is part of a layered defense. The first layer is the linter itself, running at publish time and catching the small finite set of banned APIs. The second layer is the workflow runtime, running at every DecisionTask and comparing the new decision sequence against the recorded history. The third layer is the operator review, running at PR time and catching the design-level non-determinism the linter cannot see.

Each layer has a different cost and a different coverage. The linter is cheap, automatic, and narrow. The runtime is expensive, automatic, and exact. The operator review is expensive, manual, and broad. The combination is what makes the workflow story practical — the linter handles the 80% of mistakes that are mechanical, the runtime handles the residual mechanical mistakes the linter misses, and the operator review handles the design-level mistakes that no static check can see.

For the full Cadence integration story — DecisionTasks, ActivityTasks, the replay model, the worker poll loop — see [Event Sources: Cadence Workflows](Event-Sources-Cadence-Workflows). The determinism linter is the publish-time gate; the workflow runtime is the execution-time gate. Both exist because replay correctness is non-negotiable, and the two together are what let the platform make the durability guarantee Cadence is designed to provide.
