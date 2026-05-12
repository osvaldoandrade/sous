# Cadence integration: cs-cadence-poller

This document defines how `code-sous` executes Cadence Activities.

`cs-cadence-poller` acts as a Cadence worker.
It long-polls `code-flow` and delegates execution to `cs-invoker-pool`.

## Cadence scope in v0.1

The poller supports:

- ActivityTask polling
- Activity completion and failure responses
- Activity heartbeats

The poller does not execute DecisionTasks in v0.1.

## WorkerBinding

A WorkerBinding is a tenant-owned record that defines a polling group.

A binding includes:

- `domain`
- `tasklist`
- `worker_id`
- `pollers.activity`
- `limits.max_inflight_tasks`
- `activity_map` from ActivityType to FunctionRef
- `input_codec` (optional, see "Codec selection" below)
- `output_codec` (optional, see "Codec selection" below)

Example:

```json
{
  "name": "payments-activities",
  "domain": "payments",
  "tasklist": "payments-activities",
  "worker_id": "cs-payments-01",
  "pollers": { "activity": 8 },
  "limits": { "max_inflight_tasks": 256 },
  "activity_map": {
    "SousInvokeActivity": { "function": "reconcile", "alias": "prod" }
  },
  "input_codec": "msgpack",
  "output_codec": "msgpack"
}
```

## Codec selection

Each WorkerBinding pins the wire codec used for one tasklist. The poller
picks the codec at task-poll time and routes both the input (passed to
the function) and the output (sent to `RespondActivityTaskCompleted`)
through it. Codecs live in `internal/cadence` and register themselves
into a shared registry on package init.

Supported codecs (v0.1):

| Name      | Content-Type                | Behaviour                                                                                                    |
|-----------|-----------------------------|--------------------------------------------------------------------------------------------------------------|
| `json`    | `application/json`          | Default. JSON-marshal the function's `FunctionResponse` and base64-wrap it on the Cadence wire.              |
| `msgpack` | `application/msgpack`       | Msgpack-encode (`github.com/vmihailenco/msgpack/v5`). Interoperates with Java/Go workflow workers.            |
| `raw`     | `application/octet-stream`  | Passthrough. `event.input.raw_base64` ships verbatim; the function's `response.body` ships verbatim back.    |

Selection is per-direction on the binding:

- `input_codec` annotates the `event.input` envelope so the runtime can
  decode the bytes Cadence delivered.
- `output_codec` controls how the function's `FunctionResponse` is
  serialized before `RespondActivityTaskCompleted`.

Both fields are optional. Omitting them (or sending the empty string)
preserves the pre-E8.02 behaviour of JSON for both directions, so
existing bindings keep working unchanged.

### Wire convention

The Cadence transport itself is unchanged: the poller still
base64-wraps the codec output before sending it to
`RespondActivityTaskCompleted`, and the `event.input` envelope still
carries `raw_base64`. The new addition is two annotations on the
event input that downstream runtimes can use to route the decoder:

```json
{
  "type": "cadence.activity",
  "cadence": { /* ... */ },
  "input": {
    "raw_base64": "...",
    "codec": "msgpack",
    "content_type": "application/msgpack"
  }
}
```

For the `raw` output codec the function is expected to return a
`FunctionResponse` whose `body` is the wire payload. When
`isBase64Encoded` is `true`, the poller base64-decodes the body first
and ships the resulting bytes verbatim; otherwise the body's UTF-8
bytes are shipped as-is. This makes `raw` the right choice for any
opaque binary blob the poller never needs to understand (Thrift,
length-delimited protobuf, compressed payloads).

Failure paths:

- A decode error on the input side surfaces to Cadence as
  `RespondActivityTaskFailed(reason="codec_decode_failed", details=<message>)`.
- An encode error on the output side surfaces as
  `RespondActivityTaskFailed(reason="codec_encode_failed", details=<message>)`.
- Failures (`status="error" | "timeout"`) always ship the JSON
  `InvocationError` envelope on the wire, regardless of `output_codec`,
  so workflow code keeps a stable error contract across codecs.

### Negotiation at binding-create time

`POST /v1/.../cadence/workers` validates `input_codec` and `output_codec`
against the codec registry. Unknown codec names are rejected with
HTTP 400 and `error.code = "CS_VALIDATION_UNSUPPORTED_CODEC"`. The empty
string and the names `json`, `msgpack`, `raw` are always accepted.

### Migration: flipping a tasklist to a binary codec

Codec drift will fail in-flight tasks — a task polled before the flip
but completed after will encode its response with the wrong codec.
Operators should:

1. Stop submitting new workflows against the tasklist.
2. Wait for `max_inflight_tasks` to drain (the poller's `refreshBindings`
   loop restarts pollers on every binding update, so a config flip while
   inflight is treated as a restart event).
3. Update the binding's `input_codec` / `output_codec`.
4. Confirm a smoke-test activity round-trips successfully.
5. Resume submissions.

## Determinism rules

Cadence Workflows are replayed against their history on every Decision.
Code that observes wall-clock time, system entropy, async timers, or
unmediated network IO will produce different results on the replay
than on the original run, which Cadence surfaces as a
nondeterministic-history failure hours after the offending publish.

To catch these mistakes before they reach production, `cs-control` runs
a publish-time static linter against every bundle whose manifest
declares `cadence.kind: "workflow"`. The manifest opts in via:

```json
{
  "schema": "cs.function.script.v1",
  "runtime": "cs-js",
  "entry": "function.js",
  "handler": "default",
  "cadence": { "kind": "workflow" }
}
```

When `cadence` is omitted, or `cadence.kind` is `"activity"` (the
default), the linter is skipped — activities run forward once per
attempt and may freely call any of the APIs below.

### Banned patterns

The linter scans `function.js` plus every `deps/*.js` file in the
bundle for the following call sites and rejects the publish with
`CS_WORKFLOW_NON_DETERMINISTIC` (HTTP 422) when any are present:

| Pattern                    | Why it's banned                                              | Use instead                              |
|----------------------------|--------------------------------------------------------------|------------------------------------------|
| `Date.now()`               | wall-clock read; differs across replays                      | `cs.workflow.now()`                      |
| `new Date()` (no args)     | wall-clock read; differs across replays                      | `cs.workflow.now()`                      |
| `Math.random()`            | system entropy; nondeterministic                             | `cs.workflow.sideEffect(...)`            |
| `crypto.getRandomValues()` | system entropy; nondeterministic                             | `cs.workflow.sideEffect(...)`            |
| `setTimeout`               | schedules real-time callbacks; not deterministic under replay | `cs.workflow.sleep(ms)`                  |
| `setInterval`              | schedules real-time callbacks; not deterministic under replay | model recurrence via the workflow loop   |
| `setImmediate`             | yields to host event loop                                    | keep workflow code synchronous between awaits |
| `performance.now`          | monotonic clock read                                         | `cs.workflow.now()`                      |
| `fetch(...)` (bare global) | unmediated network IO                                        | call `cs.http.fetch` from an Activity    |

The error response carries a structured `violations[]` array so the
publishing agent can render a per-call-site diagnostic:

```json
{
  "error": {
    "code": "CS_WORKFLOW_NON_DETERMINISTIC",
    "message": "workflow function contains 2 nondeterministic call(s); see violations[]",
    "violations": [
      { "file": "function.js", "line": 4, "column": 11, "pattern": "Date.now",
        "message": "Date.now() reads the wall clock; use cs.workflow.now() inside workflows." },
      { "file": "deps/uuid.js", "line": 1, "column": 28, "pattern": "Math.random",
        "message": "Math.random() is nondeterministic; use cs.workflow.sideEffect for entropy." }
    ]
  }
}
```

### Escape hatch: `cs-determinism-allow`

Some legitimate uses (e.g., a sentinel inside `cs.workflow.sideEffect`
whose body the linter cannot statically prove safe) need to opt out a
single call site. Authors annotate the line with the marker comment:

```js
const id = await cs.workflow.sideEffect(() => Math.random()); // cs-determinism-allow signed-off PR-1234
```

The linter ignores any banned-API match on a line containing
`cs-determinism-allow`. The marker is intentionally scoped to the
single line so each escape hatch is audited on its own at code-review
time — the project policy is that every marker must reference the PR
or ticket that approved it.

### Limitations of v0.1

The linter is a text scan over the raw bytes of the JS files. It does
not parse the JS AST and therefore cannot:

- distinguish `Math.random` inside a string literal from a real call
  (text-scan flags both; wrap with the escape hatch if the false
  positive is in a legitimate constant);
- follow re-exports or chained property accesses (e.g.,
  `globalThis.Date.now()` is not flagged);
- detect runtime drift introduced through `eval` or `Function(...)`.

A full AST-aware analyzer plus a replay harness are tracked under
roadmap epic E8 and will land in a later phase.

## Lifecycle

The control plane persists WorkerBindings.
The poller refreshes bindings every `refresh_seconds`.

On refresh:

- new bindings start poll loops
- disabled bindings stop poll loops
- updated bindings restart poll loops with new config

## Polling loop

Each poller goroutine runs:

1. Call `PollForActivityTask(domain, tasklist, worker_id)`.
2. If the call returns empty, continue.
3. If the call returns a task, validate the task.
4. Map the task to a FunctionRef.
5. Publish InvocationRequest to `cs.invoke`.
6. Record mapping `taskTokenHash → activation_id`.

The poller holds an in-memory inflight counter per binding.
The poller stops polling when inflight reaches `max_inflight_tasks`.

## Mapping a task into an invocation

The poller constructs InvocationRequest:

- `trigger.type = cadence`
- `trigger.source` contains Cadence identifiers
- `event` contains the raw input payload base64

Example event:

```json
{
  "type": "cadence.activity",
  "cadence": {
    "domain": "payments",
    "tasklist": "payments-activities",
    "workflowId": "wid",
    "runId": "rid",
    "activityId": "aid",
    "activityType": "SousInvokeActivity",
    "attempt": 1
  },
  "input": { "raw_base64": "..." }
}
```

The poller sets `principal` to its Tikti service identity.
The invoker enforces per-version `invoke_cadence_roles`.

## Mapping an invocation result back to Cadence

The poller consumes `cs.results`.

For each InvocationResult:

1. Look up `taskToken` for `activation_id`.
2. Encode the result into Cadence payload bytes.
3. Call Cadence respond API.

Completion:

- `RespondActivityTaskCompleted(taskToken, payload)`

Failure:

- `RespondActivityTaskFailed(taskToken, reason, details)`

Timeout:

- failure with reason `timeout`

## Heartbeats

Some Activities require heartbeats to reset Cadence heartbeat timeouts.

`cs-js` exposes `cs.cadence.heartbeat(details)` when trigger type is `cadence`.

The invoker publishes:

- topic `cs.cadence.heartbeat`
- message includes `activation_id` and heartbeat payload base64

The poller consumes heartbeat messages and calls:

- `RecordActivityTaskHeartbeat(taskToken, details)`

The poller rate-limits heartbeats per activation:

- `heartbeat.max_per_second` default 2

## Idempotency and dedup

Cadence delivers activity tasks with **at-least-once** semantics: a worker
that times out, restarts, or fails to respond on a heartbeat will see the
same `task_token` again on retry. The poller collapses these redeliveries
into a single logical activation:

- `activation_id = UUIDv5(namespace=OID, name="cadence:" + tenant + ":" + sha256(task_token))`.
  Every redelivery of the same task_token therefore reuses the same
  `activation_id`, so heartbeats and the eventual completion line up across
  retry attempts.
- Before publishing an `InvocationRequest`, the poller consults the dedup
  store (`internal/idempotency.Store`) keyed by `sha256(task_token)`. If a
  terminal record already exists (the original delivery completed but
  Cadence didn't see the response in time), the poller replays the cached
  result to Cadence via `RespondActivityTaskCompleted` /
  `RespondActivityTaskFailed` without re-running the function.
- When `consumeResults` observes a terminal `InvocationResult`, it persists
  a `cadenceCachedResult` payload (`status`, `result`, `error`) into the
  dedup store before calling Cadence. The TTL is 24h, dominating Cadence's
  activity heartbeat/retry windows.

The dedup store is the same primitive used by the HTTP gateway and codeQ
consumer (`internal/idempotency`); production backs it with KVRocks while
tests use the in-memory implementation.

## Recovery

The poller persists token mappings in KVRocks for crash recovery.

Key:

- `cs:cadence:{tenant}:{namespace}:task:{taskTokenHash}`

Value:

```json
{ "activation_id": "uuid", "created_at_ms": 1730000000000 }
```

The poller deletes this key after it responds to Cadence.

If the poller crashes:

- Cadence retries the Activity based on Cadence timeouts and retry policy.
- The repeated task_token resolves to the **same** activation_id via the
  UUIDv5 derivation above; the dedup store replays the cached completion
  rather than spawning a fresh activation.

## Workflows (E8.01)

E8.01 adds a DecisionTask path so a `cs` function can author a
Cadence workflow directly — not just an Activity. The poller now
recognises two binding kinds and dispatches each task surface
through a dedicated long-poll loop.

### Declaring a workflow binding

A workflow binding adds `kind: "workflow"` to its WorkerBinding.
The poller pivots from `PollActivityTask` to `PollDecisionTask` at
binding-refresh time; the rest of the binding shape (domain,
tasklist, worker_id, pollers.activity, limits, activity_map) is
unchanged. The `activity_map` is reused as the workflow_type-to-
function lookup: the keys are the workflow types Cadence may
deliver, and the values point at the cs function that implements
each one.

```json
{
  "name": "orders-workflows",
  "kind": "workflow",
  "domain": "orders",
  "tasklist": "orders-workflows",
  "worker_id": "cs-orders-wf-01",
  "pollers": { "activity": 4 },
  "limits": { "max_inflight_tasks": 64 },
  "activity_map": {
    "OrderWorkflow": { "function": "order-wf", "alias": "prod" }
  }
}
```

Pre-E8.01 bindings have an empty `kind` field and keep their
Activity-task semantics — the field is append-only and defaults to
the v0.1 behaviour.

### Workflow author API

The cs-js workflow runtime (`internal/cadence/workflow`) hands the
user one new host call:

| Name                              | Behaviour                                                                                                                                                                                                                                                                                                  |
|-----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `cs.cadence.scheduleActivity(type, input)` | Schedule the named Activity on the workflow's binding's tasklist. On the first Decision the call emits a `ScheduleActivityTask` decision and suspends the workflow; on subsequent Decisions the call returns the recorded result synchronously (or throws when the Activity failed).                |

A minimal workflow looks like:

```js
export default function(input, ctx) {
  const result = cs.cadence.scheduleActivity("EchoActivity", input);
  return { statusCode: 200, body: result };
}
```

The same call site runs against a possibly-empty history on every
Decision; the workflow author writes code as if it were synchronous
even though execution actually spans multiple Decisions. Async
functions (`export default async function`) are also supported —
the executor inspects the returned Promise's state to map the same
suspension semantics through the async wrapper.

### Replay semantics

Workflow functions are replayed against their history on every
DecisionTask. The executor walks the history in order and resolves
each `cs.cadence.scheduleActivity` call against the next recorded
outcome:

- A matching `ActivityTaskCompleted` event → return the decoded
  result. The same workflow code MUST run twice with identical
  decisions or Cadence rejects the response. The test suite under
  `internal/cadence/workflow` enforces this contract via
  `TestExecutorReplayDeterminism`.
- A matching `ActivityTaskFailed` event → throw inside JS; if the
  workflow does not catch, the executor surfaces a handler error
  and the poller logs it.
- No matching event yet → throw an internal "pending" marker and
  emit a `ScheduleActivityTask` decision. The workflow is now
  suspended until Cadence delivers the next DecisionTask.

Nondeterministic APIs are blocked at publish time by the static
linter in `internal/cadence/determinism` (E8.03). Workflow code
that calls `Date.now`, `Math.random`, `setTimeout`, bare `fetch`,
etc. is refused with `CS_WORKFLOW_NON_DETERMINISTIC` before it
ever reaches the executor.

### Deferred features

The v0.1 MVP supports only the smallest useful workflow shape:
schedule one or more activities, await their results, return. The
following are deferred to a follow-up task (see the E8.01-followup
issue for the tracking checklist):

- `cs.cadence.sleep` (Timer decisions)
- Signal handlers (`cs.cadence.onSignal`)
- `cs.cadence.continueAsNew`
- Selector / race over multiple pending activities
- Child workflows and cross-domain signals
- Per-activity retry policies, cancellation, search attributes
- Query handlers and `cs.cadence.sideEffect`

Until then, workflow authors should keep their workflows to a
linear sequence of scheduleActivity calls.

## Observability

The poller emits metrics:

- polls total
- tasks received
- tasks completed
- respond errors
- inflight tasks

The poller logs task identifiers:

- domain
- tasklist
- workflowId
- runId
- activityId
