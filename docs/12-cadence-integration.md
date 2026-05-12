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
