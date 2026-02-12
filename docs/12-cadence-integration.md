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
  }
}
```

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
- The repeated task generates a new Activation.

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
