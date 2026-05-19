# Reference: Schemas

This page consolidates the JSON schemas Sous uses for persisted records and on-the-wire messages. Each schema is the authoritative contract — the control plane validates at publish, the gateway validates at invoke, and the contract tests validate in CI. Where a schema lives as a `.json` file under `spec/`, the file is the canonical source and this page transcribes it verbatim. Where a schema lives as a Go struct in `internal/api/types.go`, this page documents the JSON shape the struct produces with `encoding/json`. All schemas target JSON Schema draft 2020-12. When a field is appended to a schema, the change is additive — existing producers and consumers round-trip the new schema unchanged.

## 1. Function manifest

The function manifest (`manifest.json` inside the bundle) declares the runtime, entry, handler, limits, and capabilities a function needs at activation time. The control plane validates the manifest at draft upload and again at publish, refusing any value outside the schema. Capability fields are the operative authorisation surface — the runtime enforces them at invoke time. Consumers: [Concepts-Function-Lifecycle](Concepts-Function-Lifecycle), [Runtime-cs-js](Runtime-cs-js), [Security](Security).

Source: `spec/cs.function.script.v1.json`.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.function.script.v1",
  "type": "object",
  "required": ["schema", "runtime", "entry", "handler", "limits", "capabilities"],
  "properties": {
    "schema": { "const": "cs.function.script.v1" },
    "runtime": { "const": "cs-js" },
    "entry": { "type": "string", "pattern": "^[a-zA-Z0-9._/-]+$" },
    "handler": { "type": "string", "const": "default" },
    "limits": {
      "type": "object",
      "required": ["timeoutMs", "memoryMb", "maxConcurrency"],
      "properties": {
        "timeoutMs": { "type": "integer", "minimum": 1, "maximum": 900000 },
        "memoryMb": { "type": "integer", "minimum": 16, "maximum": 4096 },
        "maxConcurrency": { "type": "integer", "minimum": 1, "maximum": 100 }
      },
      "additionalProperties": false
    },
    "capabilities": {
      "type": "object",
      "required": ["kv", "codeq", "http"],
      "properties": {
        "kv": {
          "type": "object",
          "required": ["prefixes", "ops"],
          "properties": {
            "prefixes": {
              "type": "array",
              "items": { "type": "string", "minLength": 1, "maxLength": 256 },
              "maxItems": 64
            },
            "ops": {
              "type": "array",
              "items": { "enum": ["get", "set", "del"] },
              "maxItems": 3
            }
          },
          "additionalProperties": false
        },
        "codeq": {
          "type": "object",
          "required": ["publishTopics"],
          "properties": {
            "publishTopics": {
              "type": "array",
              "items": { "type": "string", "minLength": 1, "maxLength": 256 },
              "maxItems": 64
            }
          },
          "additionalProperties": false
        },
        "http": {
          "type": "object",
          "required": ["allowHosts", "timeoutMs"],
          "properties": {
            "allowHosts": {
              "type": "array",
              "items": { "type": "string", "minLength": 1, "maxLength": 253 },
              "maxItems": 128
            },
            "timeoutMs": { "type": "integer", "minimum": 1, "maximum": 30000 }
          },
          "additionalProperties": false
        }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

The Go struct that backs the manifest at runtime — `FunctionManifest` in `internal/api/types.go` — adds two append-only fields beyond the schema above: `runtimeVersion` (optional adapter pin such as `cs-js@1` or `python3.12`) and `imports` (the publisher-declared dependency map for the cs-js bundler). Both fields are optional and existing v1 manifests round-trip without them.

## 2. InvocationRequest

Every trigger emits an `InvocationRequest` onto the cs.invoke topic. The schema is the contract between trigger and invoker — the gateway, scheduler, Cadence poller, and SDK all produce it; `cs-invoker-pool` is the sole consumer. Consumers: [HTTP-Invoke-Path](HTTP-Invoke-Path), [Scheduler](Scheduler), [Cadence-Integration](Cadence-Integration), [codeQ-Protocol](codeQ-Protocol).

Source: `spec/cs.invoke.v1.json`.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.invoke.v1",
  "type": "object",
  "required": [
    "activation_id",
    "request_id",
    "tenant",
    "namespace",
    "ref",
    "trigger",
    "principal",
    "deadline_ms",
    "event"
  ],
  "properties": {
    "activation_id": { "type": "string", "format": "uuid" },
    "request_id": { "type": "string", "minLength": 8, "maxLength": 64 },
    "tenant": { "type": "string", "minLength": 3, "maxLength": 64 },
    "namespace": { "type": "string", "minLength": 1, "maxLength": 64 },
    "ref": {
      "type": "object",
      "required": ["function"],
      "properties": {
        "function": { "type": "string", "minLength": 1, "maxLength": 64 },
        "alias": { "type": "string", "minLength": 1, "maxLength": 32 },
        "version": { "type": "integer", "minimum": 1 }
      },
      "additionalProperties": false
    },
    "trigger": {
      "type": "object",
      "required": ["type", "source"],
      "properties": {
        "type": { "enum": ["http", "schedule", "cadence", "api"] },
        "source": { "type": "object" }
      },
      "additionalProperties": false
    },
    "principal": {
      "type": "object",
      "required": ["sub", "roles"],
      "properties": {
        "sub": { "type": "string", "minLength": 1, "maxLength": 256 },
        "roles": {
          "type": "array",
          "items": { "type": "string", "minLength": 1, "maxLength": 256 },
          "maxItems": 256
        }
      },
      "additionalProperties": false
    },
    "deadline_ms": { "type": "integer", "minimum": 1 },
    "event": {}
  },
  "additionalProperties": false
}
```

The `event` field is intentionally open: each trigger type fills it with the input the user code will see (the HTTP request body, the schedule payload, the Cadence activity input). The runtime decodes `event` through the per-binding codec when the trigger is Cadence.

## 3. InvocationResult

After user code runs (or the activation times out), `cs-invoker-pool` publishes an `InvocationResult` onto the cs.results topic. The gateway correlates the result back to the originating HTTP request by `request_id`. Consumers: [HTTP-Invoke-Path](HTTP-Invoke-Path), [codeQ-Protocol](codeQ-Protocol), [Observability](Observability).

Source: `spec/cs.results.v1.json`.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.results.v1",
  "type": "object",
  "required": ["activation_id", "request_id", "status", "duration_ms"],
  "properties": {
    "activation_id": { "type": "string", "format": "uuid" },
    "request_id": { "type": "string", "minLength": 8, "maxLength": 64 },
    "status": { "enum": ["success", "error", "timeout"] },
    "duration_ms": { "type": "integer", "minimum": 0 },
    "result": {
      "type": "object",
      "properties": {
        "statusCode": { "type": "integer", "minimum": 100, "maximum": 599 },
        "headers": { "type": "object", "additionalProperties": { "type": "string" } },
        "body": { "type": "string" },
        "isBase64Encoded": { "type": "boolean" }
      },
      "additionalProperties": false
    },
    "error": {
      "type": "object",
      "properties": {
        "type": { "type": "string", "minLength": 1, "maxLength": 128 },
        "message": { "type": "string", "maxLength": 65536 },
        "stack": { "type": "string", "maxLength": 8192 }
      },
      "additionalProperties": false
    }
  },
  "additionalProperties": false
}
```

The `result` and `error` fields are mutually exclusive: a `success` status carries `result`, an `error` or `timeout` status carries `error`. The status code inside `result.statusCode` is the value the function returned, not the HTTP status of the gateway's response.

## 4. Audit event envelope

Audit events are emitted by `cs-control` on the success path of every mutation and shipped to one or more sinks (stdout JSON, codeQ topic, HMAC-signed webhook). The envelope is the wire shape the sinks marshal — the in-process Go view is `audit.Event` in `internal/audit/event.go`, and the schema below documents the JSON it produces. The contract is append-only: new fields may be added but existing ones are never renamed or removed. The `schema_version` field exists so consumers can fan out on the wire schema independently of the Go struct. Consumers: [ledgerDB-Audit](ledgerDB-Audit), [Security](Security), [Observability](Observability).

Source: `internal/audit/event.go` (no separate JSON file; the schema below is the transcribed contract).

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.audit.event.v1",
  "type": "object",
  "required": ["schema_version", "ts", "tenant", "action", "resource", "outcome"],
  "properties": {
    "schema_version": { "const": "1" },
    "event_id": { "type": "string", "minLength": 1, "maxLength": 128 },
    "ts": { "type": "string", "format": "date-time" },
    "tenant": { "type": "string", "minLength": 3, "maxLength": 64 },
    "actor": { "type": "string", "maxLength": 256 },
    "action": { "type": "string", "minLength": 1, "maxLength": 128 },
    "resource": { "type": "string", "minLength": 1, "maxLength": 512 },
    "outcome": { "enum": ["success", "denied", "error"] },
    "request_id": { "type": "string", "maxLength": 64 },
    "detail": {
      "type": "object",
      "additionalProperties": true
    }
  },
  "additionalProperties": false
}
```

`action` follows the dotted form `<entity>.<verb>` — for example `function.publish`, `alias.set`, `binding.update`. `resource` is a URN-style identifier such as `fn://tenant/ns/name@v3` or `alias://tenant/ns/name/prod`. `detail` is an open map for additional structured context (`draft_id`, `version`, `alias`); the audit recorder never places secret material or bundle bytes there.

## 5. WorkerBinding

A `WorkerBinding` ties a Cadence task list to a function. The poller in `cs-cadence-poller` reads bindings to learn which domain and task list to long-poll, how to map activity names to function refs, and which codecs to apply on the wire. The Go struct lives at `internal/api/types.go` (`WorkerBinding`); the schema below documents the JSON form. Consumers: [Cadence-Integration](Cadence-Integration), [REST-API](REST-API).

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.api.cadence.worker.v1",
  "type": "object",
  "required": [
    "tenant",
    "namespace",
    "name",
    "domain",
    "tasklist",
    "worker_id",
    "activity_map",
    "pollers",
    "limits",
    "enabled"
  ],
  "properties": {
    "tenant": { "type": "string", "minLength": 3, "maxLength": 64 },
    "namespace": { "type": "string", "minLength": 1, "maxLength": 64 },
    "name": { "type": "string", "minLength": 3, "maxLength": 64 },
    "domain": { "type": "string", "minLength": 1, "maxLength": 128 },
    "tasklist": { "type": "string", "minLength": 1, "maxLength": 128 },
    "worker_id": { "type": "string", "minLength": 1, "maxLength": 128 },
    "activity_map": {
      "type": "object",
      "additionalProperties": {
        "type": "object",
        "required": ["function"],
        "properties": {
          "function": { "type": "string", "minLength": 1, "maxLength": 64 },
          "alias": { "type": "string", "minLength": 1, "maxLength": 32 },
          "version": { "type": "integer", "minimum": 1 }
        },
        "additionalProperties": false
      }
    },
    "pollers": {
      "type": "object",
      "properties": {
        "activity": { "type": "integer", "minimum": 1, "maximum": 256 }
      },
      "additionalProperties": false
    },
    "limits": {
      "type": "object",
      "properties": {
        "max_inflight_tasks": { "type": "integer", "minimum": 1, "maximum": 100000 }
      },
      "additionalProperties": false
    },
    "enabled": { "type": "boolean" },
    "input_codec": { "enum": ["", "json", "msgpack", "raw"] },
    "output_codec": { "enum": ["", "json", "msgpack", "raw"] },
    "kind": { "enum": ["", "activity", "workflow"] }
  },
  "additionalProperties": false
}
```

`kind` selects which Cadence task surface the binding polls: `activity` (the v0.1 default, also used when omitted) long-polls ActivityTasks and routes them through `activity_map`; `workflow` long-polls DecisionTasks and dispatches them to the workflow executor. Workflow bundles are subject to the determinism linter at publish time and fail with `CS_WORKFLOW_NON_DETERMINISTIC` if they call banned APIs.

The create-time request shape, `CreateWorkerBindingRequest`, omits the server-owned fields (`tenant`, `namespace`, `enabled`) — the path parameters carry tenant and namespace and the server initialises `enabled` to true.

## 6. Schedule

A `ScheduleRecord` periodically emits an `InvocationRequest` for a function ref. The v0.1 form uses `every_seconds`; the E4.01 form adds cron expressions with timezone and jitter. An overlap policy controls what happens when the previous tick is still running. The Go struct lives at `internal/api/types.go` (`ScheduleRecord`); the schema below documents the JSON form. Consumers: [Scheduler](Scheduler), [REST-API](REST-API).

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.api.schedule.v1",
  "type": "object",
  "required": [
    "tenant",
    "namespace",
    "name",
    "every_seconds",
    "overlap_policy",
    "ref",
    "enabled",
    "created_at_ms"
  ],
  "properties": {
    "tenant": { "type": "string", "minLength": 3, "maxLength": 64 },
    "namespace": { "type": "string", "minLength": 1, "maxLength": 64 },
    "name": { "type": "string", "minLength": 3, "maxLength": 64 },
    "every_seconds": { "type": "integer", "minimum": 0, "maximum": 86400 },
    "overlap_policy": { "enum": ["skip", "queue", "parallel"] },
    "ref": {
      "type": "object",
      "required": ["function"],
      "properties": {
        "function": { "type": "string", "minLength": 1, "maxLength": 64 },
        "alias": { "type": "string", "minLength": 1, "maxLength": 32 },
        "version": { "type": "integer", "minimum": 1 }
      },
      "additionalProperties": false
    },
    "payload": {},
    "enabled": { "type": "boolean" },
    "created_at_ms": { "type": "integer", "minimum": 0 },
    "kind": { "enum": ["", "interval", "cron"] },
    "cron": { "type": "string", "maxLength": 128 },
    "tz": { "type": "string", "maxLength": 64 },
    "jitter_ms": { "type": "integer", "minimum": 0 }
  },
  "additionalProperties": false
}
```

Validation rejects schedules that mix `every_seconds` and `cron` — a schedule is either interval-based (`kind: "interval"`, default when `every_seconds` is set) or cron-based (`kind: "cron"`, used when `cron` is set). `tz` defaults to UTC when empty. `jitter_ms` adds a deterministic offset to each computed fire time so a fleet of schedules with the same period does not tick in lock-step.

The current state of a schedule — when it should fire next, which tick sequence it is on — lives in `ScheduleState`:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "cs.api.schedule.state.v1",
  "type": "object",
  "required": ["next_tick_ms", "tick_seq"],
  "properties": {
    "next_tick_ms": { "type": "integer", "minimum": 0 },
    "tick_seq": { "type": "integer", "minimum": 0 }
  },
  "additionalProperties": false
}
```

The create-time request shape, `CreateScheduleRequest`, omits the server-owned fields (`tenant`, `namespace`, `enabled`, `created_at_ms`) and otherwise mirrors the record.
