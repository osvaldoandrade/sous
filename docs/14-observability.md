# Observability

`code-sous` emits logs, metrics, and traces.

This file defines required signals and labels.

## Logging

### Structured logs

All services emit JSON logs with:

- `ts_ms`
- `level`
- `service`
- `request_id`
- `tenant`
- `namespace`
- `function`
- `activation_id`

### Activation logs

The invoker captures user stdout and stderr.
The invoker writes logs to KVRocks in chunks.

The persistence layer enforces a hard per-activation cap on cumulative log
bytes (default 1 MiB, configurable via `cs_invoker_pool.limits.max_log_bytes`).
Writes that would exceed the cap are truncated cleanly on a UTF-8 boundary;
the chunk index gets a trailing sentinel
`{"truncated": true, "reason": "log_limit_exceeded", "limit_bytes": N}` and
subsequent log writes for that activation become no-ops. See
`docs/04-api-rest.md` for the full read contract (pagination, ndjson/SSE
streaming, `X-CS-Truncated: logs` header).

The API exposes:

- `GET /v1/tenants/{tenant}/activations/{id}/logs?cursor=...&limit=...&format=ndjson|sse`

## Metrics

Metrics use Prometheus format.

### Control plane

- `cs_api_requests_total{route,method,status}`
- `cs_api_request_duration_ms_bucket{route,method}`

### Data plane

- `cs_invocations_total{tenant,namespace,function,version,trigger,status}`
- `cs_invocation_duration_ms_bucket{tenant,namespace,function,version,trigger}`
- `cs_invoker_inflight{tenant,namespace,function,version}`
- `cs_invoker_queue_lag_ms{topic="cs.invoke"}`
- `cs_invoker_cache_items`
- `cs_invoker_cache_bytes`
- `cs_invoker_cold_starts_total{runtime="cs-js"}`

### Scheduler

- `cs_scheduler_ticks_total{tenant,namespace,schedule}`
- `cs_scheduler_publish_errors_total`

### Cadence poller

- `cs_cadence_polls_total{tenant,domain,tasklist}`
- `cs_cadence_tasks_total{tenant,domain,tasklist,status}`
- `cs_cadence_heartbeat_total{tenant,domain,tasklist}`

## Tracing

The gateway accepts `traceparent`.

The gateway injects trace context into InvocationRequest:

- `trigger.source.traceparent`

The invoker creates a span:

- `cs.invoke`

The invoker attaches:

- `activation_id`
- `function`
- `version`
- `trigger.type`

### Agent decision-tree tracing (E7.01)

`code-sous` links each activation to the activation that triggered it so
operators can reconstruct an agent's call tree without grepping correlated
logs.

#### Header contract

- Inbound: `cs-http-gateway` reads `X-CS-Parent-Activation` from the
  incoming HTTP request. When present, the value is forwarded into
  `InvocationRequest.trigger.source.parent_activation_id`.
- Outbound: the `cs-js` runtime egress shim (`cs.http.fetch`) injects
  `X-CS-Parent-Activation: <current_activation_id>` on every outbound HTTP
  call so a downstream `cs` function automatically becomes a child node.
- Context: services propagate the value through Go context via
  `observability.WithParent(ctx, id)` / `observability.ParentFrom(ctx)`.

#### Activation record fields

Each `ActivationRecord` stamps two fields:

- `parent_activation_id` — empty for root activations; the parent
  activation ID otherwise.
- `root_activation_id` — equals the activation's own ID for roots; inherits
  the parent's `root_activation_id` for children. Lets callers fetch the
  full tree from any node without walking up to the root first.

#### Children index

The store maintains `cs:act:{tenant}:{parent_activation_id}:children`, a
list of child activation IDs (oldest-first traversal once `kv.Store`
reverses the underlying `LPUSH`). The index TTL matches the parent
activation TTL so it expires alongside the activation record.

#### Tree endpoint

`GET /v1/tenants/{tenant}/activations/{id}/tree` returns a bounded BFS
walk of the children index rooted at `id`. The walk:

- visits at most `max_depth = 10` levels;
- returns at most `max_nodes = 1000` nodes;
- skips already-visited IDs so a cycle cannot inflate the response;
- emits placeholder nodes (only `activation_id` / `parent_activation_id`)
  for children whose activation record has expired.

When either cap fires the response sets `"truncated": true` so the CLI can
flag the partial view.

Response shape:

```json
{
  "activation_id": "<root>",
  "tree": {
    "activation_id": "<root>",
    "root_activation_id": "<root>",
    "function": "<name>",
    "trigger_type": "http",
    "status": "success",
    "start_ms": 0,
    "duration_ms": 0,
    "children": [
      { "activation_id": "<child>", "parent_activation_id": "<root>", "children": [] }
    ]
  },
  "truncated": false,
  "max_depth": 10,
  "max_nodes": 1000
}
```

## SLO reporting

The platform computes:

- availability for HTTP invokes
- p99 latency for HTTP invokes
- error rate per function version

The platform exports these as metrics for dashboards.
