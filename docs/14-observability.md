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

The API exposes:

- `GET /v1/tenants/{tenant}/activations/{id}/logs?cursor=...`

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

## SLO reporting

The platform computes:

- availability for HTTP invokes
- p99 latency for HTTP invokes
- error rate per function version

The platform exports these as metrics for dashboards.
