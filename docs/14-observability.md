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

## SLO reporting

The platform's monthly availability targets are pinned in
`docs/02-requirements.md`:

- Control plane monthly availability: **99.9%**.
- Data plane monthly availability: **99.9%**.

Those targets are codified in the declarative SLO spec at
`deploy/observability/slo.yaml`. The same directory ships a Prometheus
rule file (`deploy/observability/alerts.rules.yaml`) with multi-window
multi-burn-rate (MWMBR) alerts. Operators load the rule file into their
Prometheus and bring their own Alertmanager receivers; receiver
configuration is out of scope.

### SLIs

| SLO | Tier | Objective | Window | SLI |
| --- | --- | --- | --- | --- |
| `control_plane_availability` | control-plane | 99.9% | 30d | `sum(rate(cs_api_requests_total{status!~"5.."}[5m])) / sum(rate(cs_api_requests_total[5m]))` |
| `data_plane_availability` | data-plane | 99.9% | 30d | `sum(rate(cs_invocations_total{status="success"}[5m])) / sum(rate(cs_invocations_total[5m]))` |
| `data_plane_latency_http` | data-plane | 99.0% | 30d | Fraction of HTTP invocations faster than the 3000 ms default invoke timeout (from `cs_invocation_duration_ms_bucket{trigger="http"}`). |

The 3000 ms latency threshold matches the default HTTP invoke timeout in
`docs/02-requirements.md` ("Timeouts"). The error budget for the 99.9%
SLOs is `0.001`; for the 99.0% latency SLO it is `0.010`.

### Burn-rate alert tiers

Each SLO produces four alerts: two PAGE (fast / slow) and two TICKET
(fast / slow). Each alert pairs a long-window burn rate with a short
confirmation window so stale spikes that have already recovered do not
keep paging.

| Action | Long window | Burn rate | Short window | Time-to-detect at full burn |
| --- | --- | --- | --- | --- |
| PAGE | 1h | 14.4x | 5m | ~2 min |
| PAGE | 6h | 6x | 30m | ~15 min |
| TICKET | 24h | 3x | 2h | ~1 h |
| TICKET | 3d | 1x | 6h | ~3 h |

A "burn rate" of `Nx` means the running failure ratio exceeds
`N * (1 - objective)`. At 14.4x against a 99.9% SLO the monthly error
budget would be exhausted in roughly two days; at 1x it is exhausted
exactly at the window end.

Alert labels (`severity`, `slo`, `service`, `tier`, `runbook`) drive
Alertmanager routing. Each alert carries a `runbook_url` annotation; the
default values point at placeholder runbooks under
`https://runbooks.example/<slug>` until operator-specific runbooks land.

### Exclusions

`slo.yaml` documents three exclusions that operators apply at the
recording-rule layer (they are not automatic in the shipped templates):

- `planned_maintenance` — announced maintenance windows.
- `tenant_throttled_429` — 429 responses from per-tenant quotas are
  by-design and not part of the 5xx availability burn.
- `user_function_runtime_errors` — user-code exceptions surface as
  `status="error"` on `cs_invocations_total` but reflect user fault, not
  platform availability; partition with the tenant/function labels when
  reporting.

### Validating the rules

Run the YAML parse plus optional `promtool check rules` step that ships
with the repo:

```bash
make slo-validate
```

`promtool` is optional; without it the target degrades to a pure YAML
parse so it can run in any minimal CI container.

The platform also exports the raw signals that feed the SLIs above
(`cs_api_requests_total`, `cs_invocations_total`,
`cs_invocation_duration_ms_bucket`) for use in dashboards.
