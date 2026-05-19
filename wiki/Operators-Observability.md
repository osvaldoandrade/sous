# Operators: Observability

Sous exposes three operational signals: structured JSON logs that every service writes to stdout, Prometheus metrics scraped from each binary's `/metrics` endpoint, and W3C trace context that flows from the gateway through the invoker into downstream services. Together they form the observability contract operators rely on to keep the platform inside its SLOs and to investigate incidents.

The signals share two cross-cutting identifiers that operators chase from end to end: `request_id` correlates every record produced for a single external request, and `activation_id` correlates every record produced for a single function execution. The gateway stamps `request_id` on entry; the invoker stamps `activation_id` when it allocates an activation record. Both identifiers ride along through log entries, metric labels (where cardinality allows), audit events, and trace attributes.

## Structured logs

Every service writes JSON to stdout, one line per record, produced by `internal/observability.Logger`. The record shape is fixed:

```json
{
  "ts_ms": 1737302400123,
  "level": "info",
  "service": "cs-invoker-pool",
  "message": "activation completed",
  "request_id": "req_2f9c…",
  "tenant": "t_acme",
  "namespace": "payments",
  "function": "charge",
  "activation_id": "act_8e1a…"
}
```

`ts_ms` is the millisecond Unix timestamp, `level` is one of `info`, `warn`, `error`, and `service` is the short name of the binary that produced the record. The correlation fields are omitted when absent (e.g. the boot record carries no `request_id`) and never logged with empty strings.

Three streams flow through the log pipeline:

- **Service logs** — operator-facing records that describe lifecycle, configuration, and unexpected conditions. Written to stdout on every service.
- **Activation logs** — user-facing records captured from the function runtime. `cs-invoker-pool` chunks the captured stdout/stderr and persists each chunk into KVRocks under the tenant's activation key. Operators do not read activation logs from stdout; tenants read them through the [REST API](REST-API) at `GET /v1/tenants/{tenant}/activations/{id}/logs?cursor=…&format=ndjson|sse`. The persistence layer caps cumulative bytes at `cs_invoker_pool.limits.max_log_bytes` (default 1 MiB). Writes that exceed the cap are truncated cleanly on a UTF-8 boundary and the chunk index gets a trailing `{"truncated": true, "reason": "log_limit_exceeded", "limit_bytes": N}` sentinel.
- **Audit events** — control-plane mutation logs covered separately in [ledgerDB Audit](Enabled-Services-ledgerDB-Audit). The audit sink is `plugins.audit.sink` and is configured independently from the service-log stream.

Production deployments scrape stdout through a node-level collector (fluent-bit, Vector, the Loki agent) and route records to whatever long-term store the operator already runs. Sous does not bundle a log aggregator.

## Metrics

Every binary exposes a Prometheus endpoint at `/metrics` on the address the binary listens on (`cs_control.http.addr`, `cs_http_gateway.http.addr`, `cs_invoker_pool.http.addr`, plus the `:8083` and `:8084` defaults for `cs-scheduler` and `cs-cadence-poller`). The registry is unauthenticated; operators put it behind a NetworkPolicy or a service mesh allowlist.

The metric inventory below is the contract honoured by the dashboards and alert rules in `deploy/observability/`. Adding a metric is additive; removing one is a breaking change tracked through [Operators Migrations](Operators-Migrations).

### Control plane

`cs-control` and `cs-http-gateway` co-emit the request-level series:

- `cs_api_requests_total{route,method,status}` — counter of API requests labelled by templated route, HTTP method, and integer status code. Drives the control-plane availability SLO.
- `cs_api_request_duration_ms_bucket{route,method}` — histogram of API request latency in milliseconds. Drives the control-plane latency panels on the Grafana dashboard.

### Data plane

`cs-invoker-pool` emits the activation-level series:

- `cs_invocations_total{tenant,namespace,function,version,trigger,status}` — counter of completed activations labelled by function ref, trigger type, and terminal status. `status` is one of `success`, `error`, `timeout`. Drives the data-plane availability SLO.
- `cs_invocation_duration_ms_bucket{tenant,namespace,function,version,trigger}` — histogram of activation duration in milliseconds. Drives the data-plane latency SLO and the per-trigger latency panels.
- `cs_invoker_inflight{tenant,namespace,function,version}` — gauge of currently executing activations. Watch this against `cs_invoker_pool.workers.max_inflight` and `internal/limits.DefaultTenantMaxInflight`.
- `cs_invoker_inflight_rejected_total` — counter of synchronous activations rejected because the per-tenant or global semaphore was saturated. A non-zero rate indicates load-shedding.
- `cs_invoker_queue_lag_ms{topic="cs.invoke"}` — gauge of the oldest in-flight envelope's queue residency. The single most useful capacity signal — sustained growth means arrival exceeds service rate.
- `cs_invoker_cache_items` — gauge of bundles resident in the per-replica LRU cache.
- `cs_invoker_cache_bytes` — gauge of bytes resident in the same LRU cache. Watch against `cs_invoker_pool.cache.bytes_max`.
- `cs_invoker_cold_starts_total{runtime}` — counter of activations that paid the warm-up cost because the bundle was not cache-resident.
- `cs_invoke_retry_total{trigger,error_code}` — counter of async invocations that consumed a retry attempt.
- `cs_invoke_retry_success_total{trigger}` — counter of async invocations that succeeded on a retry (`attempt > 1`). Pair with `cs_invoke_retry_total` to compute retry effectiveness.
- `cs_invoke_dlq_total{trigger,error_code}` — counter of envelopes routed to the dead-letter topic after the retry budget was exhausted.

### Scheduler

`cs-scheduler` emits:

- `cs_scheduler_ticks_total{tenant,namespace,schedule}` — counter of schedule fires. Drives the per-schedule rate panels.
- `cs_scheduler_publish_errors_total` — counter of schedule fires that failed to publish to codeQ. A non-zero rate paired with `cs_invoker_queue_lag_ms` growth indicates a broker or persistence issue.

### Cadence poller

`cs-cadence-poller` emits:

- `cs_cadence_polls_total{tenant,domain,tasklist}` — counter of long-poll cycles completed against the Cadence frontend.
- `cs_cadence_tasks_total{tenant,domain,tasklist,status}` — counter of tasks dispatched to the invoker, labelled by terminal status (`completed`, `failed`, `timeout`, `cancelled`).
- `cs_cadence_heartbeat_total{tenant,domain,tasklist}` — counter of heartbeat calls issued back to Cadence during long-running activities.

### Cardinality budget

Tenant and function labels appear on the data-plane series; operators that run many tenants pre-aggregate these series at scrape time through a Prometheus `metric_relabel_configs` rule or pair the dashboards with a sufficiently sized Prometheus. The control-plane series intentionally omit tenant labels because the route templates are sufficient.

## Service Level Objectives

The platform pins three SLOs in `deploy/observability/slo.yaml`. The file is declarative and is the source of truth — every dashboard, alert, and capacity decision flows from it.

### control_plane_availability

Tier `control-plane`, objective `99.9%`, window `30d`, owner `cs-control`. The SLI is the fraction of API requests that do not return a 5xx response:

```
sum(rate(cs_api_requests_total{status!~"5.."}[5m]))
/
sum(rate(cs_api_requests_total[5m]))
```

4xx responses are treated as client errors and do not burn the budget. The SLO carves out `planned_maintenance` and the by-design `tenant_throttled_429` exclusion at the recording-rule layer.

### data_plane_availability

Tier `data-plane`, objective `99.9%`, window `30d`, owner `cs-invoker-pool`. The SLI is the fraction of invocations that complete with `status="success"`:

```
sum(rate(cs_invocations_total{status="success"}[5m]))
/
sum(rate(cs_invocations_total[5m]))
```

Every trigger type (HTTP, codeQ, schedule, Cadence) shares the same series because they share the same execution engine. The `user_function_runtime_errors` exclusion partitions user-code faults from platform faults; the runbook describes the canonical query that performs the split.

### data_plane_latency_http

Tier `data-plane`, objective `99.0%`, window `30d`, owner `cs-http-gateway`. The SLI is the fraction of synchronous HTTP invocations faster than the 3000 ms default invoke timeout pinned in `docs/02-requirements.md`:

```
sum(rate(cs_invocation_duration_ms_bucket{trigger="http",le="3000"}[5m]))
/
sum(rate(cs_invocation_duration_ms_bucket{trigger="http",le="+Inf"}[5m]))
```

Operators that loosen the platform invoke timeout adjust the latency threshold to match.

### Multi-window multi-burn-rate alerts

Each SLO produces four alerts, two PAGE and two TICKET, that follow the Google SRE Workbook pattern in `deploy/observability/alerts.rules.yaml`. Pairing a long-window burn rate with a short confirmation window suppresses stale spikes that have already recovered:

| Action | Long window | Burn rate | Short window | Time-to-detect at full burn |
| --- | --- | --- | --- | --- |
| PAGE | 1h | 14.4x | 5m | ~2 min |
| PAGE | 6h | 6x | 30m | ~15 min |
| TICKET | 24h | 3x | 2h | ~1 h |
| TICKET | 3d | 1x | 6h | ~3 h |

A burn rate of `Nx` means the running failure ratio exceeds `N * (1 - objective)`. At 14.4x against a 99.9% SLO, the monthly error budget would be exhausted in roughly two days; at 1x it is exhausted exactly at the window end.

Every alert carries `severity`, `slo`, `service`, `tier`, and `runbook` labels so Alertmanager can route into the right escalation path. The bundled `runbook_url` annotations point at placeholder URLs (`https://runbooks.example/<slug>`); operators rewrite these to their internal links after copying the rule file.

### Validating the rules

The repository ships a Make target that parses the SLO and alert YAML and, when `promtool` is on the PATH, runs the canonical Prometheus rule check:

```bash
make slo-validate
```

The target degrades to a pure YAML parse in minimal CI containers so it always runs.

## Reference dashboards

Sous bundles two Grafana dashboards under `deploy/observability/`. Both target Grafana 10+ (`schemaVersion: 38`) and carry a top-level `cs_dashboard_version` field so operators can detect drift after re-import.

| File | Title | UID | Focus |
|------|-------|-----|-------|
| `control-plane.json` | code-sous / Control plane | `cs-control-plane` | API request rate, p50/p95/p99 latency, publish latency, alias-swap rate, function-create rate, version growth, draft uploads, control-plane error rate, 30-day error-budget burn. |
| `execution.json` | code-sous / Execution plane | `cs-execution-plane` | Per-tenant invocations/s by trigger, success vs error split, p50/p95/p99 invocation latency by trigger, error rate by error-code, rate-limit deny rate, invoker inflight, queue lag, cold-start rate, runtime cache occupancy, top-N functions, activation-log volume. |

Both dashboards expose `$datasource` and `$tenant` template variables. `$tenant` is multi-select and derived from `label_values(cs_invocations_total, tenant)`; choosing `All` keeps every tenant in view, while picking a subset filters every per-tenant panel without manual editing.

Import procedures (UI, HTTP API, Kubernetes sidecar ConfigMap) and the expected Prometheus scrape configuration live in `deploy/observability/README.md`. Validate updates with:

```bash
make dashboards-validate
```

which JSON-parses every `.json` under `deploy/observability/`.

## Activation sampling

Long-running clusters can drown KVRocks in activation rows, logs, and result blobs. Sampling caps the volume on a per-trigger basis. Sampling is opt-in: an empty `sampling` block keeps the historical "record everything" behaviour byte-for-byte.

`cs-invoker-pool` reads the policy from the trigger record (`http`, `schedule`, `cadence`, `codeq`) and dispatches the activation through the matching `Decider` defined in `internal/observability/sampling.go`. Four modes are supported.

### always

Default when the policy is unset. Records every activation in full — metadata row, logs, result blob.

### head

Records the first `head_per_minute` activations per (tenant, function-ref) per rolling minute. Once the cap fires, subsequent activations in that window are dropped entirely: no metadata row, no logs, no result blob. `GET /v1/tenants/{tenant}/activations/{id}` returns `404` for dropped activations. Use head sampling when a flood of identical activations would otherwise dominate retention.

### tail

Writes a skeleton row (`id`, `status`, `started_at`, `duration_ms`) at start and promotes to a full record only when the outcome matches one of the configured rules:

- `tail_on_error: true` — promote whenever the terminal status is not `success`.
- `tail_on_slow_ms: N` — promote whenever the activation duration is at least `N` milliseconds.

Tail sampling keeps a complete audit trail of failures and slow calls while dropping logs and result blobs from the well-behaved successes that dominate steady-state traffic.

### probabilistic

Hashes the activation id with FNV-1a and keeps the activation if and only if `hash < probability * MaxUint64`. The decision is deterministic across replicas, so every replica picks the same activations to retain — useful for end-to-end tracing and reproducible cost models.

### Configuration

The block lives on the trigger record. The full example pasted in `config.example.yaml` (commented because it is not a YAML knob but a trigger field):

```yaml
trigger:
  type: http
  source:
    method: POST
    path: /webhook
  sampling:
    mode: tail            # always (default) | head | tail | probabilistic
    head_per_minute: 50   # required when mode == head
    tail_on_error: true   # required (with tail_on_slow_ms) when mode == tail
    tail_on_slow_ms: 250
    probability: 0.05     # required when mode == probabilistic, in [0,1]
```

Validation lives in `api.SamplingPolicy.Normalize`:

- `mode: head` requires `head_per_minute > 0`.
- `mode: tail` requires `tail_on_error: true` or `tail_on_slow_ms > 0`.
- `mode: probabilistic` requires `probability ∈ [0, 1]`.
- An unparseable or unknown policy falls back to `always` so a misconfigured trigger never silently drops production traffic.

Every persisted activation record carries the `sampling_decision` field (`always`, `head`, `tail`, `probabilistic`, `skipped`) so operators can audit the choice through the activation read API or through a KVRocks dump.

## Tracing

`cs-http-gateway` accepts a `traceparent` header and forwards it into `InvocationRequest.trigger.source.traceparent`. `cs-invoker-pool` creates a `cs.invoke` span per activation and attaches:

- `activation_id`
- `function`
- `version`
- `trigger.type`

The runtime egress shim (`cs.http.fetch`) injects `X-CS-Parent-Activation: <current_activation_id>` on every outbound HTTP call, so a downstream Sous function automatically becomes a child node. The control plane stamps `parent_activation_id` and `root_activation_id` onto every `ActivationRecord`; `GET /v1/tenants/{tenant}/activations/{id}/tree` returns a bounded BFS of the call graph rooted at `id`, capped at `max_depth = 10` levels and `max_nodes = 1000` nodes. Operators reconstructing an agent's decision tree from an incident bookmark this endpoint.

The W3C trace context is opaque to Sous beyond propagation; operators wire their preferred collector (Tempo, Jaeger, Honeycomb) into the OTLP exporter that the runtime ships with.

## Cross-references

- [Operators Runbooks](Operators-Runbooks) — the alert-to-action wiring that consumes the signals on this page.
- [Operators Configuration Reference](Operators-Configuration-Reference) — YAML knobs that shape the signals (`cs_invoker_pool.limits.max_log_bytes`, sampling cap defaults, port bindings).
- [Operators Capacity and Limits](Operators-Capacity-and-Limits) — how the queue-lag and inflight metrics relate to the throughput model.
- [ledgerDB Audit](Enabled-Services-ledgerDB-Audit) — the audit stream that complements service logs.
- `deploy/observability/slo.yaml` — declarative source of truth for the SLOs.
- `deploy/observability/alerts.rules.yaml` — Prometheus rule file.
- `deploy/observability/control-plane.json`, `deploy/observability/execution.json` — Grafana dashboards.
- `deploy/observability/README.md` — import procedures and scrape configuration.
- `internal/observability/` — metric, log, and sampling implementations.
