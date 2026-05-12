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

## Activation sampling (E7.02)

The invoker can downsample activation records on a per-trigger basis so
the volume of activation rows, logs, and result blobs written to KVRocks
stays bounded under sustained load. Sampling is opt-in per trigger; an
unset policy (or a trigger created before E7.02) keeps the historical
"record everything" behaviour byte-for-byte.

### Modes

| Mode | Behaviour |
|------|-----------|
| `always` (default) | Records every activation in full. |
| `head` | Records the first `head_per_minute` activations per (tenant, function-ref) per rolling minute. Subsequent activations in that window are not persisted at all (no activation row, no logs). |
| `tail` | Always writes a skeleton row (`id`, `status`, `started_at`, `duration_ms`) and only promotes to a full record (result blob + logs) when the activation status is not `success` (`tail_on_error: true`) or when `duration_ms >= tail_on_slow_ms`. |
| `probabilistic` | Hashes the activation id and keeps a fraction equal to `probability` (FNV-1a, deterministic across replicas). |

### Configuration

Sampling lives on the trigger record. Example YAML for a JSON-formatted
trigger (the schema is identical when the trigger is created via the
control-plane API):

```yaml
sampling:
  mode: head            # always | head | tail | probabilistic
  head_per_minute: 50   # only meaningful when mode == head
  tail_on_error: true   # only meaningful when mode == tail
  tail_on_slow_ms: 250  # only meaningful when mode == tail
  probability: 0.05     # only meaningful when mode == probabilistic
```

Validation is enforced by `SamplingPolicy.Normalize`:

- `mode: head` requires `head_per_minute > 0`.
- `mode: tail` requires either `tail_on_error: true` or `tail_on_slow_ms > 0`.
- `mode: probabilistic` requires `probability` in `[0, 1]`.
- Unknown / unparseable policies fall back to `always` so a misconfigured
  trigger never silently drops production traffic.

### Retention impact

Sampling reduces three storage signals:

- **Activation rows** — skipped activations (head over cap, probabilistic
  below threshold) skip both `PutActivationRunning` and
  `CompleteActivationCAS`. There is no row to read; `GET .../activations/{id}`
  returns `404`.
- **Logs** — log chunks are only persisted when `PersistLogs` is true on
  the final decision. Tail-sampled successes therefore drop log chunks
  even though the skeleton row remains.
- **Result blob** — the `result` and `error` fields are only stamped on
  the terminal record when the final decision is `PersistFull`. Tail
  successes write the row but leave both blobs nil.

Each activation record carries the `sampling_decision` field
(`always`, `head`, `tail`, `probabilistic`, `skipped`) so operators can
audit the policy choice via the activation read API or KVRocks dumps.

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

## Reference dashboards

`code-sous` ships two Grafana dashboards covering the control and execution
planes. The JSON files live under
[`deploy/observability/`](../deploy/observability/) and import cleanly into
Grafana 10+ (`schemaVersion: 38`).

| File                                              | Title                       | UID                  | Focus                                                  |
|---------------------------------------------------|-----------------------------|----------------------|--------------------------------------------------------|
| [`control-plane.json`](../deploy/observability/control-plane.json) | code-sous / Control plane   | `cs-control-plane`   | API request rate, p50/p95/p99 latency, publish latency, alias-swap rate, function-create rate, version growth, draft uploads, control-plane error rate, 30d error-budget burn (99.9% target). |
| [`execution.json`](../deploy/observability/execution.json)         | code-sous / Execution plane | `cs-execution-plane` | Per-tenant invocations/s by trigger, success vs error split, p50/p95/p99 invocation latency by trigger, error rate by error-code, rate-limit deny rate (HTTP 429), invoker inflight, queue lag, cold-start rate, runtime cache occupancy, top-N functions, activation-log volume. |

Both dashboards expose two template variables:

- `$datasource` — Prometheus datasource selector.
- `$tenant` — multi-select, derived from
  `label_values(cs_invocations_total, tenant)`. Defaults to `All`.

Every dashboard carries a top-level `cs_dashboard_version` field so operators
can detect drift after re-import. The 30-day error-budget burn panel on the
control-plane dashboard reuses the same 99.9% objective codified in
`deploy/observability/slo.yaml`, so dashboards and burn-rate alerts share a
single source of truth.

Import procedures (Grafana UI, HTTP API, Kubernetes sidecar ConfigMap) and
the expected Prometheus scrape configuration are documented in
[`deploy/observability/README.md`](../deploy/observability/README.md). The
Makefile target `make dashboards-validate` JSON-parses every dashboard under
`deploy/observability/` and is wired into CI alongside `make slo-validate`.

## Audit

`cs-control` emits a structured audit event for every successful
control-plane mutation (create function, soft-delete, draft upload,
publish version, set alias, create/delete schedule, register/delete
cadence worker binding). Events are emitted **after** the KV mutation
commits — phantom mutations are never logged.

### Event shape

```json
{
  "schema_version": "1",
  "event_id": "evt_8b2…",
  "ts": "2026-05-12T18:04:33.124Z",
  "tenant": "t_acme",
  "actor": "u_42",
  "action": "function.publish",
  "resource": "fn://t_acme/orders/charge@v3",
  "outcome": "success",
  "request_id": "req_2…",
  "detail": {"sha256": "…", "alias": "prod"}
}
```

- `schema_version` is the wire-schema version. It evolves additively;
  consumers must accept unknown fields.
- `action` is the dotted name of the mutation
  (`function.create`, `function.delete`, `function.draft.upload`,
  `function.publish`, `function.alias.set`, `schedule.create`,
  `schedule.delete`, `cadence.worker.create`,
  `cadence.worker.delete`).
- `resource` is a URN-style identifier
  (`fn://<tenant>/<namespace>/<name>[@v<version>]`,
  `schedule://<tenant>/<namespace>/<name>`,
  `cadence://<tenant>/<namespace>/<name>`).
- `outcome` is one of `success`, `denied`, `error`. Mutation handlers
  only emit `success` today; the failure outcomes are reserved for
  future handler instrumentation and the schema is stable.
- `detail` is an open map of allowlisted attributes. Secret material
  and bundle bytes are never written here.

### Sinks

Three sinks ship with the binary, selected via `plugins.audit.sink`:

- `stdout` (default) — one JSON line per event to stdout. Best for
  development and Kubernetes pod logs.
- `codeq` — publish each event as an `AuditEvent` envelope on the
  topic `<topic_prefix>.<tenant>` (`topic_prefix` defaults to
  `cs.audit`).
- `webhook` — POST each event to `plugins.audit.webhook_url` with an
  HMAC-SHA256 signature in the `X-CS-Audit-Signature` header (hex
  encoded, keyed by `plugins.audit.hmac_secret`). Compatible with
  Splunk HEC, Datadog logs, and any SIEM that accepts signed JSON.

Sink failures do not roll back the control-plane mutation (the kv
commit is already durable). The recorder logs a `SinkLag` warning
through the structured logger so operators can investigate.

### Replay endpoint

`GET /v1/tenants/{tenant}/audit?since=&actor=&action=&limit=`

Returns the recent audit history for the requesting tenant. Backed by
a per-tenant ring buffer in KVRocks (TTL tracks
`cs_control.limits.activation_ttl_seconds`, default 1000 entries).
For long-term retention, subscribe to the configured codeq topic or
forward the webhook to your SIEM.

The endpoint enforces the `cs:audit:read` Tikti action and rejects
cross-tenant requests at the existing authorize() layer
(`CS_AUTHZ_RESOURCE_MISMATCH`).

Query parameters:

- `since` — Unix milliseconds; only events at or after this timestamp.
- `actor` — Tikti `sub` exact match.
- `action` — exact dotted action name.
- `limit` — page size, default 100, cap 500.
