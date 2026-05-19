# Operators: Capacity and Limits

Sous enforces two families of limits. **Hard limits** are byte caps, time caps, and concurrency caps that the platform rejects at the edge — a request that exceeds a hard limit fails with a typed error and an HTTP status code. **Soft limits** are advisory throughput knobs that bound steady-state behaviour through rate limiting and queue backpressure. The first half of this page enumerates both families; the second half walks through the throughput and queueing models operators use to size a deployment.

Every numeric limit below is centralised in the `internal/limits` package, loaded through `limits.FromConfig`. Every enforcement site consults that package so the contract is uniform across `cs-control`, `cs-http-gateway`, `cs-invoker-pool`, and `cs-cadence-poller`. Operators who change a limit through the YAML do not need to grep for every call site — the load path picks up the new value once the binary restarts.

## Hard limits per function

| Surface | Limit | Default | Enforced at | On violation |
|---|---:|---:|---|---|
| Published bundle | bytes | 16,777,216 (16 MiB) | `cs-control` `PUT .../draft` | `413 CS_BUNDLE_TOO_LARGE` |
| Draft TTL | seconds | 86,400 (24 h) | `cs-control` KV write | draft becomes unresolvable; publish rejected |
| Activation TTL | seconds | 604,800 (7 d) | `cs-invoker-pool` / `internal/kv` | read returns `410 CS_ACTIVATION_TTL_EXPIRED` |
| HTTP request body | bytes | 6,291,456 (6 MiB) | `cs-http-gateway` invoke | `413 CS_BODY_TOO_LARGE` |
| HTTP header bytes | bytes | 65,536 (64 KiB) | `cs-http-gateway` invoke | `400 CS_VALIDATION_FAILED` |
| HTTP query bytes | bytes | 16,384 (16 KiB) | `cs-http-gateway` invoke | `400 CS_VALIDATION_FAILED` |
| Function result | bytes | 262,144 (256 KiB) | `cs-invoker-pool` result write | `413 CS_RESULT_TOO_LARGE` |
| Function error | bytes | 65,536 (64 KiB) | `cs-invoker-pool` error write | truncated, `X-CS-Truncated: error` |
| Activation logs | bytes | 1,048,576 (1 MiB) | `cs-invoker-pool` log write | truncated + sentinel `CS_LOG_LIMIT_EXCEEDED`, `X-CS-Truncated: logs` |
| HTTP timeout | ms | 3,000 | `cs-http-gateway` per-version | `504 CS_CODEQ_CORRELATION_TIMEOUT` |
| HTTP timeout max | ms | 30,000 | `cs-control` publish validation | `400 CS_VALIDATION_FAILED` |
| Worker timeout | ms | 30,000 | `cs-invoker-pool` runtime | `CS_RUNTIME_TIMEOUT` |
| Worker timeout max | ms | 900,000 | `cs-control` publish validation | `400 CS_VALIDATION_FAILED` |
| Schedule interval min | seconds | 1 | `cs-control` schedule validation | `400 CS_VALIDATION_FAILED` |
| Schedule interval max | seconds | 86,400 (24 h) | `cs-control` schedule validation | `400 CS_VALIDATION_FAILED` |

The bundle and timeout caps map to `cs_control.limits.max_bundle_bytes`, `cs_control.limits.draft_ttl_seconds`, and `cs_control.limits.activation_ttl_seconds` in `config.example.yaml`. The HTTP request caps map to `cs_http_gateway.limits.*`. The function-output caps map to `cs_invoker_pool.limits.*`. The platform reserves the right to lower the worker-timeout max in a future release; operators that depend on long-running activities migrate them to Cadence workflows.

## Hard limits per tenant

| Surface | Limit | Default | Enforced at | On violation |
|---|---:|---:|---|---|
| Tenant RPS (gateway) | rps | 200 | `cs-http-gateway` token bucket | `429 CS_RATE_LIMITED` + `Retry-After` |
| Function RPS (gateway) | rps | 20 | `cs-http-gateway` token bucket | `429 CS_RATE_LIMITED` + `Retry-After` |
| Tenant inflight (invoker) | activations | 64 | `cs-invoker-pool` semaphore | sync: `429 CS_TENANT_INFLIGHT_LIMIT`; async: queued in codeQ |
| Global inflight (invoker) | activations | 2,048 | `cs-invoker-pool` semaphore | sync: `429 CS_TENANT_INFLIGHT_LIMIT`; async: queued in codeQ |
| Cadence binding inflight | tasks | 256 | `cs-cadence-poller` semaphore | binding stops polling until a slot frees |
| Scheduler resolution | ms | 1,000 | `cs-scheduler` tick | — |

The gateway rate-limit knobs map to `cs_http_gateway.rate_limits.tenant_rps` and `cs_http_gateway.rate_limits.function_rps`. The invoker inflight caps map to `cs_invoker_pool.workers.max_inflight` (global) and `internal/limits.DefaultTenantMaxInflight` (per tenant; overridable through `CS_INVOKER_TENANT_INFLIGHT`). The Cadence inflight cap maps to `cs_cadence_poller.limits.max_inflight_tasks_default`.

The per-tenant gateway limiter uses a token bucket with capacity `tenant_burst` (defaults to `tenant_rps`, giving a 1-second burst). Requests at or below the steady-state rate always pass; up to `tenant_burst` requests can arrive in the same instant and all pass; the next request is rejected with `429 CS_RATE_LIMITED` and a `Retry-After` header derived from the time until the next token. The map of buckets is keyed by tenant; entries idle for more than 10 minutes are opportunistically evicted on the next call to keep the map bounded. The scope is in-process per replica; cluster-wide rate limiting is tracked in the [Reference Roadmap](Reference-Roadmap).

The per-tenant invoker semaphore separates synchronous from asynchronous behaviour:

- Synchronous HTTP invocations at the cap fail fast with `429 CS_TENANT_INFLIGHT_LIMIT` so the caller can retry or shed load.
- Async invocations (codeQ subscription, schedule, Cadence) wait inside the worker until a slot frees; no message is dropped and draining is FIFO.

A single tenant cannot exhaust the global pool for other tenants because the per-tenant cap is strictly smaller.

## Throughput model

The throughput a single invoker pool replica delivers follows from three inputs:

- `T` — `cs_invoker_pool.workers.threads`.
- `C` — average concurrent activations per thread (typically 1 for CPU-bound workloads, higher for I/O-bound code that yields to the runtime).
- `D_seconds` — average activation duration in seconds.

The per-replica steady-state RPS is:

```
max_rps_per_replica = T * C / D_seconds
```

Across `R` replicas:

```
max_rps = R * T * C / D_seconds
```

The per-replica result is capped by `workers.max_inflight`. If `T * C > max_inflight`, the smaller value wins.

**Worked example.** A deployment runs 5 replicas (`R = 5`), each with the default `threads: 32` (`T = 32`). Activations are CPU-bound and run sequentially per thread (`C = 1`). Average duration is 250 ms (`D_seconds = 0.25`):

```
max_rps = 5 * 32 * 1 / 0.25 = 640 rps
```

The deployment sustains 640 requests per second at steady state. A burst that exceeds 640 rps queues in codeQ for async triggers; synchronous triggers above the tenant or global inflight cap fail fast.

The model excludes downstream limits. KVRocks write capacity, codeQ broker throughput, and the upstream rate limits at the gateway can each pull the effective ceiling below `max_rps`. Operators sizing a deployment confirm the model against the dashboards under load.

## Queueing model

The relationship between arrival rate `lambda`, service rate `mu` from the throughput model, and queue depth follows Little's Law:

```
queue_depth = throughput * queue_time
```

The platform exposes queue depth indirectly through `cs_invoker_queue_lag_ms{topic="cs.invoke"}`, which measures the age of the oldest in-flight envelope on the invoke topic. As `lambda` approaches `mu`, the queue depth grows; once `lambda >= mu`, `cs_invoker_queue_lag_ms` grows without bound and the system is shedding load.

Two backpressure mechanisms surface this state to clients:

- **HTTP synchronous traffic**: when the per-tenant inflight semaphore is saturated, the invoker returns `429 CS_TENANT_INFLIGHT_LIMIT`. When the gateway's per-tenant rate limit is exceeded, the gateway returns `429 CS_RATE_LIMITED`. The two errors are distinct so operators can identify which limit fired.
- **Async traffic**: envelopes queue in codeQ. The scheduler skips a tick (with a logged `cs_scheduler_publish_errors_total` increment) when codeQ cannot accept the publish. Retries follow the configured retry policy; envelopes that exhaust their retry budget land in the DLQ (`cs.dlq.invoke` by default).

Operators decide whether to scale out or shed load by reading the dashboards:

- `cs_invoker_queue_lag_ms` rising with stable `cs_invocations_total` → scale `cs-invoker-pool`.
- `cs_invocations_total{status="error"}` rising → investigate a tenant regression rather than scale.
- `cs_invoker_inflight_rejected_total` rising for a single tenant → tighten that tenant's gateway rate limit; the per-tenant semaphore is doing its job.

## Sizing recommendations

The defaults in `config.example.yaml` size a development cluster — single replica per service, modest concurrency. Production deployments scale linearly along the dimensions below.

**Small deployment** — a few tenants, predictable workload, <100 rps aggregate.

- `cs-control`: 2 replicas (leader for some control-plane primitives; pair for redundancy).
- `cs-http-gateway`: 2 replicas behind a Service.
- `cs-invoker-pool`: 2 replicas, `workers.threads = 32`, `workers.max_inflight = 2048`, `cache.bytes_max = 536870912` (512 MiB).
- `cs-scheduler`: 2 replicas (one active leader, one standby).
- `cs-cadence-poller`: 2 replicas, `limits.max_inflight_tasks_default = 256`.
- KVRocks: 1 primary, 1 read replica.
- codeQ: 3-broker minimum for quorum.

**Medium deployment** — tens of tenants, 100–1,000 rps aggregate.

- `cs-control`: 3 replicas.
- `cs-http-gateway`: 4 replicas; consider raising `rate_limits.tenant_rps` per tenant on a case-by-case basis.
- `cs-invoker-pool`: 6 replicas, `workers.threads = 64`, `workers.max_inflight = 4096`, `cache.bytes_max = 1073741824` (1 GiB).
- `cs-scheduler`: 2 replicas.
- `cs-cadence-poller`: 4 replicas.
- KVRocks: 1 primary, 2 read replicas; provision dedicated NVMe.
- codeQ: 5-broker cluster.

**Large deployment** — hundreds of tenants, >1,000 rps aggregate.

- `cs-control`: 5 replicas behind a horizontal autoscaler keyed off API request latency.
- `cs-http-gateway`: 10+ replicas; enable cluster-wide rate limiting once the [Reference Roadmap](Reference-Roadmap) item lands.
- `cs-invoker-pool`: 20+ replicas with horizontal autoscaling keyed off `cs_invoker_queue_lag_ms`. Raise `workers.max_inflight` per replica to 8192; raise `cache.bytes_max` to 2 GiB on nodes with sufficient memory.
- `cs-scheduler`: 2 replicas; the scheduler is leader-elected and does not scale horizontally beyond standby.
- `cs-cadence-poller`: 8+ replicas; pin `limits.max_inflight_tasks_default` per binding through the control plane rather than the YAML.
- KVRocks: primary + multiple replicas; consider sharding by tenant once a single primary cannot absorb the write rate.
- codeQ: dedicated cluster sized per `deploy/sous-deploy-template/`.

Every recommendation above is a starting point. The dashboards in `deploy/observability/` show whether a deployment is under-provisioned (`cs_invoker_queue_lag_ms` climbing, `cs_invoker_inflight_rejected_total` rising) or over-provisioned (low `cs_invoker_cache_bytes`, idle replicas). Operators iterate against the dashboards rather than against the recommendations.

## Cross-references

- [Operators Configuration Reference](Operators-Configuration-Reference) — the YAML knobs that drive every limit on this page.
- [Operators Observability](Operators-Observability) — the metrics that surface saturation.
- [Operators Runbooks](Operators-Runbooks) — how to respond when a limit fires.
- [Deployment Kubernetes](Deployment-Kubernetes) — Helm values that compose into the recommendations above.
- `internal/limits/` — the centralised limit constants.
- `config.example.yaml` — the default values quoted in the tables above.
