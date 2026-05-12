# Capacity and limits

This document defines platform limits and a capacity model that uses explicit inputs.

The model exists to turn configuration into numeric ceilings.

## 1. Limit table

| Surface | Limit | Default | Enforced at | On violation |
|---|---:|---:|---|---|
| Published bundle | bytes | 16,777,216 | `cs-control` `PUT .../draft` | `413 CS_BUNDLE_TOO_LARGE` |
| Draft TTL | seconds | 86,400 | `cs-control` KV write | draft becomes unresolvable, publish rejected |
| Activation TTL | seconds | 604,800 | `cs-invoker-pool` / `internal/kv` | read returns `410 CS_ACTIVATION_TTL_EXPIRED` |
| HTTP request body | bytes | 6,291,456 | `cs-http-gateway` invoke | `413 CS_BODY_TOO_LARGE` |
| HTTP header bytes | bytes | 65,536 | `cs-http-gateway` invoke | `400 CS_VALIDATION_FAILED` |
| HTTP query bytes | bytes | 16,384 | `cs-http-gateway` invoke | `400 CS_VALIDATION_FAILED` |
| Function result | bytes | 262,144 | `cs-invoker-pool` result write | `413 CS_RESULT_TOO_LARGE` |
| Function error | bytes | 65,536 | `cs-invoker-pool` error write | truncated, `X-CS-Truncated: error` |
| Activation logs | bytes | 1,048,576 | `cs-invoker-pool` log write | truncated + sentinel `CS_LOG_LIMIT_EXCEEDED`, `X-CS-Truncated: logs` |
| HTTP timeout | ms | 3,000 | `cs-http-gateway` per-version | `504 CS_CODEQ_CORRELATION_TIMEOUT` |
| HTTP timeout max | ms | 30,000 | `cs-control` publish validation | `400 CS_VALIDATION_FAILED` |
| Worker timeout | ms | 30,000 | `cs-invoker-pool` runtime | `CS_RUNTIME_TIMEOUT` |
| Worker timeout max | ms | 900,000 | `cs-control` publish validation | `400 CS_VALIDATION_FAILED` |
| Tenant RPS (gateway) | rps | 200 | `cs-http-gateway` token bucket | `429 CS_RATE_LIMITED` |
| Function RPS (gateway) | rps | 20 | `cs-http-gateway` token bucket | `429 CS_RATE_LIMITED` |
| Tenant max inflight | activations | 64 | `cs-invoker-pool` semaphore | sync: `429 CS_TENANT_INFLIGHT_LIMIT`; async: queued in codeQ |
| Scheduler resolution | ms | 1,000 | `cs-scheduler` tick | — |
| Schedule interval min | seconds | 1 | `cs-control` schedule validation | `400 CS_VALIDATION_FAILED` |
| Schedule interval max | seconds | 86,400 | `cs-control` schedule validation | `400 CS_VALIDATION_FAILED` |

Limits are centralised in the `internal/limits` package (loaded via
`limits.FromConfig`). All enforcement sites MUST consult it so the contract
remains uniform across `cs-control`, `cs-http-gateway`, `cs-invoker-pool`,
and `cs-cadence-poller`. See `docs/21-errors.md` for the error-code → HTTP
status mapping.

## 2. Invoker throughput model

Define inputs:

- `R`: invoker replicas
- `T`: worker threads per replica
- `C`: average concurrent activations per thread
- `D_ms`: average activation duration in milliseconds

Define derived capacity:

- `max_inflight = R * T * C`
- `max_rps = max_inflight / (D_ms / 1000)`

This model does not include queueing or downstream limits.

## 3. Queueing model

Define:

- `lambda_rps`: arrival rate
- `mu_rps`: service rate from section 2

Constraint:

- `lambda_rps < mu_rps`

If `lambda_rps >= mu_rps` then queue lag grows without bound.

Operational signal:

- `cs_invoker_queue_lag_ms` increases.

## 4. Scheduler load model

A schedule with interval `S` seconds emits:

- `1 / S` invocations per second

A tenant with schedules `S1..Sn` emits:

- `sum(1/Si)` invocations per second

Constraint:

- tenant schedule rps + tenant HTTP rps + tenant Cadence rps < tenant service rate

## 5. Cadence poller load model

Define:

- `P`: pollers per binding
- `I`: max inflight tasks per binding
- `A`: average activity duration in ms

Upper bound on completion rate per binding:

- `min(I, P * I) / (A / 1000)`

The poller also consumes `cs.results`.
The results consumer must keep pace with invoker output.

## 6. Storage load model

Activation writes per invocation:

- 2 metadata writes (start, terminal)
- `L` log chunk writes
- optional result pointer writes

Define:

- `W`: KVRocks writes per second capacity

Constraint:

- `invocations_per_second * (2 + L) < W`

This model requires a measured `W` from a benchmark.
The spec does not provide `W`.

## 7. Config knobs that affect capacity

- `cs_invoker_pool.workers.threads`
- `cs_invoker_pool.workers.max_inflight`
- `cs_invoker_pool.cache.bytes_max`
- `cs_http_gateway.rate_limits.tenant_rps`
- `cs_cadence_poller.limits.max_inflight_tasks_default`
- per-version `limits.maxConcurrency`
