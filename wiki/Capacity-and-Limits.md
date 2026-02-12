# Capacity and Limits

This document defines platform limits and a capacity model that uses explicit inputs.

The model exists to turn configuration into numeric ceilings.

## 1. Limit table

| Surface | Limit | Default |
|---|---:|---:|
| Published bundle | bytes | 16,777,216 |
| Draft TTL | seconds | 86,400 |
| Activation TTL | seconds | 604,800 |
| HTTP request body | bytes | 6,291,456 |
| HTTP header bytes | bytes | 65,536 |
| HTTP query bytes | bytes | 16,384 |
| Function result | bytes | 262,144 |
| Function error | bytes | 65,536 |
| Activation logs | bytes | 1,048,576 |
| HTTP timeout | ms | 3,000 |
| HTTP timeout max | ms | 30,000 |
| Worker timeout | ms | 30,000 |
| Worker timeout max | ms | 900,000 |
| Scheduler resolution | ms | 1,000 |
| Schedule interval min | seconds | 1 |
| Schedule interval max | seconds | 86,400 |

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
