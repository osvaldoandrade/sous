# Configuration reference

This file defines the YAML config for all services.

## Common

```yaml
cluster_name: cs-prod-1
environment: prod

plugins:
  authn:
    driver: tikti
    tikti:
      introspection_url: https://tikti.example.com/introspect
      cache_ttl_seconds: 60
  persistence:
    driver: kvrocks
    kvrocks:
      addr: kvrocks:6666
      auth:
        mode: none  # none|password
        password: ""
  messaging:
    driver: codeq
    codeq:
      brokers: ["codeq:9092"]
      topics:
        invoke: cs.invoke
        results: cs.results
        dlq_invoke: cs.dlq.invoke
        dlq_results: cs.dlq.results

# Legacy compatibility section (optional during migration window).
kvrocks:
  addr: kvrocks:6666
  auth:
    mode: none  # none|password
    password: ""

codeq:
  brokers: ["codeq:9092"]
  topics:
    invoke: cs.invoke
    results: cs.results
    dlq_invoke: cs.dlq.invoke
    dlq_results: cs.dlq.results

tikti:
  introspection_url: https://tikti.example.com/introspect
  cache_ttl_seconds: 60
```

## cs-control

```yaml
cs_control:
  http:
    addr: :8080
  limits:
    max_bundle_bytes: 16777216
    draft_ttl_seconds: 86400
    activation_ttl_seconds: 604800
```

## cs-http-gateway

```yaml
cs_http_gateway:
  http:
    addr: :8081
  limits:
    max_body_bytes: 6291456
    max_header_bytes: 65536
    max_query_bytes: 16384
  rate_limits:
    tenant_rps: 200
    function_rps: 20
```

## cs-invoker-pool

```yaml
cs_invoker_pool:
  http:
    addr: :8082
  workers:
    threads: 32
    max_inflight: 2048
  cache:
    bundles_max: 1000
    bytes_max: 536870912
  limits:
    max_result_bytes: 262144
    max_error_bytes: 65536
    max_log_bytes: 1048576
```

## cs-scheduler

```yaml
cs_scheduler:
  tick_ms: 1000
  max_catchup_ticks: 60
  leader_election:
    enabled: true
    lease_name: cs-scheduler
```

## cs-cadence-poller

```yaml
cs_cadence_poller:
  cadence:
    addr: code-flow:7933
  refresh_seconds: 10
  heartbeat:
    max_per_second: 2
  limits:
    max_inflight_tasks_default: 256
```
