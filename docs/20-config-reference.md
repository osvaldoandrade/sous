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
      api_key: "tikti-api-key"
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
      base_url: "http://codeq.default.svc.cluster.local:80"
      producer_token: "<ACCESS_TOKEN>"
      worker_token: "<WORKER_TOKEN>" # optional; defaults to producer_token
      brokers: [] # optional legacy mode
      topics:
        invoke: cs.invoke
        results: cs.results
        dlq_invoke: cs.dlq.invoke
        dlq_results: cs.dlq.results
  audit:
    # E6.03: control-plane audit log stream. See docs/14-observability.md
    # "Audit" for the event shape, sink contract, and replay endpoint.
    sink: stdout            # stdout (default) | codeq | webhook
    topic_prefix: cs.audit  # codeq sink only; topic becomes <prefix>.<tenant>
    webhook_url: ""         # webhook sink only; POST target URL
    hmac_secret: ""         # webhook sink only; HMAC-SHA256 signing key
    history_limit: 1000     # per-tenant ring buffer size for the replay endpoint
  secrets:
    # E6.01: external secret-provider plugin. See docs/15-security.md
    # "Secrets" for the injection contract and redaction guarantees.
    driver: memory          # memory (default) | vault
    memory:
      # Used when driver == "memory". Map of <path> -> <value>; a
      # VersionConfig.Secrets entry "STRIPE_KEY=payments/stripe_key"
      # looks up seed["payments/stripe_key"] and exposes the result to
      # user code as cs.env.get("STRIPE_KEY").
      seed: {}
    vault:
      # Used when driver == "vault". The driver speaks Vault KV v2
      # over plain HTTP through the stdlib net/http — no SDK dep.
      addr: ""               # required: https://vault.example.com:8200
      token: ""              # static token; prefer token_env in prod
      token_env: VAULT_TOKEN # env var consulted when token is empty
      kv_mount: secret       # KV v2 mount; requests hit <mount>/data/<path>
      namespace: ""          # Vault Enterprise X-Vault-Namespace header
      timeout_ms: 2000       # per-request HTTP timeout

# Legacy compatibility section (optional during migration window).
kvrocks:
  addr: kvrocks:6666
  auth:
    mode: none  # none|password
    password: ""

codeq:
  base_url: "http://codeq.default.svc.cluster.local:80"
  producer_token: "<ACCESS_TOKEN>"
  worker_token: "<WORKER_TOKEN>"
  brokers: []
  topics:
    invoke: cs.invoke
    results: cs.results
    dlq_invoke: cs.dlq.invoke
    dlq_results: cs.dlq.results

tikti:
  introspection_url: https://tikti.example.com/introspect
  cache_ttl_seconds: 60
  api_key: "tikti-api-key"
```

### plugins.secrets

`plugins.secrets` selects the secret-provider driver consumed by
`cs-invoker-pool` to resolve `VersionConfig.Secrets` at activation
start. The default driver is `memory`, intended for local development
and tests; production deployments switch to `vault`.

| Driver  | Required keys                | Notes                                                     |
|---------|------------------------------|-----------------------------------------------------------|
| memory  | `memory.seed`                | Empty seed is allowed — every lookup returns 404.          |
| vault   | `vault.addr`                 | Auth via static token; prefer `VAULT_TOKEN` env over YAML. |

Secret references on `VersionConfig.Secrets` accept three forms:

```text
NAME                          # path == name
NAME=provider/path            # explicit path
NAME=provider/path#json-field:key  # extract one field from a JSON payload
```

The resolved `{name: value}` map is stamped onto the runtime context
via `runtime.WithEnv` and exposed to JS code through `cs.env.get(name)`
and `cs.env.list()`. See `docs/15-security.md` "Secrets" for the
end-to-end injection contract and redaction guarantees, and
`internal/plugins/secrets` for the driver SDK.

## cs-control

```yaml
cs_control:
  http:
    addr: :8080
  limits:
    max_bundle_bytes: 16777216
    draft_ttl_seconds: 86400
    activation_ttl_seconds: 604800
  subscriptions:
    worker_pool_default: 4   # default max_concurrency for unordered bindings
    refresh_seconds: 10      # reconciliation interval for the consumer set
  publish:
    # E5.01: frozen-import-map resolver. The allowlist is empty by
    # default so no remote fetch is permitted out of the box; only
    # local-path imports (bytes already in the uploaded bundle)
    # resolve. Each entry is a hostname (no scheme, no port) matched
    # case-insensitively against the URL the publisher declares in
    # manifest.imports[*].url. See docs/08-runtime-cs-js.md and
    # docs/15-security.md.
    imports:
      allowed_mirrors: []
      # Per-import byte ceiling. Defaults to 4 MiB. The 16 MiB
      # max_bundle_bytes still caps the frozen bundle as a whole.
      max_bytes_per_import: 4194304
      # Per-import fetch timeout. Defaults to 10s.
      timeout_ms: 10000
```

`cs_control.subscriptions` configures the in-process codeQ subscription
trigger consumer introduced by E4.02. `worker_pool_default` seeds the
unordered-mode pool when a binding omits `max_concurrency`; ordered bindings
always run a single goroutine. `refresh_seconds` controls how often
cs-control re-reads the persisted binding set and reconciles its in-memory
consumer map. See `docs/07-codeq-protocol.md` ("Subscription triggers") for
the full delivery contract.

`publish.imports.allowed_mirrors` controls which hostnames the
cs-control resolver may fetch from when freezing JS dependencies at
publish time. An empty list disables remote fetching entirely —
`path:` imports still work. Adding `mirror.example.com` allows any
`https://mirror.example.com/...` URL declared in a function manifest.
Disallowed mirrors yield `CS_VALIDATION_FAILED` with an error message
naming `allowed_mirrors`. See docs/08-runtime-cs-js.md "Imports" and
docs/15-security.md "Curated import-map mirror allowlist".

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
    tenant_rps: 200    # steady-state per-tenant token rate
    tenant_burst: 200  # bucket capacity (defaults to tenant_rps)
    function_rps: 20   # per-function token rate (advisory; future work)
```

`rate_limits.tenant_rps` configures the in-process per-tenant token-bucket
limiter introduced by E1.04. When the bucket is empty the gateway returns
`429 CS_RATE_LIMITED` with a `Retry-After` header derived from the time
until the next token is available. `tenant_burst` overrides bucket capacity;
when zero or omitted it tracks `tenant_rps`. This is a Layer-1 limit — see
`docs/26-capacity-and-limits.md` and `docs/18-roadmap.md` for cluster-wide
work.

## cs-invoker-pool

```yaml
cs_invoker_pool:
  http:
    addr: :8082
  workers:
    threads: 32
    max_inflight: 2048      # global pool capacity (across all tenants)
  cache:
    bundles_max: 1000
    bytes_max: 536870912
  limits:
    max_result_bytes: 262144
    max_error_bytes: 65536
    max_log_bytes: 1048576
```

`workers.max_inflight` is the global concurrent-activation cap inside a
single cs-invoker-pool replica.

#### Default retry policy (E4.03)

```yaml
cs_invoker_pool:
  retry:
    # Trigger-level defaults applied when an async invocation envelope
    # does not embed an explicit RetryPolicy. See docs/02-requirements.md
    # "Retry & DLQ" for semantics.
    max_attempts: 1          # 1 = no retry (matches pre-E4.03 behaviour)
    base_ms: 500
    max_ms: 30000
    jitter_pct: 20
    retryable_errors:
      - CS_RUNTIME_TIMEOUT
      - CS_RUNTIME_DEPENDENCY_ERROR
      - CS_RATE_LIMITED
      - Timeout
    # Optional override for the per-tenant DLQ topic; empty falls back to
    # the platform-default cs.dlq.invoke.
    dlq_topic: ""
```

The policy lives on the trigger envelope at
`trigger.source.retry_policy`; this section documents the defaults a
producer applies when the operator does not set one explicitly. Tenants who
want a different policy embed the same block when creating their
schedule / subscription / cadence binding.

In addition, E1.04 introduces a **per-tenant** inflight cap enforced inside
the invoker-pool. The cap defaults to `64`
(`internal/limits.DefaultTenantMaxInflight`) and can be overridden at
process start with the `CS_INVOKER_TENANT_INFLIGHT` environment variable
until a dedicated YAML key is added. Behaviour on saturation:

- Synchronous (HTTP-triggered) invocations return
  `429 CS_TENANT_INFLIGHT_LIMIT` so the gateway can surface a typed error.
- Queued (codeQ / schedule / cadence) invocations wait until a slot frees
  — no message is lost, draining happens in FIFO order.

### Trigger sampling

E7.02 introduces a per-trigger `sampling` block. It is consumed by
`cs-invoker-pool` and is independent of the YAML configs above —
operators publish it through `cs-control` alongside the trigger
definition (`http`, `schedule`, `cadence`, `codeq`). See
`docs/14-observability.md` for the full lifecycle contract.

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
    probability: 0.05     # required when mode == probabilistic, must be in [0,1]
```

An empty / omitted block is equivalent to `mode: always`: the invoker
records every activation. Invalid policies fall back to `always` rather
than silently dropping traffic.

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
