# Operators: Configuration Reference

Every Sous service is a single Go binary that reads exactly one YAML file at start-up. The path is supplied with the `--config` flag and the file is parsed into the struct defined in `internal/config/config.go`. There is no environment-variable override layer for the operational fields; the YAML is the source of truth, and operators distribute it through ConfigMaps, sealed secrets, or whatever pipeline they already use for declarative configuration.

The schema is split into two halves. The `plugins.*` section selects the drivers that every service shares — authentication, persistence, messaging, secrets, audit, signing — and the per-service sections (`cs_control`, `cs_http_gateway`, `cs_invoker_pool`, `cs_scheduler`, `cs_cadence_poller`) tune the behaviour of one binary. A given service ignores the per-service sections that do not belong to it, so the same YAML file can be deployed to every replica of every service without per-pod templating. The legacy top-level `kvrocks:`, `codeq:`, and `tikti:` blocks are honoured for backward compatibility with pre-plugin deployments and will be removed in a future major release; new clusters set the drivers through `plugins.*`.

The canonical example lives at `config.example.yaml` in the repository root. The walkthrough below follows it block by block.

## Top-level identity

```yaml
cluster_name: cs-prod-1
environment: prod
```

`cluster_name` and `environment` are free-form strings stamped onto outbound audit events and structured log records. They do not feed any routing decision but make multi-cluster log aggregation trivial. Operators set `cluster_name` to a stable identifier that survives blue/green swaps and `environment` to one of the deployment tiers in use (`dev`, `staging`, `prod`).

## plugins.authn

```yaml
plugins:
  authn:
    driver: tikti
    tikti:
      introspection_url: http://localhost:8099/introspect
      cache_ttl_seconds: 60
      api_key: ""
```

`plugins.authn.driver` selects the authentication backend. The only shipped driver is `tikti`, which delegates bearer-token introspection to a [Tikti IAM](Enabled-Services-Tikti-IAM) deployment.

- `tikti.introspection_url` — fully qualified URL of the `POST /introspect` endpoint. Required when the driver is `tikti`.
- `tikti.cache_ttl_seconds` — in-memory cache TTL for positive introspection results. Negative results (invalid token) are not cached. Default `60`.
- `tikti.api_key` — service-to-service API key forwarded to Tikti in the `X-Tikti-Service` header. Leave blank for unauthenticated clusters and rely on Kubernetes NetworkPolicies to isolate the introspection endpoint.

## plugins.persistence

```yaml
plugins:
  persistence:
    driver: kvrocks
    kvrocks:
      addr: localhost:6666
      auth:
        mode: none
        password: ""
```

`plugins.persistence.driver` selects the key/value store that holds function metadata, version records, activation rows, alias bindings, scheduler state, audit ring buffers, and SBOMs. The shipped driver is `kvrocks`, which speaks the Redis RESP protocol against a KVRocks server.

- `kvrocks.addr` — `host:port` of the KVRocks endpoint. The client opens a small connection pool per service replica.
- `kvrocks.auth.mode` — one of `none` or `password`. `none` skips the `AUTH` handshake; `password` issues `AUTH <password>` after `CONNECT`.
- `kvrocks.auth.password` — password used when `mode == password`. Leave blank when `mode == none`.

KVRocks deployment guidance — sizing, replication, and backup — lives in [Storage KVRocks](Storage-KVRocks).

## plugins.messaging

```yaml
plugins:
  messaging:
    driver: codeq
    codeq:
      base_url: http://localhost:8080
      producer_token: ""
      worker_token: ""
      brokers: []
      topics:
        invoke: cs.invoke
        results: cs.results
        dlq_invoke: cs.dlq.invoke
        dlq_results: cs.dlq.results
```

`plugins.messaging.driver` selects the asynchronous transport between `cs-http-gateway`, `cs-invoker-pool`, `cs-scheduler`, and `cs-cadence-poller`. The shipped driver is `codeq`, which speaks the HTTP-fronted protocol detailed in [codeQ Protocol](codeQ-Protocol).

- `codeq.base_url` — codeQ broker URL. The producer and consumer share the same endpoint; codeQ multiplexes both sides over plain HTTP.
- `codeq.producer_token` — bearer token used when the service publishes envelopes. Required in production; cs-invoker-pool, cs-http-gateway, cs-scheduler, and cs-cadence-poller all carry the producer token.
- `codeq.worker_token` — bearer token used when the service consumes envelopes. May equal `producer_token` for simple deployments; production tenants often issue read-only tokens to consumer-only pods. Defaults to `producer_token` when blank.
- `codeq.brokers` — legacy Kafka-compatible broker list. Empty in default deployments. When set, the driver speaks the legacy transport instead of HTTP and treats `base_url` as advisory.
- `codeq.topics.invoke` — topic for inbound invocation envelopes. Default `cs.invoke`.
- `codeq.topics.results` — topic for terminal activation results. Default `cs.results`.
- `codeq.topics.dlq_invoke` — dead-letter topic for invocation envelopes that exhausted their retry policy. Default `cs.dlq.invoke`.
- `codeq.topics.dlq_results` — dead-letter topic for result envelopes that cannot be delivered (e.g. consumer offline past the retention window). Default `cs.dlq.results`.

## plugins.secrets

```yaml
plugins:
  secrets:
    driver: memory
    memory:
      seed: {}
    vault:
      addr: ""
      token: ""
      token_env: VAULT_TOKEN
      kv_mount: secret
      namespace: ""
      timeout_ms: 2000
```

`plugins.secrets.driver` selects the secret-provider plugin consumed by `cs-invoker-pool` to resolve `VersionConfig.Secrets` at activation start. Two drivers ship in tree:

- `memory` — reads from the seed map below. Suitable for local development and integration tests. Tenants in production are expected to switch to `vault`.
- `vault` — speaks HashiCorp Vault KV v2 over plain HTTP through the standard library, with no Vault SDK dependency.

`memory.seed` is a flat `<path>` → `<value>` map. A `VersionConfig.Secrets` entry like `"STRIPE_KEY=payments/stripe_key"` looks up `seed["payments/stripe_key"]` and exposes the result to user code as `cs.env.get("STRIPE_KEY")`.

Vault-specific settings:

- `vault.addr` — base URL of the Vault cluster, including scheme and port. Required when `driver == "vault"`.
- `vault.token` — static authentication token. Leaving this empty is the recommended posture; the driver reads `token_env` instead so the YAML can stay sealed.
- `vault.token_env` — name of the environment variable the driver consults when `token` is blank. Default `VAULT_TOKEN`.
- `vault.kv_mount` — name of the KV v2 mount. The driver rewrites `SecretRef.Path` to `<kv_mount>/data/<path>` before issuing the read.
- `vault.namespace` — Vault Enterprise namespace forwarded as `X-Vault-Namespace`. Leave blank for OSS Vault.
- `vault.timeout_ms` — per-request HTTP timeout. The runtime additionally caps the lookup at the remaining activation deadline.

The injection contract and redaction guarantees are covered in [Operators Security](Operators-Security) and [Vault Secrets](Enabled-Services-Vault-Secrets).

## plugins.audit

```yaml
plugins:
  audit:
    sink: stdout
    topic_prefix: cs.audit
    webhook_url: ""
    hmac_secret: ""
    history_limit: 1000
```

`plugins.audit` configures the control-plane audit stream. Every successful mutation in `cs-control` produces a JSON event after the persistence write commits.

- `sink` — selects the delivery target. One of:
  - `stdout` — one JSON line per event on cs-control stdout. Best for development and any deployment that already aggregates pod logs.
  - `codeq` — publishes each event as an `AuditEvent` envelope on `<topic_prefix>.<tenant>`.
  - `webhook` — POSTs each event to `webhook_url` with an HMAC-SHA256 signature in `X-CS-Audit-Signature`. Compatible with Splunk HEC, Datadog logs, and any SIEM that accepts signed JSON.
- `topic_prefix` — codeQ topic prefix when `sink == codeq`. The effective topic per event is `<topic_prefix>.<tenant>`. Default `cs.audit`.
- `webhook_url` — POST target when `sink == webhook`. The request body is the canonical event JSON.
- `hmac_secret` — symmetric key used to sign webhook payloads. The signature is `hex(hmac_sha256(secret, body))`.
- `history_limit` — number of events retained in the per-tenant ring buffer that backs `GET /v1/tenants/{tenant}/audit`. Default `1000`.

The full event schema and the replay endpoint are documented in [ledgerDB Audit](Enabled-Services-ledgerDB-Audit).

## plugins.signing

```yaml
plugins:
  signing:
    required: false
```

`plugins.signing.required` controls publish-time enforcement of the Ed25519 signature scheme described in [Signing and SBOM](Managing-Functions-Signing-and-SBOM). The default `false` keeps the pre-E5.02 contract: signatures are recorded when supplied but not required. Operators flip the knob to `true` once every active tenant has rotated a signing key; subsequent unsigned publishes then fail with `CS_SIGNATURE_MISSING`. The invoker re-verifies signatures on every cold bundle load regardless of this flag, so versions published in the unsigned window keep running unchanged while signed versions are guarded forever.

## Legacy top-level blocks

```yaml
kvrocks: { ... }
codeq:   { ... }
tikti:   { ... }
```

The three top-level blocks `kvrocks:`, `codeq:`, and `tikti:` mirror the equivalent `plugins.*` blocks and exist solely for backward compatibility with pre-plugin deployments. New deployments configure drivers exclusively through `plugins.*`; the legacy blocks will be removed in a future major release.

## cs_control

```yaml
cs_control:
  http:
    addr: :8080
  limits:
    max_bundle_bytes: 16777216
    draft_ttl_seconds: 86400
    activation_ttl_seconds: 604800
  subscriptions:
    worker_pool_default: 4
    refresh_seconds: 10
  publish:
    imports:
      allowed_mirrors: []
      max_bytes_per_import: 4194304
      timeout_ms: 10000
```

`cs_control` tunes the control-plane binary that exposes the REST API.

- `http.addr` — listen address for the HTTP server. The standard pattern is `:8080` so the service can be reached from any pod IP.
- `limits.max_bundle_bytes` — hard cap on the size of an uploaded function bundle, enforced by `PUT .../draft`. Bundles above this size fail with `413 CS_BUNDLE_TOO_LARGE`. Default `16777216` (16 MiB).
- `limits.draft_ttl_seconds` — lifetime of an uploaded draft before its KVRocks key expires. Drafts that expire become unresolvable; the publish handler refuses to promote them. Default `86400` (24 hours).
- `limits.activation_ttl_seconds` — lifetime of activation records and their logs in KVRocks. Reads past this point return `410 CS_ACTIVATION_TTL_EXPIRED`. Default `604800` (7 days).
- `subscriptions.worker_pool_default` — default worker-pool size used when a `SubscriptionBinding` omits `max_concurrency`. Only consumed for unordered bindings; ordered bindings always run a single goroutine. Default `4`.
- `subscriptions.refresh_seconds` — interval at which cs-control re-reads the persisted binding set and reconciles its in-memory consumer map. New bindings start consuming on the next refresh; deletions stop their consumer. Default `10`.
- `publish.imports.allowed_mirrors` — list of hostnames the publish-time import resolver may fetch from. Each entry is a bare hostname (no scheme, no port), matched case-insensitively. Empty by default, which disables all remote fetching; only `path:` imports already in the uploaded bundle resolve.
- `publish.imports.max_bytes_per_import` — per-import byte cap. The bundle-wide `max_bundle_bytes` still caps the frozen bundle as a whole. Default `4194304` (4 MiB).
- `publish.imports.timeout_ms` — per-import HTTP fetch timeout. Default `10000`.

## cs_http_gateway

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

`cs_http_gateway` tunes the HTTP-invoke ingress.

- `http.addr` — listen address. Standard pattern `:8081`.
- `limits.max_body_bytes` — maximum request body size on `/v1/web/{tenant}/...`. Requests above the cap fail with `413 CS_BODY_TOO_LARGE`. Default `6291456` (6 MiB).
- `limits.max_header_bytes` — total header bytes per request. Above the cap, `400 CS_VALIDATION_FAILED`. Default `65536` (64 KiB).
- `limits.max_query_bytes` — combined query-string size per request. Above the cap, `400 CS_VALIDATION_FAILED`. Default `16384` (16 KiB).
- `rate_limits.tenant_rps` — steady-state per-tenant token-bucket rate enforced in-process per replica. When the bucket empties the gateway returns `429 CS_RATE_LIMITED` with a `Retry-After` header. Default `200`.
- `rate_limits.function_rps` — per-function-ref token-bucket rate. Default `20`.

Bursts default to the value of `tenant_rps`; a dedicated `tenant_burst` knob is accepted but is not exposed in `config.example.yaml`. See [Operators Capacity and Limits](Operators-Capacity-and-Limits) for the throughput model.

## cs_invoker_pool

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
  retry:
    max_attempts: 1
    base_ms: 500
    max_ms: 30000
    jitter_pct: 20
    retryable_errors:
      - CS_RUNTIME_TIMEOUT
      - CS_RUNTIME_DEPENDENCY_ERROR
      - CS_RATE_LIMITED
      - Timeout
    dlq_topic: ""
```

`cs_invoker_pool` tunes the data-plane execution engine.

- `http.addr` — listen address used by the metrics, health, and synchronous invoke endpoints. Standard pattern `:8082`.
- `workers.threads` — fixed-size goroutine pool that dispatches activations. Each thread handles one activation at a time. Default `32`.
- `workers.max_inflight` — global concurrency cap inside a single replica. The per-tenant cap is fixed at `internal/limits.DefaultTenantMaxInflight` (`64`) and may be overridden at process start with `CS_INVOKER_TENANT_INFLIGHT` until a dedicated YAML key lands. Default `2048`.
- `cache.bundles_max` — maximum number of resident bundles in the in-memory LRU cache. A cache miss triggers a KVRocks fetch plus runtime warm-up. Default `1000`.
- `cache.bytes_max` — byte budget for the LRU cache. The smaller of the two caps wins. Default `536870912` (512 MiB).
- `limits.max_result_bytes` — maximum size of the function return value persisted to KVRocks. Above the cap, `413 CS_RESULT_TOO_LARGE`. Default `262144` (256 KiB).
- `limits.max_error_bytes` — maximum size of the error payload. Above the cap the error is truncated and `X-CS-Truncated: error` is stamped on the response. Default `65536` (64 KiB).
- `limits.max_log_bytes` — cumulative per-activation log byte cap. Above the cap, log writes become no-ops and the chunk index gets a trailing `{"truncated": true, "reason": "log_limit_exceeded", "limit_bytes": N}` sentinel. Default `1048576` (1 MiB).
- `retry.max_attempts` — default retry policy applied to async invocations (schedule, subscription, cadence) when the trigger envelope does not embed an explicit `RetryPolicy`. `1` disables retry, which matches pre-E4.03 behaviour. HTTP triggers are excluded because the client owns retry for synchronous calls.
- `retry.base_ms` — first backoff interval in milliseconds. Default `500`.
- `retry.max_ms` — cap on the exponential backoff. Default `30000`.
- `retry.jitter_pct` — random jitter applied to each backoff, expressed as a percentage of the computed interval. Default `20`.
- `retry.retryable_errors` — error codes that trigger a retry rather than a DLQ delivery. The default set covers transient runtime conditions.
- `retry.dlq_topic` — optional override of the platform-default DLQ topic for envelopes that exhaust their retry budget. Empty falls back to `cs.dlq.invoke`.

## cs_scheduler

```yaml
cs_scheduler:
  tick_ms: 1000
  max_catchup_ticks: 60
  leader_election:
    enabled: true
    lease_name: cs-scheduler
```

`cs_scheduler` tunes the periodic-trigger scheduler.

- `tick_ms` — wall-clock interval between scheduler ticks. The minimum schedule interval is implicitly bounded by this value because no schedule can fire more often than the tick. Default `1000` (1 second).
- `max_catchup_ticks` — maximum number of missed ticks the scheduler replays after a restart or a leadership change. Beyond this bound, missed fires are dropped so a recovering scheduler cannot flood the cluster. Default `60`.
- `leader_election.enabled` — when `true`, replicas elect a leader through a Kubernetes lease; only the leader fires schedules. When `false`, every replica fires schedules, which is appropriate only for single-replica development clusters.
- `leader_election.lease_name` — name of the Kubernetes `Lease` object backing the election. Default `cs-scheduler`.

## cs_cadence_poller

```yaml
cs_cadence_poller:
  cadence:
    addr: localhost:7933
  refresh_seconds: 10
  heartbeat:
    max_per_second: 2
  limits:
    max_inflight_tasks_default: 256
```

`cs_cadence_poller` tunes the Cadence ActivityTask poller.

- `cadence.addr` — `host:port` of the Cadence frontend (typically the `code-flow` service).
- `refresh_seconds` — interval at which the poller re-reads `WorkerBinding` records and reconciles its long-poll loops. Bindings added since the last refresh start polling on the next iteration; deleted bindings stop. Default `10`.
- `heartbeat.max_per_second` — per-poller heartbeat cap. The poller heartbeats every activity at most this often regardless of how long the activity runs.
- `limits.max_inflight_tasks_default` — default inflight-task cap applied to a binding that omits `limits.max_inflight_tasks`. Cadence DecisionTasks and ActivityTasks share the cap. Default `256`.

## Activation sampling

Activation sampling lives on the trigger record rather than in the YAML config. `cs-invoker-pool` reads the `sampling` block from the `Trigger` definition created through `cs-control` and dispatches every activation through the matching `Decider`. The block shape, validation rules, and retention impact are documented in [Operators Observability](Operators-Observability).

```yaml
trigger:
  type: http
  source:
    method: POST
    path: /webhook
  sampling:
    mode: tail
    head_per_minute: 50
    tail_on_error: true
    tail_on_slow_ms: 250
    probability: 0.05
```

## Environment variable overrides

Sous deliberately keeps the YAML as the single configuration surface. The only documented environment-variable overrides are:

- `CS_INVOKER_TENANT_INFLIGHT` — overrides `internal/limits.DefaultTenantMaxInflight` at `cs-invoker-pool` start. Used while a dedicated YAML key is pending.
- `VAULT_TOKEN` — read by the `vault` secrets driver when `plugins.secrets.vault.token` is blank.

No other field documented above accepts an environment-variable override; operators that need per-environment differences template the YAML at deployment time.

## Cross-references

- [Operators Capacity and Limits](Operators-Capacity-and-Limits) — how each limit knob maps to a throughput ceiling.
- [Operators Observability](Operators-Observability) — what each service emits to Prometheus.
- [Operators Security](Operators-Security) — the threat model the defaults enforce.
- [Operators Runbooks](Operators-Runbooks) — incident playbooks keyed off configuration thresholds.
- [Deployment Kubernetes](Deployment-Kubernetes) — Helm values that compose into the YAML file.
- `internal/config/config.go` — the canonical Go struct backing every field above.
- `config.example.yaml` — the example file rendered at the repository root.
