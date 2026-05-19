# Enabled Services: ledgerDB Audit

Sous emits a structured audit event for every control-plane mutation: function created, draft uploaded, version published, alias updated, schedule changed, worker binding modified, signing key rotated. These events form a tamper-evident timeline that satisfies compliance requirements and supports incident forensics. Each event carries the actor, the tenant, the affected resource, the action name, and an outcome enumerated as `success`, `denied`, or `error`, so downstream tooling can reconstruct who did what, when, and with what result.

ledgerDB is the abstract sink for this stream; the concrete implementation is pluggable, with three drivers shipping in v0.1: `stdout` (events written to the service's stdout as JSON, suitable for log shippers), `codeq` (events published to a tenant-specific `cs.audit.{tenant}` topic), and `webhook` (events POSTed to an HMAC-signed external endpoint). Operators choose a driver in `config.example.yaml` under `plugins.audit`; the sink is constructed at process start by `audit.NewSinkFromConfig` (`internal/audit/config.go`) and handed to a `Recorder` that fans events out after the underlying KV mutation commits.

Audit emission is an at-least-once advisory feed rather than a synchronous transactional gate. The mutation itself commits to KVRocks first; only on success does the recorder publish an event. Sink failures are logged as `SinkLag` warnings but do not roll back the mutation — auditors prefer occasional duplicate lines over phantom records of writes that never happened. A bounded ring buffer in KVRocks keeps the most recent thousand events per tenant available through the `GET /v1/tenants/{tenant}/audit` replay endpoint, while the chosen sink remains the long-retention source of truth.

## Event envelope

Every audit event carries the same envelope, defined as `audit.Event` in `internal/audit/event.go`:

```json
{
  "schema_version": "1",
  "event_id": "evt_01HZ2K8XW3P9V2H6QM4N1Y3T7B",
  "ts": "2026-05-19T14:23:11.482Z",
  "tenant": "t_acme",
  "actor": "user:alice@acme.example",
  "action": "function.publish",
  "resource": "fn://t_acme/payments/reconcile@v7",
  "outcome": "success",
  "request_id": "req_abc123",
  "detail": {
    "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
    "alias": "prod",
    "draft_id": "drf_01HZ2K0Z1ABCDEFGHJKMN"
  }
}
```

Fields are additive. `schema_version` increments only on a non-additive change to the wire shape; consumers fan out on that column instead of pinning to the in-process Go struct. `event_id` is the dedupe key under at-least-once delivery; the recorder generates one with `uuid.NewString` when the handler does not supply one. `outcome` is one of three enumerated values — `success`, `denied`, `error` — and SIEMs filter on it heavily, which is why the field is constrained rather than free-form. `detail` is an allowlisted structured map; callers must not place secret material or bundle bytes in it, and the audit module documents that contract in package commentary.

The control plane attaches the `X-Request-Id` header value to `request_id` so a single API request can be joined across request logs, activation logs, and audit lines without speculative timestamp correlation.

## Event types

Each control-plane handler emits exactly one `action` value. The vocabulary below mirrors the call sites in `cmd/cs-control/main.go` and the auxiliary handlers under `cmd/cs-control/`.

### FunctionCreated

Action: `function.create`.

Resource: `fn://{tenant}/{namespace}/{name}`.

Detail:

- `runtime` — the runtime adapter selected for the function (for example `cs-js`, `cs-python`, `cs-wasm`).

Emitted from `createFunction` only when the underlying record was newly inserted; idempotent re-creates do not generate a second event.

### FunctionDeleted

Action: `function.delete`.

Resource: `fn://{tenant}/{namespace}/{name}`.

Detail: empty.

A soft delete marks `deleted_at_ms` on the function record; readers that pass `?include_deleted=true` can still observe the tombstone. The audit line is the only place where the delete is reliably visible to compliance pipelines.

### DraftUploaded

Action: `function.draft.upload`.

Resource: `fn://{tenant}/{namespace}/{name}/draft/{draft_id}`.

Detail:

- `sha256` — content hash of the draft bundle
- `size_bytes` — bundle byte length

### DraftDiscarded

Drafts expire on TTL rather than on an explicit handler call, so there is no synchronous `DraftDiscarded` event in v0.1. Operators observe expiry by the absence of a corresponding `function.publish` event before the draft TTL elapsed; the storage layer is described in [Storage-KVRocks](Storage-KVRocks).

### FunctionPublished

Action: `function.publish`.

Resource: `fn://{tenant}/{namespace}/{name}@v{version}`.

Detail:

- `sha256` — content hash of the bundle that was promoted from draft
- `alias` — the alias updated atomically with the publish, if the request specified one
- `draft_id` — the draft consumed by the publish

This event is the immutable join point between a draft and a numbered version. Once emitted, the version record in KVRocks is read-only.

### AliasUpdated

Action: `function.alias.set`.

Resource: `fn://{tenant}/{namespace}/{name}/alias/{alias}`.

Detail:

- `version` — the version number the alias now points to

### AliasDeleted

Alias deletes in v0.1 are expressed as a set to an empty version through the same `function.alias.set` action; the `detail.version` field is empty. A dedicated `function.alias.delete` action is on the roadmap.

### ScheduleCreated / ScheduleUpdated / ScheduleDeleted

Actions: `schedule.create`, `schedule.delete`.

Resource: `schedule://{tenant}/{namespace}/{name}`.

Detail on create:

- `every_seconds` — the schedule period
- `overlap_policy` — one of the configured overlap modes (`skip`, `queue`, `parallel`)

Updates are expressed as a re-create against the same `name`; consumers correlate the pair through `tenant` plus `namespace` plus `name`.

### WorkerBindingCreated / WorkerBindingDeleted

Actions: `cadence.worker.create`, `cadence.worker.delete`.

Resource: `cadence://{tenant}/{namespace}/{name}`.

Detail on create:

- `domain` — the Cadence domain the binding registers under
- `tasklist` — the tasklist the poller serves

See [Cadence-Integration](Cadence-Integration) for the surrounding lifecycle.

### SigningKeyRotated

Action: `tenant.signing_key.rotate`.

Resource: `tenant://{tenant}/signing-keys/{kid}`.

Detail: empty.

The rotated key id (`kid`) is in the resource URN rather than `detail` so SIEM rules can match on resource regardless of payload shape. Key creation is implicit in the first rotate; explicit `tenant.signing_key.create` and `tenant.signing_key.revoke` actions are roadmap.

### EgressPolicyWritten

Action: `egress.policy.write`.

Resource: `egress://{tenant}`.

Detail:

- `allowed_hosts` — count of allowed host entries
- `allowed_cidrs` — count of allowed CIDR entries
- `denied_hosts` — count of explicit deny entries
- `default_deny` — boolean controlling the default behaviour

This is the audit hook for per-tenant network egress allowlists; see `cmd/cs-control/egress.go` for the call site and [Security](Security) for the broader policy model.

### SecretRead

Secret reads do not emit a Sous-native audit line in v0.1. The `vault` driver delegates audit responsibility to Vault's own audit device, which is the authoritative record for who fetched which secret. The `memory` driver is local-only and unsuited to environments that require secret-read auditing. A `secret.read` action emitted on the Sous side is on the roadmap for operators who consolidate audit pipelines. See [Enabled-Services-Vault-Secrets](Enabled-Services-Vault-Secrets) for the configuration surface.

## Sinks

Three drivers ship in v0.1. Each implements the `audit.Sink` interface (`internal/audit/sink.go`) and is selected by `plugins.audit.sink` in `config.example.yaml`.

### stdout

The simplest sink writes one JSON line per event to the cs-control process stdout. Container log shippers (Fluent Bit, Vector, Promtail) ingest the stream without additional configuration and forward to Elasticsearch, Loki, or any object store. This is the default in dev and the CI baseline.

```yaml
plugins:
  audit:
    sink: stdout
```

### codeq

Events are published to a tenant-scoped codeQ topic computed as `cs.audit.{tenant}`. Consumers subscribe per tenant so a single broker cluster can fan events out to several downstream pipelines (one team's SIEM, a separate compliance archive, an internal data lake) without cross-tenant exposure.

```yaml
plugins:
  audit:
    sink: codeq
    topic_prefix: cs.audit
```

The publisher contract is the narrow `CodeQPublisher` interface in `internal/audit/sink.go`, which mirrors the existing `messaging.Provider.Publish` signature so the messaging driver plugs in without an adapter. The wire payload is the same JSON envelope used by the stdout sink. See [codeQ-Protocol](codeQ-Protocol) for the topic semantics.

### webhook

Each event is POSTed to a configured URL with an HMAC-SHA256 signature in the `X-CS-Audit-Signature` header. The webhook sink suits operators who already host a managed SIEM with HTTP intake and prefer not to operate a broker.

```yaml
plugins:
  audit:
    sink: webhook
    webhook_url: "https://siem.example.com/sous/audit"
    hmac_secret: "${SOUS_AUDIT_HMAC_SECRET}"
```

The default HTTP client times out at five seconds. Non-2xx responses are reported as sink errors; the recorder logs a `SinkLag` warning and the operator's outer monitoring detects sustained failure.

## HMAC webhook signing

The webhook sink computes `HMAC-SHA256(secret, body)` over the exact JSON body of the request, hex-encodes the result, and sets it as the `X-CS-Audit-Signature` header. The body covered is the full `Event` JSON encoding produced by `audit.Event.Marshal`, byte-for-byte the same bytes the receiver sees in the request stream. No whitespace normalisation, no canonicalisation, no header inclusion — the JSON request body is the authoritative input.

Receivers verify by reading the raw request body before any JSON unmarshalling, recomputing the HMAC with their copy of the shared secret, and comparing the hex string in `X-CS-Audit-Signature` with `hmac.Equal`. The `audit.VerifySignature` helper in `internal/audit/sink.go` exposes the same computation so receiver test suites can pin their behaviour against the sender's implementation. A worked example:

```go
ok := audit.VerifySignature([]byte(secret), rawBody, r.Header.Get("X-CS-Audit-Signature"))
if !ok {
    http.Error(w, "bad signature", http.StatusUnauthorized)
    return
}
```

Operators rotate the shared secret by updating both ends in lock-step. The sink does not support signature chaining across rotations; brief receiver-side rejects during rotation are expected and recovered by the at-least-once retry path.

## Recorder lifecycle

The `audit.Recorder` (`internal/audit/recorder.go`) is the integration point cs-control handlers call after a successful kv mutation. The control plane constructs a single recorder at process start and shares it across every handler goroutine. The recorder holds three collaborators:

- A `Sink` chosen from the three drivers above.
- An `IndexStore`, satisfied by the persistence provider, which materialises the ring-buffer history in KVRocks.
- A `Logger`, satisfied by the structured logger, which receives `SinkLag` warnings when the sink errors.

The `IndexStore` interface is intentionally narrow:

```go
type IndexStore interface {
    KVGet(ctx context.Context, key string) (string, error)
    KVSet(ctx context.Context, key, value string, ttl time.Duration) error
}
```

It matches the existing `persistence.Provider.KVGet` and `KVSet` signatures so no adapter is needed. The recorder serialises history reads and writes under an internal mutex so concurrent handler goroutines never collide on the per-tenant ring buffer.

The two public emission methods carry the same envelope shape but signal different outcomes. `AfterCommit` is the success path; it defaults `Outcome` to `success`, generates an `event_id` if the caller left one empty, and sets `Timestamp` to wall-clock UTC. `EmitFailure` is the explicit failure path; it forces `Outcome` to `error` (or honours an explicit `denied`) and otherwise reuses the same emission machinery. The two-method split is the contract that prevents accidental phantom records: a handler must choose one, and `AfterCommit` is unreachable on the error path because the kv mutation hasn't happened yet.

## Reliability

Audit events are emitted after the KV mutation persists. The handler calls `recorder.AfterCommit` only on the success path; the `AfterCommit` name is the load-bearing contract that prevents a phantom audit line for a write that did not happen. Failure outcomes are recorded explicitly through `recorder.EmitFailure`, which the caller invokes when authn or authz blocks the request, or when validation rejects the payload before persistence.

Sink failures do not roll back the mutation. The kv commit already happened; reversing it would leave the system in a state worse than the one the audit miss would describe. Instead the recorder records a `SinkLag` warning through the structured logger and returns the sink error to the caller, who ignores it (`_ = s.recorder.AfterCommit(...)` in `cmd/cs-control/main.go`). Operators detect sustained sink lag through the warning logs and the SIEM's own ingest-rate dashboards.

Retry on transient sink failure is a roadmap item. The current implementation makes a single best-effort emission; the local ring buffer in KVRocks (`cs:audit:{tenant}`) absorbs the loss for the replay endpoint while the operator's monitoring catches the gap on the sink side. A bounded backoff queue with a local DLQ on retry exhaustion is tracked as future work; the design assumes the sink is the long-retention store and that operators detect missing events through their own coverage checks rather than through Sous-internal queues.

The replay endpoint guarantees a recent window even when the sink is down: the recorder writes to the ring buffer regardless of sink outcome, so `GET /v1/tenants/{tenant}/audit` returns the last thousand events per tenant during an incident.

## Retention

Sous itself does not retain audit events beyond the sink's responsibility. The in-process ring buffer caps at a thousand entries per tenant (configurable via `plugins.audit.history_limit`) with a default TTL matching the activation TTL; it is sized for incident triage, not compliance retention. Operators configure long-term retention at the sink:

- For the `stdout` sink, the log-shipper pipeline's storage policy (Elasticsearch ILM, Loki retention, S3 lifecycle).
- For the `codeq` sink, the broker topic retention configured on `cs.audit.{tenant}` topics; brokers like Kafka allow per-topic retention policies measured in days or bytes.
- For the `webhook` sink, the receiver's own storage policy; a typical SIEM holds raw events for ninety days hot and several years cold.

The split keeps Sous's footprint bounded and lets the operator align retention with their compliance framework without re-deploying the platform. [Capacity-and-Limits](Capacity-and-Limits) documents the ring-buffer sizing knob.

## Querying

There is no native query interface on the audit stream beyond the recent-history replay endpoint. Operators query through the chosen sink:

- For `stdout` shipped through a logger, use the indexed search the logger backend exposes (Kibana, Grafana Logs, Splunk).
- For `codeq`, consume the `cs.audit.{tenant}` topic with the broker's native tooling (Kafka consumer, codeQ subscription client) or fan it into a search-backed sink.
- For `webhook`, the receiver (commonly a SIEM) provides query and alerting; Sous emits, the SIEM stores and queries.

The replay endpoint at `GET /v1/tenants/{tenant}/audit` supports `?since={unix_ms}&actor={sub}&action={dotted_action}&limit={n}` for tenant-scoped recent reads. The endpoint reads from the ring buffer in KVRocks, never from the sink; it is designed for first-response triage, not for long-history search. The full wire contract is in [REST-API](REST-API), and the IAM action that gates access is `cs:audit:read`, documented in [IAM-with-Tikti](IAM-with-Tikti).

## Plugin interface

The pluggable contract lives in `internal/audit/sink.go`:

```go
type Sink interface {
    Emit(ctx context.Context, event Event) error
    Name() string
    Close() error
}
```

`Emit` must be safe for concurrent use; the recorder serialises history writes internally but invokes `Emit` from the calling handler goroutine. `Name` returns the driver name (`stdout`, `codeq`, `webhook`) and is used for log and metric labels; new drivers should return a short lowercase identifier. `Close` releases resources held by the sink and is idempotent so process shutdown can call it from a deferred site without checking prior state.

Adding a new driver means implementing the interface, wiring construction into `audit.NewSinkFromConfig`, and adding a config branch under `plugins.audit`. The contract is intentionally narrow so out-of-tree drivers (an organisation's internal SIEM, a queue not yet supported by codeQ) integrate without forking the core. The `CodeQPublisher` interface in the same file is an example of the same pattern applied one layer in: it scopes the dependency on the messaging package to a two-method surface that any provider can satisfy.

## Tenant isolation

Audit events are tagged with `tenant` at construction by `audit.NewEvent` and the tag is immutable for the lifetime of the event. The `codeq` sink routes each event to `cs.audit.{tenant}`; a consumer subscribed to one tenant's topic never observes another tenant's events. The replay endpoint reads the ring-buffer key `cs:audit:{tenant}` and the IAM check in `cmd/cs-control/audit.go` rejects principals whose Tikti tenant claim does not match the URL tenant, so cross-tenant reads are blocked at both the storage key and the authz layer.

The `stdout` and `webhook` sinks emit untagged at the transport layer — a single stdout stream and a single webhook endpoint receive events for every tenant — but each line still carries `tenant` in the JSON body. Operators who run a single SIEM across multiple tenants index on `tenant` and apply per-tenant access controls in the SIEM itself; operators who require strict broker-layer isolation choose the `codeq` sink and configure per-tenant topic ACLs in the broker.

## Compliance considerations

The append-only audit stream supports SOC 2, ISO 27001, and similar frameworks that require an independent record of who changed what and when. The chosen sink (codeQ topic, webhook receiver, log archive) holds the long-retention store; the immutability properties of that store satisfy the "tamper-evident" criterion auditors look for. Sous's contribution is to emit a complete, consistent, signed-where-required stream; the storage tier is the operator's choice.

The HMAC-signed webhook sink is the recommended option when the audit pipeline crosses a trust boundary: a managed SIEM operated by a separate team, a vendor compliance product, a cross-region archive. Signature verification gives the receiver cryptographic assurance that each event originated at the Sous control plane and not at an attacker who reached the network path.

The broader compliance posture, including the controls Sous applies to bundle signing, secret access, and tenant isolation, is documented in [Operators-Security](Operators-Security). The audit stream is the visibility layer; the enforcement controls described there are what the stream proves were applied.

### Operator runbook hooks

Two failure modes warrant a runbook entry. The first is sustained sink lag: the recorder logs a `SinkLag` warning every time the sink errors, and a steady stream of these warnings means the sink is unreachable, the webhook receiver is rejecting payloads, or the broker is unhealthy. The recommended response is to check the sink-side health (broker topic, webhook receiver status), rotate the HMAC secret if signature failures cluster, and confirm the ring-buffer endpoint at `GET /v1/tenants/{tenant}/audit` still serves recent events as a fallback for in-flight investigation.

The second is replay divergence: when the ring buffer disagrees with the sink-side store, the sink is the source of truth. The buffer is bounded and TTL'd; the sink is retention-grade. Reconciliation tooling reads the sink-side store, not the ring buffer. The ring buffer's role is bounded triage, not authoritative replay.

### Observability signals

In v0.1 the recorder surfaces sink health through structured log warnings rather than through dedicated metrics; the `SinkLag` log line is the primary operator signal and is correlated with the existing cs-control request logs via `request_id`. A future iteration is expected to add dedicated Prometheus counters (per-sink emit totals, per-tenant ring-buffer gauges) so that SLOs on audit coverage are alert-able rather than log-grepped. The broader signal vocabulary the control plane exposes is documented in [Observability](Observability).
