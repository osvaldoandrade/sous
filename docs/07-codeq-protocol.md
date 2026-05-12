# codeQ protocol

`code-sous` uses codeQ topics to decouple trigger ingestion from execution.

## Topics

- `cs.invoke`  
  Carries InvocationRequest messages.

- `cs.results`  
  Carries InvocationResult messages.

- `cs.dlq.invoke`  
  Stores InvocationRequest messages that fail validation.

- `cs.dlq.results`  
  Stores InvocationResult messages that fail correlation.

## Message envelope

All messages use a shared envelope:

```json
{
  "schema": "cs.envelope.v1",
  "id": "msg_01H...",
  "ts_ms": 1730000000000,
  "tenant": "t_abc123",
  "type": "InvocationRequest",
  "body": { }
}
```

## InvocationRequest

Schema: `cs.invoke.v1`

```json
{
  "activation_id": "uuid",
  "request_id": "req_01H...",
  "tenant": "t_abc123",
  "namespace": "payments",
  "ref": { "function": "reconcile", "alias": "prod", "version": 17 },
  "trigger": {
    "type": "http|schedule|cadence|api",
    "source": { }
  },
  "principal": { "sub": "user:123", "roles": ["role:app"] },
  "deadline_ms": 1730000003000,
  "event": { }
}
```

Rules:

- Producers must set `activation_id` as UUID v4.
- Producers must set `deadline_ms`.
- `ref.version` is optional when `ref.alias` exists.
- The invoker resolves alias to version when version is missing.

## InvocationResult

Schema: `cs.results.v1`

```json
{
  "activation_id": "uuid",
  "request_id": "req_01H...",
  "status": "success|error|timeout",
  "duration_ms": 12,
  "result": {
    "statusCode": 200,
    "headers": { "content-type": "application/json" },
    "body": "eyJvayI6dHJ1ZX0=",
    "isBase64Encoded": true
  },
  "error": {
    "type": "RuntimeError",
    "message": "string",
    "stack": "string"
  }
}
```

Rules:

- The invoker publishes exactly one result per activation.
- The invoker truncates `stack` to 8,192 bytes.

## Correlation

- `request_id` ties API requests to results.
- `activation_id` ties activations to logs and metadata.
- `envelope.id` ties producer messages to consumer dedup state (see "Idempotency" below).

## Delivery mode

The system supports two patterns:

- Fire-and-forget
  - producer publishes InvocationRequest
  - consumer stores ActivationRecord
- Request-response
  - producer publishes InvocationRequest
  - producer waits on `cs.results` for the same `request_id`

`cs-http-gateway` uses request-response for sync HTTP.

Delivery semantics are **at-least-once**: codeQ may redeliver a message
after a consumer crash or a slow ack. Producers and consumers cooperate to
make the end-to-end execution effectively-once via deterministic message
ids and a consumer-side dedup store.

## Idempotency

Producers compute a deterministic envelope id of the form

```
msg = "msg_" + sha256_hex(tenant + "|" + activation_id + "|" + seq)
```

where `seq` is `0` for `InvocationRequest` and `1` for `InvocationResult`.
The implementation lives in `internal/codeq.DeterministicMessageID`.

Consumers run every fetched envelope through a `SeenSet` keyed by
`envelope.id`. The first delivery records the id; any subsequent delivery
within the TTL window (default `1h`) is acknowledged but **not** dispatched
to the handler. The contract:

- A producer retry that previously succeeded but never received an ack will
  emit the same envelope id; the consumer ignores it.
- A consumer that crashed mid-handle will see the same envelope on restart;
  if the handler had already committed but the consumer offset had not, the
  dedup store collapses it.
- The `SeenSet` defaults to the in-memory `NewMemorySeenSet` (suitable for
  single-node dev and unit tests). Production deployments back it with the
  same KVRocks-resident dedup store used by the HTTP gateway
  (`internal/idempotency`).
- Disable dedup with `Kafka.WithDedup(nil, 0)` when a test needs to observe
  the raw at-least-once redelivery stream.

Function-internal side effects (HTTP writes, database mutations, etc.)
remain **at-least-once**: codeQ guarantees the message reaches the invoker,
but the invoker itself only guarantees one activation per logical
`(tenant, activation_id)`. Function authors must still make their own side
effects idempotent.

## Subscription triggers

A `SubscriptionBinding` wires a codeQ topic to a function so the function is
invoked once per matching envelope. Bindings live under cs-control and are
managed via the REST surface in `docs/04-api-rest.md`:

```
POST   /v1/tenants/{tenant}/namespaces/{namespace}/subscriptions
GET    /v1/tenants/{tenant}/namespaces/{namespace}/subscriptions
GET    /v1/tenants/{tenant}/namespaces/{namespace}/subscriptions/{name}
DELETE /v1/tenants/{tenant}/namespaces/{namespace}/subscriptions/{name}
```

The persisted record:

```json
{
  "tenant": "t_abc123",
  "namespace": "payments",
  "name": "reconcile-orders",
  "topic": "orders.created",
  "filter": "body.kind == \"premium\"",
  "function_ref": { "function": "reconcile", "alias": "prod" },
  "mode": "ordered",
  "group_id": "cs-sub-t_abc123-payments-reconcile-orders",
  "max_concurrency": 1,
  "created_at_ms": 1730000000000,
  "enabled": true
}
```

Persistence key: `cs:subscription:{tenant}:{namespace}:{name}:meta`, with a
per-namespace index at `cs:subscription:{tenant}:{namespace}:index`.

### Delivery modes

- **ordered**: a single consumer goroutine processes the topic. The consumer
  publishes one InvocationRequest per matching envelope and only commits the
  offset after the publish succeeds. Per-partition ordering is preserved and
  back-pressure flows naturally from the broker.
- **unordered**: the consumer dispatches matching envelopes to a pool of
  `max_concurrency` workers via a buffered channel. Order is not preserved
  across workers (per-partition order is preserved if the broker delivers one
  partition per consumer goroutine, which v0.1 in-memory and Kafka backends
  both satisfy). Offsets advance as the broker confirms each fetch.

`max_concurrency` is clamped to a service-side cap (`64` in v0.1) and pinned
to `1` for ordered mode. The default for unordered mode comes from
`cs_control.subscriptions.worker_pool_default`.

### Group ID semantics

Each binding carries a stable codeQ consumer `group_id`. If the caller omits
it, cs-control derives one as `cs-sub-{tenant}-{namespace}-{name}`. The
group id determines offset commit scope: multiple cs-control replicas (or
any other consumer joining the same group) cooperate on a single offset
cursor, which is the standard codeQ partition-balance contract.

### Filter grammar

The v0.1 filter language is intentionally minimal. The full grammar:

```
filter := <path> ("==" | "!=") <quoted-string>
path   := identifier ("." identifier)*
```

Where `<quoted-string>` is a Go-style double-quoted string. Path resolution:

- `event.tenant`, `event.type`, `event.id`, `event.schema` address envelope
  fields.
- `tenant` and `type` are shorthand for `event.tenant` and `event.type`.
- `body.<path>` walks the JSON-decoded envelope body. Strings compare
  literally; numbers and booleans render as their JSON-canonical form
  (`42`, `true`, `false`).
- Unprefixed identifiers (e.g. `kind == "x"`) resolve to `body.<...>` so
  the shortcut works without ceremony.
- Unknown fields resolve to the empty string; `field == ""` matches missing
  values.

An empty filter matches every envelope. Invalid expressions are rejected
with `400 CS_VALIDATION_FAILED` at create time so misconfigured bindings
never reach the consumer.

A richer language (AND/OR, comparison operators, JSONPath-style indexing,
typed equality) is **explicit future work** — see `docs/18-roadmap.md`.

### Trigger envelope

Subscription consumers publish through the same invoker path as HTTP,
schedule, and Cadence triggers. The trigger field on the InvocationRequest
is:

```json
{
  "trigger": {
    "type": "subscription",
    "source": {
      "binding": "reconcile-orders",
      "topic": "orders.created",
      "envelope_id": "msg_01H...",
      "group_id": "cs-sub-t_abc123-payments-reconcile-orders"
    }
  }
}
```

Activation records carry this through unchanged, so observability tooling
can fan out by binding name without re-decoding the payload.

### Offset commit and retries

For ordered mode, the consumer only commits the broker offset after
`PublishInvocation` returns successfully — a publish failure aborts the
consumer goroutine and the next reconcile pass restarts it. For unordered
mode, the consumer commits per fetched message; a failing worker raises an
error that propagates back through the dispatcher.

A richer retry/DLQ policy (configurable max attempts, park-to-DLQ
behaviour) is tracked under E4 task-03; until then the broker's
at-least-once retry plus the codeQ dedup store described above provides
the only redelivery safety net.
