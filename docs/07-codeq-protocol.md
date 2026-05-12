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
