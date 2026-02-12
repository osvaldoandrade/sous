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

## Delivery mode

The system supports two patterns:

- Fire-and-forget
  - producer publishes InvocationRequest
  - consumer stores ActivationRecord
- Request-response
  - producer publishes InvocationRequest
  - producer waits on `cs.results` for the same `request_id`

`cs-http-gateway` uses request-response for sync HTTP.
