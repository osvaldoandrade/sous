# HTTP invoke path

`cs-http-gateway` exposes a generic endpoint for published functions.

## Endpoint shape

```
/v1/web/{tenant}/{namespace}/{function}/{ref}/{proxyPath*}
```

`ref` accepts:

- an alias name
- a version integer

Examples:

- `/v1/web/t_abc123/payments/reconcile/prod`
- `/v1/web/t_abc123/payments/reconcile/17`

## Authentication and authorization

The gateway requires:

- `Authorization: Bearer <tikti_token>`

The gateway enforces:

- action `cs:function:invoke:http`
- per-version role allowlist `authz.invoke_http_roles`

## HTTP-to-event mapping

The gateway builds:

```json
{
  "version": "2.0",
  "routeKey": "$default",
  "rawPath": "/v1/web/...",
  "rawQueryString": "a=1&b=2",
  "headers": { "host": "...", "content-type": "..." },
  "requestContext": {
    "http": {
      "method": "POST",
      "path": "/v1/web/..."
    }
  },
  "body": "base64",
  "isBase64Encoded": true
}
```

Rules:

- The gateway base64-encodes the raw body.
- The gateway sets `isBase64Encoded=true` always.

## Response mapping

The function returns:

```json
{
  "statusCode": 200,
  "headers": { "content-type": "application/json" },
  "body": "base64 or utf8",
  "isBase64Encoded": true
}
```

The gateway:

- writes `statusCode`
- writes headers
- decodes body if `isBase64Encoded=true`
- returns the body bytes

## Sync execution semantics

The gateway publishes InvocationRequest and waits for InvocationResult.

The gateway uses:

- `request_id` correlation
- a timeout equal to function timeout + 250 ms

If the timeout fires:

- the gateway returns `504`
- the activation may still complete

## Limits

The gateway enforces:

- `max_request_body_bytes`: 6 MiB
- `max_header_bytes`: 64 KiB
- `max_query_bytes`: 16 KiB

## Idempotency

The gateway accepts an optional header:

- `Idempotency-Key`

Format:

- `[A-Za-z0-9_-]{8,128}`
- Keys that do not match this pattern are rejected with `400 CS_VALIDATION_FAILED`.

Derivation:

- If set, the gateway derives `activation_id = UUIDv5(namespace=tenant, name=Idempotency-Key + function ref)`.
- If absent, the gateway generates UUIDv4 and does **not** persist a dedup record.

Dedup contract (when the header is present):

- The gateway computes a canonical body fingerprint `sha256(request_body)`.
- The dedup record is keyed by `idem:{tenant}:{namespace}:{function}:{ref}:{key}` and persisted with TTL = `function timeout + 3600s` (configurable; default 1h when the function timeout is unknown).
- The dedup record is implemented by the `internal/idempotency.Store` contract: production deployments back it with KVRocks; tests use the in-memory implementation.

Replay behaviour:

- Same key + same body, terminal record present → the gateway replays the cached `statusCode`, headers, and body and adds the response header `X-CS-Idempotency-Replay: 1`. The function is **not** re-executed.
- Same key + different body → the gateway returns `409 CS_IDEMPOTENCY_CONFLICT` without re-executing the function. See `docs/21-errors.md`.
- New key → the gateway forwards the request to the function and persists the resulting response on success (status < 500). 5xx responses are **not** cached so the next retry can attempt a fresh execution.

Response headers:

- `X-CS-Idempotency-Replay: 1` is set only on cache-hit replays. Clients can use this header to distinguish a fresh activation from a replayed cached result.

Scope:

- The dedup key always includes the tenant, namespace, function, and ref so two tenants (or two function versions) cannot collide on the same user-supplied `Idempotency-Key`.
- Idempotency is advisory: clients that do not send the header still receive the legacy UUIDv4 activation_id and no dedup guarantee.
