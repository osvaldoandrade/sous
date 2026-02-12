# HTTP Invoke Path

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

If set, the gateway derives `activation_id` as:

- UUIDv5(namespace=tenant, name=Idempotency-Key + function ref)

If absent, the gateway generates UUIDv4.
