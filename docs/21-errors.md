# Error codes

This file defines stable error codes.

All errors use prefix `CS_`.

## Authentication

- `CS_AUTHN_MISSING_TOKEN`
- `CS_AUTHN_INVALID_TOKEN`
- `CS_AUTHN_EXPIRED_TOKEN`

## Authorization

- `CS_AUTHZ_DENIED`
- `CS_AUTHZ_ROLE_MISSING`
- `CS_AUTHZ_RESOURCE_MISMATCH`

## Validation

- `CS_VALIDATION_FAILED`
- `CS_VALIDATION_MANIFEST_INVALID`
- `CS_VALIDATION_BUNDLE_TOO_LARGE` *(legacy — superseded by `CS_BUNDLE_TOO_LARGE`)*
- `CS_VALIDATION_NAME_INVALID`

## Size limits

These errors are returned at the v0.1 boundaries documented in
`docs/26-capacity-and-limits.md` and carry HTTP `413 Payload Too Large`.

- `CS_BUNDLE_TOO_LARGE` — returned by `cs-control` when a draft bundle exceeds `cs_control.limits.max_bundle_bytes` (default 16 MiB).
- `CS_BODY_TOO_LARGE` — returned by `cs-http-gateway` when a request body exceeds `cs_http_gateway.limits.max_body_bytes` (default 6 MiB).
- `CS_RESULT_TOO_LARGE` — returned by `cs-invoker-pool` when a function result exceeds `cs_invoker_pool.limits.max_result_bytes` (default 256 KiB).
- `CS_LOG_LIMIT_EXCEEDED` — emitted (and surfaced via `X-CS-Truncated: logs`) when cumulative activation logs exceed `cs_invoker_pool.limits.max_log_bytes` (default 1 MiB). Logs are cleanly truncated on a UTF-8 boundary; the function still records a `success` status when it itself succeeded.

## Quotas

Quota errors carry HTTP `429 Too Many Requests` and include a `Retry-After`
header when known.

- `CS_RATE_LIMITED` — token bucket exhausted at `cs_http_gateway` (scope: tenant or function).
- `CS_TENANT_INFLIGHT_LIMIT` — concurrent activation cap reached at `cs-invoker-pool` (sync invoke). Async invokes are queued in codeQ instead.

## Idempotency

- `CS_IDEMPOTENCY_CONFLICT` — HTTP `409 Conflict`. Returned when the same `Idempotency-Key` is reused with a body whose canonical fingerprint differs from the original request. See `docs/10-http-invoke.md`.

## Activation lifecycle

- `CS_ACTIVATION_TTL_EXPIRED` — HTTP `410 Gone`. Returned by the activation read path when the activation record's TTL has elapsed. TTL is configurable via `cs_control.limits.activation_ttl_seconds` (default 7 days).

## Storage

- `CS_KVROCKS_UNAVAILABLE`
- `CS_KVROCKS_WRITE_FAILED`
- `CS_KVROCKS_READ_FAILED`
- `CS_KVROCKS_CAS_FAILED`

## codeQ

- `CS_CODEQ_PUBLISH_FAILED`
- `CS_CODEQ_SUBSCRIBE_FAILED`
- `CS_CODEQ_CORRELATION_TIMEOUT`

## Runtime

- `CS_RUNTIME_TIMEOUT`
- `CS_RUNTIME_MEMORY_LIMIT`
- `CS_RUNTIME_EXCEPTION`
- `CS_RUNTIME_CAPABILITY_DENIED`

## Cadence

- `CS_CADENCE_POLL_FAILED`
- `CS_CADENCE_RESPOND_FAILED`
- `CS_CADENCE_HEARTBEAT_FAILED`

## Scheduler

- `CS_SCHEDULER_STATE_WRITE_FAILED`

## Mapping to HTTP

- `CS_AUTHN_*` → 401
- `CS_AUTHZ_*` → 403
- `CS_VALIDATION_*` → 400
- `CS_BUNDLE_TOO_LARGE`, `CS_BODY_TOO_LARGE`, `CS_RESULT_TOO_LARGE`, `CS_LOG_LIMIT_EXCEEDED` → 413
- `CS_RATE_LIMITED`, `CS_TENANT_INFLIGHT_LIMIT` → 429
- `CS_IDEMPOTENCY_CONFLICT` → 409
- `CS_ACTIVATION_TTL_EXPIRED` → 410
- `CS_CODEQ_CORRELATION_TIMEOUT` → 504
- `CS_*_UNAVAILABLE` → 503
- runtime errors inside function → 200 with function-defined statusCode, when the function returns a valid response object
