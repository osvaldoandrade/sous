# Reference: Error Codes

Every Sous error response carries the same JSON envelope: `{"error": {"code": "CS_*", "message": "...", "request_id": "..."}}`. The `code` field is the stable machine-readable identifier; the `message` field is a short human-readable summary; the `request_id` field correlates the failure with the gateway, scheduler, or invoker log line that produced it. Callers should switch on the code, not the message. HTTP status is derived from the code by `internal/errors/errors.go` (`StatusCode`), so a given code always maps to the same status — this lets clients build retry policies on the code alone. The categories below enumerate the codes the platform emits today; the source of truth is `internal/errors/errors.go`.

## Authentication (HTTP 401)

These codes come from the Tikti introspection driver wired into `cs-http-gateway` and `cs-control`. They mean the platform could not verify who is making the call.

| Code | HTTP | Description | Recovery |
|------|------|-------------|----------|
| `CS_AUTHN_MISSING_TOKEN` | 401 | The request did not carry an `Authorization: Bearer` header. | Attach a valid token from Tikti. |
| `CS_AUTHN_INVALID_TOKEN` | 401 | The token failed signature or audience checks at the IdP. | Re-issue the token; verify Tikti audience and clock skew. |
| `CS_AUTHN_EXPIRED_TOKEN` | 401 | The token's `exp` claim is in the past. | Refresh the token through the standard OAuth2 flow. |

## Authorization (HTTP 403)

These codes come from the authz middleware after authentication succeeds. They mean the caller is known but lacks the required role or scope.

| Code | HTTP | Description | Recovery |
|------|------|-------------|----------|
| `CS_AUTHZ_DENIED` | 403 | The principal has no role permitting this action. | Grant the missing role in Tikti or re-target the call at a permitted tenant/namespace. |
| `CS_AUTHZ_ROLE_MISSING` | 403 | A handler-specific role check rejected the request. | Add the named role (see `VersionAuthz.invoke_*_roles` for invoke paths). |
| `CS_AUTHZ_RESOURCE_MISMATCH` | 403 | The principal's tenant does not match the resource path. | Use the token issued for the correct tenant. |
| `CS_EGRESS_DENIED` | 403 | A user-code HTTP call was blocked by the per-tenant egress allowlist. | Add the host to the tenant's egress policy, or remove the call. |

## Validation (HTTP 400 / 422)

Validation codes come from the manifest and payload validators. They indicate the caller-supplied input is malformed or out of range.

| Code | HTTP | Description | Recovery |
|------|------|-------------|----------|
| `CS_VALIDATION_FAILED` | 400 | Generic validation failure; the message names the offending field. | Fix the payload per the schema in [Reference-Schemas](Reference-Schemas). |
| `CS_VALIDATION_MANIFEST_INVALID` | 400 | `manifest.json` violates `cs.function.script.v1`. | Re-run `cs pack` and inspect the schema validator output. |
| `CS_VALIDATION_BUNDLE_TOO_LARGE` | 400 | The draft bundle exceeds the configured byte cap before persistence. | Reduce bundle size or request a larger limit from the operator. |
| `CS_VALIDATION_NAME_INVALID` | 400 | A tenant/namespace/function/alias identifier does not match its pattern. | Use the patterns documented in [Reference-Schemas](Reference-Schemas). |
| `CS_VALIDATION_UNSUPPORTED_CODEC` | 400 | A `WorkerBinding` references a codec name not in the registry. | Use `json`, `msgpack`, `raw`, or leave the field empty. |
| `CS_RUNTIME_UNSUPPORTED` | 400 | The manifest declares a runtime that has no registered adapter. | Use `cs-js`, `cs-python`, or `cs-wasm` — whatever the cluster has installed. |
| `CS_SIGNATURE_MISSING` | 400 | The cluster requires signed bundles and this publish has none. | Sign the bundle with a tenant key. |
| `CS_SIGNATURE_INVALID` | 400 | The bundle signature failed verification. | Re-sign with the correct key; verify bundle integrity. |
| `CS_IMPORT_NOT_FOUND` | 422 | cs-js code referenced a bare specifier that is not in the frozen import map. | Declare the import in `manifest.json` and re-publish. |
| `CS_WORKFLOW_NON_DETERMINISTIC` | 422 | The publish-time determinism linter flagged banned APIs in a workflow bundle. | Replace `Date.now`/`Math.random`/`setTimeout`/etc. with the workflow-safe equivalents in `cs.workflow.*`. |

## Not Found (HTTP 404 / 410)

These codes mean the path or reference is well-formed but the backing record is missing.

| Code | HTTP | Description | Recovery |
|------|------|-------------|----------|
| `CS_SECRET_NOT_FOUND` | 404 | The secret reference does not resolve in the configured vault. | Create the secret or fix the path in `VersionConfig.Secrets`. |
| `CS_SIGNATURE_KEY_NOT_FOUND` | 404 | The signing key id on the bundle is unknown to the keystore. | Re-import the key or sign with an active key. |
| `CS_ACTIVATION_TTL_EXPIRED` | 410 | The requested activation record has aged past its retention window. | Refer to fresh activations; lengthen retention if needed. |

## Conflict (HTTP 409)

| Code | HTTP | Description | Recovery |
|------|------|-------------|----------|
| `CS_IDEMPOTENCY_CONFLICT` | 409 | A retried mutation found a record with a different identity (for example, a re-create with a different runtime). | Send the original payload or pick a new resource name. |

## Rate Limit (HTTP 429)

| Code | HTTP | Description | Recovery |
|------|------|-------------|----------|
| `CS_RATE_LIMITED` | 429 | The caller exceeded the per-token rate budget at the gateway. | Back off; the response carries `Retry-After`. |
| `CS_TENANT_INFLIGHT_LIMIT` | 429 | The tenant is at its inflight-activation cap. | Wait for activations to drain or raise the tenant cap. |

## Storage (HTTP 5xx)

These codes surface failures from KVRocks. A `_UNAVAILABLE` suffix maps to 503; other failures default to 500.

| Code | HTTP | Description | Recovery |
|------|------|-------------|----------|
| `CS_KVROCKS_UNAVAILABLE` | 503 | The KV store is unreachable or refusing connections. | Investigate the KVRocks pod; check the connection pool health metric. |
| `CS_KVROCKS_WRITE_FAILED` | 500 | A write reached KVRocks but did not commit. | Inspect the KVRocks log; retries are safe on idempotent writes. |
| `CS_KVROCKS_READ_FAILED` | 500 | A read reached KVRocks but errored. | Retry; if persistent, fail over to the replica. |
| `CS_KVROCKS_CAS_FAILED` | 500 | A compare-and-swap operation lost the race. | Reload the record and retry; this is expected under high contention. |

## codeQ (HTTP 5xx / 504)

| Code | HTTP | Description | Recovery |
|------|------|-------------|----------|
| `CS_CODEQ_PUBLISH_FAILED` | 500 | The trigger could not publish an `InvocationRequest`. | Check the codeQ broker; the trigger retries with backoff. |
| `CS_CODEQ_SUBSCRIBE_FAILED` | 500 | A consumer (gateway, poller, invoker) could not subscribe to the response topic. | Check the codeQ broker; restart the service if necessary. |
| `CS_CODEQ_CORRELATION_TIMEOUT` | 504 | The gateway waited beyond `deadline_ms` for the matching `InvocationResult`. | Increase the deadline, or inspect the invoker's tail for a stuck activation. |

## Runtime (HTTP 200 or 500, depending on path)

Runtime errors occur inside the sandbox. When the function returns a structured response, the gateway returns 200 with the function's `statusCode`. The codes below cover the cases where the runtime aborted before a response could form.

| Code | HTTP | Description | Recovery |
|------|------|-------------|----------|
| `CS_RUNTIME_TIMEOUT` | 500 | User code exceeded `limits.timeoutMs`. | Increase the timeout in the manifest, or speed up the function. |
| `CS_RUNTIME_MEMORY_LIMIT` | 500 | User code exceeded `limits.memoryMb`. | Raise the memory limit or reduce in-memory work. |
| `CS_RUNTIME_EXCEPTION` | 500 | User code threw an uncaught exception. | Inspect the activation log; fix the function. |
| `CS_RUNTIME_CAPABILITY_DENIED` | 500 | User code attempted a side effect outside the declared capability block. | Add the required capability to `manifest.json` and re-publish. |
| `CS_SECRET_UNAVAILABLE` | 503 | The secret provider could not be reached or rejected the request. | Inspect the secret provider health; activations retry against other replicas. |

## Cadence (HTTP 5xx)

| Code | HTTP | Description | Recovery |
|------|------|-------------|----------|
| `CS_CADENCE_POLL_FAILED` | 500 | The Cadence long-poll failed (network or auth). | Inspect the Cadence client; the poller retries with backoff. |
| `CS_CADENCE_RESPOND_FAILED` | 500 | Responding the task back to Cadence failed. | The poller retries with backoff; persistent failures indicate Cadence-side trouble. |
| `CS_CADENCE_HEARTBEAT_FAILED` | 500 | A heartbeat for a long-running activity failed. | Inspect the Cadence client; the poller will let the task expire. |
| `CS_RETRY_EXHAUSTED` | 502 | A trigger-level retry policy used all attempts and forwarded to DLQ. | Inspect the DLQ topic; fix the downstream and re-submit. |

## Internal (HTTP 5xx / 413)

These codes are emitted by limit checks and miscellaneous internal failures.

| Code | HTTP | Description | Recovery |
|------|------|-------------|----------|
| `CS_BUNDLE_TOO_LARGE` | 413 | The bundle bytes exceed the configured cap. | Trim the bundle or request a larger limit. |
| `CS_BODY_TOO_LARGE` | 413 | An invoke body exceeds the request-size cap. | Send a smaller request or chunk the upload. |
| `CS_RESULT_TOO_LARGE` | 413 | The function's response exceeds the response-size cap. | Return less; consider streaming to KVRocks or codeQ instead. |
| `CS_LOG_LIMIT_EXCEEDED` | 413 | An activation produced more log bytes than allowed. | Reduce logging; long-running work should sample. |
| `CS_SCHEDULER_STATE_WRITE_FAILED` | 500 | The scheduler could not persist its tick state. | Inspect KVRocks; the scheduler will retry on the next tick. |

## How a client should react

A robust caller switches on the code and chooses one of three actions: re-authenticate (the `CS_AUTHN_*` family), fix and retry (`CS_VALIDATION_*`, `CS_AUTHZ_*`, `CS_*_NOT_FOUND`, `CS_IDEMPOTENCY_CONFLICT`), or back off and retry (`CS_RATE_LIMITED`, `CS_*_UNAVAILABLE`, `CS_CODEQ_*`, `CS_CADENCE_*`). Anything in the 5xx range that is not in the back-off list should be surfaced to a human — those are the cases where the platform itself is misbehaving. See [Observability](Observability) for the matching metric labels.
