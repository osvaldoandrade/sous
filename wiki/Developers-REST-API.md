# Developers: REST API

Sous exposes two production HTTP surfaces. The control plane (`cs-control`)
hosts the lifecycle and management API — function CRUD, draft upload, version
publish, alias bookkeeping, schedules, Cadence worker bindings, codeQ
subscription bindings, tenant signing keys, tenant egress policy, activation
introspection, and audit replay. The synchronous invocation surface lives in
`cs-http-gateway`, a thin admission-controlled router that turns an authenticated
HTTPS request into a codeQ invocation envelope and waits for the correlated
result. Both binaries are written in Go, both speak JSON over HTTPS, both share
the same authentication and error-envelope contracts, and both expose
`/healthz`, `/readyz`, and `/metrics` for platform operators.

Every URL is versioned at `/v1` and namespaced by tenant. The path layout is
the same on both binaries: `/v1/tenants/{tenant}/...` for control-plane
resources and `/v1/web/{tenant}/{namespace}/{function}/{ref}` for synchronous
invokes. Resource names follow the regular expressions documented in
[Architecture](Architecture) and validated by `internal/api`:

| Token            | Pattern                                |
|------------------|----------------------------------------|
| `tenant`         | `t_[a-z0-9]{6,32}`                     |
| `namespace`      | `[a-z][a-z0-9_-]{2,63}`                |
| `function` / `name` | `[a-z][a-z0-9_-]{2,63}`             |
| `alias`          | `[a-z][a-z0-9_-]{1,31}`                |
| `version`        | positive int64                         |
| `activation_id`  | UUID v4                                |
| `draft_id`       | `drf_` + 32 hex characters             |

Every request carries `Authorization: Bearer <tikti-token>`. The Tikti
integration is the source of truth for tenant scoping (see
[IAM with Tikti](IAM-with-Tikti)); the gateway middleware validates the token,
extracts the principal, and rejects mismatches between the URL `{tenant}` and
the principal's tenant claim with `403 CS_AUTHZ_RESOURCE_MISMATCH`. Each
endpoint declares a single permission action (e.g. `cs:function:publish`); the
caller's role-set must include it or the response is
`403 CS_AUTHZ_DENIED`. Mutating endpoints emit a Tikti audit event after the
underlying KVRocks write commits — see [Observability](Observability) for the
durability and shape contract.

Two idempotency mechanisms apply. Lifecycle endpoints (`POST .../functions`,
publish) are intrinsically idempotent on their natural keys: re-posting an
identical `CreateFunctionRequest` returns the existing record with `200 OK`
rather than allocating a duplicate; re-publishing a draft that has already
been consumed returns `400 CS_VALIDATION_FAILED`. The synchronous invoke
surface honors an `Idempotency-Key` header: when present, the gateway derives
a deterministic activation ID from `(tenant, function, ref, key)` and replays
the cached terminal response on retry — see the gateway section below.

Errors share a single envelope across both binaries. Every non-2xx response
carries a JSON body of the form `{"error": {"code": "CS_*", "message": "...",
"request_id": "req_..."}}`. The `request_id` is the value of the
`X-Request-Id` header that the observability middleware stamps on every
incoming request; clients should echo it back when reporting incidents. The
canonical mapping from `CS_*` code to HTTP status is enumerated in
[Error Model](Error-Model). The detailed code catalogue lives at
[Reference: Error Codes](Reference-Error-Codes).

Rate limits apply at two layers. The HTTP gateway enforces a per-tenant
token-bucket on the invoke path (`/v1/web/...`) with the rate and burst
sourced from `cs_http_gateway.limits.tenant_rps`; exceeding the bucket
returns `429 CS_RATE_LIMITED` with a `Retry-After` header in whole seconds.
The control plane does not currently enforce a per-tenant request quota at
the HTTP layer — the natural backpressure is the KVRocks store — but
publish-bound bundle, log, and result sizes are bounded by
`internal/limits.Limits` and surface as `CS_BUNDLE_TOO_LARGE` /
`CS_BODY_TOO_LARGE` / `CS_LOG_LIMIT_EXCEEDED`. See
[Capacity and Limits](Capacity-and-Limits) for the configured ceilings.

Both binaries register Prometheus metrics under `/metrics` and a Tikti-free
liveness probe under `/healthz` / `/readyz`. `/healthz` always returns `200`;
`/readyz` returns `200` when the KVRocks store is reachable via `Ping` and a
`5xx` error envelope otherwise. The full observability layer is described in
[Observability](Observability).

The rest of this document is a reference, endpoint by endpoint, organised by
binary and by resource. Endpoints are documented in the form they appear in
`cmd/cs-control/main.go` and `cmd/cs-http-gateway/main.go`; the request and
response schemas link to [Reference: Schemas](Reference-Schemas) for the
canonical JSON-Schema definitions.

---

## 1. Control plane (`cs-control`)

The control-plane HTTP server listens on `cs_control.http.addr` (default
`:8080`). Routes are defined in `cmd/cs-control/main.go` and dispatched
through `go-chi`. The middleware stack is, in order:

1. `middleware.Recoverer` — panics become a `500 CS_RUNTIME_EXCEPTION`.
2. `observability.RequestIDMiddleware` — stamps `X-Request-Id`.
3. `authz.AuthnMiddleware(authnProvider)` — Tikti bearer validation
   (skipped for `/healthz`, `/readyz`, `/metrics`).

Each handler then calls `s.authorize(...)` to extract the principal, match
the URL tenant against the principal's tenant claim, and check the requested
action against the principal's role-set.

### 1.1 Health and metrics

```
GET /healthz
GET /readyz
GET /metrics
```

`GET /healthz` returns `{"status": "ok"}` once the process is past
`http.Serve`. `GET /readyz` calls `persistence.Provider.Ping(ctx)` and
returns `{"status": "ready"}` only when KVRocks acknowledges; on failure it
returns the underlying `CS_KVROCKS_UNAVAILABLE` envelope. `GET /metrics`
serves the Prometheus exposition format from `observability.MetricsHandler`.
None of these endpoints require authentication; deployments should restrict
them to the cluster network. See cited source `cmd/cs-control/main.go:100-104`.

### 1.2 Functions

A `FunctionRecord` is the durable identity of a function (tenant, namespace,
name, runtime, entry, handler, creation/deletion timestamps). The handler
helpers live in `cmd/cs-control/lifecycle.go`; the wire types are defined in
`internal/api/types.go`.

#### 1.2.1 Create function

```
POST /v1/tenants/{tenant}/namespaces/{namespace}/functions
```

Allocates a new function in the tenant namespace. The body is the
`CreateFunctionRequest` shape:

```json
{
  "name": "reconcile",
  "runtime": "cs-js",
  "entry": "function.js",
  "handler": "default"
}
```

Required action: `cs:function:create`. Tenant, namespace, and `name` are
validated against the patterns in section 0; invalid identifiers return
`400 CS_VALIDATION_NAME_INVALID`. `runtime`, `entry`, and `handler` default
to `cs-js`, `function.js`, and `default` respectively when omitted; accepted
runtimes are `cs-js`, `cs-python`, and `cs-wasm` (see
[Runtime cs-js](Runtime-cs-js) for the full list).

Idempotency: when a function with the same `(tenant, namespace, name,
runtime, entry, handler)` already exists, the response is `200 OK` with the
existing record; when the existing record's runtime, entry, or handler
differ, the response is `409 CS_IDEMPOTENCY_CONFLICT`. Re-creating a
soft-deleted function is rejected with `409 CS_IDEMPOTENCY_CONFLICT`. The
contract is enforced in `createFunctionIdempotent` in
`cmd/cs-control/lifecycle.go`.

Response `201 Created` (new) or `200 OK` (idempotent hit):

```json
{
  "tenant": "t_abc123",
  "namespace": "payments",
  "name": "reconcile",
  "runtime": "cs-js",
  "entry": "function.js",
  "handler": "default",
  "created_at_ms": 1731600000000,
  "deleted_at_ms": null
}
```

#### 1.2.2 Read function

```
GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}
```

Returns the `FunctionRecord`, its aliases, and the latest published
`VersionRecord` (or `null` when no version has been published).

Required action: `cs:function:read`.

Query parameters:

| Param              | Type    | Default | Effect                                  |
|--------------------|---------|---------|-----------------------------------------|
| `include_deleted`  | boolean | `false` | When `true`, returns the record even if soft-deleted; otherwise responds `400 CS_VALIDATION_FAILED` for soft-deleted functions. |

Response `200`:

```json
{
  "function": { "tenant": "...", "namespace": "...", "name": "...", "runtime": "...", "entry": "...", "handler": "...", "created_at_ms": 0, "deleted_at_ms": null },
  "aliases":  [ { "alias": "prod", "version": 17, "updated_at_ms": 0 } ],
  "latest_version": { "version": 17, "sha256": "...", "config": { }, "published_at_ms": 0, "signature": null }
}
```

The `latest_version` field is `null` when no version exists. See
[Concepts: Function Lifecycle](Concepts-Function-Lifecycle) for state
transitions.

#### 1.2.3 Delete function

```
DELETE /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}
```

Soft-deletes the function record: `deleted_at_ms` is stamped, versions and
aliases are retained for audit until the activation TTL elapses. The
function name cannot be re-created while the soft-deleted record is present
(see Create idempotency rules above). Required action:
`cs:function:delete`.

Response `200`:

```json
{ "status": "deleted" }
```

### 1.3 Draft upload

```
PUT /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/draft
```

Uploads the candidate bundle for a future version. The body is the
`UploadDraftRequest` shape; `files` is a map of relative path to
base64-encoded bytes. The control plane decodes, builds the canonical bundle
(sorted entries, tar layout), and computes the SHA-256 digest. The bundle
must include `manifest.json` (parsed at publish time, not here) and the
runtime-specific entry file. Required action: `cs:function:draft:upload`.

Request body:

```json
{
  "files": {
    "function.js":    "BASE64",
    "manifest.json":  "BASE64"
  }
}
```

Limits:

- Decoded bundle size must be `<= cs_control.limits.max_bundle_bytes`
  (default 16 MiB). Larger bundles return `400 CS_BUNDLE_TOO_LARGE`.
- TTL: drafts expire `cs_control.limits.draft_ttl_seconds` after creation
  (default 24 h). Expired drafts cannot be published; the publish handler
  rejects them with `400 CS_VALIDATION_FAILED`.

Response `200`:

```json
{
  "draft_id":      "drf_01h7c...",
  "sha256":        "9f...c1",
  "size_bytes":    1234,
  "expires_at_ms": 1731686400000
}
```

The returned `draft_id` is the identifier the publish endpoint consumes.
Multiple concurrent uploads for the same function return distinct
`draft_id`s, each with its own TTL window.

### 1.4 Publish version

```
POST /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions
```

Promotes a draft to an immutable version. The control plane re-derives the
canonical SHA, verifies it against the draft record (defence against
in-flight tampering), parses the manifest, resolves declared imports into a
frozen `deps/` subtree, builds the canonical tar bundle, allocates a
monotonic `version` integer, persists the version record plus the CycloneDX
1.5 SBOM, and (optionally) attaches the alias supplied in the request body.

Required action: `cs:function:publish`.

Request body (`PublishVersionRequest`):

```json
{
  "draft_id": "drf_01h7c...",
  "alias":    "prod",
  "config": {
    "timeout_ms":      3000,
    "memory_mb":       64,
    "max_concurrency": 1,
    "env":             { "LOG_LEVEL": "info" },
    "capabilities": {
      "kv":    { "prefixes": ["ctr:"], "ops": ["get", "set"] },
      "codeq": { "publish_topics": ["jobs.*"] },
      "http":  { "allow_hosts": ["api.example.com"], "timeout_ms": 1500 }
    },
    "authz": {
      "invoke_http_roles":     ["role:app"],
      "invoke_schedule_roles": ["role:worker"],
      "invoke_cadence_roles":  ["role:cadence"]
    },
    "secrets": ["DB_PASSWORD=vault/payments/db#json-field:value"]
  }
}
```

Omitted `config.timeout_ms`, `memory_mb`, and `max_concurrency` inherit
from the manifest `limits` block. Omitted `config.capabilities` inherits
from the manifest `capabilities` block. The legal ranges enforced by
`validatePublishConfig`:

| Field             | Min | Max     |
|-------------------|-----|---------|
| `timeout_ms`      | 1   | 900000  |
| `memory_mb`       | 16  | 4096    |
| `max_concurrency` | 1   | 100     |

Out-of-range values return `400 CS_VALIDATION_FAILED`.

Optional request header `X-CS-Signature`: a base64-encoded Ed25519
signature over the canonical payload
`signing.CanonicalPayloadFromHexSHA(sha, tenant, namespace, function, 0)`.
When `plugins.signing.required` is `true`, the header is mandatory; when
it is `false`, the header is optional but, if supplied, must verify. See
[Security](Security) for the trust model and section 1.10 below for the
key-management endpoints.

Signature outcomes:

| Condition                                            | Status | Code                          |
|------------------------------------------------------|--------|-------------------------------|
| `required=true`, header absent                       | 400    | `CS_SIGNATURE_MISSING`        |
| Malformed base64 / wrong length                      | 400    | `CS_SIGNATURE_INVALID`        |
| Tenant has not rotated yet                           | 404    | `CS_SIGNATURE_KEY_NOT_FOUND`  |
| Signature does not verify                            | 400    | `CS_SIGNATURE_INVALID`        |

When the manifest declares `cadence.kind == "workflow"`, the control plane
runs the static determinism linter from `internal/cadence/determinism`. Any
banned API call (`Date.now`, `Math.random`, `setTimeout`, bare `fetch`,
etc.) fails the publish with `422 CS_WORKFLOW_NON_DETERMINISTIC`. The
response body extends the standard error envelope with a `violations[]`
array describing each call site so the publishing agent can render
actionable diagnostics:

```json
{
  "error": {
    "code":       "CS_WORKFLOW_NON_DETERMINISTIC",
    "message":    "workflow function contains 1 nondeterministic call(s); see violations[]",
    "request_id": "req_01h7c...",
    "violations": [
      { "file": "function.js", "line": 12, "col": 5, "api": "Date.now" }
    ]
  }
}
```

Response `201`:

```json
{
  "version":         17,
  "sha256":          "9f...c1",
  "published_at_ms": 1731600000000
}
```

On success the publish handler also:

1. Persists the CycloneDX 1.5 SBOM for the new version (see section 1.5).
2. Marks the draft consumed; a second publish call with the same
   `draft_id` is rejected with `400 CS_VALIDATION_FAILED` ("draft already
   consumed").
3. Stamps the optional alias in the request body to point at the new
   version (when supplied).

### 1.5 SBOM

```
GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions/{version}/sbom
```

Returns the persisted CycloneDX 1.5 Software Bill of Materials for the
named version. The handler lives in `cmd/cs-control/sbom.go`. Required
action: `cs:function:read` (the same as version metadata read).

Response `200`:

- Header: `Content-Type: application/vnd.cyclonedx+json; version=1.5`
- Body: the canonical CycloneDX 1.5 JSON document, byte-identical across
  re-publishes of the same canonical bundle. The document lists the
  runtime, every bundled file with its SHA-256, every resolved import
  with its SRI hash, and exposes the bundle digest and signing identity
  (when present) as `cs:bundle.sha256` and `cs:signing.*` properties.

Errors:

| Status | Code                     | When                                              |
|--------|--------------------------|---------------------------------------------------|
| 400    | `CS_VALIDATION_FAILED`   | `version` is non-numeric or `<= 0`                |
| 400    | `CS_VALIDATION_FAILED`   | Version exists but no SBOM persisted (pre-E5.03)  |
| 403    | `CS_AUTHZ_DENIED`        | Principal lacks `cs:function:read`                |

Versions published before SBOM persistence shipped must be re-published to
backfill their SBOM. See [Security](Security) for the supply-chain trust
model.

### 1.6 Aliases

A function alias is a tenant-scoped pointer from a stable label (e.g.
`prod`, `canary`) to a numeric version. Aliases are how the synchronous
invoke surface refers to versions without callers pinning to a specific
integer.

#### 1.6.1 Set alias

```
PUT /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/aliases/{alias}
```

Points `{alias}` at a published version. The control plane verifies that
the target version exists; missing versions return `400
CS_VALIDATION_FAILED`. Required action: `cs:function:alias:set`.

Request body:

```json
{ "version": 17 }
```

Response `200`:

```json
{ "alias": "prod", "version": 17, "updated_at_ms": 1731600000000 }
```

Alias mutations are single-key writes; concurrent readers see either the
previous or the new value, never a torn state. There is no DELETE
endpoint for aliases in the current control plane — re-point or leave
stale; the wire surface is intentionally narrow until a use case emerges.

#### 1.6.2 List aliases

```
GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/aliases
```

Required action: `cs:function:read`.

Response `200`:

```json
{
  "aliases": [
    { "alias": "prod",   "version": 17, "updated_at_ms": 1731600000000 },
    { "alias": "canary", "version": 18, "updated_at_ms": 1731603600000 }
  ]
}
```

### 1.7 Invoke (control-plane API)

```
POST /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}:invoke
```

Direct invocation via the control plane. This path is intended for
operators and administrative tooling — the production application flow is
the gateway invoke described in section 2.1. Required action:
`cs:function:invoke:api`.

Request body (`InvokeAPIRequest`):

```json
{
  "ref":   { "alias": "prod" },
  "mode":  "sync",
  "event": { "x": 1 }
}
```

`ref.version` is also accepted; when both are set, `version` wins (the
resolver matches the gateway). `mode` is `sync` (wait for the result, the
default) or `async` (return immediately with the activation ID).

Response `200` (sync, function returned successfully):

```json
{
  "activation_id": "8c3f...",
  "status":        "success",
  "result":        { "statusCode": 200, "headers": {}, "body": "ok" },
  "duration_ms":   12,
  "error":         null
}
```

Response `202` (async):

```json
{ "activation_id": "8c3f...", "status": "queued" }
```

The sync path is bounded by the function's `timeout_ms` plus 250 ms
correlation slack; exceeding it returns `408 CS_CODEQ_CORRELATION_TIMEOUT`.

### 1.8 Activations

The control plane exposes the durable activation record produced by the
invoker pool. Records are tenant-scoped; the `{namespace}` and function
name are not in the path because activation IDs are globally unique within
a tenant. Required action: `cs:activation:read` for all three endpoints.

#### 1.8.1 Get activation

```
GET /v1/tenants/{tenant}/activations/{activation_id}
```

Returns the `ActivationRecord`. Response shape:

```json
{
  "activation_id":     "8c3f...",
  "tenant":            "t_abc123",
  "namespace":         "payments",
  "function":          "reconcile",
  "ref":               { "function": "reconcile", "alias": "prod", "version": 17 },
  "trigger":           { "type": "http", "source": { "path": "/v1/web/..." } },
  "status":            "success",
  "start_ms":          1731600000000,
  "end_ms":            1731600000012,
  "duration_ms":       12,
  "result_truncated":  false,
  "result":            { "statusCode": 200, "headers": {}, "body": "ok" },
  "error":             null,
  "request_id":        "req_01h7c...",
  "resolved_version":  17,
  "parent_activation_id": "",
  "root_activation_id":   "8c3f...",
  "sampling_decision":    ""
}
```

When the persisted `result` body was truncated (`>
cs_invoker_pool.limits.max_result_bytes`, default 256 KiB), the response
sets `result_truncated: true` and stamps the header `X-CS-Truncated:
result`. When the activation record has aged past
`cs_control.limits.activation_ttl_seconds` (default 7 days), the response
is `410 CS_ACTIVATION_TTL_EXPIRED` rather than `404` — the platform
retains a small tombstone marker that distinguishes "expired" from "never
existed" for a generous window after expiry.

#### 1.8.2 Activation logs

```
GET /v1/tenants/{tenant}/activations/{activation_id}/logs
```

Returns the log chunks the runtime wrote for the activation. The response
shape switches between a JSON envelope (default), newline-delimited JSON
(`Accept: application/x-ndjson` or `?format=ndjson`), and Server-Sent
Events (`Accept: text/event-stream` or `?format=sse`). The mode is
negotiated in `negotiateLogStream` in `cmd/cs-control/main.go`.

Query parameters:

| Param      | Type    | Default | Notes                                                             |
|------------|---------|---------|-------------------------------------------------------------------|
| `cursor`   | string  | (start) | Opaque pagination cursor returned as `next_cursor` previously.    |
| `limit`    | int     | 100     | Max 500; out-of-range values fall back to 100.                    |
| `format`   | string  | json    | One of `ndjson`, `sse`. Wins over the `Accept` header.            |

Default JSON response (`200`):

```json
{
  "chunks":      ["[info] hello", "[info] world"],
  "cursor":      "2",
  "next_cursor": "2",
  "truncated":   false
}
```

`next_cursor` is the value to pass back as `cursor` on the next request.
When the server returns fewer chunks than `limit`, `next_cursor` is empty
to signal end-of-stream; the legacy `cursor` field is preserved for
backward-compatibility with existing clients.

Streaming variants:

- **NDJSON** (`Content-Type: application/x-ndjson`): one JSON object per
  line; the trailing object is
  `{"eof": true, "next_cursor": "...", "truncated": ...}`.
- **SSE** (`Content-Type: text/event-stream`): each chunk is an
  `event: log` frame; the final frame is an `event: eof` carrying the
  same cursor/truncation payload as the NDJSON trailer.

When the cumulative log bytes for the activation reach
`cs_invoker_pool.limits.max_log_bytes` (default 1 MiB), the response sets
the header `X-CS-Truncated: logs` and the chunk list contains a trailing
sentinel `{"truncated": true, "reason": "log_limit_exceeded",
"limit_bytes": 1048576}`. Subsequent log writes for that activation are
dropped to keep the cap strict.

#### 1.8.3 Activation tree

```
GET /v1/tenants/{tenant}/activations/{activation_id}/tree
```

Returns the bounded BFS walk of the activation's child tree, used by the
CLI and dashboards to render agent decision trees. The traversal caps
out at `activation_tree_max_depth = 10` and
`activation_tree_max_nodes = 1000`; reaching either cap sets `truncated:
true`. Cycles are deduplicated by activation ID.

Response `200`:

```json
{
  "activation_id": "8c3f...",
  "tree": {
    "activation_id":        "8c3f...",
    "parent_activation_id": "",
    "root_activation_id":   "8c3f...",
    "function":             "reconcile",
    "version":              17,
    "trigger_type":         "http",
    "status":               "success",
    "start_ms":             1731600000000,
    "duration_ms":          12,
    "children":             [ ]
  },
  "truncated": false,
  "max_depth": 10,
  "max_nodes": 1000
}
```

Activations that have aged out of the tree but whose children still
exist appear as placeholder nodes with empty function/status so callers
can still see the shape of the partial tree.

### 1.9 Schedules

Schedules let an operator drive a function on an interval or a cron
expression. The poller process is `cs-scheduler`; see [Scheduler](Scheduler).

#### 1.9.1 Create schedule

```
POST /v1/tenants/{tenant}/namespaces/{namespace}/schedules
```

Required action: `cs:schedule:create`.

Request body (`CreateScheduleRequest`):

```json
{
  "name":           "reconcile_30s",
  "every_seconds":  30,
  "overlap_policy": "skip",
  "ref":            { "function": "reconcile", "alias": "prod" },
  "payload":        { "source": "schedule" },
  "kind":           "interval"
}
```

Cron form:

```json
{
  "name":           "nightly",
  "cron":           "0 3 * * *",
  "tz":             "America/Sao_Paulo",
  "jitter_ms":      5000,
  "overlap_policy": "skip",
  "ref":            { "function": "reconcile", "alias": "prod" }
}
```

Validation (enforced by `validateScheduleRequest`):

- `name` length is 3..64.
- `kind` is one of `interval` (default when `every_seconds` is set),
  `cron` (default when `cron` is set). `kind` and `cron`/`every_seconds`
  must agree; mixing `cron` with `every_seconds` returns
  `400 CS_VALIDATION_FAILED`.
- `every_seconds` (when interval) must be `1..86400`.
- `cron` (when cron) must parse with `internal/scheduler.ValidateCron`.
- `tz` must resolve via `time.LoadLocation` (default `UTC`).
- `jitter_ms` is `0..3_600_000`.
- `overlap_policy` is one of `skip` (default), `queue`, `parallel`.
- `ref.function` is the function name within the namespace; `ref.alias`
  is optional and validated against the alias pattern.

Response `201`: the persisted `ScheduleRecord` (the request shape plus
`tenant`, `namespace`, `enabled`, and `created_at_ms`).

#### 1.9.2 Delete schedule

```
DELETE /v1/tenants/{tenant}/namespaces/{namespace}/schedules/{name}
```

Required action: `cs:schedule:delete`.

Response `200`:

```json
{ "status": "deleted" }
```

There is no `GET /schedules` listing endpoint in the current surface; the
scheduler reads bindings directly from KVRocks on a refresh tick. Operator
dashboards that need a listing should query KVRocks via the platform tools
described in [Storage: KVRocks](Storage-KVRocks).

### 1.10 Cadence worker bindings

A `WorkerBinding` registers a Cadence domain/tasklist pair with the
`cs-cadence-poller`. Each binding maps Cadence activity types (or workflow
types when `kind == "workflow"`) to a function reference. See
[Cadence Integration](Cadence-Integration).

#### 1.10.1 Create worker binding

```
POST /v1/tenants/{tenant}/namespaces/{namespace}/cadence/workers
```

Required action: `cs:cadence:worker:create`.

Request body (`CreateWorkerBindingRequest`):

```json
{
  "name":         "payments-activities",
  "domain":       "payments",
  "tasklist":     "payments-activities",
  "worker_id":    "cs-payments-01",
  "activity_map": {
    "SousInvokeActivity": { "function": "reconcile", "alias": "prod" }
  },
  "pollers":      { "activity": 8 },
  "limits":       { "max_inflight_tasks": 256 },
  "input_codec":  "json",
  "output_codec": "json"
}
```

Validation (enforced by `validateWorkerBindingRequest`):

- `name` length is 3..64.
- `domain` is non-empty and `<= 128` chars.
- `tasklist` is non-empty and `<= 128` chars.
- `worker_id` is non-empty and `<= 128` chars.
- `activity_map` has at least one entry; each entry's `function` is
  non-empty and `<= 64` chars; each entry's `alias` is validated.
- `pollers.activity` is `0..256` (defaults to 1 when zero).
- `limits.max_inflight_tasks` is `0..100000` (defaults to
  `cs_cadence_poller.limits.max_inflight_tasks_default` when zero).
- `input_codec` / `output_codec` (when set) must be recognised by the
  codec registry; unknown values return
  `400 CS_VALIDATION_UNSUPPORTED_CODEC`.

Response `201`: the persisted `WorkerBinding`.

#### 1.10.2 Delete worker binding

```
DELETE /v1/tenants/{tenant}/namespaces/{namespace}/cadence/workers/{name}
```

Required action: `cs:cadence:worker:delete`.

Response `200`:

```json
{ "status": "deleted" }
```

There is no GET on worker bindings at the HTTP layer; the poller reads
them directly from KVRocks. Dashboards that need an inventory should
introspect storage directly.

### 1.11 codeQ subscription bindings

Subscription bindings turn a codeQ topic into a function trigger. The
in-process subscription worker in `cs-control` reconciles the binding map
on a refresh interval and spawns per-binding goroutines that long-poll the
topic and invoke the bound function once per matching envelope. See
[codeQ Protocol](codeQ-Protocol).

#### 1.11.1 Create subscription

```
POST /v1/tenants/{tenant}/namespaces/{namespace}/subscriptions
```

Required action: `cs:subscription:create`.

Request body (`CreateSubscriptionRequest`):

```json
{
  "name":            "order-placed-fanout",
  "topic":           "orders.placed",
  "filter":          "body.kind != \"ignored\"",
  "function_ref":    { "function": "fanout", "alias": "prod" },
  "mode":            "unordered",
  "group_id":        "cs-sub-t_abc123-payments-order-placed-fanout",
  "max_concurrency": 8
}
```

Validation:

- `name` length is 3..64.
- `topic` is non-empty and `<= 256` chars.
- `function_ref.function` is non-empty.
- `mode` is one of `ordered` (single goroutine per partition) or
  `unordered` (pool of `max_concurrency` workers). Defaults to
  `ordered`.
- `filter` is an expression parsed by `internal/codeq.ParseFilter`;
  an invalid filter returns `400 CS_VALIDATION_FAILED`.
- `max_concurrency` is bounded by the per-binding cap
  (`subscriptionMaxConcurrencyCap = 64`). Defaults to
  `cs_control.subscriptions.worker_pool_default` (4 when unset).
- `group_id` defaults to a derived value when empty.

Response `201`: the persisted `SubscriptionBinding`.

#### 1.11.2 List subscriptions

```
GET /v1/tenants/{tenant}/namespaces/{namespace}/subscriptions
```

Required action: `cs:subscription:read`.

Response `200`:

```json
{ "subscriptions": [ /* SubscriptionBinding[] */ ] }
```

The array is empty when the namespace has no bindings.

#### 1.11.3 Read subscription

```
GET /v1/tenants/{tenant}/namespaces/{namespace}/subscriptions/{name}
```

Required action: `cs:subscription:read`. Returns the persisted
`SubscriptionBinding`. Missing bindings return `400 CS_VALIDATION_FAILED`.

#### 1.11.4 Delete subscription

```
DELETE /v1/tenants/{tenant}/namespaces/{namespace}/subscriptions/{name}
```

Required action: `cs:subscription:delete`.

Response `200`:

```json
{ "status": "deleted" }
```

### 1.12 Egress policy

The per-tenant egress allowlist constrains runtime HTTP fetches. The shape
mirrors `internal/runtime/egress.Policy`; the wire definition lives in
`internal/api/egress.go`. See [Security](Security) for semantics.

#### 1.12.1 Get egress policy

```
GET /v1/tenants/{tenant}/egress-policy
```

Required action: `cs:egress:policy:read`.

Response `200` (configured tenant):

```json
{
  "allowed_hosts": ["api.partner.com", "*.example.com"],
  "allowed_cidrs": ["203.0.113.0/24"],
  "denied_hosts":  ["abuse.example.com"],
  "default_deny":  true,
  "updated_at_ms": 1731600000000
}
```

Response `200` (tenant with no installed policy): the implicit
default-deny stub `{"default_deny": true}` — operators can render "no
policy installed" without branching on `404`.

#### 1.12.2 Put egress policy

```
PUT /v1/tenants/{tenant}/egress-policy
```

Required action: `cs:egress:policy:write`.

Request body: the same shape as the GET response. Clients must omit
`updated_at_ms`; the server stamps it. Validation rules (enforced by
`internal/runtime/egress.Validate`):

- Each `allowed_hosts` / `denied_hosts` entry is either a hostname or a
  leading-`*.` wildcard; mid-label wildcards (`foo.*.example.com`) and
  bare `*` are rejected.
- Each `allowed_cidrs` entry parses with `net.ParseCIDR` or as a bare
  IP literal.
- A host cannot appear in both `allowed_hosts` and `denied_hosts`.
- The private-IP block from the security model still applies at invoke
  time; CIDRs overlapping the blocked ranges parse successfully here
  but are rejected by the runtime when used.

Response `200`: the persisted policy, including the server-stamped
`updated_at_ms`.

### 1.13 Signing keys

Tenant-scoped Ed25519 keys verify the `X-CS-Signature` header at publish
time. The handlers live in `cmd/cs-control/signing_keys.go`. Only the
public half is retained by the control plane; the private bytes are
returned exactly once on rotate.

#### 1.13.1 Rotate signing key

```
POST /v1/tenants/{tenant}/signing-keys/rotate
```

Required action: `cs:tenant:signing-key:rotate`.

The handler generates a fresh Ed25519 keypair, persists the public half
under `cs:tenant:{tenant}:signing:ed25519:active`, and returns the private
half once.

Response `200`:

```json
{
  "kid":           "kid_a1b2c3d4e5f6",
  "algorithm":     "ed25519",
  "public_key":    "<base64 32 bytes>",
  "private_key":   "<base64 64 bytes>",
  "created_at_ms": 1731600000000
}
```

The caller MUST persist `private_key` before discarding the response;
the control plane never returns the private bytes again.

#### 1.13.2 Get active signing key

```
GET /v1/tenants/{tenant}/signing-keys/active
```

Required action: `cs:tenant:signing-key:read`.

Response `200`:

```json
{
  "kid":           "kid_a1b2c3d4e5f6",
  "algorithm":     "ed25519",
  "public_key":    "<base64 32 bytes>",
  "created_at_ms": 1731600000000
}
```

Response `404 CS_SIGNATURE_KEY_NOT_FOUND` when the tenant has never
rotated.

### 1.14 Audit history

```
GET /v1/tenants/{tenant}/audit
```

Returns the per-tenant audit ring buffer maintained by
`internal/audit.Recorder`. Required action: `cs:audit:read`.

Query parameters:

| Param    | Type    | Default | Notes                                                |
|----------|---------|---------|------------------------------------------------------|
| `since`  | int64   | 0       | UNIX millis; events with `ts_ms <= since` are skipped. |
| `actor`  | string  | (any)   | Filter by principal `sub`.                           |
| `action` | string  | (any)   | Filter by event `action` (e.g. `function.publish`).  |
| `limit`  | int     | 100     | Max 500.                                             |

Response `200`:

```json
{
  "events": [
    {
      "ts_ms":      1731600000000,
      "tenant":     "t_abc123",
      "actor":      "u_alice",
      "action":     "function.publish",
      "resource":   "fn://t_abc123/payments/reconcile@v17",
      "outcome":    "success",
      "request_id": "req_01h7c...",
      "detail":     { "sha256": "...", "alias": "prod" }
    }
  ]
}
```

See [ledgerDB Audit](ledgerDB-Audit) for the persistence backend and
durability story.

---

## 2. HTTP gateway (`cs-http-gateway`)

The gateway is a single-route, admission-controlled invocation surface. It
listens on `cs_http_gateway.http.addr` (configurable) and exists so that
the control plane never sits on the hot path of customer requests. The
middleware stack:

1. `middleware.Recoverer`.
2. `observability.RequestIDMiddleware`.
3. `rateLimitMiddleware(newRateLimiter(...))` — per-tenant token bucket.
4. `authz.AuthnMiddleware(authnProvider)` — Tikti bearer validation.
5. `idempotencyMiddleware(s.idemStore, 0)` — `Idempotency-Key`
   deduplication.

### 2.1 Synchronous invoke

```
POST /v1/web/{tenant}/{namespace}/{function}/{ref}
POST /v1/web/{tenant}/{namespace}/{function}/{ref}/*
```

Triggers a synchronous invocation and returns the function's response.
The trailing path catch-all is preserved verbatim in the `event.rawPath`
field so handlers can implement path-based routing without a separate
router. Required action: `cs:function:invoke:http`. The version's
`config.authz.invoke_http_roles` must intersect with the principal's
role-set or the response is `403 CS_AUTHZ_DENIED`.

Path parameters:

| Param        | Notes                                                     |
|--------------|-----------------------------------------------------------|
| `tenant`     | Must match the principal's tenant claim.                  |
| `namespace`  | Validated against the namespace pattern.                  |
| `function`   | Validated against the function pattern.                   |
| `ref`        | Either an alias (matches the alias pattern) or a positive integer version. Integers win when both parse. |

Request body: arbitrary bytes (up to
`cs_http_gateway.limits.max_body_bytes`, default 6 MiB). The body is
base64-encoded into the event envelope as `event.body`, with
`event.isBase64Encoded == true`.

Request headers honored by the gateway:

| Header                    | Effect                                                              |
|---------------------------|---------------------------------------------------------------------|
| `Authorization`           | Bearer token; required.                                             |
| `Idempotency-Key`         | `[A-Za-z0-9_-]{8,128}`; opts the request into dedup (see 2.2).       |
| `traceparent`             | Propagated to the function as `event.requestContext.http.traceparent` and `trigger.source.traceparent`. |
| `X-CS-Parent-Activation`  | Set by the runtime egress shim on internal function-to-function calls; recorded as `trigger.source.parent_activation_id` so the activation tree wire correctly. |

Limit checks (defaults from `internal/limits`):

| Check                | Limit                                | Code                       |
|----------------------|--------------------------------------|----------------------------|
| Query string length  | `cs_http_gateway.limits.max_query_bytes` (default 16 KiB) | `CS_VALIDATION_FAILED`     |
| Headers size         | `cs_http_gateway.limits.max_header_bytes` (default 64 KiB) | `CS_VALIDATION_FAILED`     |
| Body size            | `cs_http_gateway.limits.max_body_bytes` (default 6 MiB)   | `CS_BODY_TOO_LARGE`        |
| Per-tenant RPS       | `cs_http_gateway.limits.tenant_rps`                       | `CS_RATE_LIMITED` + `Retry-After` header |

The gateway resolves the version (`ResolveVersion(ref)`), reads the
version metadata to obtain the timeout and authz roles, publishes an
`InvocationRequest` envelope onto codeQ, and waits up to `timeout_ms +
250 ms` for the correlated `InvocationResult` keyed by the synthetic
`request_id`. The activation ID is deterministic when an
`Idempotency-Key` is supplied (UUID v5 over `tenant:function:ref:key`)
and a fresh UUID v4 otherwise.

Response: the function's `FunctionResponse` rendered as HTTP. The
gateway copies `result.headers` onto the response, writes
`result.statusCode`, and decodes `result.body` (base64 when
`isBase64Encoded == true`, raw bytes otherwise). Functions are expected
to follow the AWS Lambda HTTPv2 contract:

```json
{
  "statusCode":      200,
  "headers":         { "content-type": "application/json" },
  "body":            "{\"ok\": true}",
  "isBase64Encoded": false
}
```

Error responses surface as the `CS_*` envelope at the gateway boundary:

| Status | Code                           | When                                                        |
|--------|--------------------------------|-------------------------------------------------------------|
| 400    | `CS_VALIDATION_FAILED`         | Query/header oversize, bad `ref`.                           |
| 401    | `CS_AUTHN_INVALID_TOKEN`       | Bearer missing/invalid/expired.                             |
| 403    | `CS_AUTHZ_DENIED`              | Missing `cs:function:invoke:http` or role intersection.     |
| 403    | `CS_AUTHZ_RESOURCE_MISMATCH`   | Principal tenant != URL tenant.                             |
| 408    | `CS_CODEQ_CORRELATION_TIMEOUT` | Result not received within timeout + 250 ms.                |
| 409    | `CS_IDEMPOTENCY_CONFLICT`      | Key reused with a different body fingerprint.               |
| 413    | `CS_BODY_TOO_LARGE`            | Body exceeds `max_body_bytes`.                              |
| 429    | `CS_RATE_LIMITED`              | Tenant token bucket empty; carries `Retry-After`.           |
| 500    | `CS_RUNTIME_EXCEPTION`         | Function returned an error envelope or no result body.      |

See [HTTP Invoke Path](HTTP-Invoke-Path) for the end-to-end flow.

### 2.2 Idempotency contract

When the request carries `Idempotency-Key`, the middleware:

1. Validates the key against `^[A-Za-z0-9_-]{8,128}$`. Bad keys return
   `400 CS_VALIDATION_FAILED`.
2. Computes a SHA-256 fingerprint of the request body.
3. Reserves the dedup slot keyed by `idem:{tenant}:{namespace}:{function}:{ref}:{key}`
   in the in-memory store (production deployments back this with KVRocks).
4. On a cache hit with the same fingerprint: replays the terminal
   response verbatim and stamps `X-CS-Idempotency-Replay: 1`.
5. On a cache hit with a different fingerprint: returns
   `409 CS_IDEMPOTENCY_CONFLICT` without re-executing.
6. On a miss: forwards the request, captures the terminal status,
   headers, and body, and commits them on success (status `< 500`).

Reservations expire after `defaultIdempotencyTTL` (1 h) unless the
function timeout dictates a longer window. Per-request correlation
headers (`X-Request-Id`, `X-CS-Idempotency-Replay`) are stripped from
the cached headers before replay.

### 2.3 Health and metrics

```
GET /healthz
GET /readyz
GET /metrics
```

Same contract as the control plane: `/healthz` always returns
`200 {"status": "ok"}`; `/readyz` returns `200 {"status": "ready"}` only
when KVRocks is reachable; `/metrics` serves the Prometheus exposition
format. None of these endpoints require authentication.

---

## 3. Common contracts

### 3.1 Error envelope

Every non-2xx response across both binaries shares this structure:

```json
{
  "error": {
    "code":       "CS_AUTHZ_DENIED",
    "message":    "role missing: cs:function:publish",
    "request_id": "req_01h7c..."
  }
}
```

A few endpoints extend the envelope with structured fields when the code
is well-known. The current extensions are:

- `CS_WORKFLOW_NON_DETERMINISTIC` adds `error.violations[]` (see 1.4).

The canonical mapping from `CS_*` code to HTTP status lives in
`internal/errors/errors.go::StatusCode`; the full catalogue is at
[Reference: Error Codes](Reference-Error-Codes).

### 3.2 Pagination model

Only the activation-logs and audit-history endpoints currently paginate.
Both use opaque cursors:

- `cs_control` logs: `cursor` / `next_cursor` strings; `limit` query
  parameter with default 100 and ceiling 500.
- `cs_control` audit: `since` (UNIX millis), `actor`, `action`, and
  `limit` (default 100, ceiling 500). When more events exist than
  `limit`, callers should advance `since` to the last event's `ts_ms`.

There is no global cursor convention for list endpoints — schedules,
worker bindings, and subscription bindings either return the full set
(subscriptions) or are not listed via HTTP (schedules, worker bindings;
operators introspect KVRocks directly).

### 3.3 Idempotency headers

Two distinct mechanisms apply, both keyed by a request header.

`X-Sous-Request-Id` is the legacy lifecycle idempotency knob. The control
plane does not currently consume it (lifecycle endpoints are natively
idempotent on their identity tuples); future revisions may add it for
publish replay safety. Clients should still send a `request_id` for
observability and tracing.

`Idempotency-Key` is the gateway invoke dedup knob (section 2.2). It is
purely an invocation-time contract and is not consumed by the control
plane.

### 3.4 Versioning policy

The `/v1` prefix is the major version. The Sous platform treats `/v1` as
an additive contract: new fields may be appended to request and response
bodies; new endpoints may be added under the same prefix; new error codes
may appear; existing fields and codes will not be repurposed.
Backward-incompatible changes ship under a new prefix (`/v2`) with a
deprecation window for `/v1`. The migration playbook for operators is
[Operators: Migrations](Operators-Migrations). See [Roadmap](Roadmap)
for upcoming endpoint additions.

Schema-level evolution (manifest, invoke envelope, result envelope) is
governed by the JSON Schema documents at
[Reference: Schemas](Reference-Schemas) — every wire schema in this
document links to a canonical `$id`.

### 3.5 Related references

The endpoints documented here are consumed and produced by:

- [CLI](Developers-CLI) — the `cs` binary, the reference REST client.
- [Concepts: Function Lifecycle](Concepts-Function-Lifecycle) — the
  state transitions that lifecycle endpoints implement.
- [Concepts: Invocations and Activations](Concepts-Invocations-and-Activations)
  — what happens after the gateway accepts an invoke.
- [HTTP Invoke Path](HTTP-Invoke-Path) — sequence diagrams for the
  end-to-end synchronous path.
- [Scheduler](Scheduler), [Cadence Integration](Cadence-Integration),
  and [codeQ Protocol](codeQ-Protocol) — the trigger surfaces whose
  bindings live behind the control-plane endpoints in 1.9–1.11.
- [Security](Security) and [Security Checklist](Security-Checklist) —
  the signature and egress contracts at 1.12–1.13.
- [Observability](Observability) — the request-ID, audit, and metrics
  contracts.
- [Capacity and Limits](Capacity-and-Limits) — the numeric ceilings
  applied at every endpoint.
- [Error Model](Error-Model) — the typed error catalogue.
