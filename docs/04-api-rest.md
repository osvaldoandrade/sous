# REST API

The REST API uses JSON over HTTPS.

The API uses a single versioned prefix: `/v1`.

## Authentication

Clients send:

- `Authorization: Bearer <tikti_token>`

The gateway validates tokens with Tikti and forwards:

- `X-Tikti-Subject`
- `X-Tikti-Tenant`
- `X-Tikti-Roles` (comma-separated)

The control plane re-validates authorization for every request.

## Resource naming

- `tenant`: `t_[a-z0-9]{6,32}`
- `namespace`: `[a-z][a-z0-9_-]{2,63}`
- `function`: `[a-z][a-z0-9_-]{2,63}`
- `alias`: `[a-z][a-z0-9_-]{1,31}`
- `version`: positive integer
- `activation_id`: UUID v4

## Functions

### Create

`POST /v1/tenants/{tenant}/namespaces/{namespace}/functions`

Body:

```json
{
  "name": "reconcile",
  "runtime": "cs-js",
  "entry": "function.js",
  "handler": "default"
}
```

Response `201`:

```json
{
  "tenant": "t_abc123",
  "namespace": "payments",
  "name": "reconcile",
  "created_at_ms": 1730000000000
}
```

Create is idempotent: a second `POST` with the same `(name, runtime,
entry, handler)` returns `200 OK` with the existing record rather than
`201`. A second `POST` with a conflicting `runtime`, `entry`, or
`handler` returns `409 CS_IDEMPOTENCY_CONFLICT`. See
[`19-entity-state-machines.md`](19-entity-state-machines.md) for the
full lifecycle invariants.

The `runtime` field is part of the public manifest contract. Accepted
values are `cs-js`, `cs-python`, and `cs-wasm`; the control plane treats an
omitted `runtime` as the implicit `cs-js` default for backward
compatibility with v1 manifests. Publishing a manifest whose `runtime` has
no registered adapter returns `400 CS_RUNTIME_UNSUPPORTED`. See
[`08-runtime-cs-js.md`](08-runtime-cs-js.md) for the runtime selection
rules and how additional adapters register themselves,
[`08b-runtime-cs-wasm.md`](08b-runtime-cs-wasm.md) for the cs-wasm host
ABI and capability model, and
[`08c-runtime-cs-python.md`](08c-runtime-cs-python.md) for the cs-python
subprocess MVP contract.

When `runtime` is `cs-wasm` the bundle ships a precompiled
`module.wasm` plus the `manifest.json`; the JavaScript-specific
`function.js` is not used. When `runtime` is `cs-python` the bundle
ships a single `function.py` plus the `manifest.json`. The control
plane accepts every bundle layout on the same upload endpoint and
routes execution to the matching adapter at invocation time.

### Read

`GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}`

Response `200` includes aliases and latest version.

### Delete

`DELETE /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}`

The server performs a soft delete:

- it marks the function as deleted
- it keeps versions for audit until TTL or explicit purge

## Draft upload

`PUT /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/draft`

Content-Type: `application/json`

Body:

```json
{
  "files": {
    "function.js": "BASE64_UTF8_SOURCE",
    "manifest.json": "BASE64_UTF8_JSON"
  }
}
```

Response `200`:

```json
{
  "draft_id": "drf_01H...",
  "sha256": "hex",
  "size_bytes": 1234,
  "expires_at_ms": 1730000000000
}
```

The server stores decoded bytes in KVRocks.
The server stores the `sha256` of the canonical bundle.

## Publish version

`POST /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions`

Body:

```json
{
  "draft_id": "drf_01H...",
  "config": {
    "timeout_ms": 3000,
    "memory_mb": 64,
    "max_concurrency": 1,
    "env": { "LOG_LEVEL": "info" },
    "capabilities": {
      "kv": { "prefixes": ["ctr:"], "ops": ["get","set"] },
      "codeq": { "publish_topics": ["jobs.*"] },
      "http": { "allow_hosts": ["api.example.com"], "timeout_ms": 1500 }
    },
    "authz": {
      "invoke_http_roles": ["role:app"],
      "invoke_schedule_roles": ["role:worker"],
      "invoke_cadence_roles": ["role:cadence"]
    }
  }
}
```

Response `201`:

```json
{
  "version": 17,
  "sha256": "hex",
  "published_at_ms": 1730000000000
}
```

The server rejects publish if the draft has expired.

The publish path also generates a CycloneDX 1.5 SBOM for the new
version (see the **SBOM** section below); a publish whose SBOM cannot
be produced or persisted fails so regulated tenants never observe a
version without supply-chain metadata.

### Signature header (E5.02)

A publishing agent that has rotated a signing key MUST send the
detached Ed25519 signature in the `X-CS-Signature` request header,
base64-encoded (standard or URL alphabet, padding optional). The
signed payload is produced by
`signing.CanonicalPayload(sha, tenant, namespace, function, 0)` — see
`docs/15-security.md` "Signing" for the byte layout.

Behaviour:

- `plugins.signing.required=false` (default) and header absent: publish
  succeeds without a signature; the resulting `VersionRecord.signature`
  is `null`.
- `plugins.signing.required=true` and header absent: 400
  `CS_SIGNATURE_MISSING`.
- Malformed base64: 400 `CS_SIGNATURE_INVALID`.
- Tenant has not rotated yet: 404 `CS_SIGNATURE_KEY_NOT_FOUND`.
- Signature does not verify against the tenant's active public key:
  400 `CS_SIGNATURE_INVALID`.

When verification succeeds, the response body is unchanged and the
persisted `VersionRecord.signature` carries the KID, algorithm, raw
signature bytes, and signed-at timestamp. The invoker re-verifies on
every cold bundle load.

## SBOM

`GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions/{version}/sbom`

Returns the CycloneDX 1.5 JSON Software Bill of Materials produced at
publish time for the given version. The SBOM lists the runtime, every
bundled file (with its SHA-256), and — once E5.01 lands — every
declared dependency with its SRI hash and source URL. The bundle digest
and signing identity (when present) appear as `cs:bundle.sha256` and
`cs:signing.*` properties on the document metadata.

Response `200`:

- `Content-Type: application/vnd.cyclonedx+json; version=1.5`
- Body: the canonical CycloneDX 1.5 document. Replaying publish for the
  same canonical bundle yields a byte-identical SBOM (deterministic
  serial number derived from the bundle SHA-256, sorted components).

Errors:

- `400 CS_VALIDATION_FAILED` with message `sbom not found` when the
  version exists but no SBOM has been persisted, or when the version
  identifier is non-numeric. Versions published before E5.03 must be
  re-published to backfill their SBOM.
- `403 CS_AUTHZ_DENIED` when the caller lacks `cs:function:read`.

The endpoint reuses the existing `cs:function:read` role check so the
same callers that can introspect a version metadata record can fetch
its SBOM. See `docs/15-security.md` "Supply chain artifacts" for the
trust model and `docs/25-schemas.md` for the CycloneDX 1.5 schema link.

## Aliases

### Set alias

`PUT /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/aliases/{alias}`

Body:

```json
{ "version": 17 }
```

Response `200`:

```json
{ "alias": "prod", "version": 17, "updated_at_ms": 1730000000000 }
```

### List aliases

`GET /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/aliases`

## Invoke via API

`POST /v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}:invoke`

Body:

```json
{
  "ref": { "alias": "prod" },
  "mode": "sync",
  "event": { "x": 1 }
}
```

Response `200` (sync):

```json
{
  "activation_id": "uuid",
  "status": "success",
  "result": { "statusCode": 200, "headers": {}, "body": "ok" },
  "duration_ms": 12
}
```

Response `202` (async):

```json
{
  "activation_id": "uuid",
  "status": "queued"
}
```

## Activations

`GET /v1/tenants/{tenant}/activations/{activation_id}`

Response includes:

- status
- timestamps
- duration
- error fields
- logs pointers
- `result_truncated` boolean (set when the function result exceeded the 256 KiB cap)

Response headers:

- `X-CS-Truncated: result` — emitted when the persisted result body was
  truncated to the configured `cs_invoker_pool.limits.max_result_bytes` cap.

Status `410 CS_ACTIVATION_TTL_EXPIRED` is returned when the activation
record has aged past the configured `cs_control.limits.activation_ttl_seconds`
TTL (default 7 days). The platform retains a small tombstone marker that lets
the API distinguish "expired" (410) from "never existed" (404) for a generous
window after expiry.

### Activation logs

`GET /v1/tenants/{tenant}/activations/{activation_id}/logs`

Query parameters:

- `cursor` — opaque pagination cursor returned as `next_cursor` from the
  previous page. Omit (or pass an empty string) to start at the beginning.
- `limit` — maximum number of log chunks to return in this page. Default
  `100`, max `500`; out-of-range values fall back to `100`.
- `format` — set to `ndjson` or `sse` to receive a streamed response (see
  below). Defaults to a JSON envelope.

Default JSON response (`200`):

```json
{
  "chunks": ["[info] hello", "[info] world"],
  "next_cursor": "2",
  "cursor": "2",
  "truncated": false
}
```

- `next_cursor` is the value to pass as `cursor` on the next request. When
  the server returns fewer chunks than `limit` the field is an empty string,
  signalling end-of-stream.
- `cursor` is retained as an alias of `next_cursor` for backward
  compatibility with existing clients.
- `truncated` mirrors the `X-CS-Truncated: logs` header (see below).

Streaming responses can be requested either via `Accept` or `?format=`:

- `Accept: application/x-ndjson` (or `?format=ndjson`) — emits one JSON
  object per line: each log chunk first, then a trailing
  `{"eof": true, "next_cursor": "...", "truncated": false}` object so the
  client can stop or paginate cleanly. The body is flushed eagerly so
  `cs activation logs --follow` can tail without buffering.
- `Accept: text/event-stream` (or `?format=sse`) — emits Server-Sent Events
  with one `event: log` per chunk and a final `event: eof` carrying the
  next cursor and truncation flag.

Response headers:

- `X-CS-Truncated: logs` — emitted when the cumulative log bytes for the
  activation reached the configured
  `cs_invoker_pool.limits.max_log_bytes` cap (default 1 MiB). The chunk
  list will contain a trailing sentinel record:

  ```json
  { "truncated": true, "reason": "log_limit_exceeded", "limit_bytes": 1048576 }
  ```

  Subsequent log writes for the activation are dropped to keep the strict
  cap in place.

## Schedules

`POST /v1/tenants/{tenant}/namespaces/{namespace}/schedules`

Body:

```json
{
  "name": "reconcile_30s",
  "every_seconds": 30,
  "overlap_policy": "skip",
  "ref": { "function": "reconcile", "alias": "prod" },
  "payload": { "source": "schedule" }
}
```

Response `201` returns the schedule metadata.

## Cadence worker bindings

`POST /v1/tenants/{tenant}/namespaces/{namespace}/cadence/workers`

Body:

```json
{
  "name": "payments-activities",
  "domain": "payments",
  "tasklist": "payments-activities",
  "worker_id": "cs-payments-01",
  "activity_map": {
    "SousInvokeActivity": { "function": "reconcile", "alias": "prod" }
  },
  "pollers": { "activity": 8 },
  "limits": { "max_inflight_tasks": 256 }
}
```

The server persists this WorkerBinding.
The server deploys pollers by configuration in `cs-cadence-poller`.

## Egress policy

The per-tenant network egress allowlist (roadmap E6.02). See
`docs/15-security.md` "Network egress" for semantics; the wire shape
and HTTP contract are below.

### Get policy

`GET /v1/tenants/{tenant}/egress-policy`

Required action: `cs:egress:policy:read`.

Response `200`:

```json
{
  "allowed_hosts": ["api.partner.com", "*.example.com"],
  "allowed_cidrs": ["203.0.113.0/24"],
  "denied_hosts":  ["abuse.example.com"],
  "default_deny":  true,
  "updated_at_ms": 1731450000000
}
```

When no policy has been uploaded the server returns the implicit
default-deny stub `{"default_deny": true}` so CLI callers can render
"no policy installed" without a dedicated 404 branch.

### Put policy

`PUT /v1/tenants/{tenant}/egress-policy`

Required action: `cs:egress:policy:write`.

Body: the same shape as the GET response (clients must omit
`updated_at_ms`; the server stamps it).

Validation:

- Each `allowed_hosts` / `denied_hosts` entry must be a hostname or a
  leading-`*.` wildcard (`*.example.com`). Mid-label wildcards
  (`foo.*.example.com`) and bare `*` are rejected.
- Each `allowed_cidrs` entry must parse with `net.ParseCIDR` or as a
  bare IP literal (`192.0.2.1`, `2001:db8::1`).
- A host cannot appear in both `allowed_hosts` and `denied_hosts`.
- The private-IP block from `docs/02-requirements.md` still applies
  at invoke time; CIDRs overlapping the blocked ranges parse fine
  here but will be rejected by the runtime when used.

Failures return `400` with `CS_VALIDATION_FAILED`. Success returns
`200` with the persisted policy (including the server-stamped
`updated_at_ms`).

## Signing keys (E5.02)

Tenant-scoped Ed25519 signing keys used by the publish handler to
verify the `X-CS-Signature` header (see **Publish version**). Only the
public half lives in the control plane; the private bytes are returned
exactly once on rotate. See `docs/15-security.md` "Signing".

### Rotate signing key

`POST /v1/tenants/{tenant}/signing-keys/rotate`

Required action: `cs:tenant:signing-key:rotate`.

Generates a fresh Ed25519 keypair, persists the public half (and the
new KID) under `cs:tenant:{tenant}:signing:ed25519:active`, and
returns the private bytes once.

Response `200`:

```json
{
  "kid": "kid_a1b2c3d4e5f6",
  "algorithm": "ed25519",
  "public_key":  "<base64 32 bytes>",
  "private_key": "<base64 64 bytes>",
  "created_at_ms": 1731600000000
}
```

The caller MUST persist `private_key` before discarding the response —
the control plane never returns the private bytes again.

### Get active signing key

`GET /v1/tenants/{tenant}/signing-keys/active`

Required action: `cs:tenant:signing-key:read`.

Response `200`:

```json
{
  "kid": "kid_a1b2c3d4e5f6",
  "algorithm": "ed25519",
  "public_key": "<base64 32 bytes>",
  "created_at_ms": 1731600000000
}
```

Errors:

- `404 CS_SIGNATURE_KEY_NOT_FOUND` when the tenant has never rotated.

## Errors

The API returns errors as:

```json
{
  "error": {
    "code": "CS_AUTHZ_DENIED",
    "message": "role missing: cw:function:publish",
    "request_id": "req_01H..."
  }
}
```

Error codes live in `21-errors.md`.
