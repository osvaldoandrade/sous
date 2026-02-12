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
