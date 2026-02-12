# Storage: KVRocks

`code-sous` stores every record in KVRocks with a stable key prefix.

The platform stores:

- function metadata
- drafts
- versions
- aliases
- schedules
- cadence worker bindings
- activations
- logs

This file defines the key schema and required atomicity.

## Key namespace

All keys start with:

- `cs:`

All tenant-scoped keys include `tenant` early in the key.

## Function metadata

### Function record

Key:

- `cs:fn:{tenant}:{namespace}:{function}:meta`

Value (JSON):

```json
{
  "tenant": "t_abc123",
  "namespace": "payments",
  "name": "reconcile",
  "runtime": "cs-js",
  "entry": "function.js",
  "handler": "default",
  "created_at_ms": 1730000000000,
  "deleted_at_ms": null
}
```

### Version counter

Key:

- `cs:fn:{tenant}:{namespace}:{function}:version_seq`

Value: integer.

The control plane increments this key atomically at publish time.

## Drafts

Key:

- `cs:fn:{tenant}:{namespace}:{function}:draft:{draft_id}`

Value (JSON + embedded blobs):

```json
{
  "draft_id": "drf_01H...",
  "sha256": "hex",
  "files": {
    "function.js": "BASE64",
    "manifest.json": "BASE64"
  },
  "created_at_ms": 1730000000000,
  "expires_at_ms": 1730003600000
}
```

TTL:

- 24 hours

## Versions

### Version metadata

Key:

- `cs:fn:{tenant}:{namespace}:{function}:ver:{version}:meta`

Value:

```json
{
  "version": 17,
  "sha256": "hex",
  "config": { "...": "..." },
  "published_at_ms": 1730000000000
}
```

### Version code blob

Key:

- `cs:fn:{tenant}:{namespace}:{function}:ver:{version}:bundle`

Value: bytes of the canonical bundle.

Bundle format:

- tar archive with:
  - `function.js`
  - `manifest.json`

The control plane stores a tar to avoid zip parser variance.

## Aliases

Key:

- `cs:fn:{tenant}:{namespace}:{function}:alias:{alias}`

Value:

```json
{
  "alias": "prod",
  "version": 17,
  "updated_at_ms": 1730000000000
}
```

## Schedules

Key:

- `cs:schedule:{tenant}:{namespace}:{schedule}:meta`

Value:

```json
{
  "name": "reconcile_30s",
  "every_seconds": 30,
  "overlap_policy": "skip",
  "ref": { "function": "reconcile", "alias": "prod" },
  "payload": { "source": "schedule" },
  "enabled": true,
  "created_at_ms": 1730000000000
}
```

Index:

- `cs:schedule:{tenant}:{namespace}:index` (set of schedule names)

## Cadence WorkerBinding

Key:

- `cs:cadence:{tenant}:{namespace}:worker:{name}:meta`

Value:

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
  "limits": { "max_inflight_tasks": 256 },
  "enabled": true
}
```

Index:

- `cs:cadence:{tenant}:{namespace}:workers:index` (set of worker names)

## Activations

Key:

- `cs:act:{tenant}:{activation_id}:meta`

Value:

```json
{
  "activation_id": "uuid",
  "tenant": "t_abc123",
  "namespace": "payments",
  "function": "reconcile",
  "ref": { "alias": "prod", "version": 17 },
  "trigger": { "type": "http" },
  "status": "success",
  "start_ms": 1730000000000,
  "end_ms": 1730000000012,
  "duration_ms": 12,
  "result_truncated": false,
  "error": null
}
```

TTL:

- 7 days default

## Logs

Logs store as chunked strings.

Keys:

- `cs:log:{tenant}:{activation_id}:{chunk}`

Value: UTF-8 bytes.

Index:

- `cs:log:{tenant}:{activation_id}:chunks` (sorted set of chunk ids)

## Atomicity rules

- Publish uses a transaction:
  - increment `version_seq`
  - write `ver:{version}:meta`
  - write `ver:{version}:bundle`
  - optionally update alias
- Alias update uses a single write.
- Activation terminal update uses a compare-and-set on status.

The implementation uses Lua scripts in KVRocks for these atomic writes.
