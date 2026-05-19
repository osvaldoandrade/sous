# Enabled Services: KVRocks

KVRocks is the persistent key-value store that holds all of Sous's control-plane and activation state. It speaks the Redis protocol over TCP (port 6666 by default) but persists to disk via RocksDB, giving Sous Redis-compatible client ergonomics with durability suitable for a control plane. Every function record, draft, version, alias, schedule, worker binding, activation, log chunk, and signing key the platform ever materializes lives behind this single backend, so its availability is a hard prerequisite for any cs-control mutation and for any data-plane invocation that has to resolve a bundle or persist a result.

Sous uses a flat namespaced key schema (`cs:<entity>:<...>`) rather than Redis hash/list structures for most records, because each record is a JSON document and atomicity is achieved through versioned writes coordinated by the Go client. Indexes that need set semantics (schedule names, worker binding names, log chunks) do use native Redis types (SET, ZSET), but the primary records themselves are single-string-valued keys carrying a JSON payload. This shape keeps the schema legible from `redis-cli`, makes ad-hoc operator queries possible, and avoids the impedance mismatch of streaming nested updates through HSET/HGETALL.

The `persistence.Provider` plugin interface allows swapping KVRocks for any Redis-compatible backend. The driver implemented under `internal/plugins/persistence/kvrocks/` is a thin shim that constructs an `internal/kv.Store` using the configured address, password, and consolidated `internal/limits.Limits` view. Any other store that adheres to the Redis wire protocol (and supports `INCR`, `SETNX`, `EXPIRE`, transactions, and `EVAL` for Lua) can be plugged into the same factory contract — see [Architecture](Architecture) and [Config Reference](Config-Reference) for how the driver is selected.

## Connection and configuration

The driver name is `kvrocks`, registered at process init from `internal/plugins/persistence/kvrocks/kvrocks.go`. The configuration block under `plugins.persistence.kvrocks` in `config.example.yaml` selects the network endpoint and auth mode:

```yaml
plugins:
  persistence:
    driver: kvrocks
    kvrocks:
      addr: localhost:6666
      auth:
        mode: none
        password: ""
```

The `addr` is a single `host:port` tuple. KVRocks 2.x listens on port `6666` by default and the bundled `docker-compose.yml` exposes that port directly on the host (see `apache/kvrocks:2.11.0`). The `auth.password` field is forwarded as the Redis `AUTH` password; an empty string means no authentication is attempted. TLS is not enabled at this layer in v0.1; production deployments terminate TLS at a service-mesh sidecar or front the KVRocks endpoint with stunnel.

The Go client is `github.com/redis/go-redis/v9`. The store created in `internal/kv/store.go` (`NewStore`) hard-codes a conservative pool: `PoolSize: 64`, `MinIdleConns: 8`, and `DialTimeout`, `ReadTimeout`, `WriteTimeout` all 3 seconds. The 3-second timeouts are deliberate: every Sous request path that touches KVRocks runs under a per-request deadline, and a stuck connection should fail fast rather than tie up an inflight slot. Operators tuning for very high invocation rates should size the connection pool relative to the maximum invoker concurrency — see [Capacity and Limits](Capacity-and-Limits).

A legacy compatibility block at the top level of `config.example.yaml` mirrors the persistence configuration for callers still on the pre-plugin layout:

```yaml
kvrocks:
  addr: localhost:6666
  auth:
    mode: none
    password: ""
```

The plugin loader prefers `plugins.persistence.kvrocks`; the flat block remains accepted during the migration window. See [Config Reference](Config-Reference) for the full precedence rules.

The store also receives a consolidated `internal/limits.Limits` view via `SetLimits`, so the activation write path enforces 256 KiB result and 1 MiB log caps uniformly across cs-control, cs-invoker-pool, and any other persistence-backed caller. The defaults are sourced from `internal/limits.Defaults()`; configured overrides flow in from `cs_control.limits` and `cs_invoker_pool.limits`.

## Key namespace

Every Sous-owned key starts with the literal prefix `cs:`. This makes a `KEYS cs:*` scan (or its safer `SCAN` equivalent) sufficient to enumerate every Sous artifact in a shared KVRocks cluster, and lets operators co-locate Sous with other Redis-compatible workloads without prefix collisions.

Sub-namespaces by entity follow the layout defined in `internal/kv/keys.go`:

- `cs:fn:{tenant}:{namespace}:{function}:meta` — function metadata.
- `cs:fn:{tenant}:{namespace}:{function}:version_seq` — monotonic version counter.
- `cs:fn:{tenant}:{namespace}:{function}:draft:{draft_id}` — staged draft bundle.
- `cs:fn:{tenant}:{namespace}:{function}:ver:{version}:meta` — published version metadata.
- `cs:fn:{tenant}:{namespace}:{function}:ver:{version}:bundle` — published bundle bytes.
- `cs:fn:{tenant}:{namespace}:{function}:alias:{alias}` — alias-to-version pointer.
- `cs:sbom:{tenant}:{namespace}:{function}:{version}` — CycloneDX SBOM bytes.
- `cs:schedule:{tenant}:{namespace}:{name}:meta` — schedule record.
- `cs:schedule:{tenant}:{namespace}:{name}:state` — scheduler tick state.
- `cs:schedule:{tenant}:{namespace}:{name}:inflight` — current inflight activation marker.
- `cs:schedule:{tenant}:{namespace}:index` — set of schedule names per namespace.
- `cs:cadence:{tenant}:{namespace}:worker:{name}:meta` — Cadence worker binding.
- `cs:cadence:{tenant}:{namespace}:workers:index` — set of worker binding names.
- `cs:cadence:{tenant}:{namespace}:task:{token_hash}` — short-lived task-token mapping.
- `cs:subscription:{tenant}:{namespace}:{name}:meta` — codeQ subscription binding.
- `cs:subscription:{tenant}:{namespace}:index` — set of subscription names.
- `cs:act:{tenant}:{activation_id}:meta` — activation envelope.
- `cs:act:{tenant}:{activation_id}:status` — terminal-status sentinel used by the CAS script.
- `cs:act:{tenant}:{activation_id}:tomb` — long-lived tombstone for 410 disambiguation.
- `cs:act:{tenant}:{activation_id}:children` — list of triggered child activation IDs.
- `cs:log:{tenant}:{activation_id}:{chunk}` — append-only log chunk payload.
- `cs:log:{tenant}:{activation_id}:chunks` — sorted-set index of chunk IDs.
- `cs:log:{tenant}:{activation_id}:bytes` — cumulative log byte counter.
- `cs:log:{tenant}:{activation_id}:truncated` — log-cap sentinel.
- `cs:req:{tenant}:{request_id}:result` — request-ID correlation pointer for sync HTTP.
- `cs:tenant:{tenant}:egress:policy` — per-tenant outbound network policy.
- `cs:tenant:{tenant}:signing:ed25519:active` — active ed25519 signing key.

Two structural rules hold across every record: tenant is the first variable component after the entity tag, so the keyspace shards cleanly by tenant for backup or migration tools, and namespace (where present) follows tenant so the same shard ordering applies to namespace-scoped scans.

## Function record

The function record holds the persistent metadata of a function, independent of any specific published version. It is created when a tenant first calls `POST /v1/tenants/{tenant}/namespaces/{namespace}/functions` and lives until the function is soft-deleted.

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

No TTL: function records are persistent for the life of the function. Deletion is soft — `SoftDeleteFunction` rewrites the record with a non-nil `deleted_at_ms` rather than `DEL`ing the key — so the audit chain in [ledgerDB Audit](ledgerDB-Audit) can still resolve historical function references after a delete.

The version counter for the function lives at the sibling key `cs:fn:{tenant}:{namespace}:{function}:version_seq` and is an integer-valued string. `PublishVersion` atomically `INCR`s this key before any version meta is written, so version numbers are dense, monotonic, and never reused. See `internal/kv/store.go` (`PublishVersion`).

The companion `CreateFunction` path uses `SETNX` against the meta key, so re-creating a function with the same `(tenant, namespace, name)` triple is rejected with `CS_VALIDATION_FAILED` "function already exists" rather than silently overwriting state. The schema for `FunctionRecord` is defined in `internal/api/types.go`.

## Draft record

A draft is a staged but unpublished bundle. The cs-control upload handler writes the draft, the publish handler reads it back, validates its hash, builds the canonical bundle, and writes the version record before marking the draft `consumed`. Drafts not published within their TTL are reaped by KVRocks automatically.

Key:

- `cs:fn:{tenant}:{namespace}:{function}:draft:{draft_id}`

Value (JSON):

```json
{
  "draft_id": "drf_01HZ4M5R7K8P9Q",
  "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "files": {
    "function.js": "BASE64ENCODED",
    "manifest.json": "BASE64ENCODED"
  },
  "created_at_ms": 1730000000000,
  "expires_at_ms": 1730086400000,
  "consumed": false
}
```

TTL:

- Configurable. Default `86400` seconds (24 hours), from `internal/limits.DefaultDraftTTLSeconds`. Override under `cs_control.limits.draft_ttl_seconds` (see [Config Reference](Config-Reference) and [Capacity and Limits](Capacity-and-Limits)).

Auto-discarded on TTL expiry. The control plane never explicitly deletes drafts — it lets KVRocks evict them — so an upload window that closes without a publish leaves no garbage to reclaim manually.

On publish, `MarkDraftConsumed` flips the `consumed` flag and rewrites the key with the same residual TTL. The marker exists so an idempotent retry of `PublishVersion` can distinguish "draft already used" from "draft expired" and surface the right error code.

## Version record

A version is the immutable, signed, indexed result of a successful publish. It is the unit of code execution: invokers always materialize a specific `(tenant, namespace, function, version)` tuple, never an unpublished draft. The publish handler in cs-control writes the version metadata and the bundle bytes in a single transaction (see "Atomicity" below).

Keys:

- `cs:fn:{tenant}:{namespace}:{function}:ver:{version}:meta` — JSON metadata.
- `cs:fn:{tenant}:{namespace}:{function}:ver:{version}:bundle` — raw bundle bytes.
- `cs:sbom:{tenant}:{namespace}:{function}:{version}` — CycloneDX 1.5 SBOM (E5.03).

Value of `:meta` (JSON):

```json
{
  "version": 17,
  "sha256": "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
  "config": {
    "runtime": "cs-js",
    "entry": "function.js",
    "handler": "default",
    "secrets": ["STRIPE_KEY=payments/stripe_key"]
  },
  "published_at_ms": 1730000000000,
  "signature": {
    "kid": "key_01HZ",
    "algorithm": "ed25519",
    "sig": "BASE64SIGBYTES",
    "signed_at_ms": 1730000000000
  }
}
```

Value of `:bundle`: opaque bytes. The canonical bundle is a tar archive containing `function.js`, `manifest.json`, and any auxiliary files declared in the manifest. The control plane standardizes on tar to avoid zip parser variance across runtimes. See [Function Lifecycle](Concepts-Function-Lifecycle).

Value of the SBOM key: canonical CycloneDX 1.5 JSON document bytes produced by `internal/sbom.Build` at publish time. See [Security](Security) "Supply chain artifacts" for the contract.

Immutable — no TTL, no overwrites. The publish path uses `INCR` to allocate the version number, so two concurrent publishers cannot collide on the same `:ver:N:` keys. Once written, version records are never mutated; alias movement happens at the alias layer, not by rewriting versions.

The `signature` field is optional for backward compatibility with versions published before the E5.02 ed25519 enforcement. When `plugins.signing.required` is `true`, the publish handler rejects unsigned uploads with `CS_SIGNATURE_MISSING`; when `false`, signatures are recorded if provided but missing signatures are accepted. The invoker re-verifies signatures on every cold-start bundle load when the field is non-nil — see [Security](Security).

## Alias record

An alias is a mutable named pointer to a version. Aliases are the supported mechanism for traffic-shaping ("prod" -> v17, "canary" -> v18) without rewriting trigger configurations. The pointer is rewritten by `SetAlias` and atomic at the single-key level.

Key:

- `cs:fn:{tenant}:{namespace}:{function}:alias:{alias}`

Value (JSON):

```json
{
  "alias": "prod",
  "version": 17,
  "updated_at_ms": 1730000000000
}
```

No TTL. Aliases live as long as the function does. `DEL` on the key when an alias is retired (the control plane currently treats aliases as additive, with explicit deletion handled by separate tooling).

Mutable. Updates use a plain `SET` rather than a CAS because aliases carry no version semantics of their own; the caller is responsible for confirming the target version exists before pointing at it. If multi-writer alias races become a concern, the layered call (`ResolveVersion` -> `SetAlias`) can be wrapped in `WATCH`/`MULTI`/`EXEC` at the cs-control handler level.

`ListAliases` enumerates aliases for a function by scanning `cs:fn:{tenant}:{namespace}:{function}:alias:*` with `SCAN` cursors of 100 keys at a time. `ResolveVersion` is the helper used everywhere a caller may have supplied either an alias or an explicit version; it returns the version unchanged when set, or looks up the alias otherwise.

## Schedule record

A schedule record is the persisted definition of a recurring trigger: interval or cron, target function or alias, payload, overlap policy. The scheduler reconciles schedules on every tick and writes back state to track the next fire time and any inflight activation marker. See [Scheduler](Scheduler).

Keys:

- `cs:schedule:{tenant}:{namespace}:{name}:meta` — schedule definition.
- `cs:schedule:{tenant}:{namespace}:{name}:state` — scheduler tick state (`next_tick_ms`, `tick_seq`).
- `cs:schedule:{tenant}:{namespace}:{name}:inflight` — current inflight activation ID, with TTL.
- `cs:schedule:{tenant}:{namespace}:index` — `SET` of schedule names in this namespace.

Value of `:meta` (JSON):

```json
{
  "tenant": "t_abc123",
  "namespace": "payments",
  "name": "reconcile_30s",
  "every_seconds": 30,
  "overlap_policy": "skip",
  "ref": { "function": "reconcile", "alias": "prod" },
  "payload": { "source": "schedule" },
  "enabled": true,
  "created_at_ms": 1730000000000,
  "kind": "interval",
  "cron": "",
  "tz": "UTC",
  "jitter_ms": 0
}
```

Value of `:state` (JSON): `{ "next_tick_ms": 1730000030000, "tick_seq": 7 }`. Updated atomically each time the scheduler emits an invocation.

Value of `:inflight`: a single activation ID string with a TTL bounded by the worker timeout. The marker exists so the overlap policy can be enforced without scanning activation records: `skip` policy reads the marker, refuses to fire if present; `replace` clears it and forces the next fire; the default is configurable per record.

`PutSchedule` writes the meta key and `SADD`s the name to the namespace index in a single `TxPipeline` so that listings remain consistent with the records they enumerate. `DeleteSchedule` reverses both writes and clears the state and inflight markers in the same pipeline.

`ListAllSchedules` scans `cs:schedule:*:*:index` keys and dereferences each set member; the scheduler uses this for cold-start discovery across all tenants. See `internal/kv/store.go` (`ListAllSchedules`).

## WorkerBinding record

A worker binding maps a Cadence domain and tasklist to one or more cs functions. The cs-cadence-poller long-polls Cadence for activity or decision tasks, looks up the binding, routes the task to a function via the `ActivityMap`, and ships the response back. See [Cadence Integration](Cadence-Integration).

Keys:

- `cs:cadence:{tenant}:{namespace}:worker:{name}:meta` — binding record.
- `cs:cadence:{tenant}:{namespace}:workers:index` — `SET` of binding names in this namespace.
- `cs:cadence:{tenant}:{namespace}:task:{token_hash}` — short-lived task-token-to-activation-ID mapping.

Value of `:meta` (JSON):

```json
{
  "tenant": "t_abc123",
  "namespace": "payments",
  "name": "payments-activities",
  "domain": "payments",
  "tasklist": "payments-activities",
  "worker_id": "cs-payments-01",
  "activity_map": {
    "SousInvokeActivity": {
      "function": "reconcile",
      "alias": "prod"
    }
  },
  "pollers": { "activity": 8 },
  "limits": { "max_inflight_tasks": 256 },
  "enabled": true,
  "input_codec": "json",
  "output_codec": "json",
  "kind": "activity"
}
```

No TTL on the meta or index keys. The task-token mapping under `cs:cadence:...:task:{token_hash}` is short-lived and TTL-bounded by the activation lifetime so that orphaned tokens (Cadence has already timed out the task) do not linger.

`Kind` selects whether the binding polls activity tasks (default, pre-E8.01 behavior) or decision tasks (E8.01 workflow runtime). The field is append-at-bottom so legacy bindings round-trip byte-identically with `Kind == ""`. `InputCodec` and `OutputCodec` pick the payload codec — `json`, `msgpack`, or `raw` — used to translate Cadence payloads to and from the function's `FunctionResponse` (E8.02).

The codeQ subscription binding lives under a parallel layout at `cs:subscription:{tenant}:{namespace}:{name}:meta` with its own `:index` set. The shape is identical in spirit: one record per binding, one set per namespace, atomic add/remove in a `TxPipeline`. See [codeQ Protocol](codeQ-Protocol).

## Activation record

An activation is a single execution attempt: a function ref, a trigger envelope, a status, optionally a result or error, optionally a sampling decision. The invoker writes the running record on dispatch, then performs a CAS update to flip to a terminal status when the function returns. The control plane reads activations to serve `GET /v1/tenants/{tenant}/activations/{id}` and the agent decision tree endpoint.

Keys:

- `cs:act:{tenant}:{activation_id}:meta` — JSON envelope.
- `cs:act:{tenant}:{activation_id}:status` — bare status string (`running`, `success`, `error`, `timeout`); the CAS pivot.
- `cs:act:{tenant}:{activation_id}:tomb` — tombstone marker with extended TTL.
- `cs:act:{tenant}:{activation_id}:children` — `LIST` of child activation IDs.

Value of `:meta` (JSON):

```json
{
  "activation_id": "0193af7f-1234-7890-abcd-1234567890ab",
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
  "error": null,
  "result": {
    "statusCode": 200,
    "headers": { "content-type": "application/json" },
    "body": "eyJvayI6dHJ1ZX0=",
    "isBase64Encoded": true
  },
  "request_id": "req_01HZ4M5R7K8P9Q",
  "resolved_version": 17,
  "parent_activation_id": "",
  "root_activation_id": "0193af7f-1234-7890-abcd-1234567890ab",
  "sampling_decision": ""
}
```

TTL:

- Configurable. Default `604800` seconds (7 days), from `internal/limits.DefaultActTTLSeconds`. Override under `cs_control.limits.activation_ttl_seconds`. The tombstone marker outlives the activation by `tombstoneTTLMultiplier` (4x) so a read after expiry returns `CS_ACTIVATION_TTL_EXPIRED` (HTTP 410) rather than `not found` (HTTP 404). See `internal/kv/store.go` (`tombstoneTTL`).

The `result.body` is truncated to `MaxResultBytes` (default 256 KiB) on write by `enforceResultCap`, with `result_truncated` set when truncation occurs. Truncation respects UTF-8 boundaries. See [Capacity and Limits](Capacity-and-Limits).

`parent_activation_id` and `root_activation_id` carry the agent decision tree edges. They are set from the `X-CS-Parent-Activation` header injected by the runtime egress shim when a function invokes another function through the platform. The `:children` list is populated by `AppendActivationChild` whenever a parent activation triggers a child, so the `/tree` endpoint can materialize a call graph without scanning the keyspace. See [Observability](Observability).

The `:status` key exists as a separate KVRocks string because the CAS Lua script in `internal/kv/store.go` uses it as the precondition: the script atomically checks `status == "running"` and rewrites both `:status` and `:meta` (with refreshed TTL) only if the precondition holds. This makes the "write terminal state" path safe against duplicate deliveries from codeQ. The script is registered once at `NewStore` time as a `*redis.Script` (server-side `EVALSHA` after the first call).

The request-ID correlation key `cs:req:{tenant}:{request_id}:result` is a separate, shorter-lived pointer used by the synchronous HTTP gateway to find an `InvocationResult` by its caller-supplied request ID without scanning activations.

## Activation log record

Activation logs are append-only structured log entries, written in chunks. Each chunk is a single SET; a sorted-set index orders the chunks by ID; a bytes counter enforces the per-activation log cap.

Keys:

- `cs:log:{tenant}:{activation_id}:{chunk}` — chunk payload (12-digit zero-padded chunk ID).
- `cs:log:{tenant}:{activation_id}:chunks` — `ZSET` of chunk IDs.
- `cs:log:{tenant}:{activation_id}:bytes` — cumulative byte counter (INCRBY).
- `cs:log:{tenant}:{activation_id}:truncated` — sentinel set when the cap is hit.

Value of a chunk: UTF-8 log bytes, structured (typically JSON lines but treated as opaque by the store).

TTL: matches the activation record TTL. Each chunk and the index carry the same TTL passed to `AppendLogChunk`, so logs and activations expire together.

`AppendLogChunk` enforces the per-activation log byte cap (default 1 MiB, configurable under `cs_invoker_pool.limits.max_log_bytes`) without scanning every chunk. The bytes counter is consulted before each write; when the remaining budget is smaller than the payload, the payload is truncated on a UTF-8 boundary and a truncation sentinel chunk is appended. Once the sentinel exists, subsequent appends are silent no-ops — the cap is strict. The sentinel chunk carries a JSON payload `{ "truncated": true, "reason": "log_limit_exceeded", "limit_bytes": 1048576 }` so consumers see a structured terminator. `LogTruncated` is the read-side helper that lets cs-control add the `X-CS-Truncated: logs` response header without scanning chunks.

`ListLogChunks` returns chunks in offset/limit pages, reading the `ZSET` in chunk-ID order and dereferencing each chunk SET. The 12-digit zero-padded chunk ID guarantees lexicographic and numeric orderings agree.

## Signing key record

Each tenant has at most one active ed25519 signing key in v0.1 (multi-key rotation is planned). The active key is what the publish handler uses to verify bundle signatures at upload time; the cs-invoker-pool re-verifies the same key on every cold-start bundle load.

Key:

- `cs:tenant:{tenant}:signing:ed25519:active`

Value (JSON):

```json
{
  "kid": "key_01HZ4M5R7K8P9Q",
  "algorithm": "ed25519",
  "public_key": "BASE64PUBLICKEYBYTES",
  "created_at_ms": 1730000000000
}
```

No TTL. Signing keys live for the active rotation window of the tenant.

Only the public key bytes and metadata live in KVRocks. The private half of the keypair is returned to the rotating caller once at rotation time and dropped — Sous never persists private keys. Tenant-managed keys may live in HashiCorp Vault (when the `secrets` plugin is configured for `vault` driver) or be supplied externally; only the public material reaches KVRocks. See [Security](Security) and the E5.02 signing notes in `internal/api/signing.go`.

Rotation rewrites the key with a new `kid` and new public bytes; old versions signed under the previous key fail verification at cold-start and surface `CS_SIGNATURE_INVALID`. This is the intended behavior — rotation invalidates existing artifacts and forces a republish.

The companion record `cs:tenant:{tenant}:egress:policy` holds the per-tenant outbound network policy consumed by cs-invoker-pool on every activation (E6.02). Schema lives in `internal/api/egress.go`; semantics are documented in [Security](Security) "Network egress".

## Atomicity

Sous uses three mechanisms to keep multi-key writes consistent: `TxPipeline` for grouped writes without preconditions, server-side Lua scripts for CAS-style updates, and `SETNX`/`INCR` for single-key atomic primitives.

`TxPipeline` (Redis `MULTI`/`EXEC`) is the workhorse. It batches related writes so they reach KVRocks as a single transaction. The publish path is the canonical example:

1. `INCR cs:fn:...:version_seq` allocates the version number (atomic on its own).
2. A `TxPipeline` writes:
   - `SET cs:fn:...:ver:{N}:meta` — version metadata.
   - `SET cs:fn:...:ver:{N}:bundle` — bundle bytes.
   - `SET cs:fn:...:alias:{a}` — optional alias update (only if the publish request specified an alias).
3. `EXEC` commits or rolls back as a unit.

If the pipeline fails, the version number is "leaked" (the counter has been advanced) but no record at that version exists. Subsequent reads of the missing version return `CS_VALIDATION_FAILED` "version not found"; the next successful publish gets the next number. This trades a small monotonic gap for a simpler retry story.

The terminal-state update for activations is the CAS path. `CompleteActivationCAS` invokes a Lua script registered at `NewStore` time:

```lua
local curr = redis.call('GET', KEYS[1])
if curr ~= ARGV[1] then
  return 0
end
redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[4])
redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[4])
return 1
```

`KEYS[1]` is the status key, `KEYS[2]` is the meta key. The script atomically checks `status == "running"`, then rewrites both keys with a refreshed TTL. The return value (1 on success, 0 on contention/already-terminal) lets the caller distinguish "we committed the terminal write" from "someone else already did". This makes duplicate codeQ deliveries safe: at-least-once becomes effectively at-most-one terminal write.

`SETNX` enforces single-writer semantics on resource creation. `CreateFunction` uses `SETNX` against the meta key so re-creating a function fails fast with `CS_VALIDATION_FAILED` rather than overwriting state. The lease API used by [Scheduler](Scheduler) leader election is the same primitive: `TryAcquireLease` is a `SETNX` with TTL.

`INCR` covers monotonic counters (`version_seq`) and the log byte counter. Both are atomic single-key writes; no script is required.

For multi-record reads that need a point-in-time view (rare in practice — Sous reads are typically single-record), the codebase uses `WATCH`/`MULTI`/`EXEC` patterns at the handler layer rather than embedding them in the store. The store exposes `RawClient()` for callers that need this level of control.

## Plugin interface

`internal/plugins/persistence/persistence.go` defines the `Provider` interface every persistence driver must implement. The interface is wide — about seventy methods covering every record type described above — because Sous favors explicit, typed accessors over a generic key-value contract. Each method takes a `context.Context`, returns typed records or `cserrors`-wrapped failures, and is independently mockable for tests.

The driver registry in `internal/plugins/registry/registry.go` exposes `RegisterPersistence(name, factory)` and `NewPersistence(cfg)`. Drivers register themselves at package init by importing the persistence subpackage; the KVRocks driver lives at `internal/plugins/persistence/kvrocks/kvrocks.go`:

```go
func init() {
    registry.RegisterPersistence("kvrocks", NewFromConfig)
}

func NewFromConfig(cfg config.Config) (persistence.Provider, error) {
    addr := cfg.Plugins.Persistence.KVRocks.Addr
    if addr == "" {
        return nil, fmt.Errorf("plugins.persistence.kvrocks.addr is required")
    }
    store := kv.NewStore(addr, cfg.Plugins.Persistence.KVRocks.Auth.Password)
    store.SetLimits(limits.FromConfig(&cfg))
    return store, nil
}
```

Swapping backends means writing a new factory that returns a `Provider`. The driver may be a wrapper over a different store (Dragonfly, KeyDB, an in-process fake for tests) or a translation layer over a non-Redis backend. Any conforming implementation can be selected by changing `plugins.persistence.driver` in `config.example.yaml` and importing the driver package for its `init` side effect.

The contract any new driver must satisfy:

- All key strings come from `internal/kv/keys.go` helpers; drivers should reuse them to stay binary-compatible at the keyspace level.
- All record JSON shapes come from `internal/api/types.go`; drivers must serialize identically so a cs-control instance reading from one driver and a cs-invoker-pool reading from another see the same envelope.
- TTLs are passed by the caller, not chosen by the driver, so backends differ only in their durability and replication model — never in their retention story.
- CAS semantics on the activation status pivot must be preserved; a backend without Lua support must implement equivalent atomicity through its native primitives.

The in-tree test driver (used by `internal/kv/store_test.go` and friends) is the KVRocks driver itself, exercised against a real KVRocks instance brought up by `docker-compose.yml`. There is no fake — tests run end-to-end against the wire protocol so the schema and atomicity contracts are continuously validated.

## Capacity model

A working capacity estimate for a single tenant on KVRocks needs three inputs: function and version counts, draft and activation rates, and log volume. The schema is dense enough that each row is small (a function record is roughly 200 bytes of JSON; an activation envelope without a body is around 400; a typical log chunk is the configured chunk size, bounded by the 1 MiB cap).

Approximate row counts per tenant at production scale:

- Function records: O(100s). One row per function, persistent.
- Version records: O(10,000s). Two rows per published version (`:meta` and `:bundle`) plus optional `:sbom`. Bundle size dominates: a 16 MiB cap with a typical 1-2 MiB payload puts most of the bytes here.
- Alias records: O(100s). One row per alias per function; aliases are reused across versions.
- Schedule records: O(100s) plus `:state` and `:inflight` siblings.
- Worker binding records: O(10s) per Cadence-integrated tenant.
- Activation records: O(millions/day at high TPS). Two rows per activation (`:meta` and `:status`), the tombstone, the children list when applicable. TTL-bounded — the 7-day default puts a ceiling on the steady-state count.
- Log records: O(activations * chunks). Chunks are small (typically a few KiB) but their count grows with verbosity. The 1 MiB per-activation cap and the TTL match together bound the log keyspace.

A rough byte ceiling for a tenant with 1 M activations/day, 10 KiB average activation size (meta + status + a couple of log chunks), and 7-day retention: ~70 GiB of activation data, dominated by logs and result bodies. Bundles add a flat ~32 MiB per published version. See [Capacity and Limits](Capacity-and-Limits) for the full throughput and storage models.

Backup strategy: KVRocks supports RocksDB snapshots via `BACKUP` and standard RocksDB-on-disk snapshotting. The recommended approach is filesystem-level snapshots of the RocksDB data directory at a coordinated point — KVRocks is durable on every write, so a snapshot taken between `EXEC`s is internally consistent. Restoring a snapshot recovers the entire keyspace in one shot. For tenant-scoped backups, the `cs:` prefix and tenant-first key layout make `SCAN cs:fn:{tenant}:*`, `SCAN cs:act:{tenant}:*`, etc. cheap enough to drive selective export tools.

Operators sizing a KVRocks cluster should plan for: a hot working set proportional to active functions and current activations (mostly served from RocksDB block cache), disk capacity covering the activation TTL window plus all published versions, and write throughput sized for the publish rate plus the steady activation/log write rate. See [Capacity and Limits](Capacity-and-Limits) for the queueing and write-throughput models, [Deployment: Kubernetes](Deployment-Kubernetes) for the production deployment shape, and [Runbooks](Runbooks) for operational procedures including backup verification and disaster recovery.

## Operational considerations

A handful of read/write patterns dominate the steady-state load and shape how operators should reason about KVRocks behavior under stress.

Hot paths:

- Activation start writes. Three keys per activation (`:meta`, `:status`, `:tomb`) in a single `TxPipeline`. The cost scales linearly with invocation rate. At 1,000 invocations per second this is 3,000 SET operations per second plus the implicit `EXEC` overhead, all bounded by the `WriteTimeout` of 3 seconds.
- Activation terminal updates. One `EVALSHA` against the CAS Lua script per terminal activation. KVRocks compiles the script once at first call and caches the SHA; subsequent calls amortize. The script does a `GET` and two `SET`s with `EX`.
- Log chunk appends. One `GET` of the bytes counter, one `Set` of the chunk, one `ZAdd`, two `Expire`s, one `IncrBy`, all in a `TxPipeline`. The fast path is six operations per chunk; the truncation path adds one more SET for the sentinel and one for the truncation marker.
- Bundle reads on cold start. A single `GET` per invoker-pool replica per function-version combination per cache miss. The invoker caches bundles in process (see `cs_invoker_pool.cache.bundles_max` and `bytes_max`), so cold starts dominate only after a replica restart or cache eviction.

Cold paths:

- Function/version metadata reads. Small, JSON-decoded, and rate-limited by the upstream HTTP request rate. These are cheap and largely served from the RocksDB block cache once a function is hot.
- Schedule reconciliation. The scheduler invokes `ListAllSchedules` periodically (configurable under `cs_scheduler`) which iterates `cs:schedule:*:*:index` with `SCAN` cursors of 100 keys. The cost scales with the number of `(tenant, namespace)` tuples that have any schedule.
- Subscription and worker binding listings. Same shape as schedules; both keep a per-namespace `:index` set so the cost is `O(bindings)` rather than `O(keyspace)`.

Failure modes worth knowing:

- KVRocks unreachable. The `Ping` method wraps the dial failure in `CSKVUnavailable`. Every read/write path surfaces wrapped errors so health checks and 5xx responses come back labeled with the right reason code.
- KVRocks slow. The 3-second per-operation timeouts kick in. Inflight requests fail with `CSKVReadFailed` / `CSKVWriteFailed`; the activation CAS path returns `CSKVCASFailed`. Operators see these as 5xx responses tagged with the specific reason; metrics labels make the failure visible without log diving.
- Pipeline partial failure. `TxPipeline.Exec` is atomic at the KVRocks side — either all commands commit or none do — so partial-write recovery is not a concern. The Go client error indicates whether any individual command was malformed, but a successful `EXEC` guarantees all writes are visible.
- Key eviction under disk pressure. KVRocks evicts based on its own policy (LRU/LFU over RocksDB compactions). A correctly sized cluster does not evict; an undersized one starts dropping the largest TTL'd records first, typically activation logs and old activations. Tenants notice this as `CS_ACTIVATION_TTL_EXPIRED` returns appearing earlier than the configured TTL.

`SCAN` discipline:

The store consistently uses `SCAN` with a 100-key page size for any keyspace iteration (`ListAliases`, `ListAllSchedules`, `ListAllWorkerBindings`, `ListAllSubscriptionBindings`). Production deployments should never need to run a bare `KEYS cs:*` against a populated KVRocks cluster; the per-tenant indexes (`:index` sets) and the per-function pattern scans cover all the listing endpoints.

Encoding rules:

- All values are JSON, except bundle bytes (raw tar archive) and SBOM bytes (raw CycloneDX JSON which is JSON but treated as opaque), the version counter (integer-valued string), and the log bytes counter (integer-valued string).
- All timestamps are UNIX milliseconds (`int64`), named `*_at_ms` or `*_ms` consistently. UTC, no timezone offset.
- All record sizes are bounded at write time by `internal/limits.Limits`. The store enforces 256 KiB result bodies and 1 MiB cumulative log bytes; cs-control enforces the 16 MiB bundle cap before any draft hits KVRocks.
- All keys are ASCII. Tenant, namespace, function, and alias names are validated by the regexes in `internal/api/types.go` (`ValidateTenant`, `ValidateNamespace`, `ValidateFunction`) before any KVRocks write, so injection attacks via key crafting are not possible.

Observability hooks:

The store does not emit metrics directly; instrumentation lives at the caller layer where the call site has the request context. The cs-invoker-pool exports per-call latencies; cs-control exports per-handler request rates; the scheduler exports tick-loop timings. KVRocks itself exposes operational counters over the Redis `INFO` command — operators should scrape these as part of the standard observability pipeline. See [Observability](Observability) for the full metric inventory.

## Cross-references

The KVRocks layout sits at the intersection of every Sous service: cs-control reads and writes records during lifecycle operations; cs-http-gateway and cs-scheduler write activation envelopes; cs-invoker-pool reads bundles, writes terminal activation state, and appends log chunks; cs-cadence-poller reads worker bindings and writes task-token mappings. Related pages:

- [Architecture](Architecture) — where KVRocks fits in the control/data plane split.
- [Concepts: Function Lifecycle](Concepts-Function-Lifecycle) — how draft, version, and alias records relate over time.
- [Concepts: Invocations and Activations](Concepts-Invocations-and-Activations) — activation envelope semantics, status transitions, tombstones.
- [Capacity and Limits](Capacity-and-Limits) — sizing inputs, throughput models, and the limit table that bounds record sizes.
- [Config Reference](Config-Reference) — the canonical YAML schema covering `plugins.persistence.kvrocks` and the limit blocks the store reads.
- [Scheduler](Scheduler) — schedule record consumer and leader election lease semantics.
- [Cadence Integration](Cadence-Integration) — worker binding semantics and task-token mapping lifetime.
- [codeQ Protocol](codeQ-Protocol) — subscription binding shape and the at-least-once delivery story the activation CAS path mitigates.
- [Security](Security) — signing key handling, egress policy storage, and the supply chain artifacts under `cs:sbom:`.
- [Observability](Observability) — how activation children, sampling decisions, and log truncation surface to operators.
- [Deployment: Kubernetes](Deployment-Kubernetes) — production deployment shape, persistent volume sizing, and replication topology.
- [Runbooks](Runbooks) — operational procedures: backup, restore, key inventory, and disaster recovery.
- [Error Model](Error-Model) — the typed error reasons (`CS_KV_UNAVAILABLE`, `CS_KV_READ_FAILED`, `CS_KV_WRITE_FAILED`, `CS_KV_CAS_FAILED`, `CS_ACTIVATION_TTL_EXPIRED`, `CS_VALIDATION_FAILED`) the store emits.
