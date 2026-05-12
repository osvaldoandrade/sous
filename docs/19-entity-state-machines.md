# Entity state machines

This file defines state transitions as invariants.

## Function

States:

- `active`
- `deleted`

Transitions:

- `active → deleted` via function delete
- no transition out of `deleted`

## Draft

States:

- `created`
- `expired`
- `consumed`

Transitions:

- `created → expired` via TTL
- `created → consumed` via publish
- no transition out of `expired`
- no transition out of `consumed`

## Version

States:

- `published`

Versions are immutable.

## Alias

Alias points to a version.

States:

- `unset`
- `set`

Transitions:

- `unset → set` via alias set
- `set → set` via alias update

## Schedule

States:

- `enabled`
- `disabled`
- `deleted`

Transitions:

- `enabled → disabled` via API
- `disabled → enabled` via API
- `enabled → deleted` via delete
- `disabled → deleted` via delete

## WorkerBinding

States:

- `enabled`
- `disabled`
- `deleted`

Transitions mirror Schedule.

## Activation

States:

- `queued`
- `running`
- terminal:
  - `success`
  - `error`
  - `timeout`

Transitions:

- `queued → running` when invoker starts
- `running → success|error|timeout` on completion
- no transition out of terminal

## Lifecycle invariants

The function lifecycle (create, read, delete, draft, publish, alias) is
locked by the contract suite in
`cmd/cs-control/lifecycle_contract_test.go`. The following invariants
must hold for every legal transition above; any new code in
`cmd/cs-control` or `internal/kv` that breaks them must update the
contract suite in the same change.

1. **Idempotent create.** Posting `POST .../functions` twice with the
   same `(tenant, namespace, name, runtime, entry, handler)` returns
   the same `FunctionRecord` both times. The first call returns `201`,
   subsequent identical calls return `200`. A re-create with a
   conflicting `runtime`, `entry`, or `handler` returns `409
   CS_IDEMPOTENCY_CONFLICT`. A re-create against a soft-deleted
   record returns `409 CS_IDEMPOTENCY_CONFLICT` — soft-deleted
   functions cannot be revived through create.
2. **Independent draft TTLs.** Each `PUT .../draft` call returns a
   fresh `draft_id` and `expires_at_ms`. Concurrent uploads for the
   same function never collide on `draft_id` and never shorten an
   earlier draft's TTL window. The 24h default lives in
   `internal/limits.DefaultDraftTTLSeconds`.
3. **Atomic publish with monotonic versions.** `POST .../versions`
   allocates a strictly monotonic, gap-free version integer per
   `(tenant, namespace, function)`. Concurrent publishes return
   distinct version integers; no two callers ever observe the same
   number. Version meta and bundle are written in a single KV
   transaction so failed publishes do not leave a torn record.
4. **Immutable versions.** Once `POST .../versions` returns `201`,
   the resulting `(version, sha256, config)` triple is immutable.
   Re-publishing the same draft produces a *new* version; it never
   mutates an existing one.
5. **Atomic alias swap.** `PUT .../aliases/{alias}` is a single KV
   write. A concurrent invoker resolving the alias observes either
   the pre-swap version or the post-swap version, never a torn or
   missing value (once the alias has been set at least once).
6. **Alias requires a real version.** `PUT .../aliases/{alias}` with
   a `version` that has not been published returns `400
   CS_VALIDATION_FAILED`. The control plane never persists a
   dangling pointer.
7. **Expired drafts cannot publish.** `POST .../versions` on a draft
   whose `expires_at_ms` is in the past returns `400
   CS_VALIDATION_FAILED` with `message: "draft expired"`. The CLI
   surfaces this as exit code `1`.
8. **Soft delete is one-way.** `DELETE .../functions/{name}` marks
   the record with `deleted_at_ms` but preserves versions and
   aliases for audit. Reads default to "not found";
   `?include_deleted=true` returns the tombstoned record. No
   transition resurrects a deleted function.
9. **Read-after-write.** Every successful create, publish, or alias
   write is immediately visible to the same caller on the next read.
   The control plane never serves a stale view of its own write.

Future work: when explicit version-delete is added, deleting a version
that an alias still points at must return `409` so callers cannot
break live traffic through a single DELETE.
