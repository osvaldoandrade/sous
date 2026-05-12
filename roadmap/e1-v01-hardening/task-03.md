# Idempotency keys + at-least-once delivery audit

**Parent epic:** #16
**Phase:** Now
**Estimated size:** L

## Problem
`docs/10-http-invoke.md` mentions an `Idempotency-Key` header but the gateway's UUIDv5 derivation is not consistently enforced across `cmd/cs-http-gateway`, `internal/codeq`, and `cmd/cs-cadence-poller`. Under retry conditions (gateway client retry, codeQ redelivery, Cadence activity-task heartbeat resend) we can produce duplicate activations because nothing in the data path performs an end-to-end dedup check. We need a single contract for `request_id` / `activation_id` / message-id and a dedup store the invoker pool can trust.

## Proposed solution
- Lock the HTTP contract in `cmd/cs-http-gateway`: require `Idempotency-Key` for non-GET invokes when retried (advisory, header optional but if present must be `[A-Za-z0-9_-]{8,128}`); derive `activation_id = UUIDv5(namespace=tenant_uuid, name=idem_key|tenant|namespace|function|ref)`. Persist a dedup record `idem:{tenant}:{hash}` in KVRocks with TTL = function timeout + 1 hour; on second hit return the cached `InvocationResult` (sync) or `202 Accepted` with the original `activation_id` (async).
- In `internal/codeq`, define message-id determinism: the producer computes `id = sha256("{tenant}|{activation_id}|{seq}")` so codeQ retries collapse on the consumer side; document this in `docs/07-codeq-protocol.md` under "Delivery mode". Update `internal/codeq/codeq.go` to set the id and add an idempotent producer test.
- In `cs-invoker-pool`, before executing, check the dedup store keyed by `activation_id`; if a terminal `InvocationResult` exists, re-publish it and skip execution. Add a metric `cs_invoker_dedup_hits_total`.
- In `cmd/cs-cadence-poller`, use the Cadence `task_token` as the de-dup key when constructing the `InvocationRequest`; reuse the same `activation_id` for any retried heartbeat/respond cycle so Cadence sees one logical activation per task. Document the binding in `docs/12-cadence-integration.md`.
- Update `docs/10-http-invoke.md` ("Idempotency"), `docs/07-codeq-protocol.md` ("Correlation" and a new "Idempotency" section), and `docs/21-errors.md` with a `CS_IDEMPOTENCY_CONFLICT` error code for mismatched bodies on the same key.

## Acceptance criteria
- [ ] Sending the same `Idempotency-Key` twice (sync or async) returns the same `activation_id` and the cached result; mismatched body returns `409 CS_IDEMPOTENCY_CONFLICT`. Covered by an integration test against `cs-http-gateway` + `cs-invoker-pool`.
- [ ] codeQ redelivery of the same `InvocationRequest` produces exactly one execution; the second delivery is acknowledged and the invoker emits a `dedup_hit` counter. Test uses a fake codeQ in `internal/codeq` that replays a message.
- [ ] Cadence activity retries (same `task_token`) reuse the same `activation_id`; verified in `cmd/cs-cadence-poller/main_test.go`.
- [ ] `docs/10-http-invoke.md`, `docs/07-codeq-protocol.md`, `docs/12-cadence-integration.md`, and `docs/21-errors.md` all reflect the new contract.
- [ ] Audit log shows at most one terminal activation record per `(tenant, idempotency_key, function_ref)` tuple over a 1-hour TTL window.

## Dependencies & risks
- Depends on `internal/kv` supporting a dedup key with TTL; reuses primitives from Task E1.02.
- Risk: dedup TTL too short causes false re-execution; mitigate by making `idem_ttl_seconds` configurable per `cs_http_gateway` and defaulting to `function timeout + 3600s`.
- Risk: collision between user-supplied keys across tenants — mitigated by including `tenant` in the UUIDv5 namespace.
- Risk: dedup table growth — add a metric on its size and an LRU eviction story in `docs/24-runbooks.md`.

## Out of scope
- Cross-region replication of the dedup store.
- Exactly-once semantics for user-defined side effects inside the function (still at-least-once; document explicitly).
- New SDK helpers for idempotency keys in `cs-js`.
