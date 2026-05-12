# Lock function lifecycle CRUD semantics

**Parent epic:** #16
**Phase:** Now
**Estimated size:** M

## Problem
The function lifecycle in `cmd/cs-control` covers create, read, delete, draft upload with TTL, atomic publish, and alias CRUD per `docs/02-requirements.md`, but the behavior is not yet pinned down by contract tests. We have unit coverage in `cmd/cs-control/main_test.go`, `internal/api/types_http_test.go`, and `internal/kv/store_test.go`, but no end-to-end contract suite that locks ordering rules (e.g., draft TTL vs publish race, alias-swap atomicity, version-pin immutability) before we tag v0.1.

## Proposed solution
- Author a `test/contract/lifecycle_test.go` suite that drives `cmd/cs-control` over HTTP through `internal/api` and exercises the full lifecycle: create function, upload draft, publish, alias set/list/update/delete, soft-delete, and re-read. Each scenario asserts both the response payloads from `docs/04-api-rest.md` and the KVRocks key layout from `docs/06-storage-kvrocks.md`.
- Add concurrency tests: two parallel `PUT .../draft` calls for the same function must both succeed with distinct `draft_id`s and the second TTL window must not invalidate the first; two parallel `POST .../versions` must yield distinct, monotonically increasing version integers with no gaps and no duplicates.
- Add alias-swap tests: a `PUT .../aliases/prod` racing against an invoker reading the alias must always observe either the pre or post version, never a torn read. Mirror the same invariant for `DELETE` on a version that an alias still points at (must 409).
- Document the contract in `docs/02-requirements.md` lifecycle section and add a "lifecycle invariants" subsection to `docs/19-entity-state-machines.md` enumerating the legal state transitions.
- Wire the new suite into `Makefile` (`make test-contract`) and the GitHub Actions workflow under `.github/workflows/` so the contract suite runs on every PR.

## Acceptance criteria
- [ ] `test/contract/lifecycle_test.go` exists and covers create, read, delete, draft TTL expiry, atomic publish, alias CRUD, version-pin immutability, and soft-delete read-back; suite is green in CI.
- [ ] Concurrent draft uploads, concurrent publishes, and alias swap races each have at least one dedicated test that fails if the underlying KV write loses atomicity.
- [ ] `docs/02-requirements.md` lifecycle section is updated with the locked semantics and links to `docs/19-entity-state-machines.md` for the state diagram.
- [ ] `make test-contract` runs the suite locally; the same target is invoked from the GitHub Actions workflow on PR and `main` push.
- [ ] CLI behavior matches: `cs fn publish` rejects expired drafts with a clean error (exit code 1) and `cs fn alias set` is documented in `docs/05-cli.md` as atomic.

## Dependencies & risks
- Depends on `internal/kv` exposing transactional primitives; if KVRocks transactions are not yet wired, the publish counter must be backed by a CAS loop (risk: extra round-trips).
- Risk: contract tests become flaky under parallel codeQ usage; mitigate by isolating tests to a dedicated tenant prefix per test (`t_test_<rand>`) and tearing down at end.
- Risk: documenting semantics may surface conflicts with existing implementation; record any divergence as follow-up issues.

## Out of scope
- Performance and load testing of the lifecycle APIs (covered by future capacity work in `docs/26-capacity-and-limits.md`).
- Authorization rule changes (Tikti integration stays as-is; see Task E1.03 for idempotency semantics).
- Schedules and Cadence WorkerBindings — those are lifecycle-adjacent but tracked separately.
