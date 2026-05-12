# Epic: v0.1 core hardening & GA readiness

**Phase:** Now (0–2 months)
**Theme:** Lock the existing surface area so v0.1 is stable, contracted, and quota-enforced.

## Why
The v0.1 surface is implemented across `cmd/cs-control`, `cmd/cs-http-gateway`, `cmd/cs-invoker-pool`, and `cmd/cs-cadence-poller`, but its contracts are not yet locked: `docs/02-requirements.md` declares limits and TTLs that are unevenly enforced, the lifecycle has no end-to-end contract suite, idempotency under retry is undefined, and there is no per-tenant rate or concurrency cap. Until these are pinned and tested, v0.1 cannot be tagged as GA without risking silent regressions and tenant noisy-neighbor incidents.

## Scope
- Contract-test coverage for the function lifecycle (create/read/delete, draft TTL, atomic publish, alias CRUD) in `cmd/cs-control`.
- Activation persistence and log API hardening: TTL enforcement, result/log size caps with clean truncation, paginated and streaming log reads.
- End-to-end idempotency contract across HTTP, codeQ, and Cadence paths with a shared dedup store.
- Hard enforcement of size limits and per-tenant rate / concurrency quotas configurable via `config.yaml`.
- Documentation updates: `docs/02-requirements.md`, `docs/04-api-rest.md`, `docs/07-codeq-protocol.md`, `docs/10-http-invoke.md`, `docs/20-config-reference.md`, `docs/21-errors.md`, `docs/24-runbooks.md`, `docs/26-capacity-and-limits.md`.

## Outcomes / success metrics
- 100% of lifecycle paths in `docs/02-requirements.md` covered by a passing contract suite running in CI.
- Zero duplicate activations observed in a 10k-message replay test across codeQ and Cadence retry paths.
- p99 limit-violation responses (`413`, `429`, `409 CS_IDEMPOTENCY_CONFLICT`) returned under 50 ms in load tests.
- Activation log volume per tenant capped at 1 MiB with measurable truncation metric; TTL eviction observed on test fixtures within configured window.
- Operator runbooks updated and validated for each new error code and metric.

## Tasks
- [ ] #1 — Lock function lifecycle CRUD semantics
- [ ] #3 — Activation persistence + log API hardening
- [ ] #5 — Idempotency keys + at-least-once delivery audit
- [ ] #8 — Quota enforcement for size + rate limits

## Non-goals
- New runtimes (`cs-python`, `cs-wasm`) — tracked in `docs/18-roadmap.md` runtimes section.
- Cron schedules, signed bundles, and event-driven (codeQ subscription) triggers.
- Cross-region replication, distributed rate limiting, or external vault integration.
- Billing-aware adaptive quotas or per-SLA tiering.
