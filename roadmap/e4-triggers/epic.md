# Epic: Trigger expansion — cron + event

**Phase:** Next (2–5 months)
**Theme:** Cover the two remaining first-class trigger types and give triggers a retry/DLQ policy.

## Why
`code-sous` ships HTTP (sync), async codeQ enqueue, interval schedule, and Cadence Activity triggers, but `docs/18-roadmap.md` calls out cron schedules and codeQ subscription (event-driven) triggers as the missing first-class trigger surfaces. Without them, tenants either bolt on out-of-band schedulers or write polling glue functions — both violate the uniform-execution-fabric invariant in `docs/02-requirements.md`. Layering a shared retry/DLQ policy across every trigger closes the gap with mature serverless platforms and makes async invocations safe to depend on for production workloads.

## Scope
- Cron schedules in `cs-scheduler` with IANA timezone, jitter, DST/leap handling, reusing existing overlap/misfire policies.
- codeQ subscription bindings managed by `cs-control` and consumed by `cs-invoker-pool`, with ordered and unordered delivery modes and a small filter expression language.
- Trigger-level retry policy (max attempts, exponential backoff with cap, retryable error classes) plus a per-tenant DLQ topic for terminal failures.
- Docs updates in `docs/11-scheduler.md`, `docs/07-codeq-protocol.md`, `docs/02-requirements.md`, and a roadmap-status entry in `docs/18-roadmap.md`.

## Outcomes / success metrics
- Cron tick drift p99 ≤ 500 ms across DST transitions (measured via `cs_schedule_cron_drift_ms`).
- Event subscription end-to-end lag p95 ≤ 2 s from `publish(topic)` to Activation start at steady state.
- Retry success rate (succeeds within `max_attempts`) ≥ 70% on synthetic transient-error workloads.
- DLQ rate ≤ 0.1% of total async invocations at steady state; DLQ topic count per tenant bounded and observable.
- Zero double-fires across a forced DST forward+backward integration test.

## Tasks
- [ ] #4 — Cron schedule trigger
- [ ] #7 — codeQ subscription trigger
- [ ] #10 — Trigger-level retry & DLQ policy

## Non-goals
- New runtime trigger types beyond cron and codeQ subscription (no S3-style storage events, no Kafka direct, no gRPC streaming triggers).
- DLQ replay UI / `cs dlq replay` tooling — tracked as a follow-up epic.
- Exactly-once delivery semantics; the system remains at-least-once.
- Per-function retry overrides (retry stays attached to the trigger binding).
- Sub-minute cron precision.
