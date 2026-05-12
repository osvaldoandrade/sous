# Trigger-level retry & DLQ policy

**Parent epic:** #19
**Phase:** Next
**Estimated size:** M

## Problem
Today an InvocationRequest that fails (timeout, runtime error, transient codeQ error) is observed in `cs.results` but is not automatically retried, and there is no per-tenant dead-letter destination beyond the platform-internal `cs.dlq.invoke`/`cs.dlq.results` (which `docs/07-codeq-protocol.md` reserves for validation/correlation failures). Tenants therefore lose async events on the first failure or rebuild retry logic inside every function — breaking the uniform-execution-fabric invariant in `docs/02-requirements.md`.

## Proposed solution
- Add an optional `RetryPolicy` block attached to every trigger (HTTP-async, schedule, cadence, subscription): `{max_attempts, backoff_base_ms, backoff_cap_ms, retryable_errors: [RuntimeError, Timeout, TransientCodeQError, ...], dlq_topic}`. Default policy is `{max_attempts: 1}` (no retry) to preserve existing behavior.
- Implement the retry loop inside `cmd/cs-invoker-pool/main.go`: on a non-success terminal result whose `error.type` is in `retryable_errors`, increment `attempt`, compute backoff `min(cap, base * 2^(attempt-1))` with full jitter, and re-publish the original `InvocationRequest` to `cs.invoke` with `trigger.source.attempt` set and an `available_at_ms` field honored by `internal/codeq`.
- When `attempt > max_attempts`, publish the original `InvocationRequest` plus terminal `error` and attempt history to a per-tenant DLQ topic `cs.dlq.tenant.{tenant}` (configurable via `dlq_topic`); update `internal/codeq` topic conventions to reserve this namespace.
- Surface retry+DLQ counters: `cs_invoke_retry_total{trigger,error_class}`, `cs_invoke_dlq_total{trigger}`, `cs_invoke_retry_success_total` so success-after-retry rate is observable.
- Update `docs/02-requirements.md` "Invocation" section to declare retry/DLQ as a first-class invariant, and add a "Retry & DLQ" section to `docs/07-codeq-protocol.md` documenting envelope additions (`attempt`, `available_at_ms`, `dlq_topic`) and the tenant-DLQ naming.

## Acceptance criteria
- [ ] An InvocationRequest whose function returns a retryable `error.type` is re-invoked up to `max_attempts` with exponential backoff capped at `backoff_cap_ms`; verified by a unit test against `cmd/cs-invoker-pool` using a stub runtime that fails N times then succeeds.
- [ ] A non-retryable error (e.g. `ValidationError`, or any type not in `retryable_errors`) is not retried and goes straight to the DLQ on the first failure.
- [ ] After `max_attempts` retryable failures, the original payload plus an `error` block and an `attempts` array land on `cs.dlq.tenant.{tenant}` exactly once.
- [ ] CLI/REST: trigger create commands (schedule, subscription, cadence binding) accept `--retry-max`, `--retry-base`, `--retry-cap`, `--retry-on`, `--dlq-topic` and persist them with the binding.
- [ ] `cs_invoke_retry_total`, `cs_invoke_dlq_total`, `cs_invoke_retry_success_total` are emitted with `{trigger, error_class}` labels.
- [ ] `docs/02-requirements.md` and `docs/07-codeq-protocol.md` describe the retry policy, default behavior, and tenant DLQ topic naming.

## Dependencies & risks
- Prereqs: `internal/codeq` must support a delayed-delivery primitive (`available_at_ms`); if not present, implement a per-tenant delay queue keyed in KVrocks.
- Risk: retry storms amplify a downstream outage — mitigated by exponential backoff with full jitter and a global `max_attempts` cap; DLQ acts as the circuit breaker.
- Risk: per-tenant DLQ topic proliferation strains codeQ — mitigated by lazy creation, a documented hard cap, and a metric on DLQ topic count per tenant.
- Cross-dep: E4 task-02 (subscription trigger) should adopt this policy as its default-failure path; ship task-03 first or in the same release.

## Out of scope
- Cross-region retry replication.
- Automatic DLQ replay tooling (`cs dlq replay --topic ...`); listed for a future epic.
- Adaptive backoff based on downstream latency / error-rate signals.
- Per-function (rather than per-trigger) retry overrides.
