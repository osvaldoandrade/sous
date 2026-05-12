# Quota enforcement for size + rate limits

**Parent epic:** #16
**Phase:** Now
**Estimated size:** M

## Problem
`docs/02-requirements.md` and `docs/26-capacity-and-limits.md` specify hard size limits (bundle 16 MiB, body 6 MiB, result 256 KiB, logs 1 MiB) and `docs/20-config-reference.md` exposes RPS knobs for `cs-http-gateway`, but enforcement is not uniform: `cmd/cs-control` draft upload, `cmd/cs-http-gateway` invoke, and `cs-invoker-pool` result/log writes each have partial coverage. There is also no per-tenant concurrent-activation cap, which lets a single tenant exhaust the invoker pool.

## Proposed solution
- Add a shared `internal/limits` package that exposes typed limits (`MaxBundleBytes`, `MaxBodyBytes`, `MaxResultBytes`, `MaxLogBytes`, `TenantRPS`, `FunctionRPS`, `TenantMaxInflight`) loaded from `config.yaml`. Wire every enforcement site through it so the limits are testable in one place.
- Enforce size limits at the boundaries: `cmd/cs-control` rejects `PUT .../draft` over 16 MiB with `413 CS_BUNDLE_TOO_LARGE`; `cmd/cs-http-gateway` rejects bodies over 6 MiB with `413 CS_BODY_TOO_LARGE`; `cs-invoker-pool` truncates results and logs (delegated to Task E1.02) and emits `CS_RESULT_TOO_LARGE` when the function returns more than 256 KiB.
- Add per-tenant and per-function token-bucket rate limiting in `cmd/cs-http-gateway` using `golang.org/x/time/rate`; keys are `rps:tenant:{t}` and `rps:function:{t}:{ns}:{fn}`. Return `429 CS_RATE_LIMITED` with `Retry-After` seconds and emit a `cs_gateway_rate_limited_total{scope=...}` metric.
- Add a concurrent-activation cap enforced in `cs-invoker-pool`: a semaphore per tenant with capacity `TenantMaxInflight` (default 64); when full, async invokes are queued in codeQ and sync invokes return `429 CS_TENANT_INFLIGHT_LIMIT`. Tests in `internal/runtime` + `cmd/cs-invoker-pool` use a fake clock to validate.
- Update `docs/20-config-reference.md` with the new `tenant_max_inflight`, `function_rps`, and `tenant_rps` keys per service; cross-link `docs/21-errors.md` and `docs/26-capacity-and-limits.md`. Add example `config.example.yaml` entries.

## Acceptance criteria
- [ ] Each size limit (bundle/body/result/log) returns a typed error and a non-zero metric increment; covered by table-driven tests in `internal/limits`, `cmd/cs-control`, and `cmd/cs-http-gateway`.
- [ ] A burst of N+1 requests per second to a tenant configured for N RPS yields exactly one `429 CS_RATE_LIMITED` response with a `Retry-After` header; `function_rps` independently throttles on top of `tenant_rps`.
- [ ] When `tenant_max_inflight` is set to 1 and two long-running invokes race, exactly one runs and the other returns `429 CS_TENANT_INFLIGHT_LIMIT` (sync) or remains queued (async); test asserts both modes.
- [ ] `docs/20-config-reference.md`, `docs/21-errors.md`, `docs/26-capacity-and-limits.md`, and `config.example.yaml` are updated; CLI surfaces the limits via `cs config show`.
- [ ] Metrics `cs_gateway_rate_limited_total`, `cs_invoker_inflight_rejected_total`, and `cs_control_size_rejected_total` are exposed and documented in `docs/14-observability.md`.

## Dependencies & risks
- Risk: in-process rate limiters miss cluster-wide enforcement when `cs-http-gateway` is horizontally scaled; mitigation: document as Layer-1 limit and note future shared-store rate-limit work in `docs/18-roadmap.md`.
- Risk: aggressive `tenant_max_inflight` defaults may break dev workloads; mitigate by shipping default `64` with a configurable override and a clear log line on first rejection.
- Risk: token-bucket allocation per tenant grows memory unboundedly; add an LRU with TTL eviction and a metric on entry count.

## Out of scope
- Distributed (cross-instance) rate limiting via Redis or shared store.
- Adaptive quotas based on tenant SLA tier (future billing work).
- Network egress quotas (covered separately by the security epic when scheduled).
