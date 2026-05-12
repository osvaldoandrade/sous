# Activation sampling controls (head / tail / probabilistic)

**Parent epic:** #38
**Phase:** Later
**Estimated size:** M

## Problem
Today every activation is fully recorded: metadata, user logs, and result blob land in KVRocks per `docs/14-observability.md`. At agent scale this produces an unbounded write rate and storage footprint, while operators only ever need a small slice (errors, slow calls, a head sample for typical-case debugging). The platform has no per-trigger control to reduce activation/log volume without losing signal.

## Proposed solution
- Add a `sampling` block to the trigger config surface owned by `cmd/cs-control` (HTTP, schedule, Cadence WorkerBinding, codeQ subscription). Fields: `head_per_minute` (always record the first N activations per (tenant, function, version) per minute), `tail_always` (record activations where `status != success` OR `duration_ms >= threshold_ms`), `probabilistic_one_in_n` (record 1 in N otherwise). Defaults preserve current "record everything" behavior.
- Persist sampling decisions on the Activation record itself: `sampling.decision = "head" | "tail" | "probabilistic" | "off"` and `sampling.policy_version`. Non-sampled activations still write a minimal "skeleton" row (id, function, version, status, duration_ms, ts_ms) so SLO math stays exact; logs and result blob are skipped.
- Enforce in `cmd/cs-invoker-pool` via a `sampler` package under `internal/observability` that exposes `Decide(ctx, policy, result) Decision`. The invoker calls it after the function completes (so tail rules see status/duration), but before persisting logs/result.
- Expose Prometheus counters `cs_activations_sampled_total{tenant,namespace,function,decision}` and `cs_activations_skipped_bytes_total` so operators can quantify the savings. Emit `policy_version` as a metric label only when bounded (max 8 versions per function) to avoid cardinality blow-ups.
- Update `docs/14-observability.md` with the sampling model and a worked example; document the config schema in `docs/20-config-reference.md` and the CLI surface (`cs trigger sampling set/get/clear`) in `docs/05-cli.md`.

## Acceptance criteria
- [ ] Sampling config is read/written through `cmd/cs-control` and persisted under the trigger record in `internal/kv`; round-trip test asserts immutability of policy_version on a no-op update.
- [ ] `cs-invoker-pool` honors head/tail/probabilistic rules in that order; integration test drives 1k synthetic activations and asserts (a) head guarantee, (b) all errors and slow calls recorded, (c) sample rate within tolerance for the probabilistic remainder.
- [ ] Non-sampled activations still produce a skeleton record sufficient for SLO computation; reading logs/result returns `404 Not Found` with `reason: "sampled_out"` and the new metrics are exported.
- [ ] `docs/14-observability.md`, `docs/20-config-reference.md`, and `docs/05-cli.md` are updated; the schema is added under `docs/25-schemas.md`.
- [ ] Default sampling remains "off" so existing deployments are byte-for-byte compatible; enabling per-trigger requires explicit config or CLI command.

## Dependencies & risks
- Prereq: activation TTL + log caps from the E1 activation-hardening work; both features write to the same activation key family and must agree on the skeleton schema.
- Prereq: SLO computation must consume the skeleton rows, not only fully sampled rows — coordinate with task E7.04.
- Risk: tail sampling needs the full result/log to decide, then discards it — memory pressure on hot functions. Mitigate by capping in-memory tail buffer at 1 MiB (matches log limit) and falling back to "always record" above the cap.
- Risk: misconfigured `probabilistic_one_in_n=0` silently disables recording; validation rejects zero and the CLI requires `--confirm` for ratios above 1/1000.

## Out of scope
- Adaptive / load-aware samplers (only static policy in this task).
- Per-user-code sampling decisions inside the runtime.
- Long-term cold storage tiering of sampled-in activations.
- Sampling of audit events to ledgerDB (audit retention is a separate invariant).
