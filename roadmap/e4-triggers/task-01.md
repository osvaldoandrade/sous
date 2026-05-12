# Cron schedule trigger

**Parent epic:** #19
**Phase:** Next
**Estimated size:** M

## Problem
`cs-scheduler` only supports fixed-interval (`every_seconds`) schedules today, as documented in `docs/11-scheduler.md`. Most real workloads (nightly billing, hourly reconciliations, business-hour-aware jobs) are expressed naturally as cron expressions with a timezone, not "every N seconds". Without cron, tenants either over-fire (interval that approximates cron) or build out-of-band schedulers, which defeats the uniform execution fabric invariant in `docs/02-requirements.md`.

## Proposed solution
- Extend the schedule model in `cmd/cs-scheduler/main.go` so a schedule can carry either `every_seconds` (existing) or `cron` (new) plus a `timezone` (IANA name, default `UTC`) and an optional `jitter_ms` window applied per tick.
- Add a small cron evaluator package under `cmd/cs-scheduler` (or `internal/scheduler` if it materializes) that computes `next_tick_ms` from `(cron, timezone, now)`, handling DST forward/backward jumps and leap days; pin to a vetted library (e.g. `github.com/robfig/cron/v3` parser) wrapped behind an interface for testability.
- Persist `cron`, `timezone`, `jitter_ms`, and computed `next_tick_ms` in the existing `cs:schedule:{tenant}:{namespace}:{schedule}:state` key; reuse overlap/misfire policies unchanged so cron benefits from the same `skip|queue|parallel` and `max_catchup_ticks` semantics described in `docs/11-scheduler.md`.
- Add `cs schedule create --cron "0 */15 * * *" --tz America/Sao_Paulo [--jitter 30s]` to the CLI (and matching REST surface on `cs-control`); reject schedules that specify both `--cron` and `--every`.
- Update `docs/11-scheduler.md` with a "Cron schedules" section covering syntax, timezone handling, jitter, DST rules, and worked examples; add metrics `cs_schedule_cron_drift_ms` (next_tick - actual_fire) histogram.

## Acceptance criteria
- [ ] `cs schedule create --cron "0 */15 * * *" --tz America/Sao_Paulo` creates a schedule whose stored state has `cron`, `timezone`, and a `next_tick_ms` matching the next quarter-hour in São Paulo wall time.
- [ ] Cron schedules fire at the configured wall-clock time within `jitter_ms` ± scheduler loop interval; verified by a fake-clock test that advances across a DST transition and confirms no missed or duplicate ticks.
- [ ] Mixing `--cron` and `--every` on the same schedule returns a 400/validation error from `cs-control` and the CLI exits non-zero with a clear message.
- [ ] Overlap policy (`skip|queue|parallel`) and misfire `max_catchup_ticks` apply identically to cron and interval schedules; covered by table tests in `cmd/cs-scheduler/main_test.go`.
- [ ] `docs/11-scheduler.md` documents the cron grammar accepted, timezone rules, DST handling, jitter semantics, and the `cs schedule create --cron` flag.
- [ ] `cs_schedule_cron_drift_ms` histogram is emitted per tick and visible in the metrics endpoint.

## Dependencies & risks
- Prereqs: agreement on cron library (robfig/cron/v3 parser is the obvious pick; spec macros `@daily`, `@hourly` should resolve through it). No external services.
- Risks: incorrect DST handling silently fires twice or skips a tick — mitigated by fake-clock unit tests pinned to known DST transitions (America/Sao_Paulo Oct/Feb, America/New_York Mar/Nov) and a freeze-time integration test.
- Risk: high-fanout schedules (every minute, many tenants) stampede codeQ — mitigated by `jitter_ms` spread and reuse of `max_catchup_ticks` cap.

## Out of scope
- Per-second cron (sub-minute precision) — keep minimum granularity at 1 minute for v1.
- Calendar-style holiday skipping or business-hour windows; revisit if demand appears.
- Cron expression UI/builder in the dashboard.
- Migration tooling to auto-convert existing `every_seconds` schedules to cron.
