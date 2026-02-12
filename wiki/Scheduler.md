# Scheduler

`cs-scheduler` creates schedule-driven InvocationRequest messages.

The scheduler runs as a single leader per tenant, per cluster.

## Schedule model

A schedule defines:

- `every_seconds`
- `overlap_policy`
- `ref` (function + alias or version)
- optional `payload`

The scheduler uses a fixed interval, not cron.

## Tick generation

For each enabled schedule, the scheduler stores:

- `next_tick_ms`
- `tick_seq`

Storage key:

- `cs:schedule:{tenant}:{namespace}:{schedule}:state`

The scheduler loop:

1. Load schedule list for the tenant.
2. For each schedule, compare `now_ms` and `next_tick_ms`.
3. If due, create InvocationRequest and publish to codeQ.
4. Advance `next_tick_ms += every_seconds * 1000`.
5. Persist state.

## Overlap policies

### skip

If an activation exists in `running` for the same schedule, the scheduler does not publish a new request.

The scheduler uses a per-schedule in-flight marker:

- `cs:schedule:{tenant}:{namespace}:{schedule}:inflight`

The invoker clears this marker on terminal completion.

### queue

The scheduler publishes every tick.
The invoker serializes execution by `max_concurrency=1`.

### parallel

The scheduler publishes every tick.
The invoker allows parallelism up to `max_concurrency`.

## Misfire policy

If the scheduler falls behind, it performs catch-up with a cap.

Config:

- `max_catchup_ticks`: default 60

If a schedule is behind by more than `max_catchup_ticks` ticks:

- the scheduler publishes only `max_catchup_ticks` invocations
- the scheduler advances state to the current wall time

## Tenant isolation

The scheduler runs per tenant.
A tenant cannot create schedules in another tenant.
