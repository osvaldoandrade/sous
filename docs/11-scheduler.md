# Scheduler (`cs-scheduler`)

`cs-scheduler` creates schedule-driven InvocationRequest messages.

The scheduler runs as a single leader per tenant, per cluster.

## Schedule model

A schedule defines a trigger of one of two kinds:

- `kind: "interval"` (default; backward-compatible)
  - `every_seconds` (1 .. 86400)
- `kind: "cron"`
  - `cron` — a 5-field CRON expression (see grammar below)
  - `tz` — IANA timezone, default `UTC`

Both kinds share:

- `overlap_policy` (`skip` | `queue` | `parallel`)
- `ref` (function + alias or version)
- optional `payload`
- optional `jitter_ms` — deterministic spread in `[0, jitter_ms)` ms
  applied to each computed fire time (hashed from tenant+namespace+name
  so replicas converge on the same offset).

Exactly one of `every_seconds` or `cron` must be set; cs-control returns
`CS_VALIDATION_FAILED` (HTTP 400) for requests that supply both.

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
4. Advance `next_tick_ms`:
   - interval kind: `+= every_seconds * 1000`
   - cron kind: evaluate `NextAfter(prev, tz)` in the schedule's
     timezone (see "Cron schedules" below).
5. If `jitter_ms > 0`, add a deterministic offset in `[0, jitter_ms)`.
6. Persist state.

## Cron schedules

E4.01 added cron-based triggers. Cron schedules pick the next fire time
from a 5-field expression evaluated in the schedule's IANA timezone.

### Grammar

```
minute hour day-of-month month day-of-week
```

| Field        | Range  | Notes                              |
|--------------|--------|------------------------------------|
| minute       | 0-59   |                                    |
| hour         | 0-23   |                                    |
| day-of-month | 1-31   |                                    |
| month        | 1-12   |                                    |
| day-of-week  | 0-7    | 0 and 7 both denote Sunday         |

Per-field forms: `*`, `N`, `A-B`, `*/S`, `A-B/S`, comma-separated lists.

Macros: `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`,
`@midnight`, `@hourly`.

Per-second cron is **not** supported in v1; the minimum granularity is
one minute.

### Day-of-month / day-of-week semantics

If both day-of-month and day-of-week are restricted (non-`*`), a match
on **either** field fires the schedule (standard cron OR-semantics).
If one of them is `*`, only the restricted field gates firing.

### Timezone

`tz` is an IANA name resolved with `time.LoadLocation`. An empty value
defaults to `UTC`. cs-control validates the timezone at create time and
returns `CS_VALIDATION_FAILED` for unknown zones.

### DST handling

Forward jumps (e.g. `02:00` → `03:00` in America/New_York on the second
Sunday of March) skip non-existent local times: an hourly cron fires at
`01:00`, then `03:00`, etc. Backward jumps (fall-back) do not double-fire
the duplicated hour; the evaluator walks forward in absolute time.

Brazil suspended DST in 2019, but historic 2018 transitions for
America/Sao_Paulo are exercised by the test suite to confirm the walker
never wedges or steps backward during a forward jump.

### Jitter

`jitter_ms` is capped at 1 hour (3,600,000 ms). The offset is derived
from `fnv64a(tenant|namespace|name)` so each replica computes the same
offset and the same schedule always fires at the same offset relative
to its cron-aligned base.

### Worked examples

```
{"kind":"cron","cron":"0 */15 * * *","tz":"America/Sao_Paulo"}
{"kind":"cron","cron":"0 9-17 * * 1-5","tz":"Europe/Berlin","jitter_ms":30000}
{"kind":"cron","cron":"@daily","tz":"UTC"}
{"kind":"cron","cron":"0 3 1 * *","tz":"America/New_York"}
```

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
