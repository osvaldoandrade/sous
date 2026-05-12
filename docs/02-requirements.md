# Requirements

This document defines product scope as invariants.

## Product intent

Agents create functions to close a task loop.

A task loop looks like this:

1. An agent receives a task.
2. The agent generates a function that encodes the task logic.
3. The agent runs the function locally with the `cs` CLI.
4. The agent publishes the function to the cluster.
5. The agent invokes the published function through HTTP, schedule, or Cadence.

The platform exists to reduce friction in steps 3 to 5.

## Core invariants

### No build step

- The platform must accept function code as UTF-8 text.
- The platform must accept a JSON manifest as UTF-8 text.
- The platform must not require a compilation step at publish time.

### Runtime parity

- The CLI must use the same runtime semantics as the server.
- The runtime must expose the same host APIs locally and remotely.
- The runtime must return the same response shape for the same input.

### Immutable versions

- A published version is immutable.
- An alias points to a specific version.
- An alias update changes the alias pointer, not the version.

### Uniform execution fabric

- HTTP, schedule, and Cadence triggers must use the same execution engine.
- `cs-invoker-pool` is that engine.

### Explicit privileges

- A function version must declare:
  - allowed roles per trigger type
  - allowed side effects through capabilities
- The runtime must enforce capabilities on every call.

## Functional requirements

### Function lifecycle

- The control plane must support create, read, delete.
- The control plane must support draft upload with TTL.
- The control plane must support publish with atomic version assignment.
- The control plane must support alias CRUD.

The full set of locked semantics — idempotent create, draft TTL,
monotonic version assignment, version immutability, atomic alias swap,
read-after-write — is enumerated in
[`19-entity-state-machines.md` ("Lifecycle invariants")](19-entity-state-machines.md)
and pinned by the contract suite in
`cmd/cs-control/lifecycle_contract_test.go`. Any change to a
lifecycle handler in `cmd/cs-control` must update both.

### Invocation

- The system must support synchronous HTTP invocation.
- The system must support asynchronous invocation via codeQ.
- The system must support interval schedules in seconds.
- The system must support Cadence Activity execution.
- Async triggers (schedule, subscription, cadence) must apply a
  trigger-level retry policy and fall back to a DLQ when the retry budget
  is exhausted, per the "Retry & DLQ" subsection below.

### Retry & DLQ

Async invocations are first-class re-tryable units. The platform encodes the
contract as a `RetryPolicy` attached to each trigger
(`internal/api.RetryPolicy`), with these defaults:

| Field             | Default | Semantics                                              |
|-------------------|---------|--------------------------------------------------------|
| `max_attempts`    | `1`     | Inclusive total attempts. `1` disables retry.          |
| `base_ms`         | `500`   | Initial backoff in milliseconds.                       |
| `max_ms`          | `30000` | Upper bound on a single backoff sleep.                 |
| `jitter_pct`      | `20`    | Symmetric jitter band ±%, applied to each backoff.     |
| `retryable_errors`| see below | Allow-list of error codes that trigger a retry.       |
| `dlq_topic`       | `""`    | Optional override; default is `cs.dlq.invoke`.         |

Default `retryable_errors`: `CS_RUNTIME_TIMEOUT`,
`CS_RUNTIME_DEPENDENCY_ERROR`, `CS_RATE_LIMITED`, plus the runtime-friendly
labels `Timeout` so both shapes are honored. Hard-deny (never retried even
when listed): `CS_VALIDATION_*`, `CS_NOT_FOUND`, `CS_IDEMPOTENCY_CONFLICT`.

Backoff is computed as `delay = min(MaxMs, BaseMs * 2^(attempt-1))` with full
symmetric jitter `±JitterPct%`. No third-party rate-limit dependency is used.

HTTP triggers are synchronous and intentionally NOT covered — the calling
client owns retry. Schedule, subscription and cadence triggers are queued and
the platform owns the retry contract for them.

On exhaustion, `cs-invoker-pool` publishes an `api.DLQEnvelope`
(`cs.dlq.invoke.v1`) to the configured DLQ topic carrying:

- `original_payload` — the unmodified `InvocationRequest`.
- `last_error_code`, `last_error_message` — the terminal failure.
- `attempt_count`, `first_seen_at_ms`, `last_seen_at_ms` — observability.
- `attempts[]` — per-attempt history `{attempt, started_at_ms, duration_ms, error_code, error_message}`.

The DLQ envelope shape is documented in `docs/07-codeq-protocol.md` "Retry &
DLQ". Counters emitted (label: `trigger`):

- `cs_invoke_retry_total` — incremented on each retry decision.
- `cs_invoke_retry_success_total` — incremented when an attempt > 1 succeeds.
- `cs_invoke_dlq_total` — incremented on DLQ publish.

### Activations

- The system must persist an Activation record for every invocation.
- The system must persist user logs per Activation.
- The system must expose activation metadata and logs via API.

### Cadence worker mode

- The system must let a tenant register WorkerBindings.
- Each WorkerBinding must include:
  - domain
  - tasklist
  - worker identity
  - mapping from ActivityType to FunctionRef
- The poller must long-poll for Activity tasks.
- The poller must respond to Cadence with completion or failure.

## Non-functional requirements

### Timeouts

- Default HTTP invoke timeout: 3,000 ms.
- Max HTTP invoke timeout: 30,000 ms.
- Default worker timeout: 30,000 ms.
- Max worker timeout: 900,000 ms.

### Size limits

- Max published bundle size: 16 MiB. Enforced at `cs-control` `PUT .../draft`; violations return `413 CS_BUNDLE_TOO_LARGE`.
- Max HTTP request body size: 6 MiB. Enforced at `cs-http-gateway`; violations return `413 CS_BODY_TOO_LARGE`.
- Max function result size: 256 KiB. Enforced at `cs-invoker-pool`; violations return `413 CS_RESULT_TOO_LARGE`.
- Max logs per activation: 1 MiB. Enforced at `cs-invoker-pool`; logs are cleanly truncated and the response carries `X-CS-Truncated: logs` plus a `CS_LOG_LIMIT_EXCEEDED` sentinel.

All size limits are centralised in `internal/limits` and the exact contract
(default, enforcement site, error code) is enumerated in
`docs/26-capacity-and-limits.md` and `docs/21-errors.md`.

### Quotas

- Per-tenant gateway RPS (default 200) and per-function gateway RPS (default 20) are enforced by `cs-http-gateway` token buckets; violations return `429 CS_RATE_LIMITED` with a `Retry-After` header.
- Per-tenant concurrent activation cap (default 64) is enforced by `cs-invoker-pool`; synchronous overflow returns `429 CS_TENANT_INFLIGHT_LIMIT`, async overflow remains queued in codeQ.

### Idempotency

- HTTP invoke accepts an optional `Idempotency-Key` header (`[A-Za-z0-9_-]{8,128}`). When supplied, the resulting `activation_id` is derived deterministically from `(tenant, function, ref, idempotency_key)` and the dedup store collapses retries onto a single activation.
- Mismatched body for the same key returns `409 CS_IDEMPOTENCY_CONFLICT`.
- codeQ and Cadence retry paths reuse the same `activation_id` so at-least-once delivery does not produce duplicate activations.

### Retention

- Draft TTL: 24 hours.
- Activation TTL default: 7 days. Reads after expiry return `410 CS_ACTIVATION_TTL_EXPIRED`.

### Availability targets

- Control plane monthly availability target: 99.9%.
- Data plane monthly availability target: 99.9%.

### Security invariants

- The runtime must block filesystem access by user code.
- The runtime must block process spawn by user code.
- The runtime must deny network egress by default.
- The runtime must block private IP ranges even when egress allowlists exist.
