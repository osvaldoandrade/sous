# Invoker pool: cs-invoker-pool

This document specifies the execution engine.

The invoker pool exists to run untrusted code with explicit isolation.

## Input and output

Input: `InvocationRequest` on `codeQ` topic `cs.invoke`.

Output: `InvocationResult` on `codeQ` topic `cs.results`.

The invoker also writes Activation records and logs to KVRocks.

## Execution pipeline

For each InvocationRequest:

1. Validate the envelope and schema version.
2. Resolve alias to version when version is missing.
3. Load bundle bytes from KVRocks.
4. Verify bundle `sha256`.
5. Acquire the per-version semaphore.
6. Write ActivationRecord with `status=running`.
7. Execute `cs-js`.
8. Capture stdout and stderr as logs.
9. Write ActivationRecord with terminal status.
10. Publish InvocationResult.

## Alias resolution

Alias resolution is a read of:

- `cs:fn:{tenant}:{namespace}:{function}:alias:{alias}`

The invoker records the resolved version in Activation metadata.

## Idempotency

InvocationRequest contains `activation_id`.

The invoker reads:

- `cs:act:{tenant}:{activation_id}:meta`

If terminal state exists, the invoker does not re-run code.
The invoker republishes the stored result.

## Concurrency and fairness

The invoker has two layers of concurrency control.

Replica control
- `workers.threads` defines goroutine count.
- `workers.max_inflight` caps total in-flight per replica.

Version control
- `maxConcurrency` caps in-flight per function version per replica.

This model prevents one version from saturating all threads of a replica.

## Caching

Bundle cache key: `sha256`.

A cache entry includes:

- tar bundle bytes
- parsed manifest
- compiled module representation

The cache uses LRU eviction.

Cache limits:

- `cache.bundles_max` default 1000
- `cache.bytes_max` default 512 MiB

## Activation persistence

The invoker writes three groups of data.

Metadata
- ActivationRecord under `cs:act:{tenant}:{activation_id}:meta`

Logs
- chunk keys `cs:log:{tenant}:{activation_id}:{chunk}`

Correlation
- request id to activation id mapping for sync waits

TTL rules:

- activation metadata TTL default 7 days
- log keys share the same TTL as the activation

## Result size and truncation

The invoker enforces:

- `max_result_bytes` 256 KiB
- `max_error_bytes` 64 KiB
- `max_log_bytes` 1 MiB

If a field exceeds its limit, the invoker truncates it.
The invoker sets `result_truncated=true` in ActivationRecord.

## Failure mapping

Sandbox timeout
- `InvocationResult.status = timeout`
- `error.type = Timeout`

Sandbox memory limit
- `InvocationResult.status = error`
- `error.type = MemoryLimit`

Unhandled exception
- `InvocationResult.status = error`
- `error.type = Exception`

Validation failure
- publish InvocationRequest to `cs.dlq.invoke`

## Health

The invoker serves:

- `/healthz` for liveness
- `/readyz` for readiness

Readiness checks:

- KVRocks connectivity
- codeQ subscription status
