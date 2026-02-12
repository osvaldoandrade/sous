# Runbooks

This file lists operational procedures.

## Incident: elevated HTTP 5xx

1. Check `cs_api_requests_total` by status.
2. Check `cs_invoker_queue_lag_ms`.
3. Check codeQ broker health.
4. Check KVRocks latency and error rate.
5. Check `cs_invoker_inflight`.

Mitigation:

- scale `cs-invoker-pool` replicas
- scale codeQ brokers
- reduce per-tenant rate limits

## Incident: schedule lag

1. Check `cs_scheduler_ticks_total`.
2. Check leader election status.
3. Check KVRocks write errors.
4. Check codeQ publish errors.

Mitigation:

- restart `cs-scheduler`
- increase `max_catchup_ticks` if backlog exists

## Incident: Cadence tasks time out

1. Check `cs_cadence_polls_total` and poll errors.
2. Check inflight task count against limit.
3. Check `cs_invoker_queue_lag_ms`.
4. Check `cs_cadence_respond_failed` logs.

Mitigation:

- scale `cs-cadence-poller`
- raise `max_inflight_tasks` for the binding
- scale `cs-invoker-pool`

## Procedure: rotate Tikti service tokens

1. Create new token in Tikti.
2. Update Kubernetes Secret `cs-secrets`.
3. Restart deployments.
4. Verify `/readyz` passes.

## Procedure: purge deleted functions

1. List deleted functions by API.
2. Run `cs-control purge` job with function name list.
3. Delete version blobs and aliases keys in KVRocks.
