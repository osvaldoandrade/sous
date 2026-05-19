# Operators: Runbooks

This page collects the incident playbooks operators run when an alert fires or a tenant escalates. Each runbook is keyed off the symptoms the operator observes — almost always a burn-rate alert, a queue-lag spike, or a tenant report — and unfolds into a triage path, a list of likely root causes ordered by frequency, the remediation steps that resolve each cause, and the escalation path when the runbook does not close the incident.

The platform-wide posture is "respond fast, recover safely, then write up." Sous emits enough signal that an operator with paged context can usually mitigate within ten minutes; the post-incident write-up updates this page or the [Operators Administrative Operations](Operators-Administrative-Operations) page so the next on-call sees the lesson. Runbook commands assume `kubectl` reaches the cluster and `cscli` is on the operator's PATH; substitute the equivalent tooling for clusters that do not use Kubernetes.

For non-incident procedures — token rotation, scheduled key rolls, planned KVRocks compactions, deletion-purge jobs — see [Operators Administrative Operations](Operators-Administrative-Operations). For the signals these runbooks consume, see [Operators Observability](Operators-Observability).

## 1. Elevated HTTP 5xx on cs-http-gateway

**Symptoms.** The `CSControlPlaneAvailabilityBurnFast` or `CSDataPlaneLatencyBurnFast` burn-rate alert fires. The Grafana control-plane dashboard shows a spike in `cs_api_requests_total{status=~"5.."}`. Tenants report `503` responses against `/v1/web/...`.

**Triage.**

1. Confirm the burn rate is genuine. The `_PageFast` alert pairs a 1h burn-rate window with a 5m confirmation; a single isolated spike has already cleared the short window if it has recovered.
2. Inspect the status breakdown: `sum(rate(cs_api_requests_total[1m])) by (status, route)`.
3. Inspect the queue lag: `cs_invoker_queue_lag_ms{topic="cs.invoke"}`.
4. Inspect the per-tenant inflight rejection rate: `rate(cs_invoker_inflight_rejected_total[5m])`.
5. Check codeQ broker health (consumer lag, producer success rate).
6. Check KVRocks latency and error rate.

**Likely causes, ordered by frequency.**

- **Invoker pool saturated.** `cs_invoker_inflight` plateaus at `workers.max_inflight`, `cs_invoker_inflight_rejected_total` increases, and `cs_invoker_queue_lag_ms` grows. Remediation: scale `cs-invoker-pool` replicas; if a single tenant dominates, lower their `rate_limits.tenant_rps` at the gateway.
- **codeQ unavailable.** The gateway publishes to `cs.invoke` and times out; the invoker stops receiving envelopes. Remediation: see runbook 8.
- **KVRocks slow or unavailable.** The gateway and the invoker both depend on KVRocks for trigger lookup and activation writes. Remediation: see runbook 7.
- **Tenant-specific runtime regression.** A recently published version throws on warm-up. The breakdown by `function` on `cs_invocations_total{status="error"}` isolates the offender. Remediation: instruct the tenant to roll back through `PUT .../aliases/prod`; alias updates are atomic.

**Escalation.** If two scale-outs of `cs-invoker-pool` and `cs-http-gateway` do not reduce the burn rate, escalate to the platform owner channel and engage the codeQ on-call.

## 2. Schedule lag

**Symptoms.** `rate(cs_scheduler_ticks_total[5m])` flatlines or drops well below the expected `1 / tick_ms` rate. Schedules fire late or skip. Tenants report missed schedule fires. The control-plane dashboard's schedule-tick panel drops or goes flat.

**Triage.**

1. Inspect `cs_scheduler_ticks_total` — is the rate zero or stalled?
2. Inspect leader-election status: `kubectl get lease -n <ns> cs-scheduler -o yaml` (the `holderIdentity` should rotate at most once per minute).
3. Inspect KVRocks write errors from the scheduler logs (grep `service=cs-scheduler level=error`).
4. Inspect `cs_scheduler_publish_errors_total` for codeQ publish failures.

**Likely causes.**

- **Leadership churn.** Two replicas alternate leadership because the lease duration is too short or the Kubernetes API server is slow. Remediation: confirm the lease duration in the Helm values and the kube-apiserver health.
- **KVRocks write contention.** The scheduler's tick handler cannot commit fast enough; ticks queue up. Remediation: see runbook 7.
- **codeQ publish errors.** The scheduler computes the next fire but cannot enqueue. Remediation: see runbook 8.
- **Excessive missed ticks after restart.** A restart that exceeded `max_catchup_ticks * tick_ms` deliberately drops the backlog. Operators expecting recovery raise `cs_scheduler.max_catchup_ticks`, but only when the surge will not flood downstream.

**Remediation.** Restart `cs-scheduler` if leadership is stuck; raise `cs_scheduler.max_catchup_ticks` for planned catch-up; address downstream causes through the linked runbooks otherwise.

**Escalation.** If schedule lag persists after a clean leadership election against a healthy KVRocks and codeQ, the schedule-set itself may be pathological (a tenant publishing thousands of sub-minute schedules). Engage the platform owner to negotiate a quota or to enable a tenant-scoped rate limit.

## 3. Cadence ActivityTask timeouts spiking

**Symptoms.** `cs_cadence_tasks_total{status="timeout"}` increases against a binding. The Cadence frontend reports activity timeouts on the corresponding workflow runs.

**Triage.**

1. Inspect `cs_cadence_polls_total` — is the poller still polling?
2. Inspect the inflight count against `limits.max_inflight_tasks` for the binding.
3. Inspect `cs_invoker_queue_lag_ms` — is the invoker backed up?
4. Search for `cs_cadence_respond_failed` log entries from `cs-cadence-poller`.

**Likely causes.**

- **Invoker pool saturated.** Activities reach the invoker but cannot acquire a slot before Cadence times them out. Remediation: scale `cs-invoker-pool`.
- **Cadence frontend slow.** Polls succeed but completion responses time out. Remediation: scale `cs-cadence-poller` replicas; check Cadence frontend health.
- **Heartbeat starvation.** Long activities exhaust the heartbeat budget (`heartbeat.max_per_second`) and Cadence times them out as unresponsive. Remediation: raise `heartbeat.max_per_second`; instruct tenants to chunk long activities.
- **Tenant activity exceeds its declared timeout.** The `WorkerBinding`'s configured activity timeout is shorter than the activity's actual duration. Remediation: tenant updates the binding to match the workload.

**Escalation.** If Cadence frontend latency drives the symptom, engage the [code-flow](https://example/code-flow) team.

## 4. Cadence DecisionTask determinism violations

**Symptoms.** Cadence frontend logs `NonDeterministicWorkflowPolicyError` against workflows that previously ran clean. The `cs-cadence-poller` logs emit `determinism_violation` warnings.

**Triage.**

1. Identify the workflow type from the Cadence error.
2. Inspect the function ref the workflow targets through the `WorkerBinding.activity_map`.
3. Pull the function's recent publish history: `cscli versions list <tenant>/<namespace>/<function>`.
4. Compare the bundle SHA of the version Cadence loaded against the alias the binding resolves to.

**Likely causes.**

- **Tenant published a new version with non-deterministic code.** A `Date.now()` call, a `Math.random()` call, or a non-deterministic library was introduced.
- **Alias updated mid-workflow.** A `PUT .../aliases/prod` swap during a long-running workflow can cause the workflow to resume against a version it did not start with.

**Remediation.** Instruct the tenant to roll back the alias to the previous version. Alias updates are always reversible. Once the workflow drains, the tenant fixes the determinism issue and re-publishes.

**Escalation.** If the determinism violation persists after rollback, the issue may be in the Sous runtime rather than the user code; engage the runtime owner and capture the workflow history through the Cadence CLI for offline replay.

## 5. Audit sink drop

**Symptoms.** The `cs-control` logs emit `SinkLag` warnings repeatedly. Audit events stop arriving in the configured webhook target or codeq topic. Tenants depending on `GET .../audit` may still see recent history (the ring buffer is unaffected).

**Triage.**

1. Confirm the configured sink: `kubectl get cm cs-config -o yaml | grep -A 5 audit`.
2. For `webhook`: `curl -fsS <webhook_url>` from inside the `cs-control` pod to confirm reachability. Verify the HMAC secret has not rotated out from under the receiver.
3. For `codeq`: confirm the codeQ broker is healthy (see runbook 8). The topic name is `<topic_prefix>.<tenant>`.

**Likely causes.**

- **Webhook receiver unreachable.** The SIEM is down or a network change broke the route. Remediation: restore the receiver; the audit ring buffer in KVRocks preserves recent events for replay through the API while the sink is down.
- **HMAC secret out of sync.** The receiver rotated its key but the operator did not update `plugins.audit.hmac_secret`. Remediation: align the keys and restart `cs-control`.
- **codeQ broker offline.** See runbook 8.

**Remediation.** The kv mutation always commits; events are not lost from the ring buffer. After restoring the sink, tenants who need to backfill replay through `GET /v1/tenants/{tenant}/audit?since=<incident_start_ms>`.

**Escalation.** If the ring buffer overflows during the outage (more mutations than `plugins.audit.history_limit`), the dropped events are unrecoverable. Engage the platform owner to assess the compliance impact.

## 6. Signing-key compromise: emergency rotation

**Symptoms.** A tenant reports that their Ed25519 signing private key has leaked (or is suspected to have leaked). A signed publish appears that the tenant did not authorize.

**Triage.**

1. Confirm the leak with the tenant. Capture the suspected disclosure channel and the timestamp.
2. Identify the active key: `GET /v1/tenants/{tenant}/signing-keys/active`.
3. Enumerate recent publishes: `cscli versions list <tenant>/...` and compare timestamps against the tenant's expected schedule.

**Remediation.**

1. Rotate the tenant's signing key immediately: `POST /v1/tenants/{tenant}/signing-keys/rotate`. The endpoint returns the new private key bytes **once** in the response body; deliver them through an out-of-band channel.
2. Quarantine the suspect publishes. The invoker re-verifies signatures on every cold load; once the active key rotates, the suspect versions stop running. If they happen to verify against the new key (rare; the new key is freshly generated), use `PUT .../aliases/prod` to roll the alias back to a known-good version.
3. If `plugins.signing.required` is `false`, the suspect publishes may have been unsigned. Flip the knob to `true` once the tenant confirms they have re-published every active version under the new key.
4. Force the suspect versions to re-publish. Old signed versions verify against the new key only if it produced them; otherwise the invoker refuses to execute. Operators re-publish the affected versions under the new key.
5. Update the tenant's audit ring buffer: `GET /v1/tenants/{tenant}/audit?action=function.publish&since=<incident_start_ms>` documents the suspect publishes for the post-incident write-up.

**Escalation.** If the leaked key was the operator's own (e.g. a CI artifact contained a tenant-owned key with operator co-signature), engage security on-call and freeze the tenant's publish endpoint at the gateway level until rotation completes.

## 7. KVRocks unavailable

**Symptoms.** Every service emits structured log entries with `error="kvrocks: dial tcp ...: connection refused"` or `error="kvrocks: i/o timeout"`. The gateway returns `503` on every request. The invoker rejects every activation.

**Triage.**

1. Check the KVRocks pod state.
2. Check the KVRocks logs for OOM, disk-full, or replica-failure.
3. Check the disk fill on the KVRocks volume.
4. If KVRocks runs in primary/replica mode, check replication lag.

**Likely causes.**

- **Disk full.** Activation logs are the highest-volume series; a sustained traffic spike at the default 1 MiB log cap fills the volume. See runbook 10.
- **Pod evicted.** Kubernetes evicted the KVRocks pod under node pressure. Remediation: reschedule onto a node with adequate resources; raise the pod's priority class to protect it from future evictions.
- **Network partition.** A NetworkPolicy or service-mesh change cut off `cs-control`/`cs-invoker-pool`/`cs-scheduler` from KVRocks. Remediation: roll back the network change.

**Remediation.** Restore KVRocks reachability. The platform recovers automatically: in-flight HTTP requests fail with `503`, async envelopes queue in codeQ until the consumer reconnects, and scheduler ticks pile up to `max_catchup_ticks`.

**Escalation.** If KVRocks data is corrupted, restore from the most recent backup. Backups and restore procedures live in [Storage KVRocks](Storage-KVRocks).

## 8. codeQ unavailable

**Symptoms.** The gateway returns `502` on async invocations. Schedulers emit `cs_scheduler_publish_errors_total` increments. The invoker pool drains and idles.

**Triage.**

1. Check the codeQ pod state and broker health.
2. From inside a `cs-control` pod: `curl -fsS <plugins.messaging.codeq.base_url>/healthz`.
3. Verify the producer and worker tokens have not rotated unexpectedly.

**Likely causes.**

- **codeQ broker down.** Engage the [codeQ Protocol](codeQ-Protocol) on-call.
- **Token rotation skew.** A token rotation that did not roll into the `cs-secrets` Kubernetes Secret leaves producers authenticating with the old token. Remediation: align the Kubernetes Secret and restart the affected deployments.

**Remediation.** Once codeQ recovers, queued envelopes drain at the invoker pool's service rate; the retry policy attached to async triggers absorbs the gap up to `retry.max_attempts`. Operators raise `retry.max_attempts` temporarily if the recovery exceeds the default budget.

**Escalation.** If codeQ data loss is suspected, engage the codeQ on-call to retrieve from the broker's dead-letter retention.

## 9. Invoker OOM kills

**Symptoms.** Kubernetes restarts `cs-invoker-pool` pods with `OOMKilled` exit reason. The Grafana execution-plane dashboard shows the inflight panel reset on each pod restart.

**Triage.**

1. Confirm the OOM through `kubectl describe pod cs-invoker-pool-...`.
2. Inspect `cs_invoker_cache_bytes` — is it near `cs_invoker_pool.cache.bytes_max`?
3. Inspect per-tenant inflight (`cs_invoker_inflight`) — is one tenant dominating?
4. Inspect the runtime metrics for an outlier function holding excessive memory.

**Likely causes.**

- **Cache budget too high.** `cache.bytes_max` plus the steady-state working set of in-flight activations exceeds the pod's memory limit. Remediation: lower `cache.bytes_max` or raise the pod memory limit in the Helm values.
- **Tenant function leaks memory.** A `cs-js` or `cs-wasm` function allocates beyond its declared memory bound. Remediation: enforce the manifest memory cap; instruct the tenant to fix the leak.
- **Concurrency too high for the workload.** `workers.max_inflight` and the average activation footprint together exceed the pod's memory limit. Remediation: lower `workers.max_inflight`.

**Remediation.** Lower `cache.bytes_max` or `workers.max_inflight` first; both are safe rollouts. Raise the pod memory limit only after confirming the cause is not a regressive function.

**Escalation.** If the cause is a regression in the runtime itself, engage the runtime owner and pin the previous image digest in the Helm values.

## 10. Activation log spillover

**Symptoms.** KVRocks disk fill rate accelerates. The execution-plane dashboard's activation-log volume panel climbs without a matching activation-rate spike.

**Triage.**

1. Compute the per-tenant log volume: `sum by (tenant) (rate(cs_invocations_total[5m])) * <average log bytes per activation>`. Operators that have not instrumented average log bytes can sample through `cscli activations get` against random tenants.
2. Inspect the largest activation logs: `cscli activations list --order-by log_size`.
3. Inspect the `sampling_decision` field on recent activation records — a tenant may have moved from `tail` to `always` and amplified retention.

**Likely causes.**

- **Tenant disabled sampling.** A trigger update flipped `sampling.mode` from `tail` to `always`. Remediation: instruct the tenant to restore `tail` for high-volume triggers; the [Operators Observability](Operators-Observability) page documents the modes.
- **Per-activation log cap raised excessively.** An operator raised `cs_invoker_pool.limits.max_log_bytes` beyond what KVRocks can sustain. Remediation: roll back to the default 1 MiB.
- **Activation TTL too long.** `cs_control.limits.activation_ttl_seconds` defaults to 7 days; clusters that retain longer fill the volume proportionally. Remediation: lower the TTL or scale the KVRocks volume.

**Remediation.** Apply the sampling change, lower `max_log_bytes`, or shorten `activation_ttl_seconds`. KVRocks reclaims space as TTLs expire; operators with immediate disk pressure run a manual compaction (see [Operators Administrative Operations](Operators-Administrative-Operations)).

**Escalation.** If disk fill threatens KVRocks availability before retention catches up, fall through to runbook 7 (KVRocks unavailable) and restore from backup if necessary.

## Cross-references

- [Operators Observability](Operators-Observability) — the alert definitions and the metric inventory consumed by these runbooks.
- [Operators Configuration Reference](Operators-Configuration-Reference) — the YAML knobs each runbook adjusts.
- [Operators Capacity and Limits](Operators-Capacity-and-Limits) — the throughput model that informs scale-out decisions.
- [Operators Administrative Operations](Operators-Administrative-Operations) — non-incident procedures (token rotation, purge jobs, compactions).
- [Operators Security](Operators-Security) — the threat model behind the signing-key rotation runbook.
- `deploy/observability/alerts.rules.yaml` — the alert definitions that page these runbooks.
- `deploy/observability/slo.yaml` — the SLO definitions whose burn rates page these runbooks.
