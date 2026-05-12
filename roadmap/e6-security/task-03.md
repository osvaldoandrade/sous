# Audit log stream — every control-plane mutation

**Parent epic:** #30
**Phase:** Later
**Estimated size:** M

## Problem
`docs/14-observability.md` defines operational logs and metrics, but there is no
tenant-consumable audit trail for control-plane mutations (publish, alias swap,
binding create, secret rotation, etc.). Tenants and auditors need an immutable,
ordered, structured stream they can replay to prove who changed what, when, and from
where. Without it, agent-driven changes are untraceable and compliance reviews fail.

## Proposed solution
- Define an `AuditEvent` schema in `internal/observability/audit.go`:
  `{event_id, ts_ms, tenant, namespace, actor{sub, roles[], service_principal?},
  action, resource_arn, request_id, source_ip, user_agent, result{status, error?},
  before?, after?, attributes{}}`. Schema is documented in `docs/14-observability.md`
  and is append-only (additive evolution only).
- Add an `Auditor` interface alongside the structured logger and emit events from every
  mutating handler in `cmd/cs-control` (function create/delete/publish, alias set,
  draft upload commit, schedule CRUD, cadence worker CRUD, egress policy mutations,
  secret-reference mutations). Each mutation in the control plane wraps its commit in
  `auditor.Emit(...)` with the resolved Tikti action name.
- Provide pluggable sinks under `internal/observability/audit/sinks` matching the
  driver pattern: `codeq` (publishes to a per-tenant topic such as `cs.audit.{tenant}`),
  `stdout-json`, and `webhook` (HMAC-signed POST to a tenant URL). Configuration lives
  under `plugins.audit` in `docs/20-config-reference.md`.
- Expose `GET /v1/tenants/{tenant}/audit?cursor=...` on `cs-control` for replay over the
  last `audit_retention_seconds` window, with cursor pagination matching the activation
  logs API.
- Add Prometheus signals to `internal/observability/metrics.go`:
  `cs_audit_events_total{tenant,action,result}`,
  `cs_audit_sink_failures_total{sink,reason}`,
  `cs_audit_emit_duration_ms_bucket{sink}`.

## Acceptance criteria
- [ ] Every mutating control-plane handler emits exactly one `AuditEvent` on success and
      one on failure (no double-emit, no missed paths); covered by a code-review checklist
      and an integration test that diffs handler list against emitted events.
- [ ] Events include actor identity from Tikti (`sub`, `roles[]`, optional
      `service_principal`), source IP, user agent, and the Tikti action used for authz.
- [ ] At-least-once delivery to the configured sink; sink failures retry with bounded
      backoff and surface through `cs_audit_sink_failures_total`. The control-plane
      mutation does not roll back on sink failure but emits a `SinkLag` warning.
- [ ] `GET /v1/tenants/{tenant}/audit` returns ordered events for the requesting tenant
      only; cross-tenant access denied at the Tikti layer.
- [ ] Audit events redact secret material and bundle contents; before/after deltas are
      structural only (hashes for binary blobs).
- [ ] Docs updated: `docs/14-observability.md` (new "Audit" section with the event
      schema and sink contract) and `docs/20-config-reference.md` (`plugins.audit`).
- [ ] Tests: emitter unit tests, codeq sink round-trip, webhook HMAC verification,
      retention pagination integration test.

## Dependencies & risks
- Prereq: stable codeQ topic-per-tenant semantics (already used for activation streams).
- External: webhook sink interoperates with common SIEM destinations (Splunk HEC, Datadog
  logs). Mitigation: HMAC + standardized payload schema, document examples.
- Risk: high-volume tenants produce audit floods. Mitigation: per-tenant rate metrics,
  optional sampling for read events (writes always emit).
- Risk: PII in `attributes`. Mitigation: an allowlist of attribute keys per action;
  unknown keys dropped at the emitter.
- Risk: events emitted before persistence commit may misrepresent state. Mitigation:
  emit after commit, include `result.status` to distinguish committed vs. rejected.

## Out of scope
- Forwarding data-plane invocation events (activations already streamed elsewhere).
- Long-term archival to object storage — covered by sink implementations downstream.
- Cryptographic chaining / tamper-evident ledger (potential phase-2 follow-up).
- UI dashboard for audit exploration.
