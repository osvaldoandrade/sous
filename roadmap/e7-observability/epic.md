# Epic: Observability & operations

**Phase:** Later (5–9 months)
**Theme:** Make the platform debuggable for agent-driven workloads.

## Why
Agent workloads call functions that call functions: a single user task fans out into deep, dynamic call trees, and today the platform records each activation in isolation with no parent-child link. As scale grows, per-activation records and logs become an unbounded firehose that no operator can ingest. The 99.9% control-plane and data-plane SLOs in `docs/02-requirements.md` are aspirational without codified SLIs, burn-rate alerts, and shipped dashboards — operators currently triage incidents from raw Prometheus and ad-hoc Grafana panels. This epic closes those gaps so we can keep both planes debuggable and provably reliable as agent throughput scales.

## Scope
- Trace context propagation across `cs-http-gateway`, `cs-invoker-pool`, and `cs-cadence-poller`, plus a `/v1/tenants/{tenant}/activations/{id}/tree` endpoint that renders the full agent call tree.
- Per-trigger sampling controls (head, tail, probabilistic) configured through `cs-control` and enforced in `cs-invoker-pool`, with skeleton activation rows preserved so SLO math stays exact.
- Two reference Grafana dashboards (control-plane health, execution) shipped as versioned JSON under `deploy/observability/`, optionally provisioned via Helm.
- Codified SLI/SLO definitions, recording rules, and multi-window multi-burn-rate alert templates that map directly to the 99.9% targets in `docs/02-requirements.md`.
- Documentation updates across `docs/14-observability.md`, `docs/04-api-rest.md`, `docs/20-config-reference.md`, `docs/24-runbooks.md`, and `docs/25-schemas.md`.

## Outcomes / success metrics
- Trace coverage: ≥95% of multi-hop agent activations have a non-empty `parent_activation_id` chain reaching a root within one tenant.
- Sampled storage reduction: enabling default head+tail policy on a sample tenant cuts activation/log bytes written to KVRocks by ≥70% with zero loss of error-and-slow recordings.
- Alert MTTA: first responder acknowledges a page within 5 minutes of a 14.4x burn-rate alert on the control plane (measured on staging fire drills).
- Dashboard adoption: ≥3 deployments importing the reference dashboards within one release after publish; smoke test guarantees zero drift between dashboards and `docs/14-observability.md`.
- Runbook completeness: every burn-rate alert has a `runbook_url` resolving to a section in `docs/24-runbooks.md`.

## Tasks
- [ ] #31 — Per-node decision tracing for agent function call trees
- [ ] #34 — Activation sampling controls (head / tail / probabilistic)
- [ ] #35 — Built-in Grafana dashboards
- [ ] #36 — SLO definitions + burn-rate alerting templates

## Non-goals
- OpenTelemetry collector / OTLP export pipeline (the platform stays Prometheus-native; trace context is propagated but spans are not yet exported off-cluster).
- Alertmanager receiver configuration (PagerDuty, Slack, email) — templates only.
- Adaptive or load-aware sampling — only static per-trigger policies.
- Per-tenant SLOs beyond the platform-level 99.9% targets.
- A bespoke web UI shipped by `cs-control` for visualizing trees or dashboards.
- Long-term cold storage tiering for sampled-in activations.
