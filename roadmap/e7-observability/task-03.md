# Built-in Grafana dashboards

**Parent epic:** #38
**Phase:** Later
**Estimated size:** M

## Problem
`docs/14-observability.md` enumerates Prometheus metrics for both planes, but operators currently build dashboards by hand from those names. There is no shipped reference visualization for control-plane health (publish latency, alias-swap rate, error budget) or for execution (per-tenant invocation latency / error / throughput). Adoption is slow and inconsistent across deployments, which makes incident triage and SLO reviews ad-hoc.

## Proposed solution
- Add `deploy/observability/dashboards/control-plane.json` covering: API request rate, p50/p95/p99 latency, error ratio, publish latency (`cs_api_request_duration_ms_bucket{route="/v1/.../versions"}`), alias-swap rate (counter on alias PUTs), audit emission rate from `ledgerDB`, and a 30-day error-budget burn panel keyed off the 99.9% target in `docs/02-requirements.md`.
- Add `deploy/observability/dashboards/execution.json` covering: per-tenant invocations/s, success vs error split, p50/p95/p99 invocation latency by trigger, invoker inflight + queue lag, cold-start rate, runtime cache occupancy, and a per-function top-N table; include template variables for `tenant`, `namespace`, `function`, `trigger`.
- Versioned JSON: every dashboard carries a top-level `cs_dashboard_version` field (semver) and a matching git tag-equivalent in the panel description. A `deploy/observability/dashboards/README.md` documents the import procedure (Grafana UI + `grafana-cli` + provisioning YAML).
- Provide a sidecar provisioning ConfigMap manifest under `deploy/helm/templates/observability-dashboards.yaml`, gated by a Helm value `observability.dashboards.enabled` (default false to keep the chart opt-in).
- Add a smoke test in `test/dashboards_test.go` that parses each JSON, validates the schema (Grafana dashboard v8+), and asserts that every referenced metric exists in `docs/14-observability.md` so dashboards cannot drift past metrics.

## Acceptance criteria
- [ ] `deploy/observability/dashboards/{control-plane,execution}.json` exist, are importable into Grafana 10+, and render against a Prometheus scraping the documented metrics.
- [ ] Each dashboard exposes the documented panels, template variables, and a visible `cs_dashboard_version` annotation; README documents import via UI, `grafana-cli`, and Kubernetes provisioning.
- [ ] Helm value `observability.dashboards.enabled` provisions the ConfigMap; default `false` keeps existing chart users unaffected.
- [ ] `test/dashboards_test.go` validates dashboard schema and cross-references every queried metric against `docs/14-observability.md`; CI fails when a dashboard query references an undocumented metric.
- [ ] `docs/14-observability.md` gains a "Reference dashboards" section linking the new files and showing thumbnails or panel descriptions.

## Dependencies & risks
- Prereq: metric names and labels in `docs/14-observability.md` are stable; any rename must update dashboards in the same PR (enforced by the smoke test).
- Prereq: SLO/burn-rate templates (E7.04) define the error-budget computation; dashboards reuse the same recording rules to avoid divergence.
- Risk: high-cardinality labels (`tenant`, `function`) blow up Grafana queries on large deployments. Mitigate by using `topk()` and capping `tenant` template variable suggestion list at 500.
- Risk: Grafana JSON schema changes between major versions; pin to v10 schema and document the minimum supported Grafana version in the README.

## Out of scope
- Datasource provisioning (operators bring their own Prometheus).
- Alert routing inside Grafana (alerts live as Prometheus rules — see E7.04).
- Per-tenant or per-customer custom dashboards beyond the two reference dashboards.
- A bespoke web UI shipped by `cs-control`.
