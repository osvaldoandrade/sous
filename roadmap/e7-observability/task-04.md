# SLO definitions + burn-rate alerting templates

**Parent epic:** #38
**Phase:** Later
**Estimated size:** M

## Problem
`docs/02-requirements.md` declares 99.9% monthly availability for both the control plane and the data plane, but those targets are not codified anywhere operators can ingest: there are no SLI definitions, no recording rules, and no alert templates. `docs/14-observability.md` mentions "SLO reporting" as a vague export, with no multi-window multi-burn-rate (MWMBR) wiring. Today an outage at 14x burn is indistinguishable from a 1.5x burn in the alerting stack.

## Proposed solution
- Add `deploy/observability/slo.yaml` defining each SLI and SLO: control-plane availability (success ratio of `cs_api_requests_total{status!~"5.."}`), control-plane latency (p99 < 500 ms on lifecycle routes), data-plane availability (`cs_invocations_total{status="success"}` ratio), data-plane latency (p99 by trigger), each with `objective: 99.9`, `window: 30d`, and a documented exclusion list (planned maintenance, tenant-throttled 429s).
- Generate Prometheus recording rules under `deploy/observability/rules/recording.yml`: 5m/30m/1h/6h burn rates per SLO using the standard `slo:sli_error:ratio_rate<window>` naming, plus `slo:error_budget:remaining` over the 30-day window.
- Generate alert rules under `deploy/observability/rules/alerts.yml` using the Google SRE workbook MWMBR pairs: page when (1h burn > 14.4 AND 5m burn > 14.4) OR (6h burn > 6 AND 30m burn > 6); ticket when (24h burn > 3 AND 2h burn > 3) OR (3d burn > 1 AND 6h burn > 1). Each alert carries `severity`, `slo`, `service`, `runbook_url` (link to `docs/24-runbooks.md#slo-burn`), and a templated summary.
- Provide a generator script `deploy/observability/render_rules.sh` driven by `slo.yaml` so SLO edits stay declarative; the rendered YAML is checked in for auditability.
- Document the SLI definitions, exclusion rules, alert thresholds, and operator response in `docs/14-observability.md` ("SLO reporting" section gets fleshed out) and add a new runbook in `docs/24-runbooks.md` for each severity tier.

## Acceptance criteria
- [ ] `deploy/observability/slo.yaml`, `recording.yml`, and `alerts.yml` exist; `promtool check rules deploy/observability/rules/*.yml` passes in CI.
- [ ] Burn-rate alerts implement the page + ticket pairs from the SRE workbook with documented thresholds; each alert has a `runbook_url` pointing at `docs/24-runbooks.md`.
- [ ] `render_rules.sh` regenerates the rule files deterministically from `slo.yaml`; a CI step asserts the committed YAML is byte-identical to the rendered output.
- [ ] `docs/14-observability.md` "SLO reporting" section enumerates each SLI, the 30-day error budget math, and the exclusion list; `docs/24-runbooks.md` covers the operator response for page and ticket alerts.
- [ ] A synthetic outage test in `test/slo_burn_test.go` feeds canned metrics into a local Prometheus and asserts the expected alert fires within one evaluation interval.

## Dependencies & risks
- Prereq: dashboards (E7.03) consume the same recording rules; both tasks must agree on rule names to avoid duplication.
- Prereq: sampling (E7.02) must keep skeleton activation rows so success/error counts remain exact even when full sampling is off.
- Risk: alert thresholds are noisy on low-traffic tenants; mitigate with a minimum traffic floor (`vector(0) or rate(cs_invocations_total[5m]) > 0.05`) and document the trade-off.
- Risk: clock skew between Prometheus and the cluster shifts 5m windows; recommend `evaluation_interval: 30s` and document in the runbook.
- External: operators bring their own Alertmanager — receivers and routing are out of scope (templates only).

## Out of scope
- Alertmanager receiver configuration (PagerDuty, Slack, email).
- Per-tenant SLOs beyond the platform-level targets in `docs/02-requirements.md`.
- Cost / capacity SLOs (covered separately under `docs/26-capacity-and-limits.md`).
- Auto-remediation actions on burn-rate breach.
