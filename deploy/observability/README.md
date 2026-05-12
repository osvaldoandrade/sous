# Observability deployment artifacts

This directory holds the operator-facing observability templates for
`code-sous`: SLO definitions and Prometheus burn-rate alert rules. The
files are intentionally declarative and self-contained so a fresh
deployment can wire them into an existing monitoring stack without
touching Go code.

## Files

| File | Purpose |
| --- | --- |
| `slo.yaml` | Declarative source of truth for the platform SLOs (objective, window, SLI query, exclusions, MWMBR tiers). |
| `alerts.rules.yaml` | Prometheus rule file with multi-window multi-burn-rate (MWMBR) alerts per SLO. Load this into Prometheus. |

The canonical SLO targets are defined in `docs/02-requirements.md`
("Availability targets"). The narrative section that backs these files
lives in `docs/14-observability.md` ("SLO reporting" and "Burn-rate
alerting").

## SLO burn-rate alerts (`alerts.rules.yaml`)

`alerts.rules.yaml` follows the Google SRE Workbook multi-window
multi-burn-rate pattern. Each SLO produces four alerts: two PAGE alerts
(fast / slow) and two TICKET alerts (fast / slow). Each alert pairs a
long-window burn rate with a short-window confirmation so the alert
ignores stale spikes that have already recovered.

| Action | Long window | Burn rate | Short window |
| --- | --- | --- | --- |
| PAGE | 1h | 14.4x | 5m |
| PAGE | 6h | 6x | 30m |
| TICKET | 24h | 3x | 2h |
| TICKET | 3d | 1x | 6h |

The burn rate is `(1 - SLI) / (1 - objective)`. For the 99.9% SLOs the
budget is `0.001`, so a "14.4x burn" means the failure ratio exceeds
`14.4 * 0.001 = 0.0144`. For the 99% latency SLO the budget is `0.01`.

### Loading the rules into Prometheus

1. Copy `alerts.rules.yaml` next to your other Prometheus rule files
   (for example `/etc/prometheus/rules/cs-slo-alerts.yaml`).
2. Reference the file from `prometheus.yml`:

   ```yaml
   rule_files:
     - rules/cs-slo-alerts.yaml
   ```

3. Reload Prometheus (`SIGHUP` or `POST /-/reload`).
4. Validate locally before rolling out:

   ```bash
   promtool check rules deploy/observability/alerts.rules.yaml
   ```

5. To run the lightweight YAML parse plus optional `promtool` check that
   ships with the repo, run from the repo root:

   ```bash
   make slo-validate
   ```

Alertmanager receiver wiring (PagerDuty, Slack, e-mail) is intentionally
out of scope for these templates. Operators bring their own routing
configuration; the alerts expose `severity`, `slo`, `service`, `tier`,
and `runbook` labels so routing rules can match cleanly.

### Editing SLOs

Treat `slo.yaml` as the source of truth. When you change an objective,
window, or SLI query in `slo.yaml`, update the corresponding burn-rate
expressions in `alerts.rules.yaml` so the two files stay aligned. A
follow-up task tracks rendering `alerts.rules.yaml` from `slo.yaml`
automatically; until then the two files are hand-kept in sync.

### Metric dependencies

The alert expressions reference the metrics catalogued in
`docs/14-observability.md`:

- `cs_api_requests_total` (control-plane availability) — emitted by
  `cs-control` and `cs-http-gateway`.
- `cs_invocations_total` (data-plane availability) — emitted by
  `cs-invoker-pool`.
- `cs_invocation_duration_ms_bucket` (data-plane latency) — emitted by
  `cs-invoker-pool` / `cs-http-gateway`.

If any of these series are not yet exported by your build, the
corresponding alert group will simply not fire (Prometheus evaluates
absent series as no-data). Re-enable each group once the emitter lands.
