# Observability deployment artifacts

This directory holds the operator-facing observability templates for
`code-sous`: SLO definitions, Prometheus burn-rate alert rules, and
bundled Grafana dashboards. The files are intentionally declarative and
self-contained so a fresh deployment can wire them into an existing
monitoring stack without touching Go code.

## Files

| File | Purpose |
| --- | --- |
| `slo.yaml` | Declarative source of truth for the platform SLOs (objective, window, SLI query, exclusions, MWMBR tiers). |
| `alerts.rules.yaml` | Prometheus rule file with multi-window multi-burn-rate (MWMBR) alerts per SLO. Load this into Prometheus. |
| `control-plane.json` | Reference Grafana dashboard for the cs-control surface (API rate / latency, publish & alias, 30d error-budget burn). |
| `execution.json` | Reference Grafana dashboard for the execution plane (per-tenant invocation rate / latency / errors, invoker inflight, queue lag, cold starts, cache occupancy, top-N functions). |

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

## Grafana dashboards (`control-plane.json`, `execution.json`)

| File                 | Dashboard title              | UID                  | Audience                              |
|----------------------|------------------------------|----------------------|---------------------------------------|
| `control-plane.json` | code-sous / Control plane    | `cs-control-plane`   | API SLO, publish/alias, error budget  |
| `execution.json`     | code-sous / Execution plane  | `cs-execution-plane` | Per-tenant invocation traffic & latency |

Both dashboards are bundled as Grafana 10+ JSON (`schemaVersion: 38`) and
carry a top-level `cs_dashboard_version` semver field so operators can
detect drift after re-import. Bump the field whenever a panel PromQL
expression or template-variable definition changes.

### Prometheus scrape requirements

The dashboards assume a Prometheus that scrapes the `/metrics` endpoint on
every `cs-*` binary. A minimal `scrape_configs` block:

```yaml
scrape_configs:
  - job_name: cs-control
    metrics_path: /metrics
    static_configs:
      - targets: ['cs-control:8080']
  - job_name: cs-http-gateway
    metrics_path: /metrics
    static_configs:
      - targets: ['cs-http-gateway:8081']
  - job_name: cs-invoker-pool
    metrics_path: /metrics
    static_configs:
      - targets: ['cs-invoker-pool:8082']
  - job_name: cs-scheduler
    metrics_path: /metrics
    static_configs:
      - targets: ['cs-scheduler:8083']
  - job_name: cs-cadence-poller
    metrics_path: /metrics
    static_configs:
      - targets: ['cs-cadence-poller:8084']
```

For Kubernetes, pair the bundled Helm chart in `deploy/helm/code-sous`
with `ServiceMonitor` resources that target each `cs-*` Service on the
`/metrics` path.

### Template variables

Both dashboards expose:

- `$datasource` — Prometheus datasource selector. Defaults to the first
  datasource of type `prometheus`.
- `$tenant` — multi-select, sourced from
  `label_values(cs_invocations_total, tenant)`. Choose `All` to compare
  across the cluster, or pick a subset to drill into a specific customer.

The execution-plane dashboard additionally filters every per-tenant
query through `tenant=~"$tenant"`, so a focused tenant view does not
require editing panels.

### Importing dashboards

#### Grafana UI

1. Navigate to **Dashboards -> New -> Import**.
2. Upload `control-plane.json` (or `execution.json`).
3. On the import screen pick the target Prometheus datasource.
4. Click **Import**.
5. Repeat for the second dashboard.

#### `grafana-cli` / HTTP API

`grafana-cli` itself does not import dashboards (it is for plugins). Use
the HTTP API instead:

```bash
GRAFANA_URL=http://grafana.example.com
GRAFANA_TOKEN=...  # an API key with Editor permissions

for f in deploy/observability/control-plane.json deploy/observability/execution.json; do
  jq --arg overwrite true \
    '{dashboard: ., overwrite: ($overwrite == "true"), folderUid: ""}' "$f" \
    | curl -fsS -X POST "$GRAFANA_URL/api/dashboards/db" \
        -H "Authorization: Bearer $GRAFANA_TOKEN" \
        -H "Content-Type: application/json" \
        --data-binary @-
done
```

The `overwrite: true` flag makes the loop idempotent: an existing
dashboard at the same UID is updated in place rather than rejected.

#### Kubernetes provisioning (sidecar ConfigMap)

The Grafana sidecar pattern (`grafana/helm-charts` chart, or any
deployment with `--watch-folder=/var/lib/grafana/dashboards`) picks up
dashboards from `ConfigMap` objects labelled `grafana_dashboard=1`.
Create one ConfigMap per file:

```bash
kubectl create configmap cs-dashboard-control-plane \
  --from-file=control-plane.json=deploy/observability/control-plane.json \
  --namespace monitoring
kubectl label configmap cs-dashboard-control-plane \
  grafana_dashboard=1 --namespace monitoring

kubectl create configmap cs-dashboard-execution \
  --from-file=execution.json=deploy/observability/execution.json \
  --namespace monitoring
kubectl label configmap cs-dashboard-execution \
  grafana_dashboard=1 --namespace monitoring
```

The sidecar mounts the JSON into `/var/lib/grafana/dashboards` and
Grafana provisions them on the next sync interval (default 60s).

### Validation

Run the bundled JSON syntax check from the repo root:

```bash
make dashboards-validate
```

The target parses every `.json` under `deploy/observability/` with
`python3 -c 'import json; json.load(open(...))'` and exits non-zero on
the first failure. `make slo-validate` covers the YAML files.

### Minimum versions

- Grafana 10.0+ (dashboard schema version 38).
- Prometheus 2.30+ (the queries rely on `histogram_quantile`,
  `clamp_min`, and the multi-value template `$tenant` regex behaviour).

### Updating dashboards

1. Edit the JSON file. Keep `schemaVersion` at 38 unless you have
   re-tested panels against the new Grafana release.
2. Bump `cs_dashboard_version` (semver). Patch for cosmetic, minor for
   new panels, major for breaking PromQL or variable renames.
3. Run `make dashboards-validate` and `go test ./...`.
4. Update the `Reference dashboards` section in
   `docs/14-observability.md` if you add panels or change variables.
