# Operators: Resources

Operational artifacts live alongside the source under `deploy/` and `roadmap/`. This page maps each directory to the operator decisions it supports, so an on-call engineer can find the right file without grepping the repository.

## deploy/observability

`deploy/observability/` ships the declarative observability stack: SLO definitions, Prometheus alert rules, and reference Grafana dashboards. The directory is intentionally declarative so operators can wire it into an existing monitoring deployment without touching Go.

- `slo.yaml` — declarative source of truth for the platform SLOs. Names the SLI queries, objectives, windows, exclusions, and the multi-window multi-burn-rate (MWMBR) tiers. Operators treat it as the authoritative reference and update it before any dashboard or alert change. See [Operators Observability](Operators-Observability).
- `alerts.rules.yaml` — Prometheus rule file with MWMBR burn-rate alerts per SLO. Operators load it into Prometheus through `rule_files:` and reload. Each alert carries `severity`, `slo`, `service`, `tier`, and `runbook_url` labels so Alertmanager can route into the right escalation channel.
- `control-plane.json` — reference Grafana dashboard for `cs-control` (API rate / latency, publish & alias, error-budget burn).
- `execution.json` — reference Grafana dashboard for the execution plane (per-tenant invocations, latency, errors, invoker inflight, queue lag, cold starts, cache occupancy, top-N functions).
- `README.md` — import procedures, scrape configuration, validation commands.

Validation lives behind two Make targets: `make slo-validate` parses the YAML and (when `promtool` is present) checks the rules, and `make dashboards-validate` JSON-parses every dashboard.

## deploy/sous-deploy-template

`deploy/sous-deploy-template/` is the reference template for building and deploying Sous from source. It contains the canonical GitHub Actions workflow (`workflow-build-deploy.yml`) that builds every binary, publishes the container images, and rolls out the Helm release. Operators that run a fork copy this directory into their own repo and adapt the secrets and the target cluster.

- `README.md` — narrative walkthrough of the template, including the required GitHub secrets and the expected container registry.
- `workflow-build-deploy.yml` — the workflow itself. Designed to be drop-in: the workflow expects the Helm chart in `deploy/helm/code-sous/` and the source tree intact.

The template is operator-facing tooling. It is not on the binary release artifact, so operators upgrading Sous do not need to update it; they update it only when their deployment pipeline itself changes.

## deploy/helm/code-sous

`deploy/helm/code-sous/` is the Helm chart that packages every Sous binary into a Kubernetes release. The chart renders one Deployment per binary, the Service definitions that expose the metrics and HTTP endpoints, the ConfigMap that carries the YAML configuration, the Secret that carries token material, and the leader-election Lease used by `cs-scheduler`. The chart values feed directly into the YAML schema documented in [Operators Configuration Reference](Operators-Configuration-Reference); operators set Helm values rather than hand-editing a ConfigMap.

The chart also renders the `cs-migrate` Job template documented in [Operators Migrations](Operators-Migrations) and pins every container image by digest so a rollback to a previous chart version is byte-reproducible. See [Deployment Kubernetes](Deployment-Kubernetes) for the end-to-end install procedure.

## roadmap

`roadmap/` collects forward-looking plans for each release epic. Each subdirectory captures the acceptance criteria, the design notes, and the in-flight implementation status for one epic. Operators consult the roadmap when planning capacity for a future feature, evaluating whether a tenant request fits an existing initiative, or scoping a custom contribution.

- `roadmap/e1-v01-hardening/` — control-plane and data-plane reliability work.
- `roadmap/e2-dx-cli/` — developer-experience and CLI evolution.
- `roadmap/e3-runtimes/` — runtime additions beyond `cs-js`.
- `roadmap/e4-triggers/` — trigger types beyond HTTP.
- `roadmap/e5-packaging/` — bundle packaging, signing, SBOM, import maps.
- `roadmap/e6-security/` — egress policies, secret providers, audit sinks.
- `roadmap/e7-observability/` — activation sampling, agent decision-tree tracing.
- `roadmap/e8-cadence/` — Cadence integration evolution.

The roadmap is not a release schedule; it is the design corpus that backs each shipped feature. The aggregate, operator-facing roadmap view lives in [Reference Roadmap](Reference-Roadmap).

## Cross-references

- [Operators Configuration Reference](Operators-Configuration-Reference) — YAML knobs surfaced by the Helm chart.
- [Operators Observability](Operators-Observability) — narrative behind `deploy/observability/`.
- [Operators Runbooks](Operators-Runbooks) — incident playbooks that consume the alerts and dashboards.
- [Operators Capacity and Limits](Operators-Capacity-and-Limits) — sizing recommendations applied through the chart values.
- [Operators Migrations](Operators-Migrations) — the `cs-migrate` job that ships with the chart.
- [Deployment Kubernetes](Deployment-Kubernetes) — the end-to-end install procedure.
- [Reference Roadmap](Reference-Roadmap) — the operator-facing roadmap view.
