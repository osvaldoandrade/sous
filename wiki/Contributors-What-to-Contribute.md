# What to contribute

Sous is planned through an epic-and-task roadmap rather than an ad-hoc backlog.

The `roadmap/` folder at the repo root organises committed work into eight epics (E1 through E8) spread across three phases.
The phases are **Now** (0–2 months, v0.1 GA readiness), **Next** (2–5 months, surface expansion), and **Later** (5–9 months, platform depth).
Each epic has its own folder with an `epic.md` overview and one `task-NN.md` per implementable unit.
Both the epic and each task are mirrored as GitHub issues, which are the source of truth for status.

Contributors looking for somewhere to start should orient by phase.

The Now phase is about locking the contract.
That covers the wire format, the lifecycle CRUD surface, idempotency, rate limits, and the CLI polish that an external caller will depend on after v0.1 GA.

The Next phase widens the platform.
More runtimes, more triggers, and the packaging story (dependencies, signatures, SBOMs) all land here.

The Later phase is platform depth.
Security hardening, observability and SLOs, and workflow expressiveness in Cadence are the three axes.

The roadmap README at `roadmap/README.md` carries the canonical table and the GitHub issue links.
This page focuses on where contributors can plug in.

## The eight epics

Each epic is keyed by a code (`E1`..`E8`) and has a folder under `roadmap/`.
The status sketched below is current as of the repository's `git log` history.
For ground truth, check the linked GitHub issues on the epic — new tasks may have landed since.

### E1 — v0.1 core hardening & GA readiness

Folder: `roadmap/e1-v01-hardening/`.

E1 locks the v0.1 contract.
The tasks are lifecycle CRUD contract tests, activation TTL and size caps, idempotency keys and dedup across HTTP / codeQ / Cadence, and per-tenant rate limits with inflight semaphores.

E1 is largely landed in the Now phase.
Every recent commit prefixed `E1.0x:` belongs here.
Remaining work tends to be hardening, additional edge-case tests, and contract documentation.

### E2 — Developer experience & CLI polish

Folder: `roadmap/e2-dx-cli/`.

E2 owns the `cs` CLI surface.
Typed errors, the `cs doctor` subcommand, `cs fn logs --follow`, and template scaffolding are the headline tasks.

Most of E2 has shipped.
Remaining work is incremental — more templates, better diagnostic messages, deeper `cs doctor` checks.

### E3 — Runtime expansion: Python and WASM

Folder: `roadmap/e3-runtimes/`.

E3 brings polyglot execution under the same capability model.
The tasks are the cs-python subprocess adapter (E3.01), the cs-wasm wazero adapter (E3.02), the manifest `runtime` field plus registry slot (E3.03), and the cross-runtime parity harness (E3.04).

E3 is in flight.
cs-python landed in PR #77 and cs-wasm in PR #69.
Future work covers more parity fixtures and the embedded-CPython upgrade path.

See [Contributors: Adding a Runtime](Contributors-Adding-a-Runtime) for the bar a new adapter must meet.

### E4 — Trigger expansion: cron and event

Folder: `roadmap/e4-triggers/`.

E4 fills in the missing trigger types.
The tasks are the cron schedule trigger with timezone and jitter (E4.01), the codeQ subscription trigger (E4.02), and the retry-and-DLQ story for every trigger (E4.03).

Mostly shipped.
Open work is around deeper backoff strategies and DLQ replay tooling.

### E5 — Packaging, dependencies, and supply chain

Folder: `roadmap/e5-packaging/`.

E5 treats function bundles like software you ship.
The tasks are JS dependency bundles with frozen import maps (E5.01), ed25519 signed bundles with tenant keys (E5.02), and per-version SBOMs in CycloneDX 1.5 (E5.03).

E5 is in flight.
Most of the core has landed.
The area is open for hardening, additional package managers, and reproducibility work.

### E6 — Security hardening

Folder: `roadmap/e6-security/`.

E6 moves the platform from "sandbox blocks egress" to enterprise-grade security posture.
The tasks are the secrets provider plugin with memory and Vault adapters (E6.01), per-tenant egress allowlists enforced in `bindHTTP` (E6.02), and the audit log stream for control-plane mutations (E6.03).

E6 is in flight.
The foundations are in place.
Deeper destination-level egress control, richer secret backends, and additional audit event types remain.

### E7 — Observability and operations

Folder: `roadmap/e7-observability/`.

E7 makes agent-tree workloads debuggable at scale.
The tasks are per-activation parent / root linkage and a tree endpoint (E7.01), head / tail / probabilistic activation sampling (E7.02), bundled Grafana dashboards for control and execution planes (E7.03), and SLO definitions with multi-burn-rate alert templates (E7.04).

E7's foundations have shipped.
Ongoing work is in dashboard panels, alert rules, and trace propagation depth.

### E8 — Cadence workflow depth

Folder: `roadmap/e8-cadence/`.

E8 lets agents author workflows, not just activities.
The tasks are the DecisionTask MVP for schedule-activity workflows (E8.01), per-tasklist Cadence payload codecs (E8.02), and the publish-time static determinism linter (E8.03).

E8 is in flight.
Workflow expressiveness, replay coverage, and timer / signal support are open.

## Out-of-scope items

The roadmap README also tracks deliberate out-of-scope items.

Billing, metering, and quotas-as-product are separate commercial work.
A marketplace for function sharing across tenants depends on E5 signing and provenance.
A multi-region active/active control plane depends on E1 contract lock and E7 SLO baseline.
A GUI dashboard for non-agent users waits until CLI and API demand justifies it.

Proposals in those areas are welcome but will likely be deferred until their dependencies land.

## Status: in flight, open, and landed

GitHub issues drive status.

Each task in `roadmap/` has a numbered issue (`#1`..`#36+`).
Each epic has its own issue (`#16`..`#38`).

The canonical filter for in-flight work is `is:issue is:open label:roadmap` on the GitHub repository.
The roadmap README maintains the mapping from task code to issue number.

Commits on `main` are the second status signal.
Every roadmap-task commit follows the `E{epic}.{task}: {summary} (#PR)` convention — see [Contributors: Resources](Contributors-Resources).
That convention makes `git log --oneline` a quick scan of what has shipped.
Cross-reference the open issue list against the recent log to find tasks that have landed but are not yet closed, or tasks that are coded but not yet merged.

When a task closes, the epic checkbox auto-ticks.
When every checkbox in an epic is ticked, the epic issue closes too.
The lifecycle is mechanical — the roadmap stays in sync without human bookkeeping.

## Good-first contributions

Not every contribution needs to be a roadmap task.

The classes below are well-scoped.
They do not require deep platform context.
They meaningfully improve the system.
They are also a good way to learn the codebase before taking on a roadmap task.

### New parity fixtures

The cross-runtime parity harness in `internal/runtime/parity` consumes JSON fixtures from `test/parity/fixtures/`.

Today the corpus has five fixtures: `simple-echo`, `kv-roundtrip`, `log-emission`, `capability-denied`, and `timeout`.

Adding a fixture is a single JSON file plus the assertions in the fixture body.
Every registered runtime is automatically exercised against it.
New fixtures lock parity behaviour and pay for themselves the next time a runtime adapter regresses.

See [Testing](Testing) for the fixture format.

### New sample apps

The CLI ships templates surfaced through `cs init`.

A well-shaped sample is a self-contained PR.
Candidates include an HTTP handler that calls KV, a cron-triggered job that publishes to codeQ, and a Cadence activity that returns a typed payload.
Samples live alongside the template registry in `cmd/cs-cli/`.

### New error-code documentation

Every typed error lives in `internal/errors/`.

Each code has a canonical description, an example trigger, and a remediation path.
Documenting an under-covered code in [Error Model](Error-Model) is high-leverage and requires no platform changes.

### New dashboard panels

`deploy/observability/control-plane.json` and `deploy/observability/execution.json` ship Grafana dashboards under E7.03.

Adding a panel that covers a gap is a small JSON edit plus a screenshot in the PR.
Useful gap candidates: a saturation view of the inflight semaphore, a histogram of activation tail latency by runtime, an error-rate breakdown by typed error code, an SLO burn-rate strip per service.

### New SLO or alert rule

`deploy/observability/slo.yaml` defines the SLO targets.
`deploy/observability/alerts.rules.yaml` defines the multi-burn-rate alert templates.

Tightening a target or adding an alert for an uncovered failure mode is a self-contained change.
Run `make slo-validate` and `make dashboards-validate` locally before pushing.

### Runbooks for incident shapes

[Runbooks](Runbooks) collects the operator-side playbooks.

New runbooks for incident shapes that the team has hit but not yet written up are immediately useful.
A runbook does not need to be long.
"Symptom, hypothesis, check, fix, escalate" in five paragraphs is the right shape.

## How to take on a roadmap task

When you find a task you want to drive, comment on the GitHub issue first.

A maintainer will assign it.
The task's `task-NN.md` file in the epic folder becomes your written brief.

Branch from `main`, follow the [Contributors: Resources](Contributors-Resources) flow, and tag your commits with the `E{epic}.{task}:` prefix so the log stays scannable.

For broader context — how the project is structured, where each binary lives, how the wiring fits together — see [Contributors: Project Structure](Contributors-Project-Structure).
For the rendered roadmap and its themes, see [Roadmap](Roadmap).
