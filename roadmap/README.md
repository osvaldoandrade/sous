# `code-sous` product roadmap

This folder contains the live product roadmap for `code-sous` — a function-execution layer for AI agents. The roadmap is organized into **8 epics across 3 phases (Now / Next / Later)**. Each epic and each task is mirrored as a GitHub issue so progress is tracked publicly.

> Source of truth: the GitHub issues. The Markdown files in this folder are written-once specifications; the issues are where status moves.

## How to read this

- **Epic** = a coherent body of work spanning ~3–4 tasks, owned end-to-end. Has the `epic` + `roadmap` labels.
- **Task** = an implementable unit (issue-sized, usually 1–2 weeks of work). Has the `roadmap` + `enhancement` labels and a "Parent epic" pointer in its body.
- **Phase** =
  - **Now** (0–2 months) — finish v0.1, lock the contract.
  - **Next** (2–5 months) — expand the surface (runtimes, triggers, packaging).
  - **Later** (5–9 months) — platform depth (security, observability, workflows).

## The epics

### NOW — v0.1 GA readiness

| Code | Epic | Tasks | Folder |
|------|------|-------|--------|
| [E1](https://github.com/osvaldoandrade/sous/issues/16) | v0.1 core hardening & GA readiness | [#1](https://github.com/osvaldoandrade/sous/issues/1) · [#3](https://github.com/osvaldoandrade/sous/issues/3) · [#5](https://github.com/osvaldoandrade/sous/issues/5) · [#8](https://github.com/osvaldoandrade/sous/issues/8) | [`e1-v01-hardening/`](e1-v01-hardening/) |
| [E2](https://github.com/osvaldoandrade/sous/issues/17) | Developer experience & CLI polish | [#2](https://github.com/osvaldoandrade/sous/issues/2) · [#6](https://github.com/osvaldoandrade/sous/issues/6) · [#9](https://github.com/osvaldoandrade/sous/issues/9) | [`e2-dx-cli/`](e2-dx-cli/) |

### NEXT — expand the surface

| Code | Epic | Tasks | Folder |
|------|------|-------|--------|
| [E3](https://github.com/osvaldoandrade/sous/issues/22) | Runtime expansion: Python + WASM | [#11](https://github.com/osvaldoandrade/sous/issues/11) · [#13](https://github.com/osvaldoandrade/sous/issues/13) · [#18](https://github.com/osvaldoandrade/sous/issues/18) · [#20](https://github.com/osvaldoandrade/sous/issues/20) | [`e3-runtimes/`](e3-runtimes/) |
| [E4](https://github.com/osvaldoandrade/sous/issues/19) | Trigger expansion: cron + event | [#4](https://github.com/osvaldoandrade/sous/issues/4) · [#7](https://github.com/osvaldoandrade/sous/issues/7) · [#10](https://github.com/osvaldoandrade/sous/issues/10) | [`e4-triggers/`](e4-triggers/) |
| [E5](https://github.com/osvaldoandrade/sous/issues/21) | Packaging, dependencies & supply chain | [#12](https://github.com/osvaldoandrade/sous/issues/12) · [#14](https://github.com/osvaldoandrade/sous/issues/14) · [#15](https://github.com/osvaldoandrade/sous/issues/15) | [`e5-packaging/`](e5-packaging/) |

### LATER — platform depth

| Code | Epic | Tasks | Folder |
|------|------|-------|--------|
| [E6](https://github.com/osvaldoandrade/sous/issues/30) | Security hardening | [#23](https://github.com/osvaldoandrade/sous/issues/23) · [#24](https://github.com/osvaldoandrade/sous/issues/24) · [#25](https://github.com/osvaldoandrade/sous/issues/25) | [`e6-security/`](e6-security/) |
| [E7](https://github.com/osvaldoandrade/sous/issues/38) | Observability & operations | [#31](https://github.com/osvaldoandrade/sous/issues/31) · [#34](https://github.com/osvaldoandrade/sous/issues/34) · [#35](https://github.com/osvaldoandrade/sous/issues/35) · [#36](https://github.com/osvaldoandrade/sous/issues/36) | [`e7-observability/`](e7-observability/) |
| [E8](https://github.com/osvaldoandrade/sous/issues/37) | Cadence workflow depth | [#26](https://github.com/osvaldoandrade/sous/issues/26) · [#28](https://github.com/osvaldoandrade/sous/issues/28) · [#32](https://github.com/osvaldoandrade/sous/issues/32) | [`e8-cadence/`](e8-cadence/) |

**Totals:** 8 epics, 27 tasks, 35 tracked issues. Filter on GitHub with `is:issue label:roadmap`.

## Bigger themes the roadmap is solving for

1. **From v0.0.0-dev to a versioned GA**, so external agents can rely on the contract (E1, E2).
2. **Polyglot execution without breaking runtime parity** — Python and WASM join JS under the same capability model (E3).
3. **Cover the missing trigger types** — cron and event subscriptions — and give every trigger a real retry/DLQ story (E4).
4. **Treat function bundles like software you ship** — dependencies, signatures, SBOMs (E5).
5. **Move from "sandbox blocks egress" to enterprise-grade security posture** — external secrets, destination-level egress control, audit trail (E6).
6. **Make agent-tree workloads debuggable at scale** — trace whole call trees, control activation volume with sampling, ship dashboards and SLOs that match the 99.9% targets (E7).
7. **Let agents author workflows, not just activities**, with replay-determinism enforced at publish (E8).

## Out of scope for this roadmap (deliberately deferred)

- Billing / metering / quotas-as-product (separate commercial work).
- Marketplace / function-sharing across tenants (depends on E5 signing + provenance).
- Multi-region active/active control plane (depends on E1 contract lock + E7 SLO baseline).
- GUI dashboard for non-agent users (CLI + API are first-class; GUI follows demand).

## Process

- Anything labeled `roadmap` is on this roadmap. Anything not labeled `roadmap` is fair-game but not committed to.
- The MD spec in this folder is written-once at planning time. As the implementation progresses, the **GitHub issue body** is the authoritative status — comments, sub-checkboxes, scope changes go there.
- When an epic completes, close its issue. The task issues should auto-close as their checkboxes are ticked on the epic.
- New work that doesn't fit any epic gets filed as an issue with the `enhancement` label first; if it grows, we promote it to a roadmap task and add the `roadmap` label.

## See also

- [`/docs/18-roadmap.md`](../docs/18-roadmap.md) — the original bullet list this roadmap supersedes.
- [`/docs/02-requirements.md`](../docs/02-requirements.md) — product invariants every task must respect.
- [`/docs/03-architecture.md`](../docs/03-architecture.md) — system boundaries.
