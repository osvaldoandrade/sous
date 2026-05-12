# Epic: Developer experience & CLI polish

**Phase:** Now (0–2 months)
**Theme:** Make the `cs` CLI feel sharp and self-teaching.

## Why
Functions are how agents close a task loop (see `docs/02-requirements.md` "Product
intent"). The CLI is the only surface they touch in steps 3–5 of that loop, so its
ergonomics directly govern first-time success rate and agent-driven adoption. Today
`cs fn init` emits a single hard-coded handler, errors lack remediation hints, and
there is no way to tail activation logs without hand-crafting HTTP calls. Closing
those gaps gets a new user (human or agent) from `cs auth login` to a successful
invocation in under five minutes, and lets agents self-recover from common
misconfigurations.

## Scope
- Built-in scaffolding library exposed through `cs fn init --template <name>` with
  embedded templates for HTTP handler, scheduled job, Cadence activity, and codeQ
  consumer patterns.
- Typed, actionable CLI errors (`cause:` + `next step:`) plus a `cs doctor`
  subcommand that probes auth, control-plane reachability, runtime parity, config
  sanity, and clock skew in one pass.
- `cs fn logs --function <ref> --follow` streaming view backed by the existing
  per-activation log API, with `json`/`pretty`/`compact` outputs and time-range
  filtering.
- Documentation refresh across `docs/05-cli.md`, `docs/21-errors.md`, and
  `docs/24-runbooks.md` to mirror the new commands and error shapes.

## Outcomes / success metrics
- Time-to-first-invocation (fresh clone → successful `cs fn invoke`) drops below
  five minutes for both `http-handler` and `scheduled-job` templates.
- First-command success rate (no error on `cs fn init` → `cs fn test`) for each
  bundled template is 100% on a clean checkout.
- Every CLI error includes a `next step:` line; manual triage time on common
  failure modes (control-plane down, missing auth) drops to near-zero because
  `cs doctor` names the failing check.
- Agents can tail logs without inventing curl commands: ≥1 documented runbook in
  `docs/24-runbooks.md` uses `cs fn logs --follow`.

## Tasks
- [ ] #2 — `cs fn template` library
- [ ] #6 — Diagnostic CLI errors + `cs doctor`
- [ ] #9 — `cs fn logs` streaming view

## Non-goals
- Rewriting the CLI in a TUI framework or adopting Cobra/Kong — keep the existing
  `flag` package usage.
- User-supplied / remote templates (filesystem or Git-hosted scaffolds).
- Server-side push for logs (WebSocket / SSE); polling-only in this epic.
- Language runtimes beyond `cs-js`; the templates and `doctor` parity check target
  the current Goja runtime only.
- Telemetry / phone-home from `cs doctor`.
