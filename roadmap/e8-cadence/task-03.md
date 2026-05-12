# Replay-determinism harness — catch nondeterministic workflow code at publish time

**Parent epic:** #37
**Phase:** Later
**Estimated size:** L

## Problem
Once Task 1 ships DecisionTask support, `cs` users will write workflow functions that Cadence will replay on every history page. Nondeterministic code (`Date.now()`, `Math.random()`, direct network/IO, environment reads, unsorted map iteration) silently corrupts replay state and causes nondeterministic-history failures hours or days after publish. We need a static + replay-based check wired into `cs-control` so violations block `cs fn publish` before the bad code ever reaches production.

## Proposed solution
- Build `internal/runtime/determinism` with a static analyzer that walks the bundle's JS AST (reusing the `cs-js` parser surfaced from `internal/runtime`) and flags a configurable list of banned identifiers/expressions in any function whose manifest declares `cadence.kind: "workflow"`. Defaults: `Date.now`, `Date()` constructor, `Math.random`, `crypto.randomUUID`, `process.env`, `fetch`, `require('http' | 'https' | 'net' | 'fs')`. Each violation has a stable code (e.g. `DET001_DATE_NOW`) and source location.
- Add a replay harness: given a recorded `history.json` (the same shape returned by `internal/cadence.PollDecisionTask` from Task 1) and a workflow bundle, run the workflow twice in-process via `cs-js` and assert the emitted Decision sequence is byte-identical across both runs. Diverging decisions are reported with the first divergent step and Decision payload.
- Wire both checks into `cmd/cs-control` on the publish path (`POST /v1/functions/{name}/versions`): for workflow functions, run static analysis on the uploaded draft, then if a `replay_fixture` is attached to the publish request, run the replay harness. Either failure rejects publish with HTTP 422 and `error.code = "determinism_violation"` plus a structured `violations[]` body.
- Add a `cs fn check-determinism --history <path>` CLI command in `cmd/cs-cli` that runs the same harness locally so authors can validate before they publish; the CLI shares the analyzer with `cs-control` to keep behavior identical.
- Add fixtures under `test/determinism/` containing (a) a deterministic workflow + history that must pass, (b) a `Date.now`-using workflow that must fail static analysis, and (c) a workflow with a conditional branch on system entropy that passes static analysis but fails replay. Hook them into `make test-determinism`.
- Document the violation codes, the publish-path contract, and the override knob (`--unsafe-allow-determinism-violations` for emergencies, gated by Tikti role `role:workflow-admin`) in `docs/12-cadence-integration.md` and `docs/15-security.md`.

## Acceptance criteria
- [ ] `internal/runtime/determinism` static analyzer detects at least the seven banned APIs listed above, reports stable codes + source locations, and has table-driven tests with positive and negative cases.
- [ ] Replay harness re-runs a workflow against a recorded history and reports the first divergent Decision; fixture-based test in `test/determinism/` proves both byte-identical pass and a forced-divergence fail.
- [ ] `cs-control` publish path rejects a workflow-kind draft that fails static analysis with HTTP 422, `error.code = "determinism_violation"`, and a `violations[]` array; covered by a handler test in `cmd/cs-control/main_test.go`.
- [ ] `cs fn check-determinism` CLI command exists, exits non-zero on violation, prints human-readable + `--json` output, and is documented in `docs/05-cli.md`.
- [ ] `docs/12-cadence-integration.md` lists every violation code with example, remediation, and links to `docs/15-security.md` for the override role.

## Dependencies & risks
- Hard-depends on Task 1 (the publish path only knows a function is "a workflow" once the manifest carries `cadence.kind: "workflow"`); without Task 1 there is nothing to block.
- Soft-depends on Task 2 only insofar as the replay harness must decode history payloads with the configured codec; until Task 2 lands, the harness assumes JSON.
- Risk: false positives from static analysis (e.g. `Date.now` inside a `cs.workflow.sideEffect` block is legal). Mitigation: the analyzer skips banned-API checks inside calls to `cs.workflow.sideEffect`, and the override role + flag are available for emergencies.
- Risk: replay performance — a long history could push publish latency over CI budgets. Mitigation: cap replay at 10k events by default, and document the knob in `docs/26-capacity-and-limits.md`.

## Out of scope
- Runtime detection of nondeterminism in production (this task is publish-time only; runtime drift detection is a separate observability item).
- Auto-rewriting nondeterministic code to deterministic equivalents.
- Coverage of TypeScript-specific syntax beyond what the existing `cs-js` parser already accepts.
- Cross-language workflow determinism (only `cs-js` workflows are in scope; future runtimes will need their own analyzers).
