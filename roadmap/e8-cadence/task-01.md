# DecisionTask support — author Cadence workflows in `cs` functions

**Parent epic:** #37
**Phase:** Later
**Estimated size:** L

## Problem
Today `cmd/cs-cadence-poller` only long-polls `PollActivityTask` and routes Activities into `cs-invoker-pool` (see `docs/12-cadence-integration.md` "Cadence scope in v0.1" — DecisionTasks are explicitly out of scope). That means agents using `cs` can implement Activity workers but cannot author the orchestrating Workflow itself in a `cs` function. Workflow authorship still requires a separate Go/Java/TypeScript Cadence SDK process, which breaks the "everything is a `cs` function" promise for agentic orchestration.

## Proposed solution
- Extend `internal/cadence` (`client.go`) with a `PollDecisionTask`, `RespondDecisionTaskCompleted`, and `RespondDecisionTaskFailed` set of methods alongside the existing Activity surface; mirror the Activity-task struct with a `DecisionTask` struct carrying `history`, `previousStartedEventID`, `startedEventID`, and `nextPageToken`.
- In `cmd/cs-cadence-poller/main.go`, add a `decisionPollLoop` parallel to `pollLoop` driven by a new `binding.Pollers.Decision` count. Decision tasks are dispatched onto `cs-invoker-pool` with a new `Trigger.Type = "cadence.decision"` carrying the history bytes plus a deterministic `runId/workflowId/attempt` triple.
- Add a workflow host-call surface in `internal/runtime` (`runner.go`) exposing `cs.workflow.startActivity`, `cs.workflow.timer`, `cs.workflow.signal`, `cs.workflow.continueAsNew`, `cs.workflow.sideEffect`, and `cs.workflow.now` to `cs-js`. Each call returns a placeholder Future ID and emits a Decision into the per-activation decision buffer; the buffer is flushed to `RespondDecisionTaskCompleted` when the function returns.
- Extend the manifest schema (`internal/bundle` + the `cs init` template in `cmd/cs-cli`) with `cadence: { kind: "workflow" | "activity", tasklist: "..." }`. `cs-control` rejects publish if a function declares `kind: workflow` but uses banned host calls (deferred to Task 3 for full enforcement; this task only validates the manifest field shape).
- Add `WorkerBinding.Pollers.Decision`, `WorkerBinding.WorkflowMap` (parallel to `ActivityMap`), and a corresponding `CreateWorkerBindingRequest` field in `internal/api/types.go`. Storage and refresh logic in `cs-cadence-poller` must start/stop decision pollers symmetrically with activity pollers.
- Update `docs/12-cadence-integration.md` with a new "DecisionTask scope" section, sample WorkerBinding with `pollers.decision`, the new `trigger.type = cadence.decision` event shape, and the host-call list. Add a "Workflow vs Activity manifests" subsection to `docs/08-runtime-cs-js.md`.

## Acceptance criteria
- [ ] `cmd/cs-cadence-poller` long-polls both ActivityTask and DecisionTask queues for any binding that declares `pollers.decision > 0`; integration test in `cmd/cs-cadence-poller/main_test.go` drives a fake Cadence server returning a DecisionTask and asserts the resulting `RespondDecisionTaskCompleted` payload.
- [ ] `cs-js` exposes `cs.workflow.startActivity`, `cs.workflow.timer`, `cs.workflow.signal`, and `cs.workflow.continueAsNew`; calling any of them in a function with `cadence.kind: "activity"` returns a runtime error tagged `CSWorkflowAPIInActivity`.
- [ ] Manifest validation (`internal/bundle`) accepts and round-trips the new `cadence` block; `cs fn publish` for a workflow function persists `kind=workflow` into version metadata so `cs-cadence-poller` can route it via `WorkflowMap`.
- [ ] `internal/api.WorkerBinding` gains `Pollers.Decision` and `WorkflowMap`; existing bindings with no decision config continue to behave identically (backwards compatible).
- [ ] `docs/12-cadence-integration.md` and `docs/08-runtime-cs-js.md` document the new trigger type, host calls, and manifest fields; `docs/20-config-reference.md` lists the new poller-count knob.

## Dependencies & risks
- Depends on `internal/runtime` being able to buffer host-call effects and emit them synchronously as Decisions; this changes the runner's effect model and may require a new "decision recorder" abstraction.
- Risk: history pagination — long workflow histories require `nextPageToken` handling; if we skip pagination, large workflows fail. Mitigation: implement pagination in `internal/cadence.PollDecisionTask` from day one, with a test for >1 page.
- Risk: replay correctness depends on Task 3 (determinism harness); shipping DecisionTask support without Task 3 lets users author nondeterministic workflows that fail in production. Mitigation: gate `kind: workflow` publish behind a `--unsafe-no-determinism-check` flag until Task 3 lands.
- External: requires Cadence server reachable for the integration test; reuse the existing fake `cadence.Client` in `internal/cadence/client_test.go`.

## Out of scope
- Child workflows and cross-domain signals (defer to a follow-up "workflow composition" task).
- Versioned workflow patches / `GetVersion` API surface (deferred).
- Migrating existing Activity-only bindings to combined workflow+activity bindings (operators must opt in).
- Replay-determinism enforcement at publish — handled by Task 3 of this epic.
