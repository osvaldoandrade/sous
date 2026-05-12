# Epic: Cadence workflow depth

**Phase:** Later (5–9 months)
**Theme:** Let agents author full workflows in `cs` (not just activities), with flexible payload codecs and replay-determinism enforcement.

## Why
`code-sous` already runs Cadence Activities through `cmd/cs-cadence-poller`, but workflow authorship still requires a separate Go/Java/TypeScript Cadence SDK process — which breaks the "everything is a `cs` function" promise for agentic orchestration. Agents that need durable, signal-driven, multi-step business logic (sagas, approval flows, long-running tool chains) cannot stay inside `cs` today. This epic closes that gap by lifting `cs-cadence-poller` from Activity-only to full workflow host, adds per-tasklist codec flexibility so `cs` workflows interop with existing Java/Go workflows, and enforces replay determinism at publish time so a bad workflow never reaches production.

## Scope
- DecisionTask long-polling and workflow host-call API surface in `cs-js` (`cs.workflow.startActivity`, `timer`, `signal`, `continueAsNew`, `sideEffect`).
- Manifest schema gains `cadence: { kind: workflow | activity }`; `WorkerBinding` gains `Pollers.Decision`, `WorkflowMap`, and `PayloadCodecs`.
- Per-tasklist codec negotiation (`json`, `msgpack`, `raw`, `protobuf`) at WorkerBinding registration, applied symmetrically to inputs and outputs for both Activities and Workflows.
- Static + replay-based determinism harness wired into the `cs-control` publish path; CLI command `cs fn check-determinism` for pre-publish validation.
- Documentation updates across `docs/12-cadence-integration.md`, `docs/08-runtime-cs-js.md`, `docs/15-security.md`, and `docs/20-config-reference.md`.

## Outcomes / success metrics
- Workflow-author success rate: ≥90% of `cs fn publish --kind=workflow` attempts in week-1 dogfood succeed without a Cadence-side history-corruption error in the first 7 days post-publish.
- Codec adoption: at least two production tasklists run on a non-JSON codec (msgpack or raw) within one quarter of GA, validating real interop demand.
- Determinism-violation catch rate at publish: 100% of seeded banned-API cases (`Date.now`, `Math.random`, `crypto.randomUUID`, `process.env`, `fetch`, `http`/`https`/`net`/`fs`) are blocked by the publish-path harness; zero of those make it into a published version.
- Time-to-first-workflow: a new agent can scaffold, publish, and execute a Cadence workflow in `cs` in under 15 minutes, end-to-end, per the docs.

## Tasks
- [ ] #26 — DecisionTask support — author Cadence workflows in `cs` functions
- [ ] #28 — Activity result payload codecs per tasklist
- [ ] #32 — Replay-determinism harness — catch nondeterministic workflow code at publish time

## Non-goals
- Child workflows, cross-domain signals, and versioned workflow patches (deferred to a follow-up "workflow composition" epic).
- Replacing Cadence with Temporal or another orchestrator; this epic stays on Cadence.
- Runtime detection of nondeterminism in production (publish-time only).
- Schema-registry integration for protobuf descriptors (ships as length-delimited bytes only).
- Migrating existing Activity-only WorkerBindings to combined workflow+activity bindings on operators' behalf (opt-in).
