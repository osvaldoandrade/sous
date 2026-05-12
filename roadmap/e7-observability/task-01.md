# Per-node decision tracing for agent function call trees

**Parent epic:** #38
**Phase:** Later
**Estimated size:** L

## Problem
Agent workloads compose functions: an HTTP invoke commonly fans out into more `cs` HTTP routes or codeQ enqueues. `docs/14-observability.md` already specifies a single `cs.invoke` span and a `trigger.source.traceparent` field on `InvocationRequest`, but child activations today carry no link back to their parent, and `cmd/cs-control` has no API that materializes the resulting call tree. Operators cannot answer "which decision in which activation caused this downstream failure" without grepping correlated logs.

## Proposed solution
- Extend `internal/observability` with a `Trace` package that wraps W3C `traceparent` + `tracestate` parsing and exposes context helpers (`WithTraceContext`, `TraceContextFromContext`) alongside the existing `RequestID` helpers in `internal/observability/request.go`.
- In `cmd/cs-http-gateway`, propagate the incoming `traceparent` into the `InvocationRequest.trigger.source.traceparent` field and stamp `parent_activation_id` when the call originates from another `cs` activation (detected via a `X-CS-Parent-Activation` header set by the runtime egress shim).
- In `cmd/cs-invoker-pool`, inject the active `traceparent` and `activation_id` into every host-API egress made by user code (HTTP-out and codeQ enqueue), so the next hop's gateway/poller picks up the parent link. Persist `parent_activation_id` + `root_activation_id` on the Activation record in `internal/kv`.
- Add a new control-plane route `GET /v1/tenants/{tenant}/activations/{id}/tree` in `cmd/cs-control` that walks `child_activation_ids` (a secondary index `act:{id}:children`) and returns a JSON tree with per-node `function`, `version`, `trigger.type`, `status`, `started_ms`, `duration_ms`, and `traceparent`.
- Document the propagation contract, the new index key, and the new endpoint in `docs/14-observability.md` and `docs/04-api-rest.md`; update `docs/03-architecture.md` with the parent-child arrow between `cs-invoker-pool` and the downstream poller.

## Acceptance criteria
- [ ] `internal/observability.Trace` parses and emits W3C `traceparent`/`tracestate`; unit tests cover round-trip, malformed input, and sampling-flag preservation.
- [ ] `cs-http-gateway` and `cs-cadence-poller` propagate `traceparent` and `parent_activation_id` into `InvocationRequest`; integration test asserts that a 3-hop chain (HTTP -> HTTP -> codeQ) yields three activations sharing one `trace_id` with correct `parent_activation_id` links.
- [ ] `GET /v1/tenants/{tenant}/activations/{id}/tree` returns the full call tree for the root activation; pagination cap (`max_nodes=500`, `truncated=true` sentinel) is enforced and documented.
- [ ] `docs/14-observability.md` documents the propagation rules, the new index key, and a worked example; `docs/04-api-rest.md` describes the `/tree` endpoint.
- [ ] CLI surface: `cs activation tree <id>` renders the tree and is wired to the new endpoint (basic ASCII tree output, no follow mode).

## Dependencies & risks
- Prereq: activation records in `internal/kv` must persist `parent_activation_id` and `root_activation_id`; coordinate with the E1 activation-persistence work to avoid schema drift.
- External: downstream `code-flow` (Cadence) does not always forward `traceparent` on Activity tasks; document the gap and degrade gracefully (orphan node with `trace_break=true`).
- Risk: tree walk over a deeply fan-out activation could be expensive; mitigate by capping fan-out per node at 64 children stored, plus the `max_nodes` API cap.
- Risk: storing child indexes is a write-amplification cost on the invoker hot path; benchmark before enabling by default and gate behind `cs_invoker.tracing.tree_index=true`.

## Out of scope
- OpenTelemetry collector / OTLP export (tracked separately under the dashboards/SLO work).
- Span-level user code instrumentation inside the `cs-js` runtime.
- Cross-tenant trace stitching.
- UI for the tree beyond the CLI ASCII renderer.
