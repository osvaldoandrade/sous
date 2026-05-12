# codeQ subscription trigger

**Parent epic:** #19
**Phase:** Next
**Estimated size:** L

## Problem
`code-sous` today exposes HTTP (sync), async enqueue, interval schedule, and Cadence triggers, but has no event-driven trigger: a function cannot be wired to "fire once per message on codeQ topic X matching filter Y." This forces tenants to write a glue function that polls codeQ and re-publishes to `cs.invoke`, doubling cost and breaking the uniform-execution-fabric invariant in `docs/02-requirements.md`. The roadmap (`docs/18-roadmap.md`) already calls this out as the second remaining first-class trigger.

## Proposed solution
- Add a `SubscriptionBinding` resource managed by `cs-control` (`cmd/cs-control/main.go`): `{tenant, namespace, name, topic, filter_expr, function_ref, mode: ordered|unordered, max_concurrency, retry_ref}`. Persist under `cs:subscription:{tenant}:{namespace}:{name}` in KVrocks.
- Extend `internal/codeq` with a subscription-consumer API that supports both delivery modes: `ordered` reads a single partition serially and respects per-key ordering; `unordered` fans out across partitions up to `max_concurrency`. Filter evaluation runs on the consumer using a small expression language (JSONPath-like equality and AND/OR over envelope and body fields).
- Run subscription consumers as a worker mode inside `cmd/cs-invoker-pool/main.go`: on match, the consumer constructs an `InvocationRequest` with `trigger.type = "subscription"`, `trigger.source = {topic, partition, offset, binding}`, and publishes it via the existing invoker path so all observability, capability, and IAM checks reuse the current code path.
- CLI/REST surface: `cs subscription create --topic orders.created --filter 'event.tenant=="t_x" && body.amount>100' --function reconcile --mode ordered`, plus `list|get|delete` symmetric with existing function/schedule CRUD.
- Update `docs/07-codeq-protocol.md` with a "Subscription triggers" section: binding lifecycle, filter grammar, ordered-vs-unordered semantics, offset commit rules (commit only after invocation result lands on `cs.results` for ordered mode), and the new `trigger.type = "subscription"` envelope variant.

## Acceptance criteria
- [ ] `cs subscription create --topic orders.created --filter ... --function reconcile --mode ordered` creates a binding visible via `cs subscription list` and persisted in KVrocks under the documented key.
- [ ] Publishing a matching message to the bound codeQ topic produces exactly one Activation per message; non-matching messages do not produce Activations; verified by an integration test using the in-memory codeQ stub in `internal/codeq`.
- [ ] Ordered mode preserves per-partition order: a test that publishes 100 messages with the same partition key sees Activations completed in publish order and offsets advance only after `cs.results` confirms terminal state.
- [ ] Unordered mode respects `max_concurrency` and processes partitions in parallel; a load test demonstrates throughput scales with concurrency up to the configured cap.
- [ ] Filter expression errors (syntax, unknown field) are rejected at `cs subscription create` time with a 400; runtime filter eval failures route the message to `cs.dlq.invoke` with the binding name attached.
- [ ] `docs/07-codeq-protocol.md` documents the binding shape, filter grammar, both delivery modes, offset semantics, and the `trigger.type = "subscription"` envelope.

## Dependencies & risks
- Prereqs: codeQ consumer group / offset commit primitives exposed by `internal/codeq` — may require adding partition-aware iteration if the current client only supports fire-and-forget reads.
- Risk: ordered mode head-of-line blocking on a single slow message — mitigated by per-binding execution-timeout and an explicit "park to DLQ after N retries" path (delegates to E4 task-03).
- Risk: filter expression language scope-creep — keep v1 to equality, comparison, AND/OR, and field-existence; document non-goals.
- External: requires E4 task-03 (retry/DLQ policy) to land for production-grade ordered mode; v1 can ship with a fixed retry of 1.

## Out of scope
- Dead-letter replay UI (`cs subscription replay` from DLQ); track separately.
- Cross-topic joins or stateful stream processing — explicitly not in scope.
- Exactly-once delivery guarantees; v1 is at-least-once and documents this.
- Schema-registry integration / typed event payloads.
