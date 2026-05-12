# Activity result payload codecs per tasklist

**Parent epic:** #37
**Phase:** Later
**Estimated size:** M

## Problem
Today `cmd/cs-cadence-poller/main.go` JSON-encodes every Activity result, base64-wraps the JSON, and ships it to `RespondActivityTaskCompleted` (see lines ~289–303). Symmetrically the Activity *input* is passed to the function as `raw_base64` with no codec hint. This forces every Cadence integration to speak JSON, which breaks interop with Java/Go workflows that already use Thrift, msgpack, or raw protobuf on a given tasklist. After Task 1 lands, the same constraint will also block workflow inputs/outputs.

## Proposed solution
- Add `PayloadCodecs` to `internal/api.WorkerBinding`: an explicit map of `{"activity_input": "json|msgpack|raw|protobuf", "activity_output": "...", "workflow_input": "...", "workflow_output": "..."}` with `json` as the default. Reflect the field in `CreateWorkerBindingRequest` and persist via `internal/kv` alongside the existing binding.
- Introduce `internal/cadence/codec` with a `Codec` interface (`Encode([]byte) ([]byte, error)`, `Decode([]byte) ([]byte, error)`) and registered implementations: `json` (current behavior), `msgpack`, `raw` (passthrough bytes), and `protobuf` (length-delimited any). `NewCodec(name string)` returns the implementation or an `unsupported_codec` error.
- In `cmd/cs-cadence-poller`, resolve the codec per binding at poll time and use it both on the input path (when building `InvocationRequest.Event.input`) and on the output path (replacing the current `json.Marshal(res.Result)` step). On decode failure the poller responds `RespondActivityFailed(reason="codec_decode_failed")`.
- Negotiate codecs at `WorkerBinding` registration in `cmd/cs-control`: the create/update handlers validate that requested codecs are registered and reject unknown values with HTTP 400 and `error.code = "unsupported_codec"`. Document the negotiation contract in `docs/12-cadence-integration.md`.
- Expose codec selection to `cs-js`: when a function is invoked via Cadence, `event.input` becomes a typed value (object / Buffer / decoded msgpack map) instead of an always-base64 string, and `cs.cadence.completePayload(value)` lets workflow code emit a value the codec will encode. Document the new shape in `docs/08-runtime-cs-js.md`.

## Acceptance criteria
- [ ] `internal/api.WorkerBinding.PayloadCodecs` exists with per-direction (`activity_input`, `activity_output`, `workflow_input`, `workflow_output`) settings; bindings without the field keep behaving as if all four are `"json"`.
- [ ] `internal/cadence/codec` registers `json`, `msgpack`, `raw`, and `protobuf` codecs, has unit tests for round-trip on each, and returns a clear error for unknown codec names.
- [ ] `cmd/cs-cadence-poller` test asserts that a binding configured with `activity_output: "msgpack"` produces a msgpack-encoded payload on `RespondActivityTaskCompleted`, while `activity_output: "raw"` ships the function's raw bytes verbatim.
- [ ] `cs-control` rejects `POST /v1/worker-bindings` with `unsupported_codec` (HTTP 400) when the request names a codec not in the registry; covered by a handler test in `cmd/cs-control/main_test.go`.
- [ ] `docs/12-cadence-integration.md` documents per-tasklist codec negotiation, default behavior, and migration steps for tasklists that flip from `json` to a binary codec.

## Dependencies & risks
- Depends on `internal/api` type changes and KVRocks schema for `WorkerBinding`; if Task 1 lands first the workflow_input/workflow_output settings already have a consumer.
- Risk: codec drift — a binding that switches codec mid-flight will fail in-flight tasks. Mitigation: codec change is treated as a binding-restart event in the poller's refresh loop (`refreshBindings` already restarts on update); document that operators should drain before flipping codecs.
- Risk: msgpack/protobuf library footprint inflates the poller binary. Mitigation: gate behind `cs-cadence-poller` build tags only if footprint regresses by >5 MB.
- External: protobuf codec requires a registered descriptor source; v1 of this task ships protobuf as length-delimited `bytes` with no schema validation and defers descriptor-aware encoding.

## Out of scope
- Compression of Cadence payloads (separate concern from codec choice).
- Per-activity-type codecs within a tasklist (resolved at the tasklist level only).
- Schema registry integration for protobuf descriptors (future work).
- Codec changes for non-Cadence triggers (HTTP, schedules, etc. continue to use existing serialization).
