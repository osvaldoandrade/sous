package workflow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/dop251/goja"

	"github.com/osvaldoandrade/sous/internal/cadence"
)

// Executor runs a workflow function against a DecisionTask, applies
// history-derived activity resolutions, and collects the new
// Decisions the function produced. The Executor is stateless: it
// holds no per-run state of its own, so the same instance can drive
// many concurrent DecisionTasks. All per-run state lives on the
// activation struct constructed in Execute.
//
// The MVP exposes only one user-visible API: cs.cadence.scheduleActivity.
// See package doc for the list of features deferred to the E8.01
// follow-up.
type Executor struct {
	// MaxRunSteps caps the number of activity-schedule resolutions
	// one activation may pop from history. It exists to keep a
	// runaway loop in a malformed workflow from spinning forever on
	// a single DecisionTask. Zero means use defaultMaxRunSteps.
	MaxRunSteps int
}

// defaultMaxRunSteps is the safety cap when an Executor is
// constructed without an explicit MaxRunSteps. A workflow with more
// than 10,000 activities resolved in a single activation is almost
// certainly a bug; production workflows hit the
// DecisionTaskTimeout long before this limit.
const defaultMaxRunSteps = 10_000

// pendingMarker is the string we throw inside JS when an unresolved
// schedule is requested. The executor recognises it as "the workflow
// suspended on a pending activity" and uses it to differentiate the
// suspension flow from a real error. The marker is intentionally
// opaque to user code: workflow authors should not catch it.
const pendingMarker = "__cs_workflow_pending__"

// Result is what Execute returns to the poller. Decisions is the
// ordered list of decisions to ship back to Cadence via
// RespondDecisionTaskCompleted. Logs is the captured console.log
// output from the workflow function; reserved for future surfacing
// in cs-control's activation log endpoint (today the poller just
// drops it on the floor).
type Result struct {
	Decisions []cadence.Decision
	Logs      []string
}

// Execute runs the workflow function from the supplied bundle source
// against the DecisionTask's history. The function source is the raw
// JS body of function.js — the executor wraps it the same way the
// activity runner does so `export default` lands on globalThis.
//
// Result.Decisions contains either:
//
//   - One or more ScheduleActivityTask decisions (the workflow
//     suspended waiting for activity results).
//   - Exactly one CompleteWorkflowExecution decision (the workflow
//     returned a value).
//
// An error from Execute means the workflow function itself threw a
// non-pending exception (i.e., user-visible bug or activity-failure
// propagated through scheduleActivity). The poller surfaces this as
// a RespondDecisionTaskFailed reply in production; the v0.1 MVP only
// covers the happy paths and tests these errors via the executor
// API directly.
func (e *Executor) Execute(ctx context.Context, functionSrc []byte, task *cadence.DecisionTask) (Result, error) {
	if task == nil {
		return Result{}, errors.New("workflow executor: nil decision task")
	}
	outcomes := WalkActivityOutcomes(task.History)
	state := &activation{
		outcomes:    outcomes,
		maxSteps:    e.maxSteps(),
		scheduleSeq: int64(len(outcomes)),
	}

	rt := goja.New()
	if err := bindWorkflowAPI(rt, state); err != nil {
		return Result{}, err
	}

	// Drop a console object so workflow code can log without crashing.
	// We don't surface the lines anywhere yet — they're held on the
	// activation for the test harness and a future log-shipping hook.
	console := rt.NewObject()
	logFn := func(call goja.FunctionCall) goja.Value {
		args := make([]any, 0, len(call.Arguments))
		for _, a := range call.Arguments {
			args = append(args, a.Export())
		}
		state.logs = append(state.logs, fmt.Sprint(args...))
		return goja.Undefined()
	}
	_ = console.Set("log", logFn)
	_ = console.Set("error", logFn)
	_ = rt.Set("console", console)

	// The runner's transformESModule path is the canonical wrapper
	// for `export default` modules. The workflow executor uses a
	// trimmed-down version: we only need to land the default export
	// on globalThis so we can locate the handler.
	wrapped := wrapWorkflowSource(string(functionSrc))
	if _, err := rt.RunString(wrapped); err != nil {
		return Result{}, fmt.Errorf("workflow: source eval failed: %w", err)
	}
	handler, ok := goja.AssertFunction(rt.Get("__cs_workflow_default"))
	if !ok {
		return Result{}, errors.New("workflow: default export not found")
	}

	// We pass the workflow function an `input` payload built from
	// the first WorkflowExecutionStarted event (when present) and a
	// `ctx` shape that mirrors the activity runner's surface — both
	// keep the cs-js author surface symmetrical between the two
	// kinds. Empty objects are fine for the MVP; richer fields land
	// in the follow-up.
	input := workflowInput(task)
	ctxObj := map[string]any{
		"workflow_id":   task.WorkflowID,
		"run_id":        task.RunID,
		"workflow_type": task.WorkflowType,
	}

	val, runErr := handler(goja.Undefined(), rt.ToValue(input), rt.ToValue(ctxObj))
	if runErr != nil {
		// goja wraps thrown values in *goja.Exception. We unwrap to
		// detect the pendingMarker and treat it as "suspend".
		if isPendingException(runErr) {
			return Result{Decisions: state.decisions, Logs: state.logs}, nil
		}
		return Result{Logs: state.logs}, fmt.Errorf("workflow: handler error: %w", runErr)
	}

	// `async function` handlers return a Promise even when their
	// body threw synchronously. Inspect the promise state so the
	// pendingMarker still maps to "suspend" and a rejected promise
	// surfaces as an executor error.
	if prom, ok := val.Export().(*goja.Promise); ok {
		switch prom.State() {
		case goja.PromiseStatePending:
			// A truly pending promise means the workflow used a
			// timer or some other async API the MVP doesn't yet
			// support. Treat it as a deferred feature error so
			// the operator gets a clear message — this matches
			// the activity runner's "pending promise is not
			// supported" guard.
			return Result{Logs: state.logs}, errors.New("workflow: pending promise is not supported (use cs.cadence.scheduleActivity)")
		case goja.PromiseStateRejected:
			reason := prom.Result()
			msg := "promise rejected"
			if reason != nil {
				msg = reason.String()
			}
			if strings.Contains(msg, pendingMarker) {
				return Result{Decisions: state.decisions, Logs: state.logs}, nil
			}
			return Result{Logs: state.logs}, fmt.Errorf("workflow: handler error: %s", msg)
		case goja.PromiseStateFulfilled:
			val = prom.Result()
		}
	}

	// The handler returned a value → emit CompleteWorkflowExecution.
	// The MVP serialises whatever the function returned as JSON; a
	// future codec hook on the WorkerBinding can swap the encoding
	// without changing the executor surface.
	encoded, err := json.Marshal(val.Export())
	if err != nil {
		return Result{Logs: state.logs}, fmt.Errorf("workflow: result encode failed: %w", err)
	}
	state.decisions = append(state.decisions, cadence.Decision{
		Type: cadence.DecisionCompleteWorkflowExecution,
		CompleteWorkflowExecution: &cadence.CompleteWorkflowDecision{
			ResultBase64: base64.StdEncoding.EncodeToString(encoded),
		},
	})
	return Result{Decisions: state.decisions, Logs: state.logs}, nil
}

func (e *Executor) maxSteps() int {
	if e.MaxRunSteps > 0 {
		return e.MaxRunSteps
	}
	return defaultMaxRunSteps
}

// activation is the per-execution state the workflow API binding
// reads and writes. The Executor builds one per Execute call.
type activation struct {
	// outcomes is the slot table derived from history, in the order
	// the activities were originally scheduled. scheduleActivity
	// pops from the head of this table on each call.
	outcomes []ActivityOutcome
	// outcomeIdx is the index of the next outcome the workflow will
	// consume. Once it exceeds len(outcomes) the workflow is past
	// the "replay tail" and any new scheduleActivity call emits a
	// fresh ScheduleActivityTask decision.
	outcomeIdx int
	// scheduleSeq is the activity_id counter the workflow uses for
	// freshly-scheduled activities. It starts at len(outcomes) so
	// each replay assigns identical IDs across re-runs of the same
	// history; "act-0", "act-1", ... is the canonical sequence.
	scheduleSeq int64
	// decisions is the ordered list of decisions emitted during
	// this activation. The poller ships these back to Cadence.
	decisions []cadence.Decision
	// logs holds console.log output for the test harness.
	logs []string
	// maxSteps is the runaway-loop cap from Executor.MaxRunSteps.
	maxSteps int
	// stepsConsumed counts scheduleActivity calls in this activation.
	stepsConsumed int
}

// bindWorkflowAPI installs the cs.cadence.scheduleActivity host call
// into the goja runtime. The shape mirrors the activity-side
// cs.cadence binding so user code can pivot from activity to workflow
// without rewriting capability access patterns.
func bindWorkflowAPI(rt *goja.Runtime, state *activation) error {
	csObj := rt.NewObject()
	cad := rt.NewObject()
	if err := cad.Set("scheduleActivity", func(call goja.FunctionCall) goja.Value {
		state.stepsConsumed++
		if state.stepsConsumed > state.maxSteps {
			panic(rt.NewGoError(fmt.Errorf("workflow: exceeded max run steps (%d)", state.maxSteps)))
		}
		activityType := call.Argument(0).String()
		if activityType == "" {
			panic(rt.NewGoError(errors.New("scheduleActivity: activity_type is required")))
		}
		var inputBytes []byte
		if !goja.IsUndefined(call.Argument(1)) && !goja.IsNull(call.Argument(1)) {
			b, err := json.Marshal(call.Argument(1).Export())
			if err != nil {
				panic(rt.NewGoError(fmt.Errorf("scheduleActivity: input encode failed: %w", err)))
			}
			inputBytes = b
		}

		// Consume the next history outcome if one is available.
		if state.outcomeIdx < len(state.outcomes) {
			slot := state.outcomes[state.outcomeIdx]
			state.outcomeIdx++
			// Defensive guard: a mismatched activity_type between
			// history and current code is a nondeterminism bug.
			// Throw with a clear message so the determinism story
			// at publish time has a runtime backstop.
			if slot.ActivityType != "" && slot.ActivityType != activityType {
				panic(rt.NewGoError(fmt.Errorf(
					"workflow: nondeterministic activity at %s: history had %q, code requested %q",
					slot.ActivityID, slot.ActivityType, activityType,
				)))
			}
			if !slot.Resolved {
				// Activity was scheduled but has not completed.
				// Suspend the workflow until the next decision
				// task delivers the resolution.
				panic(rt.NewGoError(errors.New(pendingMarker)))
			}
			if slot.Failed {
				msg := slot.FailureReason
				if msg == "" {
					msg = "activity failed"
				}
				panic(rt.NewGoError(fmt.Errorf("activity %s failed: %s", slot.ActivityID, msg)))
			}
			// Success: decode the result and return it as a JS value.
			if len(slot.ResultBytes) == 0 {
				return goja.Undefined()
			}
			var v any
			if err := json.Unmarshal(slot.ResultBytes, &v); err != nil {
				// Activity payloads that aren't JSON come through
				// as a raw string so the workflow at least has
				// something to inspect.
				return rt.ToValue(string(slot.ResultBytes))
			}
			return rt.ToValue(v)
		}

		// No history slot left → this is a fresh schedule. Assign
		// the next activity ID and emit a ScheduleActivityTask
		// decision. The workflow function is expected to suspend
		// right after this call (by re-calling scheduleActivity on
		// the same line; a "single-await" workflow does that
		// naturally), so we throw the pending marker to short-
		// circuit the function body.
		actID := fmt.Sprintf("act-%d", state.scheduleSeq)
		state.scheduleSeq++
		state.decisions = append(state.decisions, cadence.Decision{
			Type: cadence.DecisionScheduleActivityTask,
			ScheduleActivityTask: &cadence.ScheduleActivityTaskDecision{
				ActivityID:   actID,
				ActivityType: activityType,
				InputBase64:  base64.StdEncoding.EncodeToString(inputBytes),
			},
		})
		panic(rt.NewGoError(errors.New(pendingMarker)))
	}); err != nil {
		return err
	}
	if err := csObj.Set("cadence", cad); err != nil {
		return err
	}
	return rt.Set("cs", csObj)
}

// wrapWorkflowSource adapts the publisher's ESM `export default`
// module into a script the goja interpreter can parse, then lands the
// default export on globalThis under a known name. The wrapper mirrors
// runner.transformESModule but uses a workflow-scoped global to keep
// the surfaces apart in mixed test rigs.
func wrapWorkflowSource(src string) string {
	switch {
	case strings.Contains(src, "export default function "):
		// `export default function name(...) { ... }` — rename then reassign.
		out := strings.Replace(src, "export default function ", "function ", 1)
		name := identAfter(out, "function ")
		return out + "\nglobalThis.__cs_workflow_default = " + name + ";\n"
	case strings.Contains(src, "export default async function "):
		out := strings.Replace(src, "export default async function ", "async function ", 1)
		name := identAfter(out, "async function ")
		return out + "\nglobalThis.__cs_workflow_default = " + name + ";\n"
	case strings.Contains(src, "export default function("):
		out := strings.Replace(src, "export default function(", "const __cs_workflow_default = function(", 1)
		return out + "\nglobalThis.__cs_workflow_default = __cs_workflow_default;\n"
	case strings.Contains(src, "export default async function("):
		out := strings.Replace(src, "export default async function(", "const __cs_workflow_default = async function(", 1)
		return out + "\nglobalThis.__cs_workflow_default = __cs_workflow_default;\n"
	case strings.Contains(src, "export default "):
		out := strings.Replace(src, "export default ", "const __cs_workflow_default = ", 1)
		return out + "\nglobalThis.__cs_workflow_default = __cs_workflow_default;\n"
	default:
		// CommonJS-style fallback: pick up module.exports.default.
		return src + "\nif (typeof module !== 'undefined' && module.exports && module.exports.default) { globalThis.__cs_workflow_default = module.exports.default; }\n"
	}
}

// identAfter returns the JS-identifier-like token immediately after
// the first occurrence of marker. Used by wrapWorkflowSource to
// recover the function name from a `function NAME(...)` shape.
func identAfter(s, marker string) string {
	idx := strings.Index(s, marker)
	if idx < 0 {
		return ""
	}
	start := idx + len(marker)
	end := start
	for end < len(s) && isIdentByte(s[end]) {
		end++
	}
	return s[start:end]
}

func isIdentByte(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_' || b == '$'
}

// workflowInput shapes the input payload passed to the workflow
// function. The MVP just hands the workflow a placeholder object;
// once WorkflowExecutionStarted attribute parsing lands we'll fill
// in the original workflow input.
func workflowInput(task *cadence.DecisionTask) map[string]any {
	for _, ev := range task.History {
		if ev.Type == "WorkflowExecutionStarted" {
			if b64, ok := ev.Attributes["input_base64"].(string); ok && b64 != "" {
				if raw := decodeBase64Tolerant(b64); raw != nil {
					var v any
					if err := json.Unmarshal(raw, &v); err == nil {
						if m, ok := v.(map[string]any); ok {
							return m
						}
						return map[string]any{"value": v}
					}
				}
			}
		}
	}
	return map[string]any{}
}

// isPendingException reports whether err is the pendingMarker thrown
// from inside a scheduleActivity call. We match on substring rather
// than identity because goja wraps the message and the marker is the
// canonical handshake between the binding and the executor.
func isPendingException(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), pendingMarker)
}

// decodeBase64Tolerant accepts std and url-safe alphabets and silently
// drops a decode failure by returning nil. The caller treats nil as
// "no payload" — workflow authors that need the byte-exact value
// should not encode it as base64 in the first place.
func decodeBase64Tolerant(s string) []byte {
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return b
	}
	if b, err := base64.URLEncoding.DecodeString(s); err == nil {
		return b
	}
	if b, err := base64.RawStdEncoding.DecodeString(s); err == nil {
		return b
	}
	return nil
}
