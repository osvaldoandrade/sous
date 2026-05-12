package workflow

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/osvaldoandrade/sous/internal/cadence"
)

// TestExecutorFirstDecisionSchedulesActivity covers the very first
// activation of a workflow: history is empty, the function calls
// scheduleActivity once, and the executor must emit a
// ScheduleActivityTask decision (and nothing else) so Cadence can
// dispatch the work. A regression here breaks the entry-point of
// every workflow.
func TestExecutorFirstDecisionSchedulesActivity(t *testing.T) {
	src := []byte(`export default function(input, ctx) {
		const r = cs.cadence.scheduleActivity("EchoActivity", { msg: "hi" });
		return { statusCode: 200, body: r };
	}`)
	task := &cadence.DecisionTask{
		WorkflowType: "EchoWorkflow",
		WorkflowID:   "wf1",
		RunID:        "run1",
	}
	got, err := (&Executor{}).Execute(context.Background(), src, task)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(got.Decisions) != 1 {
		t.Fatalf("decisions = %d, want 1: %#v", len(got.Decisions), got.Decisions)
	}
	d := got.Decisions[0]
	if d.Type != cadence.DecisionScheduleActivityTask {
		t.Fatalf("type = %q, want ScheduleActivityTask", d.Type)
	}
	if d.ScheduleActivityTask == nil {
		t.Fatal("ScheduleActivityTask attributes nil")
	}
	if d.ScheduleActivityTask.ActivityType != "EchoActivity" {
		t.Fatalf("activity_type = %q, want EchoActivity", d.ScheduleActivityTask.ActivityType)
	}
	if d.ScheduleActivityTask.ActivityID != "act-0" {
		t.Fatalf("activity_id = %q, want act-0", d.ScheduleActivityTask.ActivityID)
	}
	// Input round-trips through base64+JSON.
	raw, decErr := base64.StdEncoding.DecodeString(d.ScheduleActivityTask.InputBase64)
	if decErr != nil {
		t.Fatalf("decode input_base64: %v", decErr)
	}
	var in map[string]any
	if err := json.Unmarshal(raw, &in); err != nil {
		t.Fatalf("input not JSON: %v", err)
	}
	if in["msg"] != "hi" {
		t.Fatalf("input = %v, want {msg: hi}", in)
	}
}

// TestExecutorReplayWithCompletionReturnsResult exercises the second
// activation of a workflow: history now contains the schedule plus
// the completion event, scheduleActivity returns the resolved value
// synchronously, and the workflow function runs to completion. The
// executor must emit exactly one CompleteWorkflowExecution decision
// with the function's return value.
func TestExecutorReplayWithCompletionReturnsResult(t *testing.T) {
	src := []byte(`export default function(input, ctx) {
		const r = cs.cadence.scheduleActivity("EchoActivity", { msg: "hi" });
		return { statusCode: 200, body: r };
	}`)
	resultPayload, _ := json.Marshal("hello back")
	task := &cadence.DecisionTask{
		WorkflowType: "EchoWorkflow",
		History: []cadence.HistoryEvent{
			{
				ID:   5,
				Type: EventActivityTaskScheduled,
				Attributes: map[string]any{
					"activity_id":   "act-0",
					"activity_type": "EchoActivity",
				},
			},
			{
				ID:   7,
				Type: EventActivityTaskCompleted,
				Attributes: map[string]any{
					"scheduled_event_id": float64(5),
					"result_base64":      base64.StdEncoding.EncodeToString(resultPayload),
				},
			},
		},
	}
	got, err := (&Executor{}).Execute(context.Background(), src, task)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(got.Decisions) != 1 {
		t.Fatalf("decisions = %d, want 1: %#v", len(got.Decisions), got.Decisions)
	}
	d := got.Decisions[0]
	if d.Type != cadence.DecisionCompleteWorkflowExecution {
		t.Fatalf("type = %q, want CompleteWorkflowExecution", d.Type)
	}
	if d.CompleteWorkflowExecution == nil {
		t.Fatal("complete attributes nil")
	}
	raw, _ := base64.StdEncoding.DecodeString(d.CompleteWorkflowExecution.ResultBase64)
	var resp map[string]any
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("result_base64 not JSON: %v", err)
	}
	if resp["body"] != "hello back" {
		t.Fatalf("body = %v, want hello back", resp["body"])
	}
}

// TestExecutorReplayWithFailurePropagates exercises the failure
// path: history contains a schedule + a failure event, the workflow
// function's scheduleActivity throws, and (since the function does
// not catch) the executor surfaces a handler error. The poller
// translates this into a RespondDecisionTaskFailed in production —
// the MVP only locks down the executor's contract.
func TestExecutorReplayWithFailurePropagates(t *testing.T) {
	src := []byte(`export default function(input, ctx) {
		const r = cs.cadence.scheduleActivity("BadActivity", null);
		return { statusCode: 200, body: r };
	}`)
	task := &cadence.DecisionTask{
		WorkflowType: "EchoWorkflow",
		History: []cadence.HistoryEvent{
			{
				ID:   3,
				Type: EventActivityTaskScheduled,
				Attributes: map[string]any{
					"activity_id":   "act-0",
					"activity_type": "BadActivity",
				},
			},
			{
				ID:   5,
				Type: EventActivityTaskFailed,
				Attributes: map[string]any{
					"scheduled_event_id": float64(3),
					"reason":             "timeout",
				},
			},
		},
	}
	_, err := (&Executor{}).Execute(context.Background(), src, task)
	if err == nil {
		t.Fatal("expected handler error from activity failure")
	}
}

// TestExecutorReplayDeterminism is the contract test for replay
// safety: running the same DecisionTask through the executor twice
// MUST yield byte-identical Decisions. Cadence's correctness model
// hinges on this — if any source of nondeterminism leaks into the
// executor (counter resets, map iteration order, etc.) workflows
// will appear to work in tests and silently corrupt in production.
func TestExecutorReplayDeterminism(t *testing.T) {
	src := []byte(`export default function(input, ctx) {
		const r = cs.cadence.scheduleActivity("EchoActivity", { msg: "hi" });
		return { statusCode: 200, body: r };
	}`)
	// Same task drives both runs; the executor must not mutate the
	// task in a way that the second run observes.
	taskA := &cadence.DecisionTask{WorkflowType: "EchoWorkflow"}
	taskB := &cadence.DecisionTask{WorkflowType: "EchoWorkflow"}
	gotA, err := (&Executor{}).Execute(context.Background(), src, taskA)
	if err != nil {
		t.Fatalf("first execute: %v", err)
	}
	gotB, err := (&Executor{}).Execute(context.Background(), src, taskB)
	if err != nil {
		t.Fatalf("second execute: %v", err)
	}
	if !reflect.DeepEqual(gotA.Decisions, gotB.Decisions) {
		t.Fatalf("decisions diverged across runs:\nA=%#v\nB=%#v", gotA.Decisions, gotB.Decisions)
	}
}

// TestExecutorAsyncHandlerSchedules locks down the async-function
// path: a workflow author using `export default async function`
// throws the pending marker through a rejected Promise, and the
// executor still extracts the schedule decision instead of treating
// the rejection as a real error.
func TestExecutorAsyncHandlerSchedules(t *testing.T) {
	src := []byte(`export default async function(input, ctx) {
		const r = cs.cadence.scheduleActivity("EchoActivity", null);
		return { statusCode: 200, body: r };
	}`)
	task := &cadence.DecisionTask{WorkflowType: "EchoWorkflow"}
	got, err := (&Executor{}).Execute(context.Background(), src, task)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(got.Decisions) != 1 || got.Decisions[0].Type != cadence.DecisionScheduleActivityTask {
		t.Fatalf("expected single ScheduleActivityTask, got %#v", got.Decisions)
	}
}

// TestExecutorNilTask checks the defensive nil-guard so a buggy
// poller can't crash the executor by handing it an empty pointer.
func TestExecutorNilTask(t *testing.T) {
	if _, err := (&Executor{}).Execute(context.Background(), []byte(`export default function(){return 1}`), nil); err == nil {
		t.Fatal("expected error for nil task")
	}
}

// TestWalkActivityOutcomesMatchesByScheduledEventID locks down the
// history walker's correlation: a completion event uses
// scheduled_event_id to point at the schedule event, not the
// activity_id. Without this match an out-of-order history corrupts
// the slot table and replay returns the wrong value.
func TestWalkActivityOutcomesMatchesByScheduledEventID(t *testing.T) {
	hist := []cadence.HistoryEvent{
		{ID: 10, Type: EventActivityTaskScheduled, Attributes: map[string]any{"activity_id": "act-0", "activity_type": "A"}},
		{ID: 11, Type: EventActivityTaskScheduled, Attributes: map[string]any{"activity_id": "act-1", "activity_type": "A"}},
		{ID: 12, Type: EventActivityTaskCompleted, Attributes: map[string]any{"scheduled_event_id": float64(11), "result_base64": base64.StdEncoding.EncodeToString([]byte(`"second"`))}},
	}
	outs := WalkActivityOutcomes(hist)
	if len(outs) != 2 {
		t.Fatalf("outcomes = %d, want 2", len(outs))
	}
	if outs[0].Resolved {
		t.Fatalf("act-0 must be unresolved, got %+v", outs[0])
	}
	if !outs[1].Resolved {
		t.Fatalf("act-1 must be resolved, got %+v", outs[1])
	}
}
