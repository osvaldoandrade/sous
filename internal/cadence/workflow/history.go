// Package workflow implements the v0.1 Cadence DecisionTask runtime
// for cs-js workflows. It runs the user's workflow function against
// the history Cadence delivered, resolves outstanding activity
// schedules from history-recorded completions, and collects fresh
// Decisions (schedule a new activity, complete the workflow) to ship
// back via RespondDecisionTaskCompleted.
//
// Scope of the v0.1 MVP:
//
//   - The workflow function can schedule activities via
//     cs.cadence.scheduleActivity(activityType, input) and consume
//     their results.
//   - Returning a value from the workflow function emits a
//     CompleteWorkflowExecution decision with the JSON-encoded result.
//   - Workflow code must be replay-safe; nondeterministic APIs are
//     blocked at publish time by internal/cadence/determinism.
//
// Deferred (tracked in the E8.01 follow-up issue):
//
//   - Timer (sleep), Signal handlers, ContinueAsNew, Selector/race,
//     child workflows, retry policies on activities, cancellation,
//     query handlers, side-effect/local-activity, search attributes.
package workflow

import (
	"github.com/osvaldoandrade/sous/internal/cadence"
)

// Event types relevant to the MVP's history walker. Names match the
// Cadence wire types so a fake-server fixture file is portable
// between cs and a stand-in workflow service.
const (
	EventActivityTaskScheduled = "ActivityTaskScheduled"
	EventActivityTaskCompleted = "ActivityTaskCompleted"
	EventActivityTaskFailed    = "ActivityTaskFailed"
)

// ActivityOutcome captures the state of one activity slot derived
// from history. Slots are indexed by the workflow-assigned ActivityID
// (the executor uses a monotonic "act-<n>" sequence) so two replays
// of the same history produce identical slot maps in identical order.
//
// State transitions for a single slot:
//
//	(no event)         → Resolved == false, Failed == false.
//	ActivityTaskScheduled                → Scheduled == true, still pending.
//	ActivityTaskScheduled + Completed    → Resolved == true.
//	ActivityTaskScheduled + Failed       → Failed == true, Resolved == true.
//
// The executor inspects only the Resolved/Failed flags; the rest is
// debugging detail surfaced through tests.
type ActivityOutcome struct {
	// ActivityID is the workflow-assigned slot key (e.g., "act-0").
	ActivityID string
	// ActivityType is the type recorded in the schedule event;
	// useful for error reporting when a replay finds a mismatch
	// between the workflow's intent and history.
	ActivityType string
	// ScheduledEventID is the EventID of the ActivityTaskScheduled
	// event. The completion/failure event references this ID via
	// its scheduled_event_id attribute; the walker uses it to
	// correlate the two events.
	ScheduledEventID int64
	// Scheduled is true once an ActivityTaskScheduled event with
	// the matching activity_id is in history. The executor treats
	// "scheduled but not resolved" as "do not re-schedule, just
	// suspend": Cadence already knows about this activity.
	Scheduled bool
	// Resolved is true once history contains either a completion
	// or a failure event for this slot. A resolved slot is what
	// the workflow's scheduleActivity returns from on replay.
	Resolved bool
	// Failed is true when the resolution was a failure event.
	// When Failed is true the workflow's scheduleActivity throws
	// instead of returning a value.
	Failed bool
	// ResultBytes is the activity's success payload (base64-decoded
	// from history). Empty when Failed is true or when the
	// activity returned no payload.
	ResultBytes []byte
	// FailureReason is the short failure category Cadence recorded
	// (e.g., "error", "timeout"). Surfaced to the workflow as the
	// thrown error message.
	FailureReason string
	// FailureDetails is the structured failure payload Cadence
	// recorded (typically JSON-encoded InvocationError). Reserved
	// for richer error surfaces in a follow-up.
	FailureDetails []byte
}

// WalkActivityOutcomes scans the supplied history events in order
// and returns one ActivityOutcome per ActivityID seen. The returned
// slice is ordered by ScheduledEventID so the executor's index ↔
// slot mapping is stable across replays.
//
// The walker is intentionally lenient: events whose Type is not in
// the activity-task triple are skipped (the MVP doesn't react to
// WorkflowExecutionStarted, DecisionTaskScheduled, etc.). Unknown
// attributes are ignored — the v0.1 wire surface is a strict subset
// of Cadence's full event model and the rest is forward-compat noise.
func WalkActivityOutcomes(history []cadence.HistoryEvent) []ActivityOutcome {
	if len(history) == 0 {
		return nil
	}
	bySlot := make(map[string]*ActivityOutcome)
	bySchedID := make(map[int64]*ActivityOutcome)
	order := make([]string, 0, len(history))
	// First pass: pick up ActivityTaskScheduled events. They give
	// us the slot key (activity_id) and the ScheduledEventID we'll
	// use to correlate completion/failure events.
	for _, ev := range history {
		if ev.Type != EventActivityTaskScheduled {
			continue
		}
		id, _ := ev.Attributes["activity_id"].(string)
		if id == "" {
			continue
		}
		if _, ok := bySlot[id]; ok {
			// Duplicate schedule for the same activity_id is a
			// nondeterminism bug in the workflow author's code,
			// but history is authoritative — keep the first.
			continue
		}
		actType, _ := ev.Attributes["activity_type"].(string)
		slot := &ActivityOutcome{
			ActivityID:       id,
			ActivityType:     actType,
			ScheduledEventID: ev.ID,
			Scheduled:        true,
		}
		bySlot[id] = slot
		bySchedID[ev.ID] = slot
		order = append(order, id)
	}
	// Second pass: match completions and failures by
	// scheduled_event_id via the O(1) index built above.
	for _, ev := range history {
		switch ev.Type {
		case EventActivityTaskCompleted:
			slot := bySchedID[readInt64(ev.Attributes, "scheduled_event_id")]
			if slot == nil {
				continue
			}
			slot.Resolved = true
			if b64, ok := ev.Attributes["result_base64"].(string); ok && b64 != "" {
				slot.ResultBytes = decodeBase64Tolerant(b64)
			}
		case EventActivityTaskFailed:
			slot := bySchedID[readInt64(ev.Attributes, "scheduled_event_id")]
			if slot == nil {
				continue
			}
			slot.Resolved = true
			slot.Failed = true
			if reason, ok := ev.Attributes["reason"].(string); ok {
				slot.FailureReason = reason
			}
			if b64, ok := ev.Attributes["details_base64"].(string); ok && b64 != "" {
				slot.FailureDetails = decodeBase64Tolerant(b64)
			}
		}
	}
	out := make([]ActivityOutcome, 0, len(order))
	for _, id := range order {
		out = append(out, *bySlot[id])
	}
	return out
}

// readInt64 pulls an int64 out of a Cadence event attribute map. JSON
// decoding lands numeric values as float64, so we accept both for
// resilience against fake servers and msgpack-derived test fixtures.
func readInt64(attrs map[string]any, key string) int64 {
	v, ok := attrs[key]
	if !ok {
		return 0
	}
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case float32:
		return int64(t)
	}
	return 0
}
