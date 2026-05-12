package cadence

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type ActivityTask struct {
	TaskToken    string `json:"task_token"`
	Domain       string `json:"domain"`
	Tasklist     string `json:"tasklist"`
	WorkflowID   string `json:"workflow_id"`
	RunID        string `json:"run_id"`
	ActivityID   string `json:"activity_id"`
	ActivityType string `json:"activity_type"`
	Attempt      int    `json:"attempt"`
	InputBase64  string `json:"input_base64"`
}

// DecisionTask is the Cadence DecisionTask delivered to a workflow
// worker. The poller hands the task to the workflow executor which
// replays history, runs the user's workflow function, collects the
// new decisions it emits, and ships them back via
// RespondDecisionTaskCompleted. See docs/12-cadence-integration.md
// "Workflows (E8.01)" and roadmap task E8.01.
//
// History is the ordered list of HistoryEvent entries Cadence has
// recorded for this run. The executor walks the events in order to
// rebuild local state (activity result resolutions, etc.). For the
// v0.1 MVP only ActivityTaskScheduled / ActivityTaskCompleted /
// ActivityTaskFailed events are consumed; everything else is opaque
// metadata the executor preserves but does not interpret.
type DecisionTask struct {
	// TaskToken is opaque bytes from Cadence; it MUST be returned
	// verbatim on RespondDecisionTaskCompleted so the workflow
	// service can correlate the response with the outstanding task.
	TaskToken []byte `json:"task_token"`
	// WorkflowType identifies the workflow function to dispatch to;
	// the poller's binding maps this to a FunctionRef the same way
	// ActivityType is mapped for activity tasks.
	WorkflowType string `json:"workflow_type"`
	// WorkflowID is the per-instance identifier (stable across
	// retries). Used for logging and for routing replies on a
	// child-workflow follow-up (deferred).
	WorkflowID string `json:"workflow_id"`
	// RunID is the per-run identifier (changes on each
	// ContinueAsNew). RunID + WorkflowID uniquely identifies a
	// single execution attempt.
	RunID string `json:"run_id"`
	// History is the ordered list of events Cadence has recorded
	// for this run; the executor replays it before running fresh
	// workflow code. Empty on the very first decision of a new run.
	History []HistoryEvent `json:"history"`
	// PreviousStartedEventID is the EventID of the last
	// DecisionTaskStarted event that produced a completed decision.
	// Events with ID <= this value are "settled" history; events
	// with ID > this value are deltas the workflow has not seen yet.
	PreviousStartedEventID int64 `json:"previous_started_event_id"`
	// StartedEventID is the EventID of the DecisionTaskStarted that
	// gave rise to this poll. Returned to Cadence as part of the
	// completion so the service can mark it as observed.
	StartedEventID int64 `json:"started_event_id"`
}

// HistoryEvent is one entry in a workflow run's history. Type is the
// Cadence event type (e.g., "WorkflowExecutionStarted",
// "ActivityTaskScheduled", "ActivityTaskCompleted",
// "ActivityTaskFailed"). Attributes is the event-specific payload
// keyed by Cadence's well-known names. For the MVP the executor reads
// only a handful of keys:
//
//   - ActivityTaskScheduled: "activity_id", "activity_type",
//     "input_base64" (the bytes the workflow originally scheduled).
//   - ActivityTaskCompleted: "scheduled_event_id", "result_base64"
//     (the activity worker's return payload).
//   - ActivityTaskFailed: "scheduled_event_id", "reason",
//     "details_base64" (the failure envelope).
type HistoryEvent struct {
	ID         int64          `json:"id"`
	Type       string         `json:"type"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// DecisionType enumerates the decisions the v0.1 MVP can emit. The
// workflow executor produces a list of Decision values from each
// workflow activation; the poller serialises them onto the wire as
// part of RespondDecisionTaskCompleted.
//
// "ScheduleActivityTask" requests Cadence to schedule the named
// activity. "CompleteWorkflowExecution" requests Cadence to close
// the workflow with the provided result payload. Timer / Signal /
// ContinueAsNew / Cancel decisions are deferred (see the E8.01
// follow-up issue).
type DecisionType string

const (
	// DecisionScheduleActivityTask is the decision emitted by
	// cs.cadence.scheduleActivity inside a workflow function. It
	// instructs Cadence to enqueue an ActivityTask on the bound
	// tasklist; the resulting completion event surfaces back to
	// the workflow via the next DecisionTask's history.
	DecisionScheduleActivityTask DecisionType = "ScheduleActivityTask"
	// DecisionCompleteWorkflowExecution closes the workflow run
	// with the provided result. The executor emits this when the
	// user's workflow function returns a value (i.e., the JS
	// function awaited all its activities and produced a result).
	DecisionCompleteWorkflowExecution DecisionType = "CompleteWorkflowExecution"
)

// Decision is a single decision the workflow executor wants Cadence
// to enact. Exactly one of the attribute pointers is populated based
// on Type; the others are nil. Keeping the union explicit (rather
// than encoding everything into Attributes map[string]any) means the
// HTTPClient can validate the payload shape at marshal time and
// downstream consumers don't have to type-assert into untyped maps.
type Decision struct {
	Type                      DecisionType                  `json:"type"`
	ScheduleActivityTask      *ScheduleActivityTaskDecision `json:"schedule_activity_task,omitempty"`
	CompleteWorkflowExecution *CompleteWorkflowDecision     `json:"complete_workflow_execution,omitempty"`
}

// ScheduleActivityTaskDecision carries the attributes for a
// DecisionScheduleActivityTask. ActivityID is a workflow-unique
// identifier the workflow assigns (the executor uses a monotonic
// counter rooted at "act-<n>" so replays produce identical IDs).
// ActivityType maps to the WorkerBinding.ActivityMap key on the
// activity worker side; the workflow does not need to know which
// function backs it.
type ScheduleActivityTaskDecision struct {
	ActivityID   string `json:"activity_id"`
	ActivityType string `json:"activity_type"`
	// InputBase64 is the activity input the workflow wants to ship
	// to the activity worker, base64-encoded so binary codecs ride
	// through JSON unchanged. The workflow author passes a plain
	// JS value to cs.cadence.scheduleActivity; the runner
	// JSON-encodes it before constructing the decision.
	InputBase64 string `json:"input_base64,omitempty"`
	// Tasklist optionally overrides the workflow's default
	// activity tasklist. Empty means "use the workflow's binding's
	// tasklist" which is the only path the MVP exercises.
	Tasklist string `json:"tasklist,omitempty"`
}

// CompleteWorkflowDecision carries the attributes for a
// DecisionCompleteWorkflowExecution. ResultBase64 is the final
// payload sent to Cadence; the consumer of the workflow result
// (a client polling the workflow execution, or a parent workflow
// awaiting a child) reads it via Cadence's standard
// GetWorkflowExecutionHistory API.
type CompleteWorkflowDecision struct {
	ResultBase64 string `json:"result_base64,omitempty"`
}

type Client interface {
	PollActivityTask(ctx context.Context, domain, tasklist, workerID string) (*ActivityTask, error)
	RespondActivityCompleted(ctx context.Context, taskToken string, payload []byte) error
	RespondActivityFailed(ctx context.Context, taskToken, reason string, details []byte) error
	RecordActivityHeartbeat(ctx context.Context, taskToken string, details []byte) error
	// PollDecisionTask long-polls Cadence for the next DecisionTask
	// on the given tasklist. A nil task (with nil error) means the
	// long-poll completed empty and the caller should re-poll.
	// E8.01 MVP: only one task is returned per call; pagination of
	// the embedded history.NextPageToken is deferred.
	PollDecisionTask(ctx context.Context, domain, tasklist, workerID string) (*DecisionTask, error)
	// RespondDecisionTaskCompleted ships the workflow executor's
	// decisions back to Cadence. taskToken is the opaque token from
	// the DecisionTask; decisions is the ordered list the executor
	// emitted during this activation.
	RespondDecisionTaskCompleted(ctx context.Context, taskToken []byte, decisions []Decision) error
}

type HTTPClient struct {
	baseURL string
	http    *http.Client
}

func NewHTTPClient(addr string) *HTTPClient {
	baseURL := addr
	if !strings.HasPrefix(baseURL, "http") {
		baseURL = "http://" + baseURL
	}
	return &HTTPClient{baseURL: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: 20 * time.Second}}
}

func (c *HTTPClient) PollActivityTask(ctx context.Context, domain, tasklist, workerID string) (*ActivityTask, error) {
	payload := map[string]any{"domain": domain, "tasklist": tasklist, "worker_id": workerID}
	var resp struct {
		Task *ActivityTask `json:"task"`
	}
	if err := c.postJSON(ctx, "/v1/poll-activity-task", payload, &resp); err != nil {
		return nil, err
	}
	return resp.Task, nil
}

func (c *HTTPClient) RespondActivityCompleted(ctx context.Context, taskToken string, payload []byte) error {
	return c.postJSON(ctx, "/v1/respond-activity-completed", map[string]any{
		"task_token":     taskToken,
		"payload_base64": string(payload),
	}, nil)
}

func (c *HTTPClient) RespondActivityFailed(ctx context.Context, taskToken, reason string, details []byte) error {
	return c.postJSON(ctx, "/v1/respond-activity-failed", map[string]any{
		"task_token":     taskToken,
		"reason":         reason,
		"details_base64": string(details),
	}, nil)
}

func (c *HTTPClient) RecordActivityHeartbeat(ctx context.Context, taskToken string, details []byte) error {
	return c.postJSON(ctx, "/v1/record-activity-heartbeat", map[string]any{
		"task_token":     taskToken,
		"details_base64": string(details),
	}, nil)
}

// PollDecisionTask long-polls the workflow service for the next
// DecisionTask. The HTTP contract mirrors PollActivityTask: the
// service returns {"task": null} when the long-poll deadline expires
// without a delivery, which decodes to (nil, nil) so the caller can
// re-poll without distinguishing "timeout" from "no work".
func (c *HTTPClient) PollDecisionTask(ctx context.Context, domain, tasklist, workerID string) (*DecisionTask, error) {
	payload := map[string]any{"domain": domain, "tasklist": tasklist, "worker_id": workerID}
	var resp struct {
		Task *DecisionTask `json:"task"`
	}
	if err := c.postJSON(ctx, "/v1/poll-decision-task", payload, &resp); err != nil {
		return nil, err
	}
	return resp.Task, nil
}

// RespondDecisionTaskCompleted ships the executor-produced decisions
// back to Cadence. taskToken is sent verbatim (base64-encoded by the
// JSON encoder, since it's a byte slice); decisions is the ordered
// list the workflow emitted during this activation. The HTTP service
// returns 2xx with an empty body on success; any non-2xx surfaces as
// a wrapped error so the caller's retry/backoff path engages.
func (c *HTTPClient) RespondDecisionTaskCompleted(ctx context.Context, taskToken []byte, decisions []Decision) error {
	return c.postJSON(ctx, "/v1/respond-decision-task-completed", map[string]any{
		"task_token": taskToken,
		"decisions":  decisions,
	}, nil)
}

func (c *HTTPClient) postJSON(ctx context.Context, path string, payload any, out any) error {
	raw, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("cadence endpoint %s returned %d", path, resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
