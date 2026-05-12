package main

// Decision-task integration tests for the E8.01 workflow poller.
// They drive pollDecisionLoop end-to-end via the fake Cadence client:
// the poller pulls a synthetic DecisionTask, resolves the workflow
// function from the binding's ActivityMap (the workflow-kind binding
// reuses the same map keyed by WorkflowType), runs the executor, and
// must ship the resulting Decisions back through
// RespondDecisionTaskCompleted. A regression here means a real
// Cadence workflow would either silently drop decisions or never
// progress past the first activity.

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/cadence"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// buildWorkflowBundle synthesises a minimal tarball with function.js
// + manifest.json so the fake persistence's GetVersion can serve it.
// We keep this here (rather than in a shared helper) so the test
// file is self-contained and reviewable in isolation.
func buildWorkflowBundle(t *testing.T, source string) []byte {
	t.Helper()
	manifest := api.FunctionManifest{
		Schema:  "cs.function.script.v1",
		Runtime: "cs-js",
		Entry:   "function.js",
		Handler: "default",
		Limits: api.ManifestLimits{
			TimeoutMS:      1000,
			MemoryMB:       64,
			MaxConcurrency: 1,
		},
		Cadence: &api.CadenceConfig{Kind: api.CadenceKindWorkflow},
	}
	manifestJSON, _ := json.Marshal(manifest)
	var buf bytes.Buffer
	w := tar.NewWriter(&buf)
	files := map[string][]byte{
		"manifest.json": manifestJSON,
		"function.js":   []byte(source),
	}
	for name, data := range files {
		hdr := &tar.Header{Name: name, Mode: 0644, Size: int64(len(data))}
		if err := w.WriteHeader(hdr); err != nil {
			t.Fatalf("tar header %s: %v", name, err)
		}
		if _, err := w.Write(data); err != nil {
			t.Fatalf("tar write %s: %v", name, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("tar close: %v", err)
	}
	return buf.Bytes()
}

// TestPollDecisionLoopSchedulesActivity exercises the happy path on
// a brand-new workflow run: the DecisionTask carries an empty
// history, the workflow function calls scheduleActivity once, and
// the poller MUST forward the resulting ScheduleActivityTask
// decision verbatim to Cadence. This is the load-bearing contract
// of E8.01 — without it no workflow can ever schedule its first
// activity.
func TestPollDecisionLoopSchedulesActivity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source := `export default function(input, ctx) {
		const r = cs.cadence.scheduleActivity("EchoActivity", { msg: "hi" });
		return { statusCode: 200, body: r };
	}`
	bundleBytes := buildWorkflowBundle(t, source)

	store := &testutil.FakePersistence{
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
			return 1, nil
		},
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				Config: api.VersionConfig{
					TimeoutMS: 500,
					Authz: api.VersionAuthz{
						InvokeCadenceRoles: []string{"role:cadence"},
					},
				},
			}, bundleBytes, nil
		},
	}

	var taskReturned atomic.Bool
	var respondedToken []byte
	var respondedDecisions []cadence.Decision
	done := make(chan struct{})
	client := &fakeCadenceClient{
		pollDecisionFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.DecisionTask, error) {
			if taskReturned.Load() {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			taskReturned.Store(true)
			return &cadence.DecisionTask{
				TaskToken:    []byte("decision-token-1"),
				WorkflowType: "EchoWorkflow",
				WorkflowID:   "wf-1",
				RunID:        "run-1",
			}, nil
		},
		respondDecisionFn: func(ctx context.Context, taskToken []byte, decisions []cadence.Decision) error {
			respondedToken = append([]byte{}, taskToken...)
			respondedDecisions = decisions
			close(done)
			cancel()
			return nil
		},
	}

	p := newTestPoller(store, &testutil.FakeMessaging{}, client)
	binding := api.WorkerBinding{
		Tenant:    "t1",
		Namespace: "n1",
		Name:      "wf-binding",
		Domain:    "payments",
		Tasklist:  "tl1",
		WorkerID:  "wid1",
		Kind:      api.CadenceBindingKindWorkflow,
		ActivityMap: map[string]api.WorkerBindingRef{
			"EchoWorkflow": {Function: "echo-wf", Alias: "prod"},
		},
	}
	sem := make(chan struct{}, 1)
	go p.pollDecisionLoop(ctx, binding, sem)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("respond-decision-completed was not invoked")
	}

	if string(respondedToken) != "decision-token-1" {
		t.Fatalf("task token = %q, want decision-token-1", respondedToken)
	}
	if len(respondedDecisions) != 1 {
		t.Fatalf("decisions = %d, want 1: %#v", len(respondedDecisions), respondedDecisions)
	}
	d := respondedDecisions[0]
	if d.Type != cadence.DecisionScheduleActivityTask {
		t.Fatalf("decision type = %q, want ScheduleActivityTask", d.Type)
	}
	if d.ScheduleActivityTask == nil || d.ScheduleActivityTask.ActivityType != "EchoActivity" {
		t.Fatalf("schedule attrs = %+v, want activity_type=EchoActivity", d.ScheduleActivityTask)
	}
}

// TestPollDecisionLoopUnmappedWorkflowAcks proves the poller doesn't
// stall on a misconfigured binding: a DecisionTask whose
// WorkflowType is not in ActivityMap is acked with an empty
// completion (so Cadence stops re-delivering it) and a warning is
// logged. Failing this test would mean a single bad workflow type
// could pin a poll slot forever.
func TestPollDecisionLoopUnmappedWorkflowAcks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var taskReturned atomic.Bool
	var ackedToken []byte
	done := make(chan struct{})
	client := &fakeCadenceClient{
		pollDecisionFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.DecisionTask, error) {
			if taskReturned.Load() {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			taskReturned.Store(true)
			return &cadence.DecisionTask{
				TaskToken:    []byte("unmapped-1"),
				WorkflowType: "UnknownWorkflow",
			}, nil
		},
		respondDecisionFn: func(ctx context.Context, taskToken []byte, decisions []cadence.Decision) error {
			ackedToken = append([]byte{}, taskToken...)
			close(done)
			cancel()
			return nil
		},
	}
	p := newTestPoller(&testutil.FakePersistence{}, &testutil.FakeMessaging{}, client)
	binding := api.WorkerBinding{
		Tenant:      "t1",
		Namespace:   "n1",
		Name:        "wf-binding",
		Kind:        api.CadenceBindingKindWorkflow,
		ActivityMap: map[string]api.WorkerBindingRef{},
	}
	sem := make(chan struct{}, 1)
	go p.pollDecisionLoop(ctx, binding, sem)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("unmapped task was not acked")
	}
	if string(ackedToken) != "unmapped-1" {
		t.Fatalf("ack token = %q, want unmapped-1", ackedToken)
	}
}

// TestRefreshBindingsSpawnsWorkflowPoller confirms that a binding
// with Kind == "workflow" causes refreshBindings to start
// pollDecisionLoop instead of pollLoop. We verify by asserting the
// fake client's PollDecisionTask method is called and
// PollActivityTask is never touched.
func TestRefreshBindingsSpawnsWorkflowPoller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var decisionPolls, activityPolls atomic.Int32
	client := &fakeCadenceClient{
		pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
			activityPolls.Add(1)
			<-ctx.Done()
			return nil, ctx.Err()
		},
		pollDecisionFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.DecisionTask, error) {
			decisionPolls.Add(1)
			cancel() // first observation is enough; tear down
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	store := &testutil.FakePersistence{
		ListAllWorkerBindingsFn: func(context.Context) ([]api.WorkerBinding, error) {
			b := api.WorkerBinding{
				Tenant:    "t1",
				Namespace: "n1",
				Name:      "wf-only",
				Enabled:   true,
				Kind:      api.CadenceBindingKindWorkflow,
			}
			b.Pollers.Activity = 1
			b.Limits.MaxInflightTasks = 1
			return []api.WorkerBinding{b}, nil
		},
	}
	p := newTestPoller(store, &testutil.FakeMessaging{}, client)
	if err := p.refreshBindings(ctx); err != nil {
		t.Fatalf("refreshBindings: %v", err)
	}
	// Allow the goroutine started by refreshBindings to schedule
	// its first poll before we observe the counters.
	select {
	case <-ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("decision poller never observed")
	}
	if decisionPolls.Load() == 0 {
		t.Fatal("PollDecisionTask was not invoked for workflow-kind binding")
	}
	if activityPolls.Load() != 0 {
		t.Fatalf("PollActivityTask was invoked %d times despite workflow-kind binding", activityPolls.Load())
	}
}
