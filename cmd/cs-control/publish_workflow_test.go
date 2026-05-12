package main

// E8.01 publish-time tests for workflow-kind manifests. These
// complement determinism_test.go (the linter-focused tests landed
// in E8.03) by asserting the *combined* contract a workflow author
// faces: a workflow bundle that calls scheduleActivity legitimately
// publishes, but the same source with a nondeterministic API (e.g.,
// Date.now) is refused so the same code never reaches the workflow
// runtime added in E8.01.
//
// If these tests start failing, the determinism linter and the
// workflow runtime have drifted apart: either a workflow can be
// published with banned APIs (and corrupt replay state in
// production), or a clean workflow is being incorrectly rejected at
// publish time (and operators lose the ability to ship workflows).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// TestPublishWorkflowAcceptsScheduleActivityCleanSource locks down
// the happy path: a workflow that only uses cs.cadence.scheduleActivity
// publishes successfully. The linter must not have any pattern that
// false-positives on the workflow host API surface, or every E8.01
// workflow would be unpublishable.
func TestPublishWorkflowAcceptsScheduleActivityCleanSource(t *testing.T) {
	src := `export default function(input, ctx) {
		const r = cs.cadence.scheduleActivity("EchoActivity", { msg: "hi" });
		return { statusCode: 200, body: r };
	}`
	files := map[string][]byte{
		"function.js":   []byte(src),
		"manifest.json": cadenceManifest(t, api.CadenceKindWorkflow),
	}
	draft, _ := freezeDraftBundle(t, files)
	publishCalled := false
	store := &testutil.FakePersistence{
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
		PublishVersionFn: func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error) {
			publishCalled = true
			return 1, nil
		},
		MarkDraftConsumedFn: func(context.Context, string, string, string, string, time.Duration) error { return nil },
	}
	s := newControlServer(store, &testutil.FakeMessaging{})
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "wf-clean",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 for clean workflow, got %d body=%s", w.Code, w.Body.String())
	}
	if !publishCalled {
		t.Fatal("PublishVersion was not called for a clean workflow that schedules an activity")
	}
}

// TestPublishWorkflowRejectsDateNowEndToEnd is the integration-style
// proof that the determinism linter (E8.03) actually gates the new
// workflow runtime (E8.01): a workflow source that calls Date.now()
// must be refused at publish time with CS_WORKFLOW_NON_DETERMINISTIC
// + 422 + a populated violations[]. Without this guard, the executor
// in internal/cadence/workflow would replay the workflow against its
// history and observe a different Date.now value on every Decision,
// silently corrupting state.
func TestPublishWorkflowRejectsDateNowEndToEnd(t *testing.T) {
	src := `export default function() {
		const t = Date.now();
		return { statusCode: 200, body: String(t) };
	}`
	files := map[string][]byte{
		"function.js":   []byte(src),
		"manifest.json": cadenceManifest(t, api.CadenceKindWorkflow),
	}
	draft, _ := freezeDraftBundle(t, files)
	store := &testutil.FakePersistence{
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
		PublishVersionFn: func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error) {
			t.Fatal("PublishVersion called despite Date.now in workflow source")
			return 0, nil
		},
	}
	s := newControlServer(store, &testutil.FakeMessaging{})
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "wf-bad",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422 for Date.now in workflow, got %d body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Error struct {
			Code       cserrors.Code `json:"code"`
			Violations []struct {
				Pattern string `json:"pattern"`
			} `json:"violations"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v body=%s", err, w.Body.String())
	}
	if body.Error.Code != cserrors.CSWorkflowNonDeterministic {
		t.Fatalf("code = %q, want %q", body.Error.Code, cserrors.CSWorkflowNonDeterministic)
	}
	if len(body.Error.Violations) == 0 || body.Error.Violations[0].Pattern != "Date.now" {
		t.Fatalf("expected Date.now violation, got %+v", body.Error.Violations)
	}
}
