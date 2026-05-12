package main

// Publish-time tests for the Cadence workflow determinism linter
// (roadmap task E8.03). These tests exercise the cs-control publish
// handler with cadence-flavoured manifests and assert the static
// linter's behaviour:
//
//   - A workflow manifest with a banned API (e.g., Date.now) is rejected
//     with CS_WORKFLOW_NON_DETERMINISTIC (HTTP 422) and a violations[]
//     payload the publishing agent can render per call site.
//   - A workflow manifest whose code is clean publishes successfully —
//     the linter is a guard, not a gate on every publish.
//   - An activity manifest with the same banned API is allowed through;
//     activities run forward once per attempt and may freely call
//     nondeterministic APIs.
//
// The fixtures use the local-path import resolver so the test stays
// hermetic. See internal/cadence/determinism for the scanner under
// test and docs/12-cadence-integration.md "Determinism rules" for the
// publish-time contract.

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

// cadenceManifest returns a baseline cs-js manifest with an optional
// cadence section. Limits and capabilities match the fixture used in
// publish_imports_test.go so we don't need a separate validate path.
func cadenceManifest(t *testing.T, kind string) []byte {
	t.Helper()
	m := api.FunctionManifest{
		Schema:  "cs.function.script.v1",
		Runtime: "cs-js",
		Entry:   "function.js",
		Handler: "default",
		Limits: api.ManifestLimits{
			TimeoutMS:      1000,
			MemoryMB:       64,
			MaxConcurrency: 1,
		},
		Capabilities: api.ManifestCapabilities{
			KV:    api.ManifestKVCaps{Prefixes: []string{"ctr:"}, Ops: []string{"get", "set", "del"}},
			CodeQ: api.ManifestCodeQCaps{PublishTopics: []string{"jobs.*"}},
			HTTP:  api.ManifestHTTPCaps{AllowHosts: []string{"example.com"}, TimeoutMS: 1000},
		},
	}
	if kind != "" {
		m.Cadence = &api.CadenceConfig{Kind: kind}
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	return raw
}

// TestPublishWorkflowRejectsBannedAPI is the load-bearing assertion of
// E8.03: a workflow manifest whose function.js calls Date.now must be
// refused at publish time with CS_WORKFLOW_NON_DETERMINISTIC + 422 +
// structured violations[]. A regression here means nondeterministic
// workflow code can reach production undetected, which is the exact
// failure mode this epic exists to prevent.
func TestPublishWorkflowRejectsBannedAPI(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte(`export default async () => ({ statusCode: 200, body: String(Date.now()) });`),
		"manifest.json": cadenceManifest(t, api.CadenceKindWorkflow),
	}
	draft, _ := freezeDraftBundle(t, files)
	store := &testutil.FakePersistence{
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
		// PublishVersion must NEVER be called when the linter trips; if it
		// is, the bad bundle reached storage and the guard is broken.
		PublishVersionFn: func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error) {
			t.Fatal("PublishVersion called despite determinism violation")
			return 0, nil
		},
	}
	s := newControlServer(store, &testutil.FakeMessaging{})
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected 422, got status=%d body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Error struct {
			Code       cserrors.Code `json:"code"`
			Message    string        `json:"message"`
			Violations []struct {
				File    string `json:"file"`
				Line    int    `json:"line"`
				Column  int    `json:"column"`
				Pattern string `json:"pattern"`
				Message string `json:"message"`
			} `json:"violations"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v body=%s", err, w.Body.String())
	}
	if body.Error.Code != cserrors.CSWorkflowNonDeterministic {
		t.Fatalf("code = %q, want %q", body.Error.Code, cserrors.CSWorkflowNonDeterministic)
	}
	if len(body.Error.Violations) != 1 {
		t.Fatalf("violations length = %d, want 1: %#v", len(body.Error.Violations), body.Error.Violations)
	}
	v := body.Error.Violations[0]
	if v.Pattern != "Date.now" {
		t.Fatalf("violation pattern = %q, want Date.now", v.Pattern)
	}
	if v.File != "function.js" || v.Line != 1 {
		t.Fatalf("violation location = %s:%d (col %d), want function.js:1", v.File, v.Line, v.Column)
	}
	if v.Message == "" {
		t.Fatal("violation message must not be empty")
	}
}

// TestPublishWorkflowAcceptsCleanCode locks down the false-positive
// budget: a workflow whose function.js only touches deterministic APIs
// must publish without the linter interfering. If this test starts
// failing, a new banned pattern was added without auditing the clean
// fixture and operators will see spurious publish rejections.
func TestPublishWorkflowAcceptsCleanCode(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte(`export default async (ctx) => ({ statusCode: 200, body: ctx.requestID });`),
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
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 for clean workflow, got status=%d body=%s", w.Code, w.Body.String())
	}
	if !publishCalled {
		t.Fatal("PublishVersion was not called despite clean workflow code")
	}
}

// TestPublishActivityAllowsBannedAPI proves the linter is workflow-only.
// Activity functions run forward once per attempt and may legitimately
// call Date.now / Math.random / fetch — refusing to publish them would
// break the v0.1 contract documented in docs/12-cadence-integration.md
// and force every activity author to discover the linter exists.
func TestPublishActivityAllowsBannedAPI(t *testing.T) {
	files := map[string][]byte{
		// Same Date.now call site that trips the workflow test above —
		// only the manifest.cadence.kind value differs.
		"function.js":   []byte(`export default async () => ({ statusCode: 200, body: String(Date.now()) });`),
		"manifest.json": cadenceManifest(t, api.CadenceKindActivity),
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
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 for activity manifest, got status=%d body=%s", w.Code, w.Body.String())
	}
	if !publishCalled {
		t.Fatal("PublishVersion not called for activity with Date.now (linter must skip activities)")
	}
}

// TestPublishWorkflowDefaultsToActivity covers the absent-cadence path:
// when the manifest omits the cadence section entirely the function
// defaults to activity semantics and the linter is skipped. This keeps
// existing v0.1 manifests round-trippable without operator action.
func TestPublishWorkflowDefaultsToActivity(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte(`export default async () => ({ statusCode: 200, body: String(Math.random()) });`),
		"manifest.json": cadenceManifest(t, ""),
	}
	draft, _ := freezeDraftBundle(t, files)
	store := &testutil.FakePersistence{
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
		PublishVersionFn: func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error) {
			return 1, nil
		},
		MarkDraftConsumedFn: func(context.Context, string, string, string, string, time.Duration) error { return nil },
	}
	s := newControlServer(store, &testutil.FakeMessaging{})
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 when cadence omitted, got status=%d body=%s", w.Code, w.Body.String())
	}
}
