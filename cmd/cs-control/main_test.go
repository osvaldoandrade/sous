package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/authz"
	"github.com/osvaldoandrade/sous/internal/bundle"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/kv"
	"github.com/osvaldoandrade/sous/internal/observability"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

func newControlServer(store *testutil.FakePersistence, broker *testutil.FakeMessaging) *server {
	cfg := config.Config{}
	cfg.CSControl.Limits.MaxBundleBytes = 1024 * 1024
	cfg.CSControl.Limits.DraftTTLSeconds = 120
	cfg.CSControl.Limits.ActTTLSeconds = 300
	cfg.CSCadencePoller.Limits.MaxInflightTasksDefault = 7
	return &server{
		cfg:    cfg,
		store:  store,
		broker: broker,
		logger: observability.NewLoggerWithWriter("test", io.Discard),
	}
}

func controlRequest(method, path, body string, params map[string]string, principal *authz.Principal) *http.Request {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rc := chi.NewRouteContext()
	for k, v := range params {
		rc.URLParams.Add(k, v)
	}
	ctx := context.WithValue(req.Context(), chi.RouteCtxKey, rc)
	ctx = observability.WithRequestID(ctx, "req_ctrl")
	if principal != nil {
		ctx = authz.WithPrincipal(ctx, *principal)
	}
	return req.WithContext(ctx)
}

func principalWith(tenant string, roles ...string) *authz.Principal {
	return &authz.Principal{
		Sub:    "u1",
		Tenant: tenant,
		Roles:  roles,
	}
}

func parseJSONBody(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("invalid json: %v body=%s", err, string(raw))
	}
	return out
}

func parseErrorCode(t *testing.T, raw []byte) cserrors.Code {
	t.Helper()
	var env cserrors.HTTPErrorEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("invalid error json: %v body=%s", err, string(raw))
	}
	return env.Error.Code
}

func TestUtilityFunctions(t *testing.T) {
	if got := requestID(httptest.NewRequest(http.MethodGet, "/", nil)); got != "" {
		t.Fatalf("requestID without context should be empty, got %q", got)
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(observability.WithRequestID(context.Background(), "req_1"))
	if got := requestID(req); got != "req_1" {
		t.Fatalf("requestID = %q", got)
	}
	if got := mustJSON(map[string]any{"a": 1}); got == "" {
		t.Fatal("mustJSON should serialize value")
	}
	if got := parseInt("7", 9); got != 7 {
		t.Fatalf("parseInt parse=%d", got)
	}
	if got := parseInt("x", 9); got != 9 {
		t.Fatalf("parseInt fallback=%d", got)
	}
	if got := clamp(1, 3, 8); got != 3 {
		t.Fatalf("clamp low=%d", got)
	}
	if got := clamp(9, 3, 8); got != 8 {
		t.Fatalf("clamp high=%d", got)
	}
	if got := clamp(5, 3, 8); got != 5 {
		t.Fatalf("clamp mid=%d", got)
	}
	if got := fmtErr(nil); got != "" {
		t.Fatalf("fmtErr nil=%q", got)
	}
	if got := fmtErr(errors.New("x")); got != "x" {
		t.Fatalf("fmtErr err=%q", got)
	}
	if got := debugf("%s-%d", "x", 2); got != "x-2" {
		t.Fatalf("debugf=%q", got)
	}
}

func TestHealthReadyAndAuthorize(t *testing.T) {
	store := &testutil.FakePersistence{
		PingFn: func(context.Context) error { return nil },
	}
	s := newControlServer(store, &testutil.FakeMessaging{})

	w := httptest.NewRecorder()
	s.healthz(w, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("healthz status=%d", w.Code)
	}
	health := parseJSONBody(t, w.Body.Bytes())
	if health["status"] != "ok" {
		t.Fatalf("healthz body=%v", health)
	}

	w = httptest.NewRecorder()
	s.readyz(w, httptest.NewRequest(http.MethodGet, "/readyz", nil).WithContext(observability.WithRequestID(context.Background(), "req_ready")))
	if w.Code != http.StatusOK {
		t.Fatalf("readyz status=%d", w.Code)
	}
	ready := parseJSONBody(t, w.Body.Bytes())
	if ready["status"] != "ready" {
		t.Fatalf("readyz body=%v", ready)
	}

	store.PingFn = func(context.Context) error { return cserrors.New(cserrors.CSKVUnavailable, "down") }
	w = httptest.NewRecorder()
	s.readyz(w, httptest.NewRequest(http.MethodGet, "/readyz", nil).WithContext(observability.WithRequestID(context.Background(), "req_ready")))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz error status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSKVUnavailable {
		t.Fatalf("readyz error code=%s", code)
	}

	// authorize missing principal
	w = httptest.NewRecorder()
	req := controlRequest(http.MethodGet, "/x", "", map[string]string{"tenant": "t_abc123", "namespace": "ns1", "name": "f1"}, nil)
	if _, _, _, _, ok := s.authorize(w, req, "cs:function:read"); ok {
		t.Fatal("authorize should fail without principal")
	}
	// authorize denied action
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/x", "", map[string]string{"tenant": "t_abc123", "namespace": "ns1", "name": "f1"}, principalWith("t_abc123", "role:user"))
	if _, _, _, _, ok := s.authorize(w, req, "cs:function:read"); ok {
		t.Fatal("authorize should fail without action role")
	}
	// authorize success
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/x", "", map[string]string{"tenant": "t_abc123", "namespace": "ns1", "name": "f1"}, principalWith("t_abc123", "cs:function:read"))
	_, tenant, namespace, name, ok := s.authorize(w, req, "cs:function:read")
	if !ok || tenant != "t_abc123" || namespace != "ns1" || name != "f1" {
		t.Fatalf("unexpected authorize success values: ok=%v tenant=%s ns=%s name=%s", ok, tenant, namespace, name)
	}
}

func TestFunctionLifecycleHandlers(t *testing.T) {
	store := &testutil.FakePersistence{}
	s := newControlServer(store, &testutil.FakeMessaging{})

	// createFunction invalid JSON
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/create", "{", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, principalWith("t_abc123", "cs:function:create"))
	s.createFunction(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("create invalid json status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
		t.Fatalf("create invalid json code=%s", code)
	}

	// createFunction success with defaults.
	store.CreateFunctionFn = func(ctx context.Context, rec api.FunctionRecord) error {
		_ = ctx
		if rec.Runtime != "cs-js" || rec.Entry != "function.js" || rec.Handler != "default" {
			t.Fatalf("expected default runtime/entry/handler, got %+v", rec)
		}
		return nil
	}
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/create", `{"name":"fn1"}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, principalWith("t_abc123", "cs:function:create"))
	s.createFunction(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("create success status=%d body=%s", w.Code, w.Body.String())
	}
	created := parseJSONBody(t, w.Body.Bytes())
	if created["tenant"] != "t_abc123" || created["namespace"] != "ns1" || created["name"] != "fn1" {
		t.Fatalf("unexpected create response: %+v", created)
	}
	if created["runtime"] != "cs-js" || created["entry"] != "function.js" || created["handler"] != "default" {
		t.Fatalf("missing default runtime fields: %+v", created)
	}

	// readFunction soft deleted without include_deleted should return validation error.
	deletedAt := time.Now().UnixMilli()
	store.GetFunctionFn = func(context.Context, string, string, string) (api.FunctionRecord, error) {
		return api.FunctionRecord{
			Tenant:      "t_abc123",
			Namespace:   "ns1",
			Name:        "fn1",
			Runtime:     "cs-js",
			Entry:       "function.js",
			Handler:     "default",
			DeletedAtMS: &deletedAt,
		}, nil
	}
	store.ListAliasesFn = func(context.Context, string, string, string) ([]api.AliasRecord, error) {
		return []api.AliasRecord{{Alias: "prod", Version: 1}}, nil
	}
	store.GetLatestVersionFn = func(context.Context, string, string, string) (api.VersionRecord, error) {
		return api.VersionRecord{Version: 4, SHA256: strings.Repeat("a", 64), PublishedAtMS: 1700000000000}, nil
	}
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/read", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:read"))
	s.readFunction(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("read deleted status=%d body=%s", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
		t.Fatalf("read deleted code=%s", code)
	}

	// include_deleted=true should work.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/read?include_deleted=true", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:read"))
	s.readFunction(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("read include_deleted status=%d body=%s", w.Code, w.Body.String())
	}
	readResp := parseJSONBody(t, w.Body.Bytes())
	if _, ok := readResp["function"]; !ok {
		t.Fatalf("read response missing function: %+v", readResp)
	}
	if aliases, ok := readResp["aliases"].([]any); !ok || len(aliases) != 1 {
		t.Fatalf("read response aliases mismatch: %+v", readResp)
	}
	latest, ok := readResp["latest_version"].(map[string]any)
	if !ok {
		t.Fatalf("read response missing latest_version: %+v", readResp)
	}
	if int(latest["version"].(float64)) != 4 {
		t.Fatalf("read latest version mismatch: %+v", latest)
	}
	if latest["sha256"] != strings.Repeat("a", 64) {
		t.Fatalf("read latest sha mismatch: %+v", latest)
	}

	// readFunction should propagate alias listing failure.
	store.ListAliasesFn = func(context.Context, string, string, string) ([]api.AliasRecord, error) {
		return nil, cserrors.New(cserrors.CSKVReadFailed, "alias listing failed")
	}
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/read?include_deleted=true", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:read"))
	s.readFunction(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("read alias list failure status=%d body=%s", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSKVReadFailed {
		t.Fatalf("read alias list failure code=%s", code)
	}

	// deleteFunction success
	store.SoftDeleteFunctionFn = func(context.Context, string, string, string, int64) error { return nil }
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodDelete, "/delete", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:delete"))
	s.deleteFunction(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("delete status=%d", w.Code)
	}
	delResp := parseJSONBody(t, w.Body.Bytes())
	if delResp["status"] != "deleted" {
		t.Fatalf("delete response=%+v", delResp)
	}
}

func TestDraftAndPublishHandlers(t *testing.T) {
	store := &testutil.FakePersistence{}
	s := newControlServer(store, &testutil.FakeMessaging{})

	// uploadDraft rejects empty files.
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPut, "/draft", `{"files":{}}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:draft:upload"))
	s.uploadDraft(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("uploadDraft empty files status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
		t.Fatalf("uploadDraft empty files code=%s", code)
	}

	files := map[string]string{
		"function.js":   base64.StdEncoding.EncodeToString([]byte(`export default function(){ return {statusCode:200, body:"ok"} }`)),
		"manifest.json": base64.StdEncoding.EncodeToString([]byte(`{"schema":"cs.function.script.v1","runtime":"cs-js","entry":"function.js","handler":"default","limits":{"timeoutMs":1000,"memoryMb":64,"maxConcurrency":1},"capabilities":{"kv":{"prefixes":["ctr:"],"ops":["get","set","del"]},"codeq":{"publishTopics":["jobs.*"]},"http":{"allowHosts":["example.com"],"timeoutMs":1000}}}`)),
	}
	store.PutDraftFn = func(context.Context, string, string, string, api.DraftRecord, time.Duration) error { return nil }
	w = httptest.NewRecorder()
	body, _ := json.Marshal(api.UploadDraftRequest{Files: files})
	req = controlRequest(http.MethodPut, "/draft", string(body), map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:draft:upload"))
	s.uploadDraft(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("uploadDraft success status=%d body=%s", w.Code, w.Body.String())
	}
	uploadResp := parseJSONBody(t, w.Body.Bytes())
	if !strings.HasPrefix(uploadResp["draft_id"].(string), "drf_") {
		t.Fatalf("draft_id format mismatch: %+v", uploadResp)
	}
	if len(uploadResp["sha256"].(string)) != 64 {
		t.Fatalf("sha256 format mismatch: %+v", uploadResp)
	}
	if uploadResp["size_bytes"].(float64) <= 0 {
		t.Fatalf("size_bytes should be > 0: %+v", uploadResp)
	}

	// publishVersion success.
	draft := api.DraftRecord{
		DraftID:     "drf_1",
		SHA256:      "will-be-replaced",
		Files:       files,
		CreatedAtMS: time.Now().Add(-time.Minute).UnixMilli(),
		ExpiresAtMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	// Compute canonical sha used by server validation path.
	decoded := map[string][]byte{}
	for name, b64 := range files {
		decoded[name], _ = base64.StdEncoding.DecodeString(b64)
	}
	_, sha, _, _ := bundle.BuildCanonical(decoded)
	draft.SHA256 = sha

	store.GetDraftFn = func(context.Context, string, string, string, string) (api.DraftRecord, error) {
		return draft, nil
	}
	store.PublishVersionFn = func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error) {
		return 7, nil
	}
	store.MarkDraftConsumedFn = func(context.Context, string, string, string, string, time.Duration) error { return nil }

	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1","alias":"prod","config":{"timeout_ms":1000,"memory_mb":64,"max_concurrency":1}}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("publishVersion status=%d body=%s", w.Code, w.Body.String())
	}
	pubResp := parseJSONBody(t, w.Body.Bytes())
	if int(pubResp["version"].(float64)) != 7 {
		t.Fatalf("publish version mismatch: %+v", pubResp)
	}
	if pubResp["sha256"] != draft.SHA256 {
		t.Fatalf("publish sha mismatch: %+v", pubResp)
	}
}

func TestAliasInvokeAndActivationHandlers(t *testing.T) {
	store := &testutil.FakePersistence{
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				Config: api.VersionConfig{
					TimeoutMS: 100,
					Authz: api.VersionAuthz{
						InvokeHTTPRoles: []string{"role:invoke"},
					},
				},
			}, nil, nil
		},
		SetAliasFn: func(context.Context, string, string, string, string, int64) error { return nil },
		ListAliasesFn: func(context.Context, string, string, string) ([]api.AliasRecord, error) {
			return []api.AliasRecord{{Alias: "prod", Version: 3}}, nil
		},
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) { return 3, nil },
		GetActivationFn: func(context.Context, string, string) (api.ActivationRecord, error) {
			return api.ActivationRecord{ActivationID: "act_1", Status: "success"}, nil
		},
		ListLogChunksFn: func(context.Context, string, string, int64, int64) ([]string, int64, error) {
			return []string{"l1", "l2"}, 2, nil
		},
	}
	broker := &testutil.FakeMessaging{
		PublishInvocationFn: func(context.Context, api.InvocationRequest) error { return nil },
		WaitForResultFn: func(context.Context, string) (api.InvocationResult, error) {
			return api.InvocationResult{ActivationID: "act_sync", Status: "success", Result: &api.FunctionResponse{StatusCode: 200, Body: "ok"}}, nil
		},
	}
	s := newControlServer(store, broker)

	// setAlias success
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPut, "/alias", `{"version":3}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1", "alias": "prod",
	}, principalWith("t_abc123", "cs:function:alias:set"))
	s.setAlias(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("setAlias status=%d body=%s", w.Code, w.Body.String())
	}
	aliasResp := parseJSONBody(t, w.Body.Bytes())
	if aliasResp["alias"] != "prod" || int(aliasResp["version"].(float64)) != 3 {
		t.Fatalf("setAlias response mismatch: %+v", aliasResp)
	}

	// listAliases success
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/aliases", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:read"))
	s.listAliases(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("listAliases status=%d", w.Code)
	}
	listAliasResp := parseJSONBody(t, w.Body.Bytes())
	if aliases, ok := listAliasResp["aliases"].([]any); !ok || len(aliases) != 1 {
		t.Fatalf("listAliases response mismatch: %+v", listAliasResp)
	}

	// invokeAPI async
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/invoke", `{"mode":"async","ref":{"alias":"prod"},"event":{"x":1}}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:invoke:api", "role:invoke"))
	s.invokeAPI(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("invoke async status=%d body=%s", w.Code, w.Body.String())
	}
	asyncResp := parseJSONBody(t, w.Body.Bytes())
	if asyncResp["status"] != "queued" || asyncResp["activation_id"] == "" {
		t.Fatalf("invoke async response mismatch: %+v", asyncResp)
	}

	// invokeAPI sync
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/invoke", `{"mode":"sync","ref":{"alias":"prod"},"event":{"x":1}}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:invoke:api", "role:invoke"))
	s.invokeAPI(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("invoke sync status=%d body=%s", w.Code, w.Body.String())
	}
	syncResp := parseJSONBody(t, w.Body.Bytes())
	if syncResp["activation_id"] != "act_sync" || syncResp["status"] != "success" {
		t.Fatalf("invoke sync response mismatch: %+v", syncResp)
	}
	if _, ok := syncResp["result"]; !ok {
		t.Fatalf("invoke sync missing result: %+v", syncResp)
	}

	// getActivation
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/activation", "", map[string]string{
		"tenant": "t_abc123", "activation_id": "act_1",
	}, principalWith("t_abc123", "cs:activation:read"))
	s.getActivation(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("getActivation status=%d", w.Code)
	}
	actResp := parseJSONBody(t, w.Body.Bytes())
	if actResp["activation_id"] != "act_1" || actResp["status"] != "success" {
		t.Fatalf("getActivation response mismatch: %+v", actResp)
	}

	// getActivationLogs with limit parsing
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/activation/logs?cursor="+kv.EncodeCursor(1)+"&limit=2", "", map[string]string{
		"tenant": "t_abc123", "activation_id": "act_1",
	}, principalWith("t_abc123", "cs:activation:read"))
	s.getActivationLogs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("getActivationLogs status=%d body=%s", w.Code, w.Body.String())
	}
	logResp := parseJSONBody(t, w.Body.Bytes())
	if chunks, ok := logResp["chunks"].([]any); !ok || len(chunks) != 2 {
		t.Fatalf("getActivationLogs chunks mismatch: %+v", logResp)
	}
	if logResp["cursor"] != "2" {
		t.Fatalf("getActivationLogs cursor mismatch: %+v", logResp)
	}
}

func TestScheduleAndWorkerBindingHandlers(t *testing.T) {
	store := &testutil.FakePersistence{
		PutScheduleFn:         func(context.Context, api.ScheduleRecord) error { return nil },
		DeleteScheduleFn:      func(context.Context, string, string, string) error { return nil },
		PutWorkerBindingFn:    func(context.Context, api.WorkerBinding) error { return nil },
		DeleteWorkerBindingFn: func(context.Context, string, string, string) error { return nil },
	}
	s := newControlServer(store, &testutil.FakeMessaging{})

	// createSchedule with defaults
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/schedule", `{"name":"sch1","every_seconds":5,"ref":{"function":"fn1","alias":"prod"}}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, principalWith("t_abc123", "cs:schedule:create"))
	s.createSchedule(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("createSchedule status=%d body=%s", w.Code, w.Body.String())
	}
	body := parseJSONBody(t, w.Body.Bytes())
	if body["overlap_policy"] != "skip" {
		t.Fatalf("expected default overlap_policy=skip, got %#v", body["overlap_policy"])
	}

	// deleteSchedule missing name
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodDelete, "/schedule", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "",
	}, principalWith("t_abc123", "cs:schedule:delete"))
	s.deleteSchedule(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("deleteSchedule missing name status=%d", w.Code)
	}

	// deleteSchedule success
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodDelete, "/schedule", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "sch1",
	}, principalWith("t_abc123", "cs:schedule:delete"))
	s.deleteSchedule(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("deleteSchedule success status=%d", w.Code)
	}
	delSched := parseJSONBody(t, w.Body.Bytes())
	if delSched["status"] != "deleted" {
		t.Fatalf("deleteSchedule response mismatch: %+v", delSched)
	}

	// createWorkerBinding applies defaults
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/worker", `{"name":"wrk1","domain":"d","tasklist":"tl","worker_id":"wid","activity_map":{"A":{"function":"fn1","alias":"prod"}}}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, principalWith("t_abc123", "cs:cadence:worker:create"))
	s.createWorkerBinding(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("createWorkerBinding status=%d body=%s", w.Code, w.Body.String())
	}
	body = parseJSONBody(t, w.Body.Bytes())
	pollers := body["pollers"].(map[string]any)
	if int(pollers["activity"].(float64)) != 1 {
		t.Fatalf("expected default pollers.activity=1, got %#v", pollers)
	}
	limits := body["limits"].(map[string]any)
	if int(limits["max_inflight_tasks"].(float64)) != 7 {
		t.Fatalf("expected default max_inflight_tasks=7, got %#v", limits)
	}

	// deleteWorkerBinding missing name
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodDelete, "/worker", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "",
	}, principalWith("t_abc123", "cs:cadence:worker:delete"))
	s.deleteWorkerBinding(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("deleteWorkerBinding missing name status=%d", w.Code)
	}

	// deleteWorkerBinding success
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodDelete, "/worker", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "w1",
	}, principalWith("t_abc123", "cs:cadence:worker:delete"))
	s.deleteWorkerBinding(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("deleteWorkerBinding status=%d", w.Code)
	}
	delWB := parseJSONBody(t, w.Body.Bytes())
	if delWB["status"] != "deleted" {
		t.Fatalf("deleteWorkerBinding response mismatch: %+v", delWB)
	}
}

func TestAdditionalHandlerErrorBranches(t *testing.T) {
	store := &testutil.FakePersistence{}
	broker := &testutil.FakeMessaging{}
	s := newControlServer(store, broker)

	// createFunction validation failure on function name.
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/create", `{"name":"x"}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, principalWith("t_abc123", "cs:function:create"))
	s.createFunction(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("createFunction validation status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationName {
		t.Fatalf("createFunction validation code=%s", code)
	}

	// deleteFunction store error.
	store.SoftDeleteFunctionFn = func(context.Context, string, string, string, int64) error {
		return cserrors.New(cserrors.CSKVWriteFailed, "delete failed")
	}
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodDelete, "/delete", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:delete"))
	s.deleteFunction(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("deleteFunction error status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSKVWriteFailed {
		t.Fatalf("deleteFunction error code=%s", code)
	}

	// uploadDraft invalid base64.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPut, "/draft", `{"files":{"function.js":"@@@","manifest.json":"e30="}}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:draft:upload"))
	s.uploadDraft(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("uploadDraft invalid b64 status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
		t.Fatalf("uploadDraft invalid b64 code=%s", code)
	}

	// publishVersion missing draft_id.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/publish", `{}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("publishVersion missing draft_id status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
		t.Fatalf("publishVersion missing draft_id code=%s", code)
	}

	// publishVersion invalid runtime limits in config.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf1","config":{"timeout_ms":900001}}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("publishVersion invalid config status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
		t.Fatalf("publishVersion invalid config code=%s", code)
	}

	// publishVersion consumed draft.
	store.GetDraftFn = func(context.Context, string, string, string, string) (api.DraftRecord, error) {
		return api.DraftRecord{
			DraftID:     "drf1",
			SHA256:      "x",
			Files:       map[string]string{"function.js": "eA==", "manifest.json": "e30="},
			Consumed:    true,
			ExpiresAtMS: time.Now().Add(time.Minute).UnixMilli(),
		}, nil
	}
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf1"}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("publishVersion consumed draft status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
		t.Fatalf("publishVersion consumed draft code=%s", code)
	}

	// setAlias invalid alias format.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPut, "/alias", `{"version":1}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1", "alias": "_",
	}, principalWith("t_abc123", "cs:function:alias:set"))
	s.setAlias(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("setAlias invalid alias status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationName {
		t.Fatalf("setAlias invalid alias code=%s", code)
	}

	// listAliases store error.
	store.ListAliasesFn = func(context.Context, string, string, string) ([]api.AliasRecord, error) {
		return nil, cserrors.New(cserrors.CSKVReadFailed, "read failed")
	}
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/aliases", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:read"))
	s.listAliases(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("listAliases error status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSKVReadFailed {
		t.Fatalf("listAliases error code=%s", code)
	}

	// invokeAPI role missing.
	store.ResolveVersionFn = func(context.Context, string, string, string, string, int64) (int64, error) { return 2, nil }
	store.GetVersionFn = func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
		return api.VersionRecord{
			Config: api.VersionConfig{
				TimeoutMS: 100,
				Authz: api.VersionAuthz{
					InvokeHTTPRoles: []string{"role:required"},
				},
			},
		}, nil, nil
	}
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/invoke", `{"mode":"sync","ref":{"alias":"prod"},"event":{}}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:invoke:api", "role:other"))
	s.invokeAPI(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("invokeAPI role-missing status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSAuthzRoleMissing {
		t.Fatalf("invokeAPI role-missing code=%s", code)
	}

	// createSchedule invalid config.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/schedule", `{"name":"","every_seconds":0}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, principalWith("t_abc123", "cs:schedule:create"))
	s.createSchedule(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("createSchedule invalid config status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
		t.Fatalf("createSchedule invalid config code=%s", code)
	}

	// createSchedule invalid overlap policy.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/schedule", `{"name":"sched","every_seconds":10,"overlap_policy":"bad","ref":{"function":"fn1"}}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, principalWith("t_abc123", "cs:schedule:create"))
	s.createSchedule(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("createSchedule invalid overlap status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
		t.Fatalf("createSchedule invalid overlap code=%s", code)
	}

	// createWorkerBinding missing required fields.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodPost, "/worker", `{"name":"wk1","domain":"d","tasklist":"tl","worker_id":"wid","activity_map":{}}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1",
	}, principalWith("t_abc123", "cs:cadence:worker:create"))
	s.createWorkerBinding(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("createWorkerBinding invalid req status=%d", w.Code)
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
		t.Fatalf("createWorkerBinding invalid req code=%s", code)
	}
}

func TestServeReturnsListenError(t *testing.T) {
	store := &testutil.FakePersistence{}
	broker := &testutil.FakeMessaging{}
	s := newControlServer(store, broker)
	s.cfg.Plugins.AuthN.Driver = "tikti"
	s.cfg.Plugins.AuthN.Tikti.IntrospectionURL = "http://127.0.0.1:1"
	s.cfg.CSControl.HTTP.Addr = ":-1"

	if err := s.serve(); err == nil {
		t.Fatal("expected listen error from invalid address")
	}
}

func TestValidationHelpersForPublishScheduleWorker(t *testing.T) {
	if err := validatePublishConfig(api.VersionConfig{TimeoutMS: 1000, MemoryMB: 64, MaxConcurrency: 1}); err != nil {
		t.Fatalf("validatePublishConfig valid failed: %v", err)
	}
	if err := validatePublishConfig(api.VersionConfig{TimeoutMS: 900001}); err == nil {
		t.Fatal("validatePublishConfig should reject timeout_ms > 900000")
	}
	if err := validatePublishConfig(api.VersionConfig{MemoryMB: 8}); err == nil {
		t.Fatal("validatePublishConfig should reject memory_mb < 16")
	}
	if err := validatePublishConfig(api.VersionConfig{MaxConcurrency: 0}); err != nil {
		t.Fatalf("validatePublishConfig should allow zero (defer to manifest defaults): %v", err)
	}
	if err := validatePublishConfig(api.VersionConfig{MaxConcurrency: 101}); err == nil {
		t.Fatal("validatePublishConfig should reject max_concurrency > 100")
	}

	req := api.CreateScheduleRequest{
		Name:         " schedule_ok ",
		EverySeconds: 30,
		Ref:          api.ScheduleRef{Function: "fn1", Alias: "prod"},
	}
	if err := validateScheduleRequest(&req); err != nil {
		t.Fatalf("validateScheduleRequest valid failed: %v", err)
	}
	if req.OverlapPolicy != "skip" {
		t.Fatalf("validateScheduleRequest should default overlap policy to skip, got %q", req.OverlapPolicy)
	}
	if req.Name != "schedule_ok" {
		t.Fatalf("validateScheduleRequest should trim name, got %q", req.Name)
	}
	badReq := api.CreateScheduleRequest{Name: "ab", EverySeconds: 1, Ref: api.ScheduleRef{Function: "fn1"}}
	if err := validateScheduleRequest(&badReq); err == nil {
		t.Fatal("validateScheduleRequest should reject short names")
	}
	badReq = api.CreateScheduleRequest{Name: "sched", EverySeconds: 86401, Ref: api.ScheduleRef{Function: "fn1"}}
	if err := validateScheduleRequest(&badReq); err == nil {
		t.Fatal("validateScheduleRequest should reject every_seconds > 86400")
	}
	badReq = api.CreateScheduleRequest{Name: "sched", EverySeconds: 1, OverlapPolicy: "bad", Ref: api.ScheduleRef{Function: "fn1"}}
	if err := validateScheduleRequest(&badReq); err == nil {
		t.Fatal("validateScheduleRequest should reject invalid overlap policy")
	}
	badReq = api.CreateScheduleRequest{Name: "sched", EverySeconds: 1, Ref: api.ScheduleRef{Function: "", Alias: "prod"}}
	if err := validateScheduleRequest(&badReq); err == nil {
		t.Fatal("validateScheduleRequest should reject missing function")
	}
	badReq = api.CreateScheduleRequest{Name: "sched", EverySeconds: 1, Ref: api.ScheduleRef{Function: "fn1", Alias: "_"}}
	if err := validateScheduleRequest(&badReq); err == nil {
		t.Fatal("validateScheduleRequest should reject invalid alias")
	}
	badReq = api.CreateScheduleRequest{Name: "sched", EverySeconds: 1, Ref: api.ScheduleRef{Function: "fn1", Version: -1}}
	if err := validateScheduleRequest(&badReq); err == nil {
		t.Fatal("validateScheduleRequest should reject negative version")
	}

	validWB := api.CreateWorkerBindingRequest{
		Name:     "worker1",
		Domain:   "payments",
		Tasklist: "payments-activities",
		WorkerID: "worker-id-1",
		ActivityMap: map[string]api.WorkerBindingRef{
			"SousInvokeActivity": {Function: "reconcile", Alias: "prod"},
		},
	}
	if err := validateWorkerBindingRequest(validWB); err != nil {
		t.Fatalf("validateWorkerBindingRequest valid failed: %v", err)
	}
	invalidWB := validWB
	invalidWB.ActivityMap = map[string]api.WorkerBindingRef{}
	if err := validateWorkerBindingRequest(invalidWB); err == nil {
		t.Fatal("validateWorkerBindingRequest should reject empty activity map")
	}
	invalidWB = validWB
	invalidWB.ActivityMap = map[string]api.WorkerBindingRef{"": {Function: "reconcile"}}
	if err := validateWorkerBindingRequest(invalidWB); err == nil {
		t.Fatal("validateWorkerBindingRequest should reject empty activity type key")
	}
	invalidWB = validWB
	invalidWB.ActivityMap = map[string]api.WorkerBindingRef{"A": {Function: "", Alias: "prod"}}
	if err := validateWorkerBindingRequest(invalidWB); err == nil {
		t.Fatal("validateWorkerBindingRequest should reject empty function references")
	}
	invalidWB = validWB
	invalidWB.ActivityMap = map[string]api.WorkerBindingRef{"A": {Function: "fn1", Alias: "_"}}
	if err := validateWorkerBindingRequest(invalidWB); err == nil {
		t.Fatal("validateWorkerBindingRequest should reject invalid alias")
	}
	invalidWB = validWB
	invalidWB.Pollers.Activity = 257
	if err := validateWorkerBindingRequest(invalidWB); err == nil {
		t.Fatal("validateWorkerBindingRequest should reject pollers.activity > 256")
	}
	invalidWB = validWB
	invalidWB.Limits.MaxInflightTasks = 100001
	if err := validateWorkerBindingRequest(invalidWB); err == nil {
		t.Fatal("validateWorkerBindingRequest should reject max inflight > 100000")
	}
}

func TestPublishVersionExceptionMatrix(t *testing.T) {
	files := map[string]string{
		"function.js":   base64.StdEncoding.EncodeToString([]byte(`export default function(){ return {statusCode:200, body:"ok"} }`)),
		"manifest.json": base64.StdEncoding.EncodeToString([]byte(`{"schema":"cs.function.script.v1","runtime":"cs-js","entry":"function.js","handler":"default","limits":{"timeoutMs":1000,"memoryMb":64,"maxConcurrency":1},"capabilities":{"kv":{"prefixes":["ctr:"],"ops":["get","set","del"]},"codeq":{"publishTopics":["jobs.*"]},"http":{"allowHosts":["example.com"],"timeoutMs":1000}}}`)),
	}
	copyFiles := func(src map[string]string) map[string]string {
		out := make(map[string]string, len(src))
		for k, v := range src {
			out[k] = v
		}
		return out
	}
	decoded := map[string][]byte{}
	for name, b64 := range files {
		decoded[name], _ = base64.StdEncoding.DecodeString(b64)
	}
	_, goodSHA, _, _ := bundle.BuildCanonical(decoded)

	makeDraft := func() api.DraftRecord {
		return api.DraftRecord{
			DraftID:     "drf_1",
			SHA256:      goodSHA,
			Files:       copyFiles(files),
			CreatedAtMS: time.Now().Add(-time.Minute).UnixMilli(),
			ExpiresAtMS: time.Now().Add(time.Minute).UnixMilli(),
		}
	}
	baseReq := func() *http.Request {
		return controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
		}, principalWith("t_abc123", "cs:function:publish"))
	}

	t.Run("draft expired", func(t *testing.T) {
		store := &testutil.FakePersistence{
			GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
				d := makeDraft()
				d.ExpiresAtMS = time.Now().Add(-time.Second).UnixMilli()
				return d, nil
			},
		}
		s := newControlServer(store, &testutil.FakeMessaging{})
		w := httptest.NewRecorder()
		s.publishVersion(w, baseReq())
		if w.Code != http.StatusBadRequest {
			t.Fatalf("expired draft status=%d body=%s", w.Code, w.Body.String())
		}
		if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
			t.Fatalf("expired draft code=%s", code)
		}
	})

	t.Run("invalid draft file encoding", func(t *testing.T) {
		store := &testutil.FakePersistence{
			GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
				d := makeDraft()
				d.Files["function.js"] = "%%%not-base64%%%"
				return d, nil
			},
		}
		s := newControlServer(store, &testutil.FakeMessaging{})
		w := httptest.NewRecorder()
		s.publishVersion(w, baseReq())
		if w.Code != http.StatusBadRequest {
			t.Fatalf("invalid encoding status=%d body=%s", w.Code, w.Body.String())
		}
		if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
			t.Fatalf("invalid encoding code=%s", code)
		}
	})

	t.Run("sha mismatch", func(t *testing.T) {
		store := &testutil.FakePersistence{
			GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
				d := makeDraft()
				d.SHA256 = strings.Repeat("b", 64)
				return d, nil
			},
		}
		s := newControlServer(store, &testutil.FakeMessaging{})
		w := httptest.NewRecorder()
		s.publishVersion(w, baseReq())
		if w.Code != http.StatusBadRequest {
			t.Fatalf("sha mismatch status=%d body=%s", w.Code, w.Body.String())
		}
		if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
			t.Fatalf("sha mismatch code=%s", code)
		}
	})

	t.Run("manifest invalid", func(t *testing.T) {
		invalidManifestFiles := map[string]string{
			"function.js":   files["function.js"],
			"manifest.json": base64.StdEncoding.EncodeToString([]byte(`{"schema":"cs.function.script.v1","runtime":"cs-js","entry":"function.js","handler":"not-default","limits":{"timeoutMs":1000,"memoryMb":64,"maxConcurrency":1},"capabilities":{"kv":{"prefixes":["ctr:"],"ops":["get"]},"codeq":{"publishTopics":["jobs.*"]},"http":{"allowHosts":["example.com"],"timeoutMs":1000}}}`)),
		}
		invalidDecoded := map[string][]byte{}
		for name, b64 := range invalidManifestFiles {
			invalidDecoded[name], _ = base64.StdEncoding.DecodeString(b64)
		}
		_, invalidSHA, _, _ := bundle.BuildCanonical(invalidDecoded)
		store := &testutil.FakePersistence{
			GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
				return api.DraftRecord{
					DraftID:     "drf_1",
					SHA256:      invalidSHA,
					Files:       invalidManifestFiles,
					CreatedAtMS: time.Now().Add(-time.Minute).UnixMilli(),
					ExpiresAtMS: time.Now().Add(time.Minute).UnixMilli(),
				}, nil
			},
		}
		s := newControlServer(store, &testutil.FakeMessaging{})
		w := httptest.NewRecorder()
		s.publishVersion(w, baseReq())
		if w.Code != http.StatusBadRequest {
			t.Fatalf("manifest invalid status=%d body=%s", w.Code, w.Body.String())
		}
		if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationManifest {
			t.Fatalf("manifest invalid code=%s", code)
		}
	})

	t.Run("publish store error", func(t *testing.T) {
		store := &testutil.FakePersistence{
			GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
				return makeDraft(), nil
			},
			PublishVersionFn: func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error) {
				return 0, cserrors.New(cserrors.CSKVWriteFailed, "publish failed")
			},
		}
		s := newControlServer(store, &testutil.FakeMessaging{})
		w := httptest.NewRecorder()
		s.publishVersion(w, baseReq())
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("publish store error status=%d body=%s", w.Code, w.Body.String())
		}
		if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSKVWriteFailed {
			t.Fatalf("publish store error code=%s", code)
		}
	})

	t.Run("mark consumed error", func(t *testing.T) {
		store := &testutil.FakePersistence{
			GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
				return makeDraft(), nil
			},
			PublishVersionFn: func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error) {
				return 11, nil
			},
			MarkDraftConsumedFn: func(context.Context, string, string, string, string, time.Duration) error {
				return cserrors.New(cserrors.CSKVWriteFailed, "mark consumed failed")
			},
		}
		s := newControlServer(store, &testutil.FakeMessaging{})
		w := httptest.NewRecorder()
		s.publishVersion(w, baseReq())
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("mark consumed error status=%d body=%s", w.Code, w.Body.String())
		}
		if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSKVWriteFailed {
			t.Fatalf("mark consumed error code=%s", code)
		}
	})
}

func TestReadFunctionLatestVersionErrorBranch(t *testing.T) {
	deletedAt := time.Now().UnixMilli()
	store := &testutil.FakePersistence{
		GetFunctionFn: func(context.Context, string, string, string) (api.FunctionRecord, error) {
			return api.FunctionRecord{
				Tenant:      "t_abc123",
				Namespace:   "ns1",
				Name:        "fn1",
				Runtime:     "cs-js",
				Entry:       "function.js",
				Handler:     "default",
				DeletedAtMS: &deletedAt,
			}, nil
		},
		ListAliasesFn: func(context.Context, string, string, string) ([]api.AliasRecord, error) {
			return []api.AliasRecord{{Alias: "prod", Version: 1}}, nil
		},
		GetLatestVersionFn: func(context.Context, string, string, string) (api.VersionRecord, error) {
			return api.VersionRecord{}, cserrors.New(cserrors.CSKVReadFailed, "latest version read failed")
		},
	}
	s := newControlServer(store, &testutil.FakeMessaging{})
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodGet, "/read?include_deleted=true", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:read"))
	s.readFunction(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("read latest version error status=%d body=%s", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSKVReadFailed {
		t.Fatalf("read latest version error code=%s", code)
	}
}

func TestHandlerErrorMatrixFromSpec(t *testing.T) {
	t.Run("authorize tenant mismatch is denied", func(t *testing.T) {
		s := newControlServer(&testutil.FakePersistence{}, &testutil.FakeMessaging{})
		w := httptest.NewRecorder()
		req := controlRequest(http.MethodGet, "/x", "", map[string]string{
			"tenant": "t_target", "namespace": "ns1", "name": "fn1",
		}, principalWith("t_other", "cs:function:read"))
		if _, _, _, _, ok := s.authorize(w, req, "cs:function:read"); ok {
			t.Fatal("authorize should fail on tenant mismatch")
		}
		if w.Code != http.StatusForbidden {
			t.Fatalf("authorize tenant mismatch status=%d body=%s", w.Code, w.Body.String())
		}
		if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSAuthzResourceMis {
			t.Fatalf("authorize tenant mismatch code=%s", code)
		}
	})

	t.Run("read function tolerates missing latest version", func(t *testing.T) {
		store := &testutil.FakePersistence{
			GetFunctionFn: func(context.Context, string, string, string) (api.FunctionRecord, error) {
				return api.FunctionRecord{
					Tenant:    "t_abc123",
					Namespace: "ns1",
					Name:      "fn1",
					Runtime:   "cs-js",
					Entry:     "function.js",
					Handler:   "default",
				}, nil
			},
			ListAliasesFn: func(context.Context, string, string, string) ([]api.AliasRecord, error) {
				return []api.AliasRecord{{Alias: "prod", Version: 1}}, nil
			},
			GetLatestVersionFn: func(context.Context, string, string, string) (api.VersionRecord, error) {
				return api.VersionRecord{}, cserrors.New(cserrors.CSValidationFailed, "version not found")
			},
		}
		s := newControlServer(store, &testutil.FakeMessaging{})
		w := httptest.NewRecorder()
		req := controlRequest(http.MethodGet, "/read", "", map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
		}, principalWith("t_abc123", "cs:function:read"))
		s.readFunction(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("read function without latest version status=%d body=%s", w.Code, w.Body.String())
		}
		body := parseJSONBody(t, w.Body.Bytes())
		if v, ok := body["latest_version"]; !ok || v != nil {
			t.Fatalf("latest_version should be null when no version exists, got %#v", body["latest_version"])
		}
	})

	t.Run("setAlias exception matrix", func(t *testing.T) {
		s := newControlServer(&testutil.FakePersistence{}, &testutil.FakeMessaging{})

		w := httptest.NewRecorder()
		req := controlRequest(http.MethodPut, "/alias", "{", map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1", "alias": "prod",
		}, principalWith("t_abc123", "cs:function:alias:set"))
		s.setAlias(w, req)
		if w.Code != http.StatusBadRequest || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSValidationFailed {
			t.Fatalf("setAlias invalid json mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		w = httptest.NewRecorder()
		req = controlRequest(http.MethodPut, "/alias", `{"version":0}`, map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1", "alias": "prod",
		}, principalWith("t_abc123", "cs:function:alias:set"))
		s.setAlias(w, req)
		if w.Code != http.StatusBadRequest || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSValidationFailed {
			t.Fatalf("setAlias non-positive version mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		store := &testutil.FakePersistence{
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{}, nil, cserrors.New(cserrors.CSValidationFailed, "version missing")
			},
		}
		s = newControlServer(store, &testutil.FakeMessaging{})
		w = httptest.NewRecorder()
		req = controlRequest(http.MethodPut, "/alias", `{"version":3}`, map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1", "alias": "prod",
		}, principalWith("t_abc123", "cs:function:alias:set"))
		s.setAlias(w, req)
		if w.Code != http.StatusBadRequest || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSValidationFailed {
			t.Fatalf("setAlias missing version mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		store = &testutil.FakePersistence{
			GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
				return api.VersionRecord{}, nil, nil
			},
			SetAliasFn: func(context.Context, string, string, string, string, int64) error {
				return cserrors.New(cserrors.CSKVWriteFailed, "set alias failed")
			},
		}
		s = newControlServer(store, &testutil.FakeMessaging{})
		w = httptest.NewRecorder()
		req = controlRequest(http.MethodPut, "/alias", `{"version":3}`, map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1", "alias": "prod",
		}, principalWith("t_abc123", "cs:function:alias:set"))
		s.setAlias(w, req)
		if w.Code != http.StatusInternalServerError || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSKVWriteFailed {
			t.Fatalf("setAlias store failure mismatch status=%d body=%s", w.Code, w.Body.String())
		}
	})

	t.Run("invoke API exception matrix", func(t *testing.T) {
		store := &testutil.FakePersistence{}
		broker := &testutil.FakeMessaging{}
		s := newControlServer(store, broker)

		w := httptest.NewRecorder()
		req := controlRequest(http.MethodPost, "/invoke", "{", map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
		}, principalWith("t_abc123", "cs:function:invoke:api", "role:invoke"))
		s.invokeAPI(w, req)
		if w.Code != http.StatusBadRequest || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSValidationFailed {
			t.Fatalf("invoke invalid json mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		store.ResolveVersionFn = func(context.Context, string, string, string, string, int64) (int64, error) {
			return 0, cserrors.New(cserrors.CSKVReadFailed, "resolve failed")
		}
		w = httptest.NewRecorder()
		req = controlRequest(http.MethodPost, "/invoke", `{"mode":"sync","ref":{"alias":"prod"}}`, map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
		}, principalWith("t_abc123", "cs:function:invoke:api", "role:invoke"))
		s.invokeAPI(w, req)
		if w.Code != http.StatusInternalServerError || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSKVReadFailed {
			t.Fatalf("invoke resolve failure mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		store.ResolveVersionFn = func(context.Context, string, string, string, string, int64) (int64, error) {
			return 3, nil
		}
		store.GetVersionFn = func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{}, nil, cserrors.New(cserrors.CSKVReadFailed, "version missing")
		}
		w = httptest.NewRecorder()
		req = controlRequest(http.MethodPost, "/invoke", `{"mode":"sync","ref":{"alias":"prod"}}`, map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
		}, principalWith("t_abc123", "cs:function:invoke:api", "role:invoke"))
		s.invokeAPI(w, req)
		if w.Code != http.StatusInternalServerError || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSKVReadFailed {
			t.Fatalf("invoke get version failure mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		store.GetVersionFn = func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				Config: api.VersionConfig{
					TimeoutMS: 100,
					Authz: api.VersionAuthz{
						InvokeHTTPRoles: []string{"role:invoke"},
					},
				},
			}, nil, nil
		}
		broker.PublishInvocationFn = func(context.Context, api.InvocationRequest) error {
			return cserrors.New(cserrors.CSCodeQPublishFailed, "publish failed")
		}
		w = httptest.NewRecorder()
		req = controlRequest(http.MethodPost, "/invoke", `{"mode":"sync","ref":{"alias":"prod"}}`, map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
		}, principalWith("t_abc123", "cs:function:invoke:api", "role:invoke"))
		s.invokeAPI(w, req)
		if w.Code != http.StatusInternalServerError || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSCodeQPublishFailed {
			t.Fatalf("invoke publish failure mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		broker.PublishInvocationFn = func(context.Context, api.InvocationRequest) error { return nil }
		broker.WaitForResultFn = func(context.Context, string) (api.InvocationResult, error) {
			return api.InvocationResult{}, cserrors.New(cserrors.CSCodeQTimeout, "wait timeout")
		}
		w = httptest.NewRecorder()
		req = controlRequest(http.MethodPost, "/invoke", `{"mode":"sync","ref":{"alias":"prod"}}`, map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
		}, principalWith("t_abc123", "cs:function:invoke:api", "role:invoke"))
		s.invokeAPI(w, req)
		if w.Code != http.StatusGatewayTimeout || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSCodeQTimeout {
			t.Fatalf("invoke wait timeout mismatch status=%d body=%s", w.Code, w.Body.String())
		}
	})

	t.Run("activation handlers propagate storage failures and limit fallback", func(t *testing.T) {
		var gotLimit int64
		store := &testutil.FakePersistence{
			GetActivationFn: func(context.Context, string, string) (api.ActivationRecord, error) {
				return api.ActivationRecord{}, cserrors.New(cserrors.CSKVReadFailed, "activation missing")
			},
			ListLogChunksFn: func(ctx context.Context, tenant, activationID string, cursor, limit int64) ([]string, int64, error) {
				_ = ctx
				_ = tenant
				_ = activationID
				_ = cursor
				gotLimit = limit
				return nil, 0, cserrors.New(cserrors.CSKVReadFailed, "logs missing")
			},
		}
		s := newControlServer(store, &testutil.FakeMessaging{})

		w := httptest.NewRecorder()
		req := controlRequest(http.MethodGet, "/activation", "", map[string]string{
			"tenant": "t_abc123", "activation_id": "act_404",
		}, principalWith("t_abc123", "cs:activation:read"))
		s.getActivation(w, req)
		if w.Code != http.StatusInternalServerError || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSKVReadFailed {
			t.Fatalf("getActivation storage error mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		w = httptest.NewRecorder()
		req = controlRequest(http.MethodGet, "/activation/logs?cursor=bad&limit=9999", "", map[string]string{
			"tenant": "t_abc123", "activation_id": "act_1",
		}, principalWith("t_abc123", "cs:activation:read"))
		s.getActivationLogs(w, req)
		if w.Code != http.StatusInternalServerError || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSKVReadFailed {
			t.Fatalf("getActivationLogs storage error mismatch status=%d body=%s", w.Code, w.Body.String())
		}
		if gotLimit != 100 {
			t.Fatalf("expected limit fallback to 100 for invalid/out-of-range input, got %d", gotLimit)
		}
	})

	t.Run("schedule and worker handlers propagate invalid json and storage errors", func(t *testing.T) {
		store := &testutil.FakePersistence{
			PutScheduleFn: func(context.Context, api.ScheduleRecord) error {
				return cserrors.New(cserrors.CSKVWriteFailed, "put schedule failed")
			},
			DeleteScheduleFn: func(context.Context, string, string, string) error {
				return cserrors.New(cserrors.CSKVWriteFailed, "delete schedule failed")
			},
			PutWorkerBindingFn: func(context.Context, api.WorkerBinding) error {
				return cserrors.New(cserrors.CSKVWriteFailed, "put worker failed")
			},
			DeleteWorkerBindingFn: func(context.Context, string, string, string) error {
				return cserrors.New(cserrors.CSKVWriteFailed, "delete worker failed")
			},
		}
		s := newControlServer(store, &testutil.FakeMessaging{})

		w := httptest.NewRecorder()
		req := controlRequest(http.MethodPost, "/schedule", "{", map[string]string{
			"tenant": "t_abc123", "namespace": "ns1",
		}, principalWith("t_abc123", "cs:schedule:create"))
		s.createSchedule(w, req)
		if w.Code != http.StatusBadRequest || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSValidationFailed {
			t.Fatalf("createSchedule invalid json mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		w = httptest.NewRecorder()
		req = controlRequest(http.MethodPost, "/schedule", `{"name":"sch1","every_seconds":5,"ref":{"function":"fn1","alias":"prod"}}`, map[string]string{
			"tenant": "t_abc123", "namespace": "ns1",
		}, principalWith("t_abc123", "cs:schedule:create"))
		s.createSchedule(w, req)
		if w.Code != http.StatusInternalServerError || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSKVWriteFailed {
			t.Fatalf("createSchedule storage failure mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		w = httptest.NewRecorder()
		req = controlRequest(http.MethodDelete, "/schedule", "", map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "sch1",
		}, principalWith("t_abc123", "cs:schedule:delete"))
		s.deleteSchedule(w, req)
		if w.Code != http.StatusInternalServerError || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSKVWriteFailed {
			t.Fatalf("deleteSchedule storage failure mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		w = httptest.NewRecorder()
		req = controlRequest(http.MethodPost, "/worker", "{", map[string]string{
			"tenant": "t_abc123", "namespace": "ns1",
		}, principalWith("t_abc123", "cs:cadence:worker:create"))
		s.createWorkerBinding(w, req)
		if w.Code != http.StatusBadRequest || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSValidationFailed {
			t.Fatalf("createWorkerBinding invalid json mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		w = httptest.NewRecorder()
		req = controlRequest(http.MethodPost, "/worker", `{"name":"wk1","domain":"d","tasklist":"tl","worker_id":"wid","activity_map":{"A":{"function":"fn1","alias":"prod"}}}`, map[string]string{
			"tenant": "t_abc123", "namespace": "ns1",
		}, principalWith("t_abc123", "cs:cadence:worker:create"))
		s.createWorkerBinding(w, req)
		if w.Code != http.StatusInternalServerError || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSKVWriteFailed {
			t.Fatalf("createWorkerBinding storage failure mismatch status=%d body=%s", w.Code, w.Body.String())
		}

		w = httptest.NewRecorder()
		req = controlRequest(http.MethodDelete, "/worker", "", map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "wk1",
		}, principalWith("t_abc123", "cs:cadence:worker:delete"))
		s.deleteWorkerBinding(w, req)
		if w.Code != http.StatusInternalServerError || parseErrorCode(t, w.Body.Bytes()) != cserrors.CSKVWriteFailed {
			t.Fatalf("deleteWorkerBinding storage failure mismatch status=%d body=%s", w.Code, w.Body.String())
		}
	})
}
