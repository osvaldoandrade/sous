package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/osvaldoandrade/sous/internal/api"
)

func setupConfigHome(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("HOME", root)
}

func mustWriteJSON(t *testing.T, path string, v any) {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("write json: %v", err)
	}
}

func TestAuthConfigAndHandleAuth(t *testing.T) {
	setupConfigHome(t)

	if err := handleAuth([]string{"login", "--tenant", "t_abc123", "--token", "tok_1", "--api-url", "http://localhost:8080"}); err != nil {
		t.Fatalf("handleAuth login failed: %v", err)
	}

	cfg, err := loadAuthConfig()
	if err != nil {
		t.Fatalf("loadAuthConfig failed: %v", err)
	}
	if cfg.Tenant != "t_abc123" || cfg.Token != "tok_1" {
		t.Fatalf("unexpected auth config: %+v", cfg)
	}

	if err := handleAuth([]string{"whoami"}); err != nil {
		t.Fatalf("handleAuth whoami failed: %v", err)
	}
	if err := handleAuth([]string{"unknown"}); err == nil {
		t.Fatal("expected unknown auth subcommand error")
	}

	setupConfigHome(t)
	t.Setenv("CS_TOKEN", "token_from_env")
	if err := handleAuth([]string{"login", "--tenant", "t_abc123"}); err != nil {
		t.Fatalf("handleAuth login with env token failed: %v", err)
	}
}

func TestFnInitAndFnTest(t *testing.T) {
	setupConfigHome(t)

	base := t.TempDir()
	dir := filepath.Join(base, "fn1")
	if err := fnInit(dir); err != nil {
		t.Fatalf("fnInit failed: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "function.js")); err != nil {
		t.Fatalf("function.js not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "manifest.json")); err != nil {
		t.Fatalf("manifest.json not created: %v", err)
	}

	eventPath := filepath.Join(base, "event.json")
	if err := os.WriteFile(eventPath, []byte(`{"k":"v"}`), 0o600); err != nil {
		t.Fatalf("write event file: %v", err)
	}
	if err := fnTest([]string{"--path", dir, "--event", eventPath}); err != nil {
		t.Fatalf("fnTest failed: %v", err)
	}
}

func TestFunctionAndTransportCommands(t *testing.T) {
	setupConfigHome(t)

	type callRecord struct {
		method string
		path   string
		body   []byte
	}
	var calls []callRecord
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		calls = append(calls, callRecord{method: r.Method, path: r.URL.Path, body: body})
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			t.Fatalf("missing authorization header for %s %s", r.Method, r.URL.Path)
		}
		switch {
		case strings.Contains(r.URL.Path, "/functions") && strings.HasSuffix(r.URL.Path, "/draft"):
			var req api.UploadDraftRequest
			if err := json.Unmarshal(body, &req); err != nil {
				t.Fatalf("invalid draft upload body: %v", err)
			}
		case strings.Contains(r.URL.Path, "/versions"):
			var req api.PublishVersionRequest
			if err := json.Unmarshal(body, &req); err != nil {
				t.Fatalf("invalid publish body: %v", err)
			}
		case strings.Contains(r.URL.Path, ":invoke"):
			var req api.InvokeAPIRequest
			if err := json.Unmarshal(body, &req); err != nil {
				t.Fatalf("invalid invoke body: %v", err)
			}
		case strings.Contains(r.URL.Path, "/schedules"):
			if r.Method == http.MethodPost {
				var req api.CreateScheduleRequest
				if err := json.Unmarshal(body, &req); err != nil {
					t.Fatalf("invalid schedule body: %v", err)
				}
			}
		case strings.Contains(r.URL.Path, "/cadence/workers"):
			var req api.CreateWorkerBindingRequest
			if err := json.Unmarshal(body, &req); err != nil {
				t.Fatalf("invalid cadence worker body: %v", err)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	if err := saveAuthConfig(authConfig{
		APIURL: srv.URL,
		Tenant: "t_abc123",
		Token:  "tok_abc",
	}); err != nil {
		t.Fatalf("saveAuthConfig failed: %v", err)
	}

	fnDir := filepath.Join(t.TempDir(), "fn1")
	if err := fnInit(fnDir); err != nil {
		t.Fatalf("fnInit failed: %v", err)
	}

	if err := fnCreate([]string{"--namespace", "ns1", "fn1"}); err != nil {
		t.Fatalf("fnCreate failed: %v", err)
	}
	if err := fnDraftUpload([]string{"--namespace", "ns1", "--path", fnDir, "fn1"}); err != nil {
		t.Fatalf("fnDraftUpload failed: %v", err)
	}
	if err := fnPublish([]string{"--namespace", "ns1", "--draft", "drf_1", "--alias", "prod", "fn1"}); err != nil {
		t.Fatalf("fnPublish failed: %v", err)
	}
	if err := fnAliasSet([]string{"--namespace", "ns1", "--version", "3", "fn1", "prod"}); err != nil {
		t.Fatalf("fnAliasSet failed: %v", err)
	}

	eventPath := filepath.Join(t.TempDir(), "invoke.json")
	if err := os.WriteFile(eventPath, []byte(`{"x":1}`), 0o600); err != nil {
		t.Fatalf("write invoke event: %v", err)
	}
	if err := fnInvoke([]string{"--namespace", "ns1", "--event", eventPath, "--mode", "sync", "fn1@prod"}); err != nil {
		t.Fatalf("fnInvoke(alias) failed: %v", err)
	}
	if err := fnInvoke([]string{"--namespace", "ns1", "fn1@3"}); err != nil {
		t.Fatalf("fnInvoke(version) failed: %v", err)
	}

	bodyPath := filepath.Join(t.TempDir(), "body.json")
	if err := os.WriteFile(bodyPath, []byte(`{"y":2}`), 0o600); err != nil {
		t.Fatalf("write body file: %v", err)
	}
	if err := handleHTTP([]string{"invoke", "-X", http.MethodPost, "-d", "@" + bodyPath, "/v1/web/t_abc123/ns1/fn1/prod"}); err != nil {
		t.Fatalf("handleHTTP failed: %v", err)
	}

	payloadPath := filepath.Join(t.TempDir(), "payload.json")
	if err := os.WriteFile(payloadPath, []byte(`{"z":3}`), 0o600); err != nil {
		t.Fatalf("write payload file: %v", err)
	}
	if err := handleSchedule([]string{"create", "--namespace", "ns1", "--fn", "fn1@prod", "--every", "15", "--payload", payloadPath, "sched1"}); err != nil {
		t.Fatalf("handleSchedule create failed: %v", err)
	}
	if err := handleSchedule([]string{"delete", "--namespace", "ns1", "sched1"}); err != nil {
		t.Fatalf("handleSchedule delete failed: %v", err)
	}

	if err := handleCadence([]string{"worker", "create", "--namespace", "ns1", "--domain", "d1", "--tasklist", "tl1", "--worker-id", "wk1", "--activity", "TypeA=fn1@prod", "w1"}); err != nil {
		t.Fatalf("handleCadence failed: %v", err)
	}

	if len(calls) != 10 {
		t.Fatalf("expected 10 API calls, got %d: %+v", len(calls), calls)
	}

	expect := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/v1/tenants/t_abc123/namespaces/ns1/functions"},
		{http.MethodPut, "/v1/tenants/t_abc123/namespaces/ns1/functions/fn1/draft"},
		{http.MethodPost, "/v1/tenants/t_abc123/namespaces/ns1/functions/fn1/versions"},
		{http.MethodPut, "/v1/tenants/t_abc123/namespaces/ns1/functions/fn1/aliases/prod"},
		{http.MethodPost, "/v1/tenants/t_abc123/namespaces/ns1/functions/fn1:invoke"},
		{http.MethodPost, "/v1/tenants/t_abc123/namespaces/ns1/functions/fn1:invoke"},
		{http.MethodPost, "/v1/web/t_abc123/ns1/fn1/prod"},
		{http.MethodPost, "/v1/tenants/t_abc123/namespaces/ns1/schedules"},
		{http.MethodDelete, "/v1/tenants/t_abc123/namespaces/ns1/schedules/sched1"},
		{http.MethodPost, "/v1/tenants/t_abc123/namespaces/ns1/cadence/workers"},
	}
	for i, e := range expect {
		if calls[i].method != e.method || calls[i].path != e.path {
			t.Fatalf("call[%d] got %s %s want %s %s", i, calls[i].method, calls[i].path, e.method, e.path)
		}
	}

	// fn create payload
	var createReq api.CreateFunctionRequest
	if err := json.Unmarshal(calls[0].body, &createReq); err != nil {
		t.Fatalf("create payload decode: %v", err)
	}
	if createReq.Name != "fn1" || createReq.Runtime != "cs-js" || createReq.Entry != "function.js" || createReq.Handler != "default" {
		t.Fatalf("create payload mismatch: %+v", createReq)
	}

	// publish payload
	var publishReq api.PublishVersionRequest
	if err := json.Unmarshal(calls[2].body, &publishReq); err != nil {
		t.Fatalf("publish payload decode: %v", err)
	}
	if publishReq.DraftID != "drf_1" || publishReq.Alias != "prod" {
		t.Fatalf("publish payload mismatch: %+v", publishReq)
	}

	// alias set payload
	var aliasReq api.SetAliasRequest
	if err := json.Unmarshal(calls[3].body, &aliasReq); err != nil {
		t.Fatalf("alias payload decode: %v", err)
	}
	if aliasReq.Version != 3 {
		t.Fatalf("alias payload mismatch: %+v", aliasReq)
	}

	// invoke payloads: alias then numeric version.
	var invAlias api.InvokeAPIRequest
	if err := json.Unmarshal(calls[4].body, &invAlias); err != nil {
		t.Fatalf("invoke(alias) payload decode: %v", err)
	}
	if invAlias.Ref.Alias != "prod" || invAlias.Mode != "sync" {
		t.Fatalf("invoke(alias) payload mismatch: %+v", invAlias)
	}
	var invVersion api.InvokeAPIRequest
	if err := json.Unmarshal(calls[5].body, &invVersion); err != nil {
		t.Fatalf("invoke(version) payload decode: %v", err)
	}
	if invVersion.Ref.Version != 3 || invVersion.Ref.Alias != "" {
		t.Fatalf("invoke(version) payload mismatch: %+v", invVersion)
	}

	// raw HTTP invoke body should match file payload.
	if strings.TrimSpace(string(calls[6].body)) != `{"y":2}` {
		t.Fatalf("http invoke payload mismatch: %s", string(calls[6].body))
	}

	// schedule create payload
	var schedReq api.CreateScheduleRequest
	if err := json.Unmarshal(calls[7].body, &schedReq); err != nil {
		t.Fatalf("schedule payload decode: %v", err)
	}
	if schedReq.Name != "sched1" || schedReq.EverySeconds != 15 || schedReq.Ref.Function != "fn1" || schedReq.Ref.Alias != "prod" {
		t.Fatalf("schedule payload mismatch: %+v", schedReq)
	}

	// cadence worker payload
	var workerReq api.CreateWorkerBindingRequest
	if err := json.Unmarshal(calls[9].body, &workerReq); err != nil {
		t.Fatalf("cadence worker payload decode: %v", err)
	}
	if workerReq.Name != "w1" || workerReq.Domain != "d1" || workerReq.Tasklist != "tl1" || workerReq.WorkerID != "wk1" {
		t.Fatalf("worker payload mismatch: %+v", workerReq)
	}
}

func TestDispatchersAndUtilityErrors(t *testing.T) {
	setupConfigHome(t)

	if err := handleFunction([]string{}); err == nil {
		t.Fatal("handleFunction should fail without subcommand")
	}
	if err := handleFunction([]string{"unknown"}); err == nil {
		t.Fatal("handleFunction should fail with unknown subcommand")
	}
	if err := handleHTTP([]string{}); err == nil {
		t.Fatal("handleHTTP should fail with invalid args")
	}
	if err := handleSchedule([]string{"unknown"}); err == nil {
		t.Fatal("handleSchedule should fail with unknown subcommand")
	}
	if err := handleCadence([]string{"x"}); err == nil {
		t.Fatal("handleCadence should fail with invalid args")
	}

	if got := safePrefix("abcdef", 3); got != "abc" {
		t.Fatalf("safePrefix truncated = %q", got)
	}
	if got := safePrefix("ab", 3); got != "ab" {
		t.Fatalf("safePrefix full = %q", got)
	}

	path, err := authPath()
	if err != nil {
		t.Fatalf("authPath failed: %v", err)
	}
	_ = os.Remove(path)
	if _, err := loadAuthConfig(); err == nil {
		t.Fatal("loadAuthConfig should fail when file is missing")
	}

	mustWriteJSON(t, path, map[string]any{"api_url": "", "tenant": "", "token": ""})
	if _, err := loadAuthConfig(); err == nil {
		t.Fatal("loadAuthConfig should fail for invalid config")
	}
}

func TestDoJSONServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad"}`))
	}))
	defer srv.Close()

	_, err := doJSON("tok", http.MethodPost, srv.URL, map[string]any{"x": 1})
	if err == nil || !strings.Contains(err.Error(), "server error") {
		t.Fatalf("expected server error from doJSON, got: %v", err)
	}

	if _, err := doJSON("tok", " ", srv.URL, nil); err == nil {
		t.Fatal("expected request creation error with invalid method")
	}
}

func TestHandleAuthValidationError(t *testing.T) {
	setupConfigHome(t)
	if err := handleAuth([]string{"login", "--tenant", "t_abc123"}); err == nil {
		t.Fatal("expected validation error when token is missing")
	}
}

func TestHandleFunctionDispatches(t *testing.T) {
	setupConfigHome(t)

	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t_abc123", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig failed: %v", err)
	}
	fnDir := filepath.Join(t.TempDir(), "f2")
	if err := handleFunction([]string{"init", fnDir}); err != nil {
		t.Fatalf("handleFunction init failed: %v", err)
	}
	if err := handleFunction([]string{"create", "--namespace", "ns1", "fn1"}); err != nil {
		t.Fatalf("handleFunction create failed: %v", err)
	}
	if err := handleFunction([]string{"test", "--path", fnDir}); err != nil {
		t.Fatalf("handleFunction test failed: %v", err)
	}
	if err := handleFunction([]string{"draft", "upload", "--namespace", "ns1", "--path", fnDir, "fn1"}); err != nil {
		t.Fatalf("handleFunction draft upload failed: %v", err)
	}
	if err := handleFunction([]string{"publish", "--namespace", "ns1", "--draft", "drf_1", "fn1"}); err != nil {
		t.Fatalf("handleFunction publish failed: %v", err)
	}
	if err := handleFunction([]string{"alias", "set", "--namespace", "ns1", "--version", "1", "fn1", "prod"}); err != nil {
		t.Fatalf("handleFunction alias set failed: %v", err)
	}
	if err := handleFunction([]string{"invoke", "--namespace", "ns1", "fn1@prod"}); err != nil {
		t.Fatalf("handleFunction invoke failed: %v", err)
	}

	if err := handleFunction([]string{"draft", "x"}); err == nil {
		t.Fatal("expected draft usage error")
	}
	if err := handleFunction([]string{"alias", "x"}); err == nil {
		t.Fatal("expected alias usage error")
	}
	if err := handleFunction([]string{"init"}); err == nil {
		t.Fatal("expected init usage error")
	}
	if calls == 0 {
		t.Fatal("expected function command HTTP calls")
	}
}

func TestUsageFunction(t *testing.T) {
	usage()
}

func TestDoJSONNetworkError(t *testing.T) {
	_, err := doJSON("tok", http.MethodGet, "http://127.0.0.1:1", nil)
	if err == nil {
		t.Fatal("expected network error for unreachable host")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected canceled context error: %v", err)
	}
}

func TestCLIValidationMatrix(t *testing.T) {
	setupConfigHome(t)

	if err := fnCreate([]string{}); err == nil {
		t.Fatal("expected usage error in fnCreate without name")
	}

	if err := fnDraftUpload([]string{}); err == nil {
		t.Fatal("expected usage error in fnDraftUpload without name")
	}

	if err := fnPublish([]string{"fn1"}); err == nil {
		t.Fatal("expected usage error in fnPublish without --draft")
	}

	if err := fnAliasSet([]string{"fn1", "prod"}); err == nil {
		t.Fatal("expected usage error in fnAliasSet without positive version")
	}

	if err := fnInvoke([]string{}); err == nil {
		t.Fatal("expected usage error in fnInvoke without ref")
	}
	if err := fnInvoke([]string{"badref"}); err == nil {
		t.Fatal("expected ref format error in fnInvoke without @")
	}

	if err := handleSchedule([]string{"create", "--namespace", "ns1", "sched1"}); err == nil {
		t.Fatal("expected usage error in schedule create without --fn")
	}
	if err := handleSchedule([]string{"delete", "--namespace", "ns1"}); err == nil {
		t.Fatal("expected usage error in schedule delete without name")
	}
	if err := handleCadence([]string{"worker", "create", "--domain", "d", "--tasklist", "tl", "--worker-id", "wid", "--activity", "invalid", "w1"}); err == nil {
		t.Fatal("expected activity format error in cadence worker create")
	}

	fnDir := filepath.Join(t.TempDir(), "fn-error")
	if err := fnInit(fnDir); err != nil {
		t.Fatalf("fnInit for validation matrix failed: %v", err)
	}
	if err := saveAuthConfig(authConfig{APIURL: "http://127.0.0.1:1", Tenant: "t_abc123", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig failed: %v", err)
	}

	badEventPath := filepath.Join(t.TempDir(), "bad-event.json")
	if err := os.WriteFile(badEventPath, []byte(`{`), 0o600); err != nil {
		t.Fatalf("write bad event file: %v", err)
	}
	if err := fnTest([]string{"--path", fnDir, "--event", badEventPath}); err == nil {
		t.Fatal("expected invalid event JSON error in fnTest")
	}
	if err := fnInvoke([]string{"--namespace", "ns1", "--event", badEventPath, "fn1@prod"}); err == nil {
		t.Fatal("expected invalid event JSON error in fnInvoke")
	}

	if err := fnDraftUpload([]string{"--namespace", "ns1", "--path", filepath.Join(t.TempDir(), "missing"), "fn1"}); err == nil {
		t.Fatal("expected filesystem error in fnDraftUpload for missing files")
	}

	if err := handleHTTP([]string{"invoke", "-d", "@missing-body.json", "/v1/tenants/t_abc123/namespaces/ns1/functions/fn1:invoke"}); err == nil {
		t.Fatal("expected body file read error in handleHTTP")
	}

	if err := handleSchedule([]string{"create", "--namespace", "ns1", "--fn", "fn1prod", "sched1"}); err == nil {
		t.Fatal("expected fn format error in schedule create")
	}

	badPayloadPath := filepath.Join(t.TempDir(), "bad-payload.json")
	if err := os.WriteFile(badPayloadPath, []byte(`{`), 0o600); err != nil {
		t.Fatalf("write bad payload file: %v", err)
	}
	if err := handleSchedule([]string{"create", "--namespace", "ns1", "--fn", "fn1@prod", "--payload", badPayloadPath, "sched1"}); err == nil {
		t.Fatal("expected payload JSON error in schedule create")
	}

	path, err := authPath()
	if err != nil {
		t.Fatalf("authPath failed: %v", err)
	}
	if err := os.WriteFile(path, []byte(`{`), 0o600); err != nil {
		t.Fatalf("write invalid auth json failed: %v", err)
	}
	if _, err := loadAuthConfig(); err == nil {
		t.Fatal("expected JSON parse error in loadAuthConfig")
	}
}

func TestAuthPathAndFnInitErrorBranches(t *testing.T) {
	setupConfigHome(t)

	blockedRoot := t.TempDir()
	blockingFile := filepath.Join(blockedRoot, "blocked")
	if err := os.WriteFile(blockingFile, []byte("x"), 0o600); err != nil {
		t.Fatalf("write blocking file: %v", err)
	}

	t.Setenv("XDG_CONFIG_HOME", blockingFile)
	t.Setenv("HOME", blockingFile)
	if _, err := authPath(); err == nil {
		t.Fatal("expected authPath failure when config root is a file")
	}
	if err := saveAuthConfig(authConfig{APIURL: "http://localhost:8080", Tenant: "t_abc123", Token: "tok"}); err == nil {
		t.Fatal("expected saveAuthConfig failure when authPath fails")
	}
	if _, err := loadAuthConfig(); err == nil {
		t.Fatal("expected loadAuthConfig failure when authPath fails")
	}

	invalidDirPath := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(invalidDirPath, []byte("x"), 0o600); err != nil {
		t.Fatalf("write invalid dir file: %v", err)
	}
	if err := fnInit(invalidDirPath); err == nil {
		t.Fatal("expected fnInit failure when target path is a file")
	}
}
