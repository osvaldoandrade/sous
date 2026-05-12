package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-chi/chi/v5"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/authz"
	"github.com/osvaldoandrade/sous/internal/bundle"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/kv"
	"github.com/osvaldoandrade/sous/internal/observability"
	"github.com/osvaldoandrade/sous/internal/sbom"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// TestGetVersionSBOMHandler exercises the read path on the SBOM
// endpoint with the FakePersistence: a successful fetch returns the
// stored bytes with the CycloneDX content type, an unknown version
// returns 404, and a non-numeric version string is rejected by
// validation. The publish-time emit path is exercised end-to-end in
// TestSBOMEmittedOnPublishE2E below.
func TestGetVersionSBOMHandler(t *testing.T) {
	const expected = `{"bomFormat":"CycloneDX","specVersion":"1.5"}`
	calls := struct {
		gotTenant string
		gotNS     string
		gotFn     string
		gotVer    int64
	}{}
	store := &testutil.FakePersistence{
		GetSBOMFn: func(_ context.Context, tenant, ns, fn string, ver int64) ([]byte, error) {
			calls.gotTenant, calls.gotNS, calls.gotFn, calls.gotVer = tenant, ns, fn, ver
			if ver == 42 {
				return []byte(expected), nil
			}
			return nil, cserrors.New(cserrors.CSValidationFailed, "sbom not found")
		},
	}
	s := newControlServer(store, &testutil.FakeMessaging{})

	// success: 200 with the SBOM bytes and CycloneDX content-type.
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodGet, "/sbom", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1", "version": "42",
	}, principalWith("t_abc123", "cs:function:read"))
	s.getVersionSBOM(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Content-Type"); got != sbom.ContentType {
		t.Fatalf("content-type=%q want=%q", got, sbom.ContentType)
	}
	if w.Body.String() != expected {
		t.Fatalf("body=%q want=%q", w.Body.String(), expected)
	}
	if calls.gotVer != 42 || calls.gotTenant != "t_abc123" || calls.gotNS != "ns1" || calls.gotFn != "fn1" {
		t.Fatalf("unexpected GetSBOM args: %+v", calls)
	}

	// not found: missing version surfaces as 400 (CS_VALIDATION_FAILED)
	// per the existing handler convention.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/sbom", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1", "version": "99",
	}, principalWith("t_abc123", "cs:function:read"))
	s.getVersionSBOM(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("not found status=%d body=%s", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSValidationFailed {
		t.Fatalf("not found code=%s", code)
	}

	// invalid version path param is rejected before hitting the store.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/sbom", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1", "version": "garbage",
	}, principalWith("t_abc123", "cs:function:read"))
	s.getVersionSBOM(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid version status=%d body=%s", w.Code, w.Body.String())
	}

	// missing read role is rejected by authorize.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/sbom", "", map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1", "version": "42",
	}, principalWith("t_abc123"))
	s.getVersionSBOM(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("authz denied status=%d body=%s", w.Code, w.Body.String())
	}
}

// TestSBOMEmittedOnPublishE2E exercises the full publish → SBOM emit →
// GET round trip against miniredis-backed kv.Store. It is the
// integration check the issue calls for: publish a function, then fetch
// the SBOM and assert it lists the runtime plus every bundled file.
func TestSBOMEmittedOnPublishE2E(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	store := kv.NewStore(mr.Addr(), "")
	t.Cleanup(func() { _ = store.Close() })

	cfg := config.Config{}
	cfg.CSControl.Limits.MaxBundleBytes = 1024 * 1024
	cfg.CSControl.Limits.DraftTTLSeconds = 120
	s := &server{
		cfg:    cfg,
		store:  store,
		broker: &testutil.FakeMessaging{},
		logger: observability.NewLoggerWithWriter("test", io.Discard),
	}

	// Build the canonical function bundle: one source file + manifest.
	manifestJSON := `{"schema":"cs.function.script.v1","runtime":"cs-js","entry":"function.js","handler":"default","limits":{"timeoutMs":1000,"memoryMb":64,"maxConcurrency":1},"capabilities":{"kv":{"prefixes":["ctr:"],"ops":["get","set","del"]},"codeq":{"publishTopics":["jobs.*"]},"http":{"allowHosts":["example.com"],"timeoutMs":1000}}}`
	files := map[string][]byte{
		"function.js":   []byte(`export default function(){ return {statusCode:200,body:"ok"} }`),
		"manifest.json": []byte(manifestJSON),
	}
	_, sha, _, err := bundle.BuildCanonical(files)
	if err != nil {
		t.Fatalf("BuildCanonical: %v", err)
	}

	// Seed a draft for publish to consume.
	encoded := map[string]string{}
	for n, b := range files {
		encoded[n] = base64.StdEncoding.EncodeToString(b)
	}
	draft := api.DraftRecord{
		DraftID:     "drf_e2e",
		SHA256:      sha,
		Files:       encoded,
		CreatedAtMS: time.Now().UnixMilli(),
		ExpiresAtMS: time.Now().Add(time.Minute).UnixMilli(),
	}
	if err := store.PutDraft(context.Background(), "t_abc123", "ns1", "fn1", draft, time.Minute); err != nil {
		t.Fatalf("PutDraft: %v", err)
	}

	// Drive publish through the real handler.
	w := httptest.NewRecorder()
	body := `{"draft_id":"drf_e2e","config":{"timeout_ms":1000,"memory_mb":64,"max_concurrency":1}}`
	req := controlRequest(http.MethodPost, "/publish", body, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", w.Code, w.Body.String())
	}
	var pubResp struct {
		Version int64  `json:"version"`
		SHA256  string `json:"sha256"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &pubResp); err != nil {
		t.Fatalf("decode publish response: %v", err)
	}

	// Now spin a real http server with chi routing so URL params get
	// extracted the same way they do in production.
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		// Inject the principal we use for the authorized request below
		// before chi runs; in production this is the authn middleware.
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := authz.WithPrincipal(r.Context(), authz.Principal{
				Sub: "u1", Tenant: "t_abc123", Roles: []string{"cs:function:read"},
			})
			ctx = observability.WithRequestID(ctx, "req_e2e")
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	})
	r.Get("/v1/tenants/{tenant}/namespaces/{namespace}/functions/{name}/versions/{version}/sbom", s.getVersionSBOM)
	ts := httptest.NewServer(r)
	t.Cleanup(ts.Close)

	url := ts.URL + "/v1/tenants/t_abc123/namespaces/ns1/functions/fn1/versions/" + strconv.FormatInt(pubResp.Version, 10) + "/sbom"
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET sbom: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("sbom status=%d", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != sbom.ContentType {
		t.Fatalf("content-type=%q want=%q", got, sbom.ContentType)
	}
	raw, _ := io.ReadAll(resp.Body)
	var parsed map[string]any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		t.Fatalf("decode sbom: %v body=%s", err, string(raw))
	}
	if parsed["bomFormat"] != "CycloneDX" || parsed["specVersion"] != "1.5" {
		t.Fatalf("not CycloneDX 1.5: %v", parsed)
	}
	components, _ := parsed["components"].([]any)
	var sawFunctionFile, sawManifestFile, sawRuntime bool
	for _, c := range components {
		m, _ := c.(map[string]any)
		switch m["name"] {
		case "function.js":
			sawFunctionFile = m["type"] == "file"
		case "manifest.json":
			sawManifestFile = m["type"] == "file"
		case "cs-js":
			sawRuntime = m["type"] == "framework"
		}
	}
	if !sawFunctionFile || !sawManifestFile {
		t.Fatalf("SBOM missing bundled files; components=%v", components)
	}
	if !sawRuntime {
		t.Fatalf("SBOM missing runtime component; components=%v", components)
	}

	// Unknown version returns 404 via 400 (CS_VALIDATION_FAILED). Same
	// shape as the rest of the lifecycle endpoints.
	missing := ts.URL + "/v1/tenants/t_abc123/namespaces/ns1/functions/fn1/versions/9999/sbom"
	resp2, err := http.Get(missing)
	if err != nil {
		t.Fatalf("GET missing sbom: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing sbom status=%d", resp2.StatusCode)
	}
	// And the wire body is the typed cserrors envelope.
	missingBody, _ := io.ReadAll(resp2.Body)
	if !strings.Contains(string(missingBody), "sbom not found") {
		t.Fatalf("missing sbom body=%s", string(missingBody))
	}
}
