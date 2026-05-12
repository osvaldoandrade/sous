package main

// Publish-time tests for the frozen import map (roadmap task E5.01).
//
// These tests cover the cs-control publish handler when the manifest
// declares `imports`. The shape of each test follows the conventions
// established in main_test.go: a FakePersistence is plumbed with the
// minimum hooks the handler needs, the request is routed through
// publishVersion directly, and the response is asserted on the typed
// error code (or success body).
//
// The fixtures intentionally use the local-path resolver
// (`{"path": "..."}`) so the tests are hermetic; the remote-mirror code
// path is exercised against an httptest.Server in internal/bundle/imports_test.go.

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// manifestWithImports returns a baseline cs-js manifest with the
// supplied imports map already attached. Limits and capabilities match
// the fixture used elsewhere in main_test.go.
func manifestWithImports(t *testing.T, imports map[string]api.ManifestImport) []byte {
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
		Imports: imports,
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	return raw
}

// freezeDraftBundle is the test counterpart of the draft-side sha256
// computation: it base64-encodes the uploaded files and recomputes the
// canonical sha256 over the raw bytes so the handler's
// "sha256 mismatch with draft" check passes.
func freezeDraftBundle(t *testing.T, files map[string][]byte) (api.DraftRecord, string) {
	t.Helper()
	encoded := make(map[string]string, len(files))
	for name, body := range files {
		encoded[name] = base64.StdEncoding.EncodeToString(body)
	}
	_, sha, _, err := bundle.BuildCanonical(files)
	if err != nil {
		t.Fatalf("build draft sha: %v", err)
	}
	return api.DraftRecord{
		DraftID:     "drf_1",
		SHA256:      sha,
		Files:       encoded,
		CreatedAtMS: time.Now().Add(-time.Minute).UnixMilli(),
		ExpiresAtMS: time.Now().Add(time.Minute).UnixMilli(),
	}, sha
}

// TestPublishVersionWithLocalPathImport asserts the happy path: a
// publisher uploads function.js + manifest.json + a local lib/zod.js
// and the control plane freezes them into a single bundle. The
// resulting sha256 in the response reflects the *uploaded* sha (the
// agent's local hash) so the contract from
// docs/19-entity-state-machines.md "Publish atomically allocates a
// monotonic version" remains untouched.
func TestPublishVersionWithLocalPathImport(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte(`import { z } from "zod"; export default function(){ return { statusCode: 200, body: "ok" } }`),
		"manifest.json": manifestWithImports(t, map[string]api.ManifestImport{"zod": {Path: "lib/zod.js"}}),
		"lib/zod.js":    []byte("export const z = 1;"),
	}
	draft, _ := freezeDraftBundle(t, files)
	var capturedBundle []byte
	store := &testutil.FakePersistence{
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
		PublishVersionFn: func(_ context.Context, _, _, _ string, _ api.VersionRecord, b []byte, _ string) (int64, error) {
			capturedBundle = b
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
		t.Fatalf("publish status=%d body=%s", w.Code, w.Body.String())
	}
	if len(capturedBundle) == 0 {
		t.Fatal("expected bundle to be captured")
	}
	// The persisted bundle must contain the frozen import-map.json plus
	// the dep under deps/.
	decoded, err := bundle.ExtractTar(capturedBundle)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if _, ok := decoded[bundle.ImportMapFileName]; !ok {
		t.Fatalf("frozen bundle missing %s: keys=%v", bundle.ImportMapFileName, keys(decoded))
	}
	loaded, err := bundle.LoadImportMap(decoded)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	entry, ok := loaded.Imports["zod"]
	if !ok {
		t.Fatalf("zod missing from frozen map: %+v", loaded.Imports)
	}
	if data, ok := decoded[entry.Path]; !ok || len(data) == 0 {
		t.Fatalf("frozen dep bytes missing at %s", entry.Path)
	}
}

// TestPublishVersionRejectsDisallowedMirror enforces the curated-mirror
// contract from docs/15-security.md: when a manifest references an http
// URL whose host is not in publish.imports.allowed_mirrors the control
// plane refuses publish with CSValidationFailed. The error message
// contains "allowed_mirrors" so the publishing agent has actionable
// guidance.
func TestPublishVersionRejectsDisallowedMirror(t *testing.T) {
	files := map[string][]byte{
		"function.js": []byte(`import { z } from "zod"; export default function(){ return { statusCode: 200, body: "ok" } }`),
		"manifest.json": manifestWithImports(t, map[string]api.ManifestImport{
			"zod": {URL: "https://denied.example.com/zod.js"},
		}),
	}
	draft, _ := freezeDraftBundle(t, files)
	store := &testutil.FakePersistence{
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
	}
	s := newControlServer(store, &testutil.FakeMessaging{})
	// Operator allowed only this fake mirror; the manifest references a
	// different host. Resolver must deny.
	s.cfg.CSControl.Publish.Imports.AllowedMirrors = []string{"mirror.example.com"}
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for disallowed mirror, got status=%d body=%s", w.Code, w.Body.String())
	}
	code := parseErrorCode(t, w.Body.Bytes())
	if code != cserrors.CSValidationFailed {
		t.Fatalf("unexpected code=%s body=%s", code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "allowed_mirrors") {
		t.Fatalf("error body should mention allowed_mirrors: %s", w.Body.String())
	}
}

// TestPublishVersionEnforcesBundleSizeAfterFreeze guards the 16 MiB
// bundle cap from docs/26-capacity-and-limits.md. The frozen bundle
// must include the dep bytes when computing the cap, so a 17 MiB local
// dep should be rejected with CSBundleTooLarge (413) even when the
// uploaded sha was under the cap.
func TestPublishVersionEnforcesBundleSizeAfterFreeze(t *testing.T) {
	// Build a dep larger than the configured cap (1 MiB in
	// newControlServer). function.js + manifest.json by themselves are
	// well under 1 MiB so the uploaded sha passes; the frozen bundle is
	// what trips the limit.
	big := make([]byte, 1024*1024+1)
	for i := range big {
		big[i] = 'a'
	}
	files := map[string][]byte{
		"function.js":   []byte(`import { z } from "zod"; export default function(){ return { statusCode: 200, body: "ok" } }`),
		"manifest.json": manifestWithImports(t, map[string]api.ManifestImport{"zod": {Path: "lib/zod.js"}}),
		"lib/zod.js":    big,
	}
	draft, _ := freezeDraftBundle(t, files)
	store := &testutil.FakePersistence{
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
	}
	s := newControlServer(store, &testutil.FakeMessaging{})
	// Bump the per-import cap so the resolver itself does not reject the
	// dep; the test asserts the *bundle*-level cap fires.
	s.cfg.CSControl.Publish.Imports.MaxBytesPerImport = 2 * 1024 * 1024
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversize frozen bundle, status=%d body=%s", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSBundleTooLarge {
		t.Fatalf("unexpected code=%s body=%s", code, w.Body.String())
	}
}

// TestPublishVersionIntegrityMismatch rejects a publish where the
// manifest's declared integrity does not match the bytes the resolver
// actually fetched. This is the SRI guard from the issue acceptance
// criteria.
func TestPublishVersionIntegrityMismatch(t *testing.T) {
	files := map[string][]byte{
		"function.js": []byte(`import { z } from "zod"; export default function(){ return { statusCode: 200, body: "ok" } }`),
		"manifest.json": manifestWithImports(t, map[string]api.ManifestImport{
			"zod": {Path: "lib/zod.js", Integrity: "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="},
		}),
		"lib/zod.js": []byte("export const z = 1;"),
	}
	draft, _ := freezeDraftBundle(t, files)
	store := &testutil.FakePersistence{
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
	}
	s := newControlServer(store, &testutil.FakeMessaging{})
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
	}, principalWith("t_abc123", "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "integrity") {
		t.Fatalf("error body should mention integrity: %s", w.Body.String())
	}
}

// TestPublishVersionSHA256RoundTrip verifies the response sha256 is
// stable for the same input. The control plane re-resolves imports at
// publish time, so the response sha covers the *frozen* bundle (the
// bytes that actually run in the invoker) rather than the agent's
// upload-time sha. Two publishes of the same draft must produce the
// same response sha — that is the determinism contract from the
// acceptance criteria. See docs/19-entity-state-machines.md.
func TestPublishVersionSHA256RoundTrip(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte(`import { z } from "zod"; export default function(){ return { statusCode: 200, body: "ok" } }`),
		"manifest.json": manifestWithImports(t, map[string]api.ManifestImport{"zod": {Path: "lib/zod.js"}}),
		"lib/zod.js":    []byte("export const z = 1;"),
	}
	draft, _ := freezeDraftBundle(t, files)
	store := &testutil.FakePersistence{
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
		PublishVersionFn: func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error) {
			return 7, nil
		},
		MarkDraftConsumedFn: func(context.Context, string, string, string, string, time.Duration) error { return nil },
	}
	s := newControlServer(store, &testutil.FakeMessaging{})

	publishOnce := func() string {
		w := httptest.NewRecorder()
		req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
			"tenant": "t_abc123", "namespace": "ns1", "name": "fn1",
		}, principalWith("t_abc123", "cs:function:publish"))
		s.publishVersion(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		body := parseJSONBody(t, w.Body.Bytes())
		return body["sha256"].(string)
	}
	first := publishOnce()
	second := publishOnce()
	if first != second {
		t.Fatalf("publish sha must be deterministic: %s != %s", first, second)
	}
	if first == "" {
		t.Fatal("publish sha should not be empty")
	}
}

// TestPublishVersionWithoutImportsUnchanged is a regression test: a
// manifest with no imports must produce the exact bytes a v0.1 publish
// would have produced. We compare to a bundle built straight from
// function.js + manifest.json.
func TestPublishVersionWithoutImportsUnchanged(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte(`export default function(){ return { statusCode: 200, body: "ok" } }`),
		"manifest.json": manifestWithImports(t, nil),
	}
	draft, _ := freezeDraftBundle(t, files)
	var captured []byte
	store := &testutil.FakePersistence{
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
		PublishVersionFn: func(_ context.Context, _, _, _ string, _ api.VersionRecord, b []byte, _ string) (int64, error) {
			captured = b
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
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	expected, _, _, err := bundle.BuildCanonical(files)
	if err != nil {
		t.Fatalf("build expected: %v", err)
	}
	if string(captured) != string(expected) {
		t.Fatalf("frozen bundle should equal v0.1 bundle when no imports declared")
	}
}

func keys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
