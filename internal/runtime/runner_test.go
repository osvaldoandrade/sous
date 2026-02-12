package runtime

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
)

func TestRunnerExecutesModule(t *testing.T) {
	manifest := api.FunctionManifest{
		Schema:  "cs.function.script.v1",
		Runtime: "cs-js",
		Entry:   "function.js",
		Handler: "default",
		Limits: api.ManifestLimits{
			TimeoutMS:      3000,
			MemoryMB:       64,
			MaxConcurrency: 1,
		},
		Capabilities: api.ManifestCapabilities{
			KV:    api.ManifestKVCaps{Prefixes: []string{"ctr:"}, Ops: []string{"get", "set", "del"}},
			CodeQ: api.ManifestCodeQCaps{PublishTopics: []string{"jobs.*"}},
			HTTP:  api.ManifestHTTPCaps{AllowHosts: []string{"example.com"}, TimeoutMS: 1500},
		},
	}
	manifestRaw, _ := json.Marshal(manifest)
	files := map[string][]byte{
		"function.js":   []byte(`export default async function handle(event, ctx) { return { statusCode: 200, headers: {"x-ok":"1"}, body: "ok" } }`),
		"manifest.json": manifestRaw,
	}
	bundleBytes, _, _, err := bundle.BuildCanonical(files)
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(NewMemoryKV(), NopCodeQ{}, 256*1024, 64*1024, 1024*1024)
	out := runner.Execute(t.Context(), bundleBytes, api.InvocationRequest{
		ActivationID: "act-1",
		RequestID:    "req-1",
		Tenant:       "t_abc123",
		Namespace:    "payments",
		Ref:          api.FunctionRef{Function: "reconcile", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		Principal:    api.Principal{Sub: "user:1", Roles: []string{"role:app"}},
		DeadlineMS:   time.Now().Add(2 * time.Second).UnixMilli(),
		Event:        map[string]any{"x": 1},
	})
	if out.Status != "success" {
		t.Fatalf("unexpected status: %s error=%+v", out.Status, out.Error)
	}
	if out.Result == nil || out.Result.StatusCode != 200 || out.Result.Body != "ok" {
		t.Fatalf("unexpected result: %+v", out.Result)
	}
}

func TestRunnerDeniesKVPrefix(t *testing.T) {
	manifest := api.FunctionManifest{
		Schema:  "cs.function.script.v1",
		Runtime: "cs-js",
		Entry:   "function.js",
		Handler: "default",
		Limits: api.ManifestLimits{
			TimeoutMS:      3000,
			MemoryMB:       64,
			MaxConcurrency: 1,
		},
		Capabilities: api.ManifestCapabilities{
			KV:    api.ManifestKVCaps{Prefixes: []string{"ctr:"}, Ops: []string{"get"}},
			CodeQ: api.ManifestCodeQCaps{PublishTopics: []string{"jobs.*"}},
			HTTP:  api.ManifestHTTPCaps{AllowHosts: []string{"example.com"}, TimeoutMS: 1500},
		},
	}
	manifestRaw, _ := json.Marshal(manifest)
	files := map[string][]byte{
		"function.js":   []byte(`export default async function handle(event, ctx) { const x = cs.kv.get("blocked:key"); return { statusCode: 200, body: String(x) } }`),
		"manifest.json": manifestRaw,
	}
	bundleBytes, _, _, err := bundle.BuildCanonical(files)
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(NewMemoryKV(), NopCodeQ{}, 256*1024, 64*1024, 1024*1024)
	out := runner.Execute(t.Context(), bundleBytes, api.InvocationRequest{
		ActivationID: "act-1",
		RequestID:    "req-1",
		Tenant:       "t_abc123",
		Namespace:    "payments",
		Ref:          api.FunctionRef{Function: "reconcile", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		Principal:    api.Principal{Sub: "user:1", Roles: []string{"role:app"}},
		DeadlineMS:   time.Now().Add(2 * time.Second).UnixMilli(),
		Event:        map[string]any{"x": 1},
	})
	if out.Status != "error" {
		t.Fatalf("expected error, got %s", out.Status)
	}
	if out.Error == nil {
		t.Fatal("expected runtime error")
	}
}
