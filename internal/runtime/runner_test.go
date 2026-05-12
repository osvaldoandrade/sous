package runtime

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
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

// importTestManifest is a tiny baseline manifest used by the import-map
// runtime tests. Capabilities are minimal because the tests only care
// about specifier resolution, not host APIs.
func importTestManifest(imports map[string]api.ManifestImport) api.FunctionManifest {
	return api.FunctionManifest{
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
		Imports: imports,
	}
}

// freezeBundle is the test-side mirror of the cs-control publish path:
// it resolves imports through the bundle.Resolver and packs the result
// into a canonical tar. Tests use this to assert the runtime can read a
// publish-time-produced bundle without any extra wiring.
func freezeBundle(t *testing.T, manifest api.FunctionManifest, uploaded map[string][]byte) []byte {
	t.Helper()
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	uploaded["manifest.json"] = raw
	resolver := bundle.NewResolver(bundle.ResolverOptions{})
	resolved, err := resolver.Resolve(context.Background(), manifest, uploaded)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	b, _, _, err := bundle.BuildCanonical(resolved.Files)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return b
}

// TestRunnerExecutesFrozenImport verifies the happy path of the import
// map at the runtime layer: a function that imports a named export from
// a frozen dep can call into it and produce the expected output. This
// is the closest in-tree analogue of the "publish then invoke" loop
// promised by the issue acceptance criteria.
func TestRunnerExecutesFrozenImport(t *testing.T) {
	manifest := importTestManifest(map[string]api.ManifestImport{
		"adder": {Path: "lib/adder.js"},
	})
	uploaded := map[string][]byte{
		"function.js": []byte(`
import { add } from "adder"
export default async function handle(event, ctx) {
  return { statusCode: 200, body: String(add(event.a, event.b)) }
}
`),
		"lib/adder.js": []byte(`export function add(a, b) { return a + b; }`),
	}
	bundleBytes := freezeBundle(t, manifest, uploaded)
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
		Event:        map[string]any{"a": 2, "b": 3},
	})
	if out.Status != "success" {
		t.Fatalf("status=%s error=%+v", out.Status, out.Error)
	}
	if out.Result == nil || out.Result.Body != "5" {
		t.Fatalf("unexpected result: %+v", out.Result)
	}
}

// TestRunnerImportNotFound asserts the contract from the issue: an
// undeclared specifier raises CS_IMPORT_NOT_FOUND (HTTP 422 once it
// reaches the gateway). The runtime returns ResolvedCode so the
// invoker-pool can translate it to the typed error envelope.
func TestRunnerImportNotFound(t *testing.T) {
	manifest := importTestManifest(nil)
	uploaded := map[string][]byte{
		"function.js": []byte(`
import { z } from "zod"
export default async function handle(event, ctx) {
  return { statusCode: 200, body: "ok" }
}
`),
	}
	raw, _ := json.Marshal(manifest)
	uploaded["manifest.json"] = raw
	bundleBytes, _, _, err := bundle.BuildCanonical(uploaded)
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
		Event:        map[string]any{},
	})
	if out.Status == "success" {
		t.Fatalf("expected error, got success: %+v", out.Result)
	}
	if out.ResolvedCode != cserrors.CSImportNotFound {
		t.Fatalf("expected CSImportNotFound, got %q error=%+v", out.ResolvedCode, out.Error)
	}
	if out.Error == nil || !strings.Contains(out.Error.Message, "zod") {
		t.Fatalf("error message should mention specifier: %+v", out.Error)
	}
}

// TestRunnerImportMultipleShapes exercises the rewriter for all four
// supported import forms: default, named, namespace, and side-effect.
// The dep deliberately has both default and named exports plus a
// runtime side effect (assignment on globalThis) so the test can prove
// each form was wired correctly without needing fancier introspection.
func TestRunnerImportMultipleShapes(t *testing.T) {
	manifest := importTestManifest(map[string]api.ManifestImport{
		"util": {Path: "lib/util.js"},
	})
	uploaded := map[string][]byte{
		"function.js": []byte(`
import util, { greet, version as v } from "util"
import * as ns from "util"
import "util"
export default async function handle(event, ctx) {
  return {
    statusCode: 200,
    body: JSON.stringify({
      defaultName: util.name,
      greeted: greet("imp"),
      version: v,
      nsKeys: Object.keys(ns).sort(),
      sideEffect: globalThis.__util_side_effect
    })
  }
}
`),
		"lib/util.js": []byte(`
export const version = "1.2.3"
export function greet(who) { return "hello " + who }
export default { name: "util" }
globalThis.__util_side_effect = "ran"
`),
	}
	bundleBytes := freezeBundle(t, manifest, uploaded)
	runner := NewRunner(NewMemoryKV(), NopCodeQ{}, 256*1024, 64*1024, 1024*1024)
	out := runner.Execute(t.Context(), bundleBytes, api.InvocationRequest{
		ActivationID: "act-1",
		RequestID:    "req-1",
		Tenant:       "t_abc123",
		Namespace:    "payments",
		Ref:          api.FunctionRef{Function: "fn", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		Principal:    api.Principal{Sub: "user:1", Roles: []string{"role:app"}},
		DeadlineMS:   time.Now().Add(2 * time.Second).UnixMilli(),
		Event:        map[string]any{},
	})
	if out.Status != "success" {
		t.Fatalf("status=%s error=%+v", out.Status, out.Error)
	}
	var parsed map[string]any
	if err := json.Unmarshal([]byte(out.Result.Body), &parsed); err != nil {
		t.Fatalf("decode body: %v body=%s", err, out.Result.Body)
	}
	if parsed["defaultName"] != "util" {
		t.Fatalf("default import wrong: %+v", parsed)
	}
	if parsed["greeted"] != "hello imp" {
		t.Fatalf("named import wrong: %+v", parsed)
	}
	if parsed["version"] != "1.2.3" {
		t.Fatalf("aliased import wrong: %+v", parsed)
	}
	if parsed["sideEffect"] != "ran" {
		t.Fatalf("side-effect import did not run: %+v", parsed)
	}
}

// TestRunnerImportIntegrityFailure asserts that a tampered dep — bytes
// in the bundle that no longer match the frozen SRI digest — is rejected
// with CS_IMPORT_NOT_FOUND rather than silently executing. This is the
// supply-chain guard from docs/15-security.md.
func TestRunnerImportIntegrityFailure(t *testing.T) {
	manifest := importTestManifest(map[string]api.ManifestImport{
		"util": {Path: "lib/util.js"},
	})
	uploaded := map[string][]byte{
		"function.js": []byte(`
import { v } from "util"
export default function(){ return { statusCode: 200, body: String(v) } }
`),
		"lib/util.js":   []byte(`export const v = 1;`),
		"manifest.json": nil,
	}
	raw, _ := json.Marshal(manifest)
	uploaded["manifest.json"] = raw
	resolver := bundle.NewResolver(bundle.ResolverOptions{})
	res, err := resolver.Resolve(context.Background(), manifest, uploaded)
	if err != nil {
		t.Fatal(err)
	}
	// Tamper: replace the dep bytes with different content but keep the
	// integrity recorded in import-map.json. The runtime must refuse.
	for spec, entry := range res.ImportMap.Imports {
		if spec == "util" {
			res.Files[entry.Path] = []byte(`export const v = 999;`)
		}
	}
	bundleBytes, _, _, err := bundle.BuildCanonical(res.Files)
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(NewMemoryKV(), NopCodeQ{}, 256*1024, 64*1024, 1024*1024)
	out := runner.Execute(t.Context(), bundleBytes, api.InvocationRequest{
		ActivationID: "act-1",
		RequestID:    "req-1",
		Tenant:       "t_abc123",
		Namespace:    "payments",
		Ref:          api.FunctionRef{Function: "fn", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		Principal:    api.Principal{Sub: "user:1", Roles: []string{"role:app"}},
		DeadlineMS:   time.Now().Add(2 * time.Second).UnixMilli(),
		Event:        map[string]any{},
	})
	if out.ResolvedCode != cserrors.CSImportNotFound {
		t.Fatalf("expected CSImportNotFound on tamper, got code=%q error=%+v", out.ResolvedCode, out.Error)
	}
}

// TestRewriteImportStatementsLeavesNonImports asserts the regex-based
// rewriter does not touch lines that resemble imports but are not. This
// is a small safety net — the rewriter runs over arbitrary publisher
// code so we must not accidentally rewrite e.g. a string literal that
// contains the word "import".
func TestRewriteImportStatementsLeavesNonImports(t *testing.T) {
	src := `const note = "see import section"
function describe() { return "import" }
`
	got := rewriteImportStatements(src)
	if got != src {
		t.Fatalf("rewriter mutated non-import source:\n%s", got)
	}
}

// TestRewriteExportStatementsAllShapes exercises the dep-side rewriter.
// Verifies that const, function, class, default-expr, and `export { ... }`
// forms all funnel through __cs_exports.
func TestRewriteExportStatementsAllShapes(t *testing.T) {
	src := `
export const A = 1
export function fn() { return 2 }
export class K { hello() { return "hi" } }
export default { d: 1 }
const NAMED = 7
export { NAMED }
`
	got := rewriteExportStatements(src)
	for _, marker := range []string{
		"__cs_exports.A",
		"__cs_exports.fn",
		"__cs_exports.K",
		"__cs_exports.default",
		"__cs_exports.NAMED",
	} {
		if !strings.Contains(got, marker) {
			t.Fatalf("missing %s in rewritten output:\n%s", marker, got)
		}
	}
}
