package parity

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/osvaldoandrade/sous/internal/api"
)

// fixturesDir resolves the repo-relative test/parity/fixtures path from
// the parity_test.go file location. The harness lives three levels deep
// (internal/runtime/parity), so the fixtures path is "../../../test/parity/fixtures".
func fixturesDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Join(filepath.Dir(file), "..", "..", "..", "test", "parity", "fixtures")
}

// TestSuiteMatrix is the production entry point: every fixture under
// test/parity/fixtures is loaded and dispatched against every runtime
// registered with runtime.DefaultRegistry. Today that's just cs-js; new
// runtimes auto-join by calling runtime.DefaultRegistry.Register in
// their init(). Failures produce one t.Errorf per drift so a CI run
// reports the full picture instead of one-shotting on the first miss.
func TestSuiteMatrix(t *testing.T) {
	fxs, err := LoadFixtures(fixturesDir(t))
	if err != nil {
		t.Fatalf("load fixtures: %v", err)
	}
	if len(fxs) == 0 {
		t.Fatal("no fixtures discovered — empty corpus would mask a regression")
	}

	suite := &Suite{Fixtures: fxs}
	suite.RunT(t)
}

// TestSuiteSkipsRuntimesWithoutFiles asserts the auto-join contract:
// adding a new runtime to DefaultRegistry should produce skips, not
// failures, for fixtures that haven't been ported yet. This is the
// guard that lets cs-python / cs-wasm land later without retrofitting
// the entire corpus.
func TestSuiteSkipsRuntimesWithoutFiles(t *testing.T) {
	fx := &Fixture{
		Name: "stub",
		Manifest: api.FunctionManifest{
			Schema:  "cs.function.script.v1",
			Entry:   "function.js",
			Handler: "default",
			Limits: api.ManifestLimits{
				TimeoutMS:      1000,
				MemoryMB:       64,
				MaxConcurrency: 1,
			},
			Capabilities: api.ManifestCapabilities{
				KV:    api.ManifestKVCaps{Prefixes: []string{"ctr:"}, Ops: []string{"get"}},
				CodeQ: api.ManifestCodeQCaps{PublishTopics: []string{"jobs.*"}},
				HTTP:  api.ManifestHTTPCaps{AllowHosts: []string{"example.com"}, TimeoutMS: 1500},
			},
		},
		Files: map[string]map[string]string{
			"cs-js": {
				"function.js": "export default async function handle() { return { statusCode: 200, body: \"ok\" } }",
			},
		},
		Trigger: api.Trigger{Type: "api"},
		Event:   map[string]any{},
		Expected: Expected{
			Status: "success",
			ResponseShape: map[string]any{
				"statusCode": float64(200),
				"body":       "ok",
			},
		},
	}

	suite := &Suite{
		Runtimes: []string{"cs-js", "cs-python", "cs-wasm"},
		Fixtures: []*Fixture{fx},
	}
	results := suite.Run(context.Background())
	if len(results) != 3 {
		t.Fatalf("want 3 cells, got %d", len(results))
	}

	seen := map[string]CellResult{}
	for _, r := range results {
		seen[r.Runtime] = r
	}

	if r, ok := seen["cs-js"]; !ok || !r.Passed() || r.Skipped {
		t.Fatalf("cs-js should have passed (not skipped), got %+v", r)
	}
	for _, rt := range []string{"cs-python", "cs-wasm"} {
		r := seen[rt]
		if !r.Skipped {
			t.Fatalf("%s should be skipped because fixture has no files for it, got %+v", rt, r)
		}
		if !r.Passed() {
			t.Fatalf("%s skipped cell must count as passed, got mismatches=%+v err=%v", rt, r.Mismatches, r.Err)
		}
	}
}

// TestLoadFixturesDeterministicOrder asserts the parity harness reads
// fixtures in stable Name order. Fixture ordering matters for the
// upcoming CI per-runtime tally — drift in the tally would be hard to
// diff if Go's directory iteration showed through.
func TestLoadFixturesDeterministicOrder(t *testing.T) {
	fxs, err := LoadFixtures(fixturesDir(t))
	if err != nil {
		t.Fatalf("load fixtures: %v", err)
	}
	for i := 1; i < len(fxs); i++ {
		if fxs[i-1].Name > fxs[i].Name {
			t.Fatalf("fixtures not sorted: %q after %q", fxs[i].Name, fxs[i-1].Name)
		}
	}
}
