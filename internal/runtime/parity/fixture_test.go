package parity

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadFixtureRejectsBadJSON ensures a malformed fixture surfaces a
// path-prefixed error rather than a cryptic "unexpected end of JSON
// input" so authors can find the offending file quickly.
func TestLoadFixtureRejectsBadJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broken.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFixture(path); err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

// TestLoadFixtureRequiresFiles asserts the parity contract: a fixture
// with no per-runtime files is meaningless and must be rejected at
// load time. A silent skip would mask a bad commit.
func TestLoadFixtureRequiresFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.json")
	if err := os.WriteFile(path, []byte(`{"name":"empty","files":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFixture(path); err == nil {
		t.Fatal("expected error for fixture with no files")
	}
}

// TestLoadFixturesSkipsNonJSON keeps the loader robust against stray
// files (e.g. a README under test/parity/fixtures) without failing the
// whole suite.
func TestLoadFixturesSkipsNonJSON(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "README.md"), []byte("docs"), 0o644); err != nil {
		t.Fatal(err)
	}
	fixturePath := filepath.Join(dir, "ok.json")
	body := `{"name":"ok","manifest":{},"files":{"cs-js":{"function.js":"export default async function() { return { statusCode: 200 } }"}},"expected":{}}`
	if err := os.WriteFile(fixturePath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := LoadFixtures(dir)
	if err != nil {
		t.Fatalf("LoadFixtures: %v", err)
	}
	if len(got) != 1 || got[0].Name != "ok" {
		t.Fatalf("unexpected fixtures: %+v", got)
	}
}

// TestSupportsRuntimeTrueOnlyWithFiles guards the auto-join semantic:
// a fixture that lists a runtime with an empty files map should not be
// dispatched against that runtime.
func TestSupportsRuntimeTrueOnlyWithFiles(t *testing.T) {
	fx := &Fixture{Files: map[string]map[string]string{
		"cs-js":     {"function.js": "x"},
		"cs-python": {},
	}}
	if !fx.SupportsRuntime("cs-js") {
		t.Fatal("cs-js with files should be supported")
	}
	if fx.SupportsRuntime("cs-python") {
		t.Fatal("cs-python with empty files should NOT be supported")
	}
	if fx.SupportsRuntime("cs-wasm") {
		t.Fatal("missing runtime entry should NOT be supported")
	}
}
