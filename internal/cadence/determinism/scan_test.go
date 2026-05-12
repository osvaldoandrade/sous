package determinism

import (
	"strings"
	"testing"
)

// TestScanWorkflowEmpty asserts the linter is a no-op on a nil/empty
// file map. Publish-time code paths that call ScanWorkflow with an
// activity manifest must not crash if (somehow) they reach the scanner
// with no JS bytes attached.
func TestScanWorkflowEmpty(t *testing.T) {
	if got := ScanWorkflow(nil); got != nil {
		t.Fatalf("ScanWorkflow(nil) = %v, want nil", got)
	}
	if got := ScanWorkflow(map[string][]byte{}); got != nil {
		t.Fatalf("ScanWorkflow(empty) = %v, want nil", got)
	}
}

// TestScanWorkflowCleanCode covers a happy-path workflow that uses only
// deterministic APIs. The linter must produce zero violations so a
// well-written workflow can publish without operator intervention.
func TestScanWorkflowCleanCode(t *testing.T) {
	src := `export default async function (ctx) {
        const now = await cs.workflow.now();
        const id = await cs.workflow.sideEffect(() => crypto.randomUUID());
        return { statusCode: 200, body: JSON.stringify({ now, id }) };
    }`
	got := ScanWorkflow(map[string][]byte{"function.js": []byte(src)})
	if len(got) != 0 {
		t.Fatalf("expected zero violations, got %d: %#v", len(got), got)
	}
}

// TestScanWorkflowBannedPatterns drives every banned-API entry through
// a single-line fixture and asserts the linter flags exactly that
// pattern, with the right line/column anchored at the start of the
// match. The test is data-driven so adding a new banned API is a
// one-line append to bannedPatterns + one entry here.
func TestScanWorkflowBannedPatterns(t *testing.T) {
	cases := []struct {
		name    string
		src     string
		pattern string
		// 1-indexed column where the pattern starts.
		wantCol int
	}{
		{name: "Date.now", src: "const t = Date.now();", pattern: "Date.now", wantCol: 11},
		{name: "Math.random", src: "const r = Math.random();", pattern: "Math.random", wantCol: 11},
		{name: "setTimeout", src: "setTimeout(() => null, 1);", pattern: "setTimeout", wantCol: 1},
		{name: "setInterval", src: "setInterval(() => null, 1);", pattern: "setInterval", wantCol: 1},
		{name: "new Date()", src: "const d = new Date();", pattern: "new Date()", wantCol: 11},
		{name: "fetch", src: "fetch('https://x');", pattern: "fetch", wantCol: 1},
		{name: "setImmediate", src: "setImmediate(() => null);", pattern: "setImmediate", wantCol: 1},
		{name: "performance.now", src: "const t = performance.now();", pattern: "performance.now", wantCol: 11},
		{name: "crypto.getRandomValues", src: "crypto.getRandomValues(buf);", pattern: "crypto.getRandomValues", wantCol: 1},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := ScanWorkflow(map[string][]byte{"function.js": []byte(tc.src)})
			if len(got) != 1 {
				t.Fatalf("want 1 violation, got %d: %#v", len(got), got)
			}
			v := got[0]
			if v.Pattern != tc.pattern {
				t.Fatalf("pattern = %q, want %q", v.Pattern, tc.pattern)
			}
			if v.File != "function.js" {
				t.Fatalf("file = %q, want function.js", v.File)
			}
			if v.Line != 1 {
				t.Fatalf("line = %d, want 1", v.Line)
			}
			if v.Column != tc.wantCol {
				t.Fatalf("column = %d, want %d", v.Column, tc.wantCol)
			}
			if v.Message == "" {
				t.Fatal("violation message must not be empty")
			}
		})
	}
}

// TestScanWorkflowDateConstructorIgnoresArgs asserts the no-arg Date()
// pattern does not flag `new Date(ms)`. History-derived timestamps are
// deterministic and a legitimate workflow construct.
func TestScanWorkflowDateConstructorIgnoresArgs(t *testing.T) {
	src := `const d = new Date(history.startedAtMs);`
	got := ScanWorkflow(map[string][]byte{"function.js": []byte(src)})
	if len(got) != 0 {
		t.Fatalf("new Date(arg) must not flag: %#v", got)
	}
}

// TestScanWorkflowFetchRequiresParen makes sure the bare-fetch pattern
// is anchored to a call expression. References to `fetchMessages` or
// member access like `client.fetch` must not match.
func TestScanWorkflowFetchRequiresParen(t *testing.T) {
	src := `client.fetch; const fetchMessages = 1; fetch(`
	got := ScanWorkflow(map[string][]byte{"function.js": []byte(src)})
	if len(got) != 1 {
		t.Fatalf("expected exactly one fetch violation, got %d: %#v", len(got), got)
	}
	if got[0].Pattern != "fetch" {
		t.Fatalf("pattern = %q, want fetch", got[0].Pattern)
	}
}

// TestScanWorkflowAllowMarker exercises the per-line escape hatch:
// any banned-API call accompanied by `// cs-determinism-allow` on the
// same line is silently dropped. Operators rely on this for legitimate
// uses behind cs.workflow.sideEffect that haven't yet been wrapped.
func TestScanWorkflowAllowMarker(t *testing.T) {
	src := strings.Join([]string{
		"const t = Date.now(); // cs-determinism-allow audit signed-off 2024-11-08",
		"const r = Math.random();",
	}, "\n")
	got := ScanWorkflow(map[string][]byte{"function.js": []byte(src)})
	if len(got) != 1 {
		t.Fatalf("want 1 violation (only Math.random), got %d: %#v", len(got), got)
	}
	if got[0].Pattern != "Math.random" {
		t.Fatalf("pattern = %q, want Math.random", got[0].Pattern)
	}
	if got[0].Line != 2 {
		t.Fatalf("line = %d, want 2", got[0].Line)
	}
}

// TestScanWorkflowAllowMarkerLineScoped verifies the escape hatch is
// scoped to the line containing the call site. A marker on a different
// line must not silence violations elsewhere — reviewers need to audit
// each exception independently.
func TestScanWorkflowAllowMarkerLineScoped(t *testing.T) {
	src := strings.Join([]string{
		"// cs-determinism-allow this comment is on its own line",
		"const t = Date.now();",
	}, "\n")
	got := ScanWorkflow(map[string][]byte{"function.js": []byte(src)})
	if len(got) != 1 {
		t.Fatalf("want 1 violation, got %d: %#v", len(got), got)
	}
	if got[0].Line != 2 {
		t.Fatalf("line = %d, want 2", got[0].Line)
	}
}

// TestScanWorkflowLineColumnTracking checks that violations on later
// lines report the correct (line, column) pair. A regression here means
// our index of newline offsets is wrong, which makes the publishing
// agent's diagnostic point at the wrong file location.
func TestScanWorkflowLineColumnTracking(t *testing.T) {
	src := strings.Join([]string{
		"// header",
		"const r = Math.random();",
		"",
		"    setTimeout(cb, 100);",
	}, "\n")
	got := ScanWorkflow(map[string][]byte{"function.js": []byte(src)})
	if len(got) != 2 {
		t.Fatalf("want 2 violations, got %d: %#v", len(got), got)
	}
	if got[0].Line != 2 || got[0].Column != 11 || got[0].Pattern != "Math.random" {
		t.Fatalf("violation[0] = %#v, want line=2 col=11 pattern=Math.random", got[0])
	}
	if got[1].Line != 4 || got[1].Column != 5 || got[1].Pattern != "setTimeout" {
		t.Fatalf("violation[1] = %#v, want line=4 col=5 pattern=setTimeout", got[1])
	}
}

// TestScanWorkflowDepsFiles makes sure imports under deps/ are
// scanned. A workflow that pulls in a banned API via a frozen
// dependency is just as broken as one that calls it directly.
func TestScanWorkflowDepsFiles(t *testing.T) {
	files := map[string][]byte{
		"function.js":     []byte(`import { fresh } from "uuid"; export default async () => ({ statusCode: 200, body: fresh() });`),
		"deps/uuid.js":    []byte(`export const fresh = () => Math.random().toString(36);`),
		"manifest.json":   []byte(`{}`),
		"deps/README.txt": []byte(`not js, must be skipped`),
	}
	got := ScanWorkflow(files)
	if len(got) != 1 {
		t.Fatalf("want 1 violation, got %d: %#v", len(got), got)
	}
	if got[0].File != "deps/uuid.js" {
		t.Fatalf("file = %q, want deps/uuid.js", got[0].File)
	}
	if got[0].Pattern != "Math.random" {
		t.Fatalf("pattern = %q, want Math.random", got[0].Pattern)
	}
}

// TestScanWorkflowSkipsNonJSFiles guards the file-filter contract:
// manifest.json, import-map.json, and any non-deps directory must be
// excluded so we don't lint a JSON literal for the substring "fetch".
func TestScanWorkflowSkipsNonJSFiles(t *testing.T) {
	files := map[string][]byte{
		"manifest.json":     []byte(`{"http":{"allowHosts":["fetch.example"]}}`),
		"lib/legacy.js":     []byte(`fetch(`),
		"import-map.json":   []byte(`{"imports":{"x":{"path":"deps/x.js"}}}`),
		"deps/manifest.txt": []byte(`Date.now`),
	}
	got := ScanWorkflow(files)
	if len(got) != 0 {
		t.Fatalf("non-js files must be skipped, got %d violations: %#v", len(got), got)
	}
}

// TestScanWorkflowDeterministicOrder asserts repeated runs over the
// same input produce the same violation order. CI diffs and human
// review both rely on a stable sort key (file, line, column, pattern).
func TestScanWorkflowDeterministicOrder(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte("const a = Date.now();\nconst b = Math.random();\n"),
		"deps/util.js":  []byte("fetch('x');\nsetTimeout(cb,1);\n"),
		"manifest.json": []byte(`{}`),
	}
	want := ScanWorkflow(files)
	for i := 0; i < 5; i++ {
		got := ScanWorkflow(files)
		if len(got) != len(want) {
			t.Fatalf("iteration %d: length drift got=%d want=%d", i, len(got), len(want))
		}
		for j := range got {
			if got[j] != want[j] {
				t.Fatalf("iteration %d index %d: got %#v want %#v", i, j, got[j], want[j])
			}
		}
	}
}
