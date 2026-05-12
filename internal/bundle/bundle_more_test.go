package bundle

import (
	"archive/tar"
	"bytes"
	"strings"
	"testing"
)

func TestBuildCanonicalValidationErrors(t *testing.T) {
	if _, _, _, err := BuildCanonical(nil); err == nil {
		t.Fatal("expected empty files error")
	}
	if _, _, _, err := BuildCanonical(map[string][]byte{"manifest.json": []byte(`{}`)}); err == nil {
		t.Fatal("expected missing function.js error")
	}
	if _, _, _, err := BuildCanonical(map[string][]byte{"function.js": []byte("x")}); err == nil {
		t.Fatal("expected missing manifest.json error")
	}
	if _, _, _, err := BuildCanonical(map[string][]byte{
		"function.js":   []byte("x"),
		"manifest.json": []byte(`{}`),
		"a/../x":        []byte("y"),
	}); err == nil {
		t.Fatal("expected invalid path error")
	}
}

// TestBuildCanonicalPerRuntimeEntry confirms the entry-file check is
// keyed by manifest.runtime: cs-python wants function.py, cs-wasm
// wants module.wasm, and a missing or empty runtime keeps the v0.1
// cs-js default of function.js. The check runs from manifest.json so
// the bundle layer does not need to import internal/api (which would
// create a dependency cycle).
func TestBuildCanonicalPerRuntimeEntry(t *testing.T) {
	cases := []struct {
		name        string
		manifest    string
		entryName   string
		entryBody   []byte
		wantMissing string // substring expected in error when entry omitted
	}{
		{
			name:        "cs-python requires function.py",
			manifest:    `{"runtime":"cs-python"}`,
			entryName:   "function.py",
			entryBody:   []byte("def handler(event, ctx):\n    return {'statusCode':200,'body':'ok'}\n"),
			wantMissing: "function.py is required",
		},
		{
			name:        "cs-wasm requires module.wasm",
			manifest:    `{"runtime":"cs-wasm"}`,
			entryName:   "module.wasm",
			entryBody:   []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00},
			wantMissing: "module.wasm is required",
		},
		{
			name:        "missing runtime defaults to cs-js",
			manifest:    `{}`,
			entryName:   "function.js",
			entryBody:   []byte("export default async () => ({statusCode:200,body:'ok'});"),
			wantMissing: "function.js is required",
		},
		{
			name:        "empty runtime defaults to cs-js",
			manifest:    `{"runtime":""}`,
			entryName:   "function.js",
			entryBody:   []byte("export default async () => ({statusCode:200,body:'ok'});"),
			wantMissing: "function.js is required",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Happy path: bundle round-trips with the canonical entry.
			files := map[string][]byte{
				"manifest.json": []byte(tc.manifest),
				tc.entryName:    tc.entryBody,
			}
			tarBytes, sha, _, err := BuildCanonical(files)
			if err != nil {
				t.Fatalf("BuildCanonical: %v", err)
			}
			if sha == "" {
				t.Fatal("expected non-empty sha")
			}
			decoded, err := ExtractTar(tarBytes)
			if err != nil {
				t.Fatalf("ExtractTar: %v", err)
			}
			if _, ok := decoded[tc.entryName]; !ok {
				t.Fatalf("decoded bundle missing %s", tc.entryName)
			}
			// Sad path: omit the entry; the runtime-specific error must surface.
			only := map[string][]byte{"manifest.json": []byte(tc.manifest)}
			_, _, _, err = BuildCanonical(only)
			if err == nil || !strings.Contains(err.Error(), tc.wantMissing) {
				t.Fatalf("BuildCanonical without entry: want %q substring, got %v", tc.wantMissing, err)
			}
		})
	}
}

func TestExtractTarErrors(t *testing.T) {
	if _, err := ExtractTar([]byte("not-a-tar")); err == nil {
		t.Fatal("expected invalid tar error")
	}

	// Tar containing only function.js should fail missing manifest.
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "function.js", Mode: 0o644, Size: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ExtractTar(buf.Bytes()); err == nil {
		t.Fatal("expected missing manifest error")
	}

	// Tar containing only manifest.json should fail missing function.js.
	buf.Reset()
	tw = tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: "manifest.json", Mode: 0o644, Size: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ExtractTar(buf.Bytes()); err == nil {
		t.Fatal("expected missing function.js error")
	}
}
