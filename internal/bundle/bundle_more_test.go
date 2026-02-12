package bundle

import (
	"archive/tar"
	"bytes"
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
