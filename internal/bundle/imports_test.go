package bundle

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
)

func newTestResolver(t *testing.T, allowed []string) *Resolver {
	t.Helper()
	return NewResolver(ResolverOptions{
		AllowedMirrors:    allowed,
		MaxBytesPerImport: 1024,
		Timeout:           500 * time.Millisecond,
	})
}

func TestResolveLocalPath(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte(`export default async function(){ return {statusCode:200,body:"ok"} }`),
		"manifest.json": []byte(`{"schema":"cs.function.script.v1"}`),
		"lib/zod.js":    []byte("export const z = { parse: x => x };"),
	}
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"zod": {Path: "lib/zod.js"},
		},
	}
	r := newTestResolver(t, nil)
	res, err := r.Resolve(context.Background(), manifest, files)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// import-map.json must be emitted, and the dep must be copied to deps/.
	if _, ok := res.Files[ImportMapFileName]; !ok {
		t.Fatal("expected import-map.json in resolved files")
	}
	var imp FrozenImport
	for spec, e := range res.ImportMap.Imports {
		if spec == "zod" {
			imp = e
		}
	}
	if !strings.HasPrefix(imp.Path, DepsPrefix) {
		t.Fatalf("frozen path should start with %s, got %q", DepsPrefix, imp.Path)
	}
	if _, ok := res.Files[imp.Path]; !ok {
		t.Fatalf("dep bytes not present at %s", imp.Path)
	}
	if !strings.HasPrefix(imp.Integrity, "sha256-") {
		t.Fatalf("expected sha256 integrity, got %q", imp.Integrity)
	}
	if imp.SizeBytes != len(files["lib/zod.js"]) {
		t.Fatalf("size mismatch: %d vs %d", imp.SizeBytes, len(files["lib/zod.js"]))
	}
}

func TestResolveDeterministicSHA(t *testing.T) {
	// Two manifests with the same imports must produce identical bundles
	// regardless of map iteration order. This is the determinism contract
	// from the issue: same input → same sha256.
	makeFiles := func() map[string][]byte {
		return map[string][]byte{
			"function.js":   []byte(`export default async function(){ return {statusCode:200,body:"ok"} }`),
			"manifest.json": []byte(`{"schema":"cs.function.script.v1"}`),
			"a.js":          []byte("export const A = 1;"),
			"b.js":          []byte("export const B = 2;"),
		}
	}
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"a": {Path: "a.js"},
			"b": {Path: "b.js"},
		},
	}
	r := newTestResolver(t, nil)
	res1, err := r.Resolve(context.Background(), manifest, makeFiles())
	if err != nil {
		t.Fatal(err)
	}
	res2, err := r.Resolve(context.Background(), manifest, makeFiles())
	if err != nil {
		t.Fatal(err)
	}
	bundle1, sha1, _, err := BuildCanonical(res1.Files)
	if err != nil {
		t.Fatal(err)
	}
	bundle2, sha2, _, err := BuildCanonical(res2.Files)
	if err != nil {
		t.Fatal(err)
	}
	if sha1 != sha2 {
		t.Fatalf("expected deterministic sha: %s vs %s", sha1, sha2)
	}
	if string(bundle1) != string(bundle2) {
		t.Fatal("bundle bytes should match exactly")
	}
}

func TestResolveURLAllowedMirror(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("export const remote = 1;"))
	}))
	defer srv.Close()
	host := strings.TrimPrefix(srv.URL, "http://")
	host = strings.Split(host, ":")[0]

	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"remote": {URL: srv.URL + "/remote.js"},
		},
	}
	r := NewResolver(ResolverOptions{AllowedMirrors: []string{host}, HTTPClient: srv.Client(), MaxBytesPerImport: 4096})
	res, err := r.Resolve(context.Background(), manifest, map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	entry, ok := res.ImportMap.Imports["remote"]
	if !ok {
		t.Fatal("expected remote in import map")
	}
	if entry.SizeBytes == 0 {
		t.Fatalf("size should be non-zero: %+v", entry)
	}
	body, ok := res.Files[entry.Path]
	if !ok || !strings.Contains(string(body), "remote") {
		t.Fatalf("dep bytes missing or wrong: %q", string(body))
	}
}

func TestResolveURLDeniedMirror(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("denied"))
	}))
	defer srv.Close()
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"x": {URL: srv.URL + "/x.js"},
		},
	}
	r := NewResolver(ResolverOptions{AllowedMirrors: []string{"only.example.com"}, HTTPClient: srv.Client()})
	_, err := r.Resolve(context.Background(), manifest, map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
	})
	if err == nil || !strings.Contains(err.Error(), "allowed_mirrors") {
		t.Fatalf("expected allowed_mirrors denial, got %v", err)
	}
}

func TestResolveSizeCapEnforced(t *testing.T) {
	big := strings.Repeat("a", 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(big))
	}))
	defer srv.Close()
	host := strings.Split(strings.TrimPrefix(srv.URL, "http://"), ":")[0]

	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"big": {URL: srv.URL + "/big.js"},
		},
	}
	r := NewResolver(ResolverOptions{
		AllowedMirrors:    []string{host},
		HTTPClient:        srv.Client(),
		MaxBytesPerImport: 1024,
	})
	_, err := r.Resolve(context.Background(), manifest, map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
	})
	if err == nil || !strings.Contains(err.Error(), "per-import cap") {
		t.Fatalf("expected size cap rejection, got %v", err)
	}
}

func TestResolveIntegrityMismatch(t *testing.T) {
	body := []byte("export const x = 1;")
	bogus := "sha256-" + base64.StdEncoding.EncodeToString([]byte("not-a-real-digest-bytes-32-chars"))
	files := map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
		"x.js":          body,
	}
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"x": {Path: "x.js", Integrity: bogus},
		},
	}
	r := newTestResolver(t, nil)
	_, err := r.Resolve(context.Background(), manifest, files)
	if err == nil || !strings.Contains(err.Error(), "integrity") {
		t.Fatalf("expected integrity mismatch, got %v", err)
	}
}

func TestResolveIntegrityRoundTrip(t *testing.T) {
	body := []byte("export const value = 42;")
	digest := sha256.Sum256(body)
	correct := "sha256-" + base64.StdEncoding.EncodeToString(digest[:])
	files := map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
		"x.js":          body,
	}
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"x": {Path: "x.js", Integrity: correct},
		},
	}
	r := newTestResolver(t, nil)
	res, err := r.Resolve(context.Background(), manifest, files)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if got := res.ImportMap.Imports["x"].Integrity; got != correct {
		t.Fatalf("integrity round-trip failed: want %q got %q", correct, got)
	}
}

func TestResolveExactlyOneSource(t *testing.T) {
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"x": {},
		},
	}
	r := newTestResolver(t, nil)
	_, err := r.Resolve(context.Background(), manifest, map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
	})
	if err == nil || !strings.Contains(err.Error(), "url or path") {
		t.Fatalf("expected url/path-required error, got %v", err)
	}

	manifest.Imports["x"] = api.ManifestImport{URL: "http://example.com/x", Path: "lib/x.js"}
	_, err = r.Resolve(context.Background(), manifest, map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
	})
	if err == nil || !strings.Contains(err.Error(), "url or path") {
		t.Fatalf("expected url-and-path conflict, got %v", err)
	}
}

func TestResolveMissingLocalPath(t *testing.T) {
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"x": {Path: "missing/x.js"},
		},
	}
	r := newTestResolver(t, nil)
	_, err := r.Resolve(context.Background(), manifest, map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
	})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected missing-path error, got %v", err)
	}
}

func TestResolveNoImportsPassThrough(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
	}
	manifest := api.FunctionManifest{}
	r := newTestResolver(t, nil)
	res, err := r.Resolve(context.Background(), manifest, files)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if _, ok := res.Files[ImportMapFileName]; ok {
		t.Fatal("import-map.json should not appear when there are no imports")
	}
	if len(res.ImportMap.Imports) != 0 {
		t.Fatalf("expected empty import map, got %d entries", len(res.ImportMap.Imports))
	}
}

func TestLoadImportMapRoundTrip(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
		"lib/zod.js":    []byte("export const z = 1;"),
	}
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"zod": {Path: "lib/zod.js"},
		},
	}
	r := newTestResolver(t, nil)
	res, err := r.Resolve(context.Background(), manifest, files)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	bundleBytes, _, _, err := BuildCanonical(res.Files)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	decoded, err := ExtractTar(bundleBytes)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	got, err := LoadImportMap(decoded)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got.Imports) != 1 {
		t.Fatalf("expected 1 import, got %d: %+v", len(got.Imports), got)
	}
	entry, ok := got.Imports["zod"]
	if !ok {
		t.Fatalf("zod missing from loaded map: %+v", got)
	}
	if err := VerifyImportBytes(entry.Integrity, decoded[entry.Path]); err != nil {
		t.Fatalf("integrity verify failed on round-trip: %v", err)
	}
}

func TestLoadImportMapAbsentReturnsEmpty(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
	}
	got, err := LoadImportMap(files)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got.Imports) != 0 {
		t.Fatalf("expected empty map, got %+v", got)
	}
}

func TestLoadImportMapBadSchemaRejected(t *testing.T) {
	bad := map[string][]byte{
		ImportMapFileName: []byte(`{"schema":"bogus","imports":{}}`),
	}
	if _, err := LoadImportMap(bad); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("expected schema error, got %v", err)
	}
	corrupt := map[string][]byte{
		ImportMapFileName: []byte(`{not json`),
	}
	if _, err := LoadImportMap(corrupt); err == nil {
		t.Fatal("expected decode error")
	}
}

func TestResolveURLNonOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	host := strings.Split(strings.TrimPrefix(srv.URL, "http://"), ":")[0]
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"missing": {URL: srv.URL + "/missing.js"},
		},
	}
	r := NewResolver(ResolverOptions{AllowedMirrors: []string{host}, HTTPClient: srv.Client(), MaxBytesPerImport: 1024})
	_, err := r.Resolve(context.Background(), manifest, map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
	})
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("expected 404, got %v", err)
	}
}

func TestVerifyImportBytesAlgorithms(t *testing.T) {
	body := []byte("hello import map")
	digest := sha256.Sum256(body)
	good := "sha256-" + base64.StdEncoding.EncodeToString(digest[:])
	if err := VerifyImportBytes(good, body); err != nil {
		t.Fatalf("sha256 verify: %v", err)
	}
	// Wrong bytes must fail.
	if err := VerifyImportBytes(good, []byte("tampered")); err == nil {
		t.Fatal("expected verify failure on tampered bytes")
	}
	if err := VerifyImportBytes("", body); err == nil {
		t.Fatal("expected verify failure on empty integrity")
	}
	if err := VerifyImportBytes("unknown-abc", body); err == nil {
		t.Fatal("expected verify failure on unknown algorithm")
	}
}

func TestSafeDepFileName(t *testing.T) {
	cases := map[string]string{
		"zod":            "zod.js",
		"@scope/pkg":     "_scope_pkg.js",
		"date-fns":       "date-fns.js",
		"path/with.json": "path_with.json",
		"":               "dep.js",
	}
	for in, want := range cases {
		if got := safeDepFileName(in); got != want {
			t.Fatalf("safeDepFileName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestResolveImportMapSchemaWritten(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
		"x.js":          []byte("export const x = 1;"),
	}
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"x": {Path: "x.js"},
		},
	}
	r := newTestResolver(t, nil)
	res, err := r.Resolve(context.Background(), manifest, files)
	if err != nil {
		t.Fatal(err)
	}
	raw := res.Files[ImportMapFileName]
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("import-map.json must be JSON: %v", err)
	}
	if decoded["schema"] != importMapSchema {
		t.Fatalf("schema mismatch: %v", decoded["schema"])
	}
}

func TestResolveURLBadScheme(t *testing.T) {
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"x": {URL: "ftp://mirror.example.com/x.js"},
		},
	}
	r := NewResolver(ResolverOptions{AllowedMirrors: []string{"mirror.example.com"}})
	_, err := r.Resolve(context.Background(), manifest, map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
	})
	if err == nil || !strings.Contains(err.Error(), "http/https") {
		t.Fatalf("expected scheme rejection, got %v", err)
	}
}

// Ensure the resolver fails clearly when the in-bundle dep path would
// collide with an existing uploaded file. This guards against the
// publisher smuggling bytes by naming a local file `deps/x.js`.
func TestResolveDepsPathCollision(t *testing.T) {
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"x": {Path: "x.js"},
		},
	}
	files := map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte("{}"),
		"x.js":          []byte("export const x = 1;"),
		"deps/x.js":     []byte("// pre-existing"),
	}
	r := newTestResolver(t, nil)
	_, err := r.Resolve(context.Background(), manifest, files)
	if err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("expected collision error, got %v", err)
	}
}

// Tiny sanity check on the integration with BuildCanonical so a frozen
// bundle that ParseManifest can read is what we ship.
func TestResolveBundleParsesBack(t *testing.T) {
	depBytes := []byte("export const ok = true;")
	manifestJSON := fmt.Sprintf(`{
		"schema":"cs.function.script.v1",
		"runtime":"cs-js",
		"entry":"function.js",
		"handler":"default",
		"limits":{"timeoutMs":1000,"memoryMb":64,"maxConcurrency":1},
		"capabilities":{"kv":{"prefixes":["ctr:"],"ops":["get"]},"codeq":{"publishTopics":[]},"http":{"allowHosts":[],"timeoutMs":1000}},
		"imports":{"ok":{"path":"ok.js"}}
	}`)
	files := map[string][]byte{
		"function.js":   []byte("export default function(){}"),
		"manifest.json": []byte(manifestJSON),
		"ok.js":         depBytes,
	}
	manifest, err := api.ParseManifest([]byte(manifestJSON))
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	r := newTestResolver(t, nil)
	res, err := r.Resolve(context.Background(), manifest, files)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	tarBytes, _, _, err := BuildCanonical(res.Files)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	decoded, err := ExtractTar(tarBytes)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if _, ok := decoded[ImportMapFileName]; !ok {
		t.Fatal("import-map.json missing from final bundle")
	}
	gotMap, err := LoadImportMap(decoded)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	entry := gotMap.Imports["ok"]
	if string(decoded[entry.Path]) != string(depBytes) {
		t.Fatalf("dep bytes round-trip mismatch")
	}
}
