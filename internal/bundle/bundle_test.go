package bundle

import (
	"context"
	"testing"

	"github.com/osvaldoandrade/sous/internal/api"
)

// TestBuildCanonicalWithImportMap verifies that BuildCanonical accepts
// the deps/ subtree + import-map.json produced by the resolver, the
// same way it accepts function.js + manifest.json. The frozen import
// map must survive a round trip through ExtractTar / LoadImportMap so
// the runtime can use it. See roadmap task E5.01.
func TestBuildCanonicalWithImportMap(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte(`export default async function(){ return {statusCode:200,body:"ok"} }`),
		"manifest.json": []byte(`{"schema":"cs.function.script.v1"}`),
		"lib/zod.js":    []byte("export const z = 1;"),
	}
	manifest := api.FunctionManifest{
		Imports: map[string]api.ManifestImport{
			"zod": {Path: "lib/zod.js"},
		},
	}
	resolver := NewResolver(ResolverOptions{})
	res, err := resolver.Resolve(context.Background(), manifest, files)
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
		t.Fatal("import-map.json missing from extracted tar")
	}
	loaded, err := LoadImportMap(decoded)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if _, ok := loaded.Imports["zod"]; !ok {
		t.Fatalf("zod missing from frozen map: %+v", loaded.Imports)
	}
}

func TestBuildCanonicalDeterministic(t *testing.T) {
	filesA := map[string][]byte{
		"manifest.json": []byte(`{"schema":"cs.function.script.v1"}`),
		"function.js":   []byte("export default async function(){ return {statusCode:200,body:'ok'} }"),
	}
	filesB := map[string][]byte{
		"function.js":   []byte("export default async function(){ return {statusCode:200,body:'ok'} }"),
		"manifest.json": []byte(`{"schema":"cs.function.script.v1"}`),
	}

	bundleA, shaA, _, err := BuildCanonical(filesA)
	if err != nil {
		t.Fatal(err)
	}
	bundleB, shaB, _, err := BuildCanonical(filesB)
	if err != nil {
		t.Fatal(err)
	}
	if string(bundleA) != string(bundleB) {
		t.Fatal("canonical tar should be deterministic")
	}
	if shaA != shaB {
		t.Fatalf("sha mismatch: %s != %s", shaA, shaB)
	}

	decoded, err := ExtractTar(bundleA)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 2 {
		t.Fatalf("unexpected file count: %d", len(decoded))
	}
	if !VerifySHA256(bundleA, shaA) {
		t.Fatal("sha verification failed")
	}
}
