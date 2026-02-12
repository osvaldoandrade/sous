package bundle

import "testing"

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
