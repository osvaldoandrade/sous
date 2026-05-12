package sbom

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/osvaldoandrade/sous/internal/api"
)

// TestBuildEmitsCanonicalCycloneDX verifies that the SBOM emitter
// produces a valid CycloneDX 1.5 JSON document with the structural
// fields callers and downstream scanners depend on:
//
//   - bomFormat / specVersion / version stable,
//   - serialNumber derived from the bundle digest,
//   - the runtime listed as a framework component,
//   - one file component per bundled file with its SHA-256 hash,
//   - the bundle digest surfaced as a metadata property.
func TestBuildEmitsCanonicalCycloneDX(t *testing.T) {
	files := map[string][]byte{
		"function.js":   []byte("export default function(){ return {statusCode:200,body:'ok'} }"),
		"manifest.json": []byte(`{"schema":"cs.function.script.v1","runtime":"cs-js"}`),
	}
	in := Input{
		Tenant:         "t_abc123",
		Namespace:      "payments",
		Function:       "reconcile",
		Version:        17,
		Runtime:        "cs-js",
		RuntimeVersion: "cs.function.script.v1",
		BundleSHA256:   strings.Repeat("ab", 32), // 64 hex chars
		BundleSize:     2048,
		Files:          files,
	}

	doc, err := Build(in)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	var parsed map[string]any
	if err := json.Unmarshal(doc, &parsed); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	if parsed["bomFormat"] != "CycloneDX" {
		t.Fatalf("bomFormat=%v", parsed["bomFormat"])
	}
	if parsed["specVersion"] != "1.5" {
		t.Fatalf("specVersion=%v", parsed["specVersion"])
	}
	serial, _ := parsed["serialNumber"].(string)
	if !strings.HasPrefix(serial, "urn:uuid:") {
		t.Fatalf("serialNumber=%q", serial)
	}

	components, _ := parsed["components"].([]any)
	if len(components) < 3 {
		t.Fatalf("expected at least 3 components, got %d (%v)", len(components), components)
	}

	// Index components by name for easy assertions.
	byName := map[string]map[string]any{}
	for _, raw := range components {
		c, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("component is not an object: %v", raw)
		}
		byName[c["name"].(string)] = c
	}
	if runtime, ok := byName["cs-js"]; !ok {
		t.Fatalf("missing runtime component, got %v", byName)
	} else if runtime["type"] != "framework" {
		t.Fatalf("runtime type=%v want=framework", runtime["type"])
	}
	for _, fn := range []string{"function.js", "manifest.json"} {
		if c, ok := byName[fn]; !ok {
			t.Fatalf("missing file component %q", fn)
		} else if c["type"] != "file" {
			t.Fatalf("file %q type=%v want=file", fn, c["type"])
		}
	}

	// Metadata properties carry the bundle sha256 so consumers can
	// short-circuit to the supply-chain anchor without scanning the
	// components list.
	meta, _ := parsed["metadata"].(map[string]any)
	props, _ := meta["properties"].([]any)
	var foundBundleDigest bool
	for _, raw := range props {
		p, _ := raw.(map[string]any)
		if p["name"] == "cs:bundle.sha256" && p["value"] == in.BundleSHA256 {
			foundBundleDigest = true
		}
	}
	if !foundBundleDigest {
		t.Fatalf("metadata.properties missing cs:bundle.sha256 anchor: %v", props)
	}
}

// TestBuildIsDeterministic asserts the property the issue calls out:
// replaying the publish for the same canonical bundle yields a
// byte-identical SBOM. Without this, every replay would produce a
// fresh serial number and break audit replays.
func TestBuildIsDeterministic(t *testing.T) {
	in := Input{
		Tenant:         "t_abc123",
		Namespace:      "payments",
		Function:       "reconcile",
		Version:        4,
		Runtime:        "cs-js",
		RuntimeVersion: "cs.function.script.v1",
		BundleSHA256:   "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		BundleSize:     1024,
		Files: map[string][]byte{
			"function.js":   []byte("export default function(){}"),
			"manifest.json": []byte(`{"schema":"cs.function.script.v1"}`),
		},
	}
	first, err := Build(in)
	if err != nil {
		t.Fatalf("Build first: %v", err)
	}
	second, err := Build(in)
	if err != nil {
		t.Fatalf("Build second: %v", err)
	}
	if string(first) != string(second) {
		t.Fatalf("SBOM not deterministic across calls\nfirst:\n%s\nsecond:\n%s", first, second)
	}

	// Mutate file map ordering (Go map iteration is randomised, but we
	// reuse the same input by rebuilding the map so determinism does
	// not rely on accidental ordering of the caller's map).
	in2 := in
	in2.Files = map[string][]byte{
		"manifest.json": in.Files["manifest.json"],
		"function.js":   in.Files["function.js"],
	}
	third, err := Build(in2)
	if err != nil {
		t.Fatalf("Build third: %v", err)
	}
	if string(first) != string(third) {
		t.Fatalf("SBOM ordering sensitive to map iteration\nwant:\n%s\ngot:\n%s", first, third)
	}
}

// TestBuildIncludesImportsWhenProvided exercises the path that lights
// up once E5.01 adds an import map to the manifest: every Import becomes
// a CycloneDX `library` component with its SRI hash and source URL.
func TestBuildIncludesImportsWhenProvided(t *testing.T) {
	in := Input{
		Tenant:       "t_abc123",
		Namespace:    "payments",
		Function:     "reconcile",
		Version:      1,
		Runtime:      "cs-js",
		BundleSHA256: strings.Repeat("cd", 32),
		Files: map[string][]byte{
			"function.js":   []byte("x"),
			"manifest.json": []byte("{}"),
		},
		Imports: []Import{
			{Name: "lodash", Version: "4.17.21", Integrity: "sha384-abc", Source: "https://cdn.example.com/lodash@4.17.21"},
			{Name: "axios", Version: "1.6.0", Integrity: "sha384-def", Source: "https://cdn.example.com/axios@1.6.0"},
		},
	}
	doc, err := Build(in)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	var parsed map[string]any
	if err := json.Unmarshal(doc, &parsed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	components, _ := parsed["components"].([]any)
	var sawAxios, sawLodash bool
	for _, raw := range components {
		c, _ := raw.(map[string]any)
		if c["name"] == "axios" && c["type"] == "library" {
			sawAxios = true
		}
		if c["name"] == "lodash" && c["type"] == "library" {
			sawLodash = true
		}
	}
	if !sawAxios || !sawLodash {
		t.Fatalf("expected both imports as library components; got %v", components)
	}
}

// TestBuildIncludesSigningProperties verifies that when the signer
// attestation from E5.02 is supplied it surfaces as cs:signing.*
// metadata properties — the field the issue lists as load-bearing for
// regulated tenants ("who signed it?").
func TestBuildIncludesSigningProperties(t *testing.T) {
	in := Input{
		Tenant:       "t_abc123",
		Namespace:    "payments",
		Function:     "reconcile",
		Version:      1,
		Runtime:      "cs-js",
		BundleSHA256: strings.Repeat("ef", 32),
		Files:        map[string][]byte{"function.js": []byte("x"), "manifest.json": []byte("{}")},
		Signer: &SigningIdentity{
			KID:         "kid-1",
			Algorithm:   "ed25519",
			Fingerprint: "fp:deadbeef",
		},
	}
	doc, err := Build(in)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	var parsed map[string]any
	_ = json.Unmarshal(doc, &parsed)
	meta, _ := parsed["metadata"].(map[string]any)
	props, _ := meta["properties"].([]any)
	wantNames := map[string]string{
		"cs:signing.kid":         "kid-1",
		"cs:signing.algorithm":   "ed25519",
		"cs:signing.fingerprint": "fp:deadbeef",
	}
	seen := map[string]string{}
	for _, raw := range props {
		p, _ := raw.(map[string]any)
		seen[p["name"].(string)] = p["value"].(string)
	}
	for name, want := range wantNames {
		if seen[name] != want {
			t.Fatalf("metadata.properties[%s]=%q want=%q (all=%v)", name, seen[name], want, seen)
		}
	}
}

// TestBuildRequiresBundleSHA256 is the simplest validation contract: an
// SBOM without an anchor digest cannot be audited, so Build must
// surface the gap loudly rather than emit a bogus document.
func TestBuildRequiresBundleSHA256(t *testing.T) {
	if _, err := Build(Input{Files: map[string][]byte{"function.js": []byte("x")}}); err == nil {
		t.Fatal("expected error when BundleSHA256 is empty")
	}
}

// TestRuntimeFromManifestDefaults checks that the helper falls back to
// "cs-js" when the manifest is missing a runtime so the SBOM still
// reports the platform default.
func TestRuntimeFromManifestDefaults(t *testing.T) {
	m := api.FunctionManifest{Schema: "cs.function.script.v1"}
	if rt, _ := RuntimeFromManifest(m); rt != "cs-js" {
		t.Fatalf("default runtime=%q want=cs-js", rt)
	}
	m.Runtime = "cs-py"
	if rt, _ := RuntimeFromManifest(m); rt != "cs-py" {
		t.Fatalf("manifest runtime=%q want=cs-py", rt)
	}
}
