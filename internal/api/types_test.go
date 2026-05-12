package api

import (
	"testing"
)

func TestManifestRuntimeAcceptsKnownValues(t *testing.T) {
	for _, rt := range []string{"cs-js", "cs-python", "cs-wasm"} {
		m := newValidManifestWithRuntime(rt)
		if err := m.Validate(); err != nil {
			t.Fatalf("runtime %q should be accepted: %v", rt, err)
		}
	}
}

func TestManifestRuntimeRejectsUnknownValues(t *testing.T) {
	for _, rt := range []string{"cs-unknown", "CS-JS", "js", "node"} {
		m := newValidManifestWithRuntime(rt)
		if err := m.Validate(); err == nil {
			t.Fatalf("runtime %q must be rejected", rt)
		}
	}
}

func TestManifestRuntimeImplicitDefault(t *testing.T) {
	// v1 manifests omitting the field continue to validate. Callers reach
	// for NormalizeRuntime to materialise the implicit cs-js default.
	m := newValidManifestWithRuntime("")
	if err := m.Validate(); err != nil {
		t.Fatalf("empty runtime (implicit default) should pass: %v", err)
	}
	if got := NormalizeRuntime(m.Runtime); got != RuntimeCSJS {
		t.Fatalf("NormalizeRuntime(empty) = %q, want %q", got, RuntimeCSJS)
	}
}

func TestManifestRuntimeVersionAcceptsValidFormats(t *testing.T) {
	for _, v := range []string{"", "cs-js@1", "node20-goja", "python3.12", "wasi-preview1", "adapter:tag+build"} {
		m := newValidManifestWithRuntime("cs-js")
		m.RuntimeVersion = v
		if err := m.Validate(); err != nil {
			t.Fatalf("runtimeVersion %q should be accepted: %v", v, err)
		}
	}
}

func TestManifestRuntimeVersionRejectsInvalidFormats(t *testing.T) {
	for _, v := range []string{" leading", "!bang", "-leading", "way-too-long-runtime-version-string-that-definitely-exceeds-the-limit-set"} {
		m := newValidManifestWithRuntime("cs-js")
		m.RuntimeVersion = v
		if err := m.Validate(); err == nil {
			t.Fatalf("runtimeVersion %q must be rejected", v)
		}
	}
}

func TestIsKnownRuntime(t *testing.T) {
	for _, rt := range []string{"", "cs-js", "cs-python", "cs-wasm"} {
		if !IsKnownRuntime(rt) {
			t.Fatalf("runtime %q should be known", rt)
		}
	}
	for _, rt := range []string{"cs-unknown", "CS-JS", "js"} {
		if IsKnownRuntime(rt) {
			t.Fatalf("runtime %q should not be known", rt)
		}
	}
}

func TestNormalizeRuntime(t *testing.T) {
	if got := NormalizeRuntime(""); got != RuntimeCSJS {
		t.Fatalf("NormalizeRuntime(empty) = %q, want %q", got, RuntimeCSJS)
	}
	for _, rt := range []string{"cs-js", "cs-python", "cs-wasm", "cs-unknown"} {
		if got := NormalizeRuntime(rt); got != rt {
			t.Fatalf("NormalizeRuntime(%q) = %q, want %q", rt, got, rt)
		}
	}
}

func TestIsValidRuntimeVersion(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", true},
		{"cs-js@1", true},
		{"node20-goja", true},
		{"python3.12", true},
		{"wasi-preview1", true},
		{"adapter:tag+build", true},
		{" leading-space", false},
		{"!bang", false},
		{"-leading-hyphen", false},
		{"way-too-long-runtime-version-string-that-definitely-exceeds-the-limit-set", false},
	}
	for _, tc := range cases {
		if got := IsValidRuntimeVersion(tc.in); got != tc.want {
			t.Fatalf("IsValidRuntimeVersion(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestSupportedManifestRuntimesShape(t *testing.T) {
	want := []string{RuntimeCSJS, RuntimeCSPython, RuntimeCSWASM}
	if len(SupportedManifestRuntimes) != len(want) {
		t.Fatalf("SupportedManifestRuntimes = %v, want %v", SupportedManifestRuntimes, want)
	}
	for i, rt := range want {
		if SupportedManifestRuntimes[i] != rt {
			t.Fatalf("SupportedManifestRuntimes[%d] = %q, want %q", i, SupportedManifestRuntimes[i], rt)
		}
	}
}

// newValidManifestWithRuntime returns a baseline-valid FunctionManifest
// whose only unusual field is Runtime. Helpers in tests stamp the value
// they want exercised on top of this prototype.
func newValidManifestWithRuntime(runtime string) FunctionManifest {
	return FunctionManifest{
		Schema:  "cs.function.script.v1",
		Runtime: runtime,
		Entry:   "function.js",
		Handler: "default",
		Limits: ManifestLimits{
			TimeoutMS:      1000,
			MemoryMB:       64,
			MaxConcurrency: 1,
		},
		Capabilities: ManifestCapabilities{
			KV: ManifestKVCaps{
				Prefixes: []string{"ctr:"},
				Ops:      []string{"get"},
			},
			CodeQ: ManifestCodeQCaps{
				PublishTopics: []string{"jobs.*"},
			},
			HTTP: ManifestHTTPCaps{
				AllowHosts: []string{"example.com"},
				TimeoutMS:  1000,
			},
		},
	}
}
