// Package parity defines a runtime-agnostic fixture suite that the
// control plane uses to enforce the runtime-parity invariant stated in
// docs/02-requirements.md: for the same input, every adapter
// (cs-js, cs-python, cs-wasm, ...) must produce the same response shape,
// the same typed error code, and the same observable side effects.
//
// The harness is driven by JSON fixtures stored under test/parity/fixtures.
// Each fixture is loaded once, materialised into a bundle per runtime, and
// executed through the standard runtime.Runner. The Suite type drives the
// matrix; the comparison helpers in assertions.go normalise the
// runtime-specific noise (timing, header casing, log slice headers) before
// asserting the recorded golden shape.
//
// New runtimes opt into the matrix by registering a Handler with
// runtime.DefaultRegistry and supplying a per-runtime files map under the
// fixture's Files entry. A fixture without a files entry for a given
// runtime is skipped for that runtime, which lets cs-js fixtures land
// today without blocking on the cs-python / cs-wasm adapters.
package parity

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/osvaldoandrade/sous/internal/api"
)

// Fixture is the on-disk form of a parity scenario.
//
// Files is a runtime → (path → content) map. The harness selects the
// entry whose key matches the runtime under test (e.g. "cs-js") and
// builds the canonical bundle from it. A fixture that does not declare
// files for a given runtime is silently skipped — this is intentional so
// the cs-js corpus can land before the cs-python / cs-wasm adapters do,
// and so any future adapter (e.g. a hypothetical "cs-deno") auto-joins
// without rewriting fixtures.
//
// Manifest is the publish-time function manifest. The harness fills in
// the Runtime field per matrix cell and re-marshals it into the
// generated bundle, so fixture authors should leave Runtime empty (or
// "cs-js" — it is overwritten either way).
//
// Trigger and Event mirror the api.InvocationRequest fields the runner
// receives at invoke time. Event is decoded as an arbitrary JSON value
// so fixtures can express maps, arrays, scalars, or null.
//
// Expected captures the golden output shape. Fields are checked
// permissively: only the keys present in Expected are compared, so a
// fixture that asserts statusCode + body does not have to enumerate the
// entire headers map. See assertions.go for the comparison rules.
type Fixture struct {
	// Name uniquely identifies the fixture and becomes the Go subtest
	// name. Should be a short slug — letters, digits, dashes.
	Name string `json:"name"`

	// Description is a human-readable one-liner shown in failure
	// messages and the eventual per-runtime parity report.
	Description string `json:"description"`

	// Manifest is the FunctionManifest the fixture is published under.
	// The Runtime field is overwritten by the harness for each matrix
	// cell so a single fixture can be run against every adapter.
	Manifest api.FunctionManifest `json:"manifest"`

	// Files is the per-runtime guest code. The key is the runtime slug
	// (matching api.RuntimeCSJS et al.); the value is a path → content
	// map appended to the bundle alongside the synthesised manifest.
	// The harness skips the fixture for any runtime missing from this
	// map, which keeps the matrix forward-compatible with adapters that
	// have not been written yet.
	Files map[string]map[string]string `json:"files"`

	// Trigger seeds api.InvocationRequest.Trigger. Most fixtures use
	// the default {Type: "api"} but timing/HTTP scenarios may need a
	// specific trigger source.
	Trigger api.Trigger `json:"trigger"`

	// Event is the JSON-decoded event handed to the function. May be a
	// scalar, map, list, or null.
	Event any `json:"event"`

	// DeadlineOffsetMS sets the invocation deadline relative to "now"
	// at run time. Zero defaults to the manifest's TimeoutMS, which is
	// the common case. Negative values are reserved for timeout
	// fixtures that need the deadline to already be in the past.
	DeadlineOffsetMS int64 `json:"deadlineOffsetMs,omitempty"`

	// Expected is the golden shape the harness asserts after the
	// runner returns. Permissive deep-compare; see assertions.go.
	Expected Expected `json:"expected"`
}

// Expected is the parity-checked shape. All fields are optional — only
// the fields explicitly set in the fixture are compared.
//
// The status mapping mirrors runtime.ExecutionOutput.Status:
//   - "success" — the runner returned a FunctionResponse,
//   - "error"   — the runner trapped a typed error,
//   - "timeout" — the runner hit a deadline.
//
// ResolvedCode is the cserrors.Code (e.g. "CS_RUNTIME_CAPABILITY_DENIED")
// the runtime surfaces for failed invocations. The string form is used
// in fixtures so the cserrors package does not have to be JSON-aware.
type Expected struct {
	Status          string            `json:"status,omitempty"`
	ResolvedCode    string            `json:"resolvedCode,omitempty"`
	ResponseShape   map[string]any    `json:"responseShape,omitempty"`
	LogSubstrings   []string          `json:"logSubstrings,omitempty"`
	ErrorSubstrings []string          `json:"errorSubstrings,omitempty"`
	Headers         map[string]string `json:"headers,omitempty"`
}

// LoadFixture parses a single fixture file from disk. Returns an error
// with a path-prefixed message on malformed JSON so failures can be
// traced back to the offending fixture without `dlv`-style spelunking.
func LoadFixture(path string) (*Fixture, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("parity: read fixture %q: %w", path, err)
	}
	var f Fixture
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parity: parse fixture %q: %w", path, err)
	}
	if strings.TrimSpace(f.Name) == "" {
		return nil, fmt.Errorf("parity: fixture %q is missing a name", path)
	}
	if len(f.Files) == 0 {
		return nil, fmt.Errorf("parity: fixture %q has no per-runtime files", path)
	}
	return &f, nil
}

// LoadFixtures discovers and loads every *.json file beneath dir,
// returning them sorted by Name. The deterministic order is important
// so the matrix output is stable across runs and the CI diff stays
// readable. Empty directories return (nil, nil); missing directories
// surface as errors.
func LoadFixtures(dir string) ([]*Fixture, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("parity: read fixtures dir %q: %w", dir, err)
	}
	out := make([]*Fixture, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(strings.ToLower(e.Name()), ".json") {
			continue
		}
		fx, err := LoadFixture(filepath.Join(dir, e.Name()))
		if err != nil {
			return nil, err
		}
		out = append(out, fx)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// SupportsRuntime reports whether the fixture carries guest code for the
// given runtime slug. The harness consults this before dispatching a
// matrix cell so a missing entry is treated as "skip" instead of "fail".
func (f *Fixture) SupportsRuntime(runtime string) bool {
	if f == nil {
		return false
	}
	files, ok := f.Files[runtime]
	return ok && len(files) > 0
}
