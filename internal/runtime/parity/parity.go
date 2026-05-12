package parity

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	"github.com/osvaldoandrade/sous/internal/runtime"
)

// Suite orchestrates the (runtime × fixture) matrix. It is intentionally
// a value type with explicit slices instead of a fluent builder: the
// goal is for `go test ./internal/runtime/parity/...` to print the
// matrix verbatim and for new fixtures / new runtimes to plug in with a
// single-line edit.
//
// Runtimes defaults to runtime.DefaultRegistry.Names() at Run time when
// nil. Fixtures must be loaded explicitly via LoadFixtures — the suite
// does not own a default fixtures path so unit tests and the CLI can
// reuse the same type without a hidden global.
//
// The Suite is the central type referenced by docs/17-testing.md
// "Runtime parity harness": a fixture either passes for every
// registered runtime or the build fails.
type Suite struct {
	// Runtimes is the set of runtime slugs to dispatch fixtures
	// against. When empty, the suite reads
	// runtime.DefaultRegistry.Names() at Run time so adapters
	// registered via init() auto-join the matrix.
	Runtimes []string

	// Fixtures is the corpus to execute. Order is preserved so
	// LoadFixtures' sorted output produces deterministic subtest names.
	Fixtures []*Fixture

	// NewRunner constructs the runtime adapter under test. The default
	// returns the in-process cs-js runner with conservative limits.
	// Tests inject this to swap the runner (e.g. point cs-js at a
	// recording KV, or in the future to dispatch to a cs-python
	// process). The runtime argument is the slug for the matrix cell —
	// adapters will use it to pick the right execution backend once
	// cs-python and cs-wasm land.
	NewRunner func(runtimeName string) Executor
}

// Executor is the narrow surface the parity harness needs from a
// runtime adapter. It mirrors runtime.Runner.Execute so today's only
// implementation — the cs-js adapter — wires in trivially, while
// future adapters (cs-python over IPC, cs-wasm in-process) can supply
// their own Executor without inheriting goja-specific concerns.
type Executor interface {
	Execute(ctx context.Context, bundleBytes []byte, request api.InvocationRequest) runtime.ExecutionOutput
}

// CellResult is one (runtime, fixture) outcome. The Skipped flag lets
// the matrix report cells where the fixture did not declare guest code
// for the runtime — those cells are not failures but they are visible
// in the per-runtime tally so engineers know what is and isn't covered.
type CellResult struct {
	Runtime    string
	Fixture    string
	Skipped    bool
	Mismatches []Mismatch
	Err        error
}

// Passed reports whether the cell ran cleanly. A skipped cell counts as
// passed because the missing-files case is the intentional opt-in for
// future runtimes; cells failing because of harness errors (bundle
// build, manifest marshal) count as failures.
func (c CellResult) Passed() bool {
	return c.Err == nil && len(c.Mismatches) == 0
}

// Run executes the full matrix and returns the per-cell results. The
// caller is expected to surface failures to *testing.T via RunT; Run is
// exposed so the eventual `cs parity` CLI can render a custom report.
func (s *Suite) Run(ctx context.Context) []CellResult {
	runtimes := s.runtimes()
	results := make([]CellResult, 0, len(runtimes)*len(s.Fixtures))

	for _, rt := range runtimes {
		exec := s.executor(rt)
		for _, fx := range s.Fixtures {
			results = append(results, s.execCell(ctx, exec, rt, fx))
		}
	}
	return results
}

// RunT is the testing.T-friendly wrapper. Each (runtime, fixture) cell
// becomes a subtest so `go test -run TestSuiteMatrix/cs-js/simple-echo`
// filters down to a single matrix cell — the same idiom the rest of
// the repo uses.
func (s *Suite) RunT(t *testing.T) {
	t.Helper()
	runtimes := s.runtimes()
	if len(runtimes) == 0 {
		t.Skip("parity: no runtimes registered")
		return
	}
	if len(s.Fixtures) == 0 {
		t.Skip("parity: no fixtures loaded")
		return
	}
	for _, rt := range runtimes {
		rt := rt
		exec := s.executor(rt)
		t.Run(rt, func(t *testing.T) {
			for _, fx := range s.Fixtures {
				fx := fx
				t.Run(fx.Name, func(t *testing.T) {
					cell := s.execCell(t.Context(), exec, rt, fx)
					switch {
					case cell.Skipped:
						t.Skipf("fixture has no %s implementation", rt)
					case cell.Err != nil:
						t.Fatalf("%v", cell.Err)
					case len(cell.Mismatches) > 0:
						t.Errorf("parity drift: runtime=%s fixture=%s", rt, fx.Name)
						for _, m := range cell.Mismatches {
							t.Errorf("  - %s", m)
						}
					}
				})
			}
		})
	}
}

// execCell runs one (runtime, fixture) cell and packages the result.
// Shared between Run (CLI report) and RunT (Go test driver) so the
// dispatch path stays in one place.
func (s *Suite) execCell(ctx context.Context, exec Executor, rt string, fx *Fixture) CellResult {
	cell := CellResult{Runtime: rt, Fixture: fx.Name}
	if !fx.SupportsRuntime(rt) {
		cell.Skipped = true
		return cell
	}
	bundleBytes, err := buildFixtureBundle(fx, rt)
	if err != nil {
		cell.Err = fmt.Errorf("build bundle for runtime=%s fixture=%s: %w", rt, fx.Name, err)
		return cell
	}
	out := exec.Execute(ctx, bundleBytes, buildRequest(fx))
	cell.Mismatches = Compare(fx.Expected, out)
	return cell
}

func (s *Suite) runtimes() []string {
	if len(s.Runtimes) > 0 {
		out := append([]string(nil), s.Runtimes...)
		sort.Strings(out)
		return out
	}
	return runtime.DefaultRegistry.Names()
}

func (s *Suite) executor(rt string) Executor {
	if s.NewRunner != nil {
		return s.NewRunner(rt)
	}
	return defaultExecutor()
}

// defaultExecutor returns the in-process runner the harness uses when
// the caller does not inject one. Today the only adapter we ship is
// cs-js (goja) and runtime.NewRunner serves every fixture regardless of
// runtime slug, which is fine — the harness still asserts that the
// fixtures published as e.g. cs-python produce the same shape, because
// the same Runner is what would back any future bridge until the real
// cs-python adapter lands.
func defaultExecutor() Executor {
	return runtime.NewRunner(runtime.NewMemoryKV(), runtime.NopCodeQ{}, 256*1024, 64*1024, 1024*1024)
}

// buildFixtureBundle assembles the canonical bundle for a single
// matrix cell: it picks the per-runtime files, injects the manifest
// (with Runtime overwritten to the matrix's slug), and asks
// bundle.BuildCanonical to do the actual tarring. Returning the raw
// bytes keeps the caller decoupled from bundle's internal layout.
func buildFixtureBundle(fx *Fixture, rt string) ([]byte, error) {
	files := fx.Files[rt]
	if len(files) == 0 {
		return nil, fmt.Errorf("no files for runtime %q", rt)
	}

	manifest := fx.Manifest
	manifest.Runtime = rt
	if manifest.Schema == "" {
		manifest.Schema = "cs.function.script.v1"
	}
	if manifest.Handler == "" {
		manifest.Handler = "default"
	}
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("marshal manifest: %w", err)
	}

	bytesByPath := make(map[string][]byte, len(files)+1)
	for path, content := range files {
		bytesByPath[path] = []byte(content)
	}
	bytesByPath["manifest.json"] = manifestRaw

	out, _, _, err := bundle.BuildCanonical(bytesByPath)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// buildRequest constructs the InvocationRequest the runner sees. The
// fields are filled with deterministic test values so the matrix has no
// hidden dependency on uuid/random sources. DeadlineOffsetMS controls
// the timeout fixture; the default is "manifest TimeoutMS from now".
func buildRequest(fx *Fixture) api.InvocationRequest {
	offset := fx.DeadlineOffsetMS
	if offset == 0 {
		offset = int64(fx.Manifest.Limits.TimeoutMS)
		if offset == 0 {
			offset = 2000
		}
	}
	deadline := time.Now().Add(time.Duration(offset) * time.Millisecond)
	return api.InvocationRequest{
		ActivationID: "act-parity-" + fx.Name,
		RequestID:    "req-parity-" + fx.Name,
		Tenant:       "t_parity",
		Namespace:    "parity",
		Ref:          api.FunctionRef{Function: fx.Name, Version: 1},
		Trigger:      fx.Trigger,
		Principal:    api.Principal{Sub: "user:parity", Roles: []string{"role:app"}},
		DeadlineMS:   deadline.UnixMilli(),
		Event:        fx.Event,
	}
}
