package parity

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/runtime"
)

// Mismatch describes a single comparison failure surfaced by the parity
// harness. Field names match the on-disk fixture so engineers can grep
// from a CI log line back to the JSON they need to edit. Multiple
// mismatches are accumulated per (runtime, fixture) cell so a single
// failing run reports every drift instead of one-shotting on the first
// difference.
type Mismatch struct {
	Field string
	Want  any
	Got   any
	Note  string
}

func (m Mismatch) String() string {
	if m.Note != "" {
		return fmt.Sprintf("%s: %s (want=%v got=%v)", m.Field, m.Note, m.Want, m.Got)
	}
	return fmt.Sprintf("%s: want=%v got=%v", m.Field, m.Want, m.Got)
}

// Compare evaluates the fixture's Expected block against the runner
// output and returns the list of mismatches. An empty list means the
// runtime matched the golden shape for this fixture.
//
// The comparison is intentionally permissive:
//
//   - ResponseShape uses a deep partial match — only keys named in the
//     fixture are checked, so a fixture asserting statusCode does not
//     accidentally lock in headers it doesn't care about.
//   - LogSubstrings asserts presence (not order, not exclusivity) so a
//     runtime that adds debug noise on top of the contract still passes.
//   - ErrorSubstrings asserts presence on out.Error.Message.
//   - Numeric values are compared after JSON round-trip so int/float
//     differences across runtimes (cs-js returns numbers as float64,
//     cs-python may return int) don't flag false positives.
//
// This is the comparator described in the issue "normalisation step
// strips/sorts before comparison".
func Compare(exp Expected, out runtime.ExecutionOutput) []Mismatch {
	var miss []Mismatch

	if exp.Status != "" && exp.Status != out.Status {
		miss = append(miss, Mismatch{Field: "status", Want: exp.Status, Got: out.Status})
	}

	if exp.ResolvedCode != "" && exp.ResolvedCode != string(out.ResolvedCode) {
		miss = append(miss, Mismatch{Field: "resolvedCode", Want: exp.ResolvedCode, Got: string(out.ResolvedCode)})
	}

	if len(exp.ResponseShape) > 0 {
		got := flattenResponse(out.Result)
		for k, v := range exp.ResponseShape {
			gv, ok := got[k]
			if !ok {
				miss = append(miss, Mismatch{Field: "responseShape." + k, Want: v, Got: nil, Note: "missing key"})
				continue
			}
			if !jsonEqual(v, gv) {
				miss = append(miss, Mismatch{Field: "responseShape." + k, Want: v, Got: gv})
			}
		}
	}

	if len(exp.Headers) > 0 {
		got := map[string]string{}
		if out.Result != nil {
			got = out.Result.Headers
		}
		for k, v := range exp.Headers {
			gv, ok := got[k]
			if !ok {
				miss = append(miss, Mismatch{Field: "headers." + k, Want: v, Got: nil, Note: "missing header"})
				continue
			}
			if gv != v {
				miss = append(miss, Mismatch{Field: "headers." + k, Want: v, Got: gv})
			}
		}
	}

	if len(exp.LogSubstrings) > 0 {
		joined := strings.Join(out.Logs, "\n")
		for _, want := range exp.LogSubstrings {
			if !strings.Contains(joined, want) {
				miss = append(miss, Mismatch{Field: "logs", Want: want, Got: joined, Note: "log substring missing"})
			}
		}
	}

	if len(exp.ErrorSubstrings) > 0 {
		msg := ""
		if out.Error != nil {
			msg = out.Error.Message
		}
		for _, want := range exp.ErrorSubstrings {
			if !strings.Contains(msg, want) {
				miss = append(miss, Mismatch{Field: "error", Want: want, Got: msg, Note: "error substring missing"})
			}
		}
	}

	return miss
}

// flattenResponse projects a FunctionResponse into a map keyed by the
// JSON field name. Callers compare against this rather than reflecting
// on the struct so fixtures can keep using lower-camel keys verbatim.
// Numeric fields go through int64 → float64 normalisation to dodge the
// "Go decodes 200 as int, fixture has 200 as float" trap.
func flattenResponse(r *api.FunctionResponse) map[string]any {
	if r == nil {
		return map[string]any{}
	}
	out := map[string]any{
		"statusCode":      float64(r.StatusCode),
		"isBase64Encoded": r.IsBase64Encoded,
	}
	if r.Body != "" {
		out["body"] = r.Body
	}
	if len(r.Headers) > 0 {
		hdrs := make(map[string]any, len(r.Headers))
		for k, v := range r.Headers {
			hdrs[k] = v
		}
		out["headers"] = hdrs
	}
	return out
}

// jsonEqual normalises two values through json.Marshal/Unmarshal so
// they compare structurally even when one came from a Go struct and the
// other from a JSON literal. This is the same trick we use elsewhere in
// the repo (e.g. cmd/cs-control tests).
func jsonEqual(a, b any) bool {
	ab, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bb, err := json.Marshal(b)
	if err != nil {
		return false
	}
	var av, bv any
	if err := json.Unmarshal(ab, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(bb, &bv); err != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}
