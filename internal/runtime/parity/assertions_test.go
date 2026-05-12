package parity

import (
	"testing"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/runtime"
)

// TestCompareMatchesShape covers the happy path: the response carries
// the expected statusCode, body, and headers, so Compare returns no
// mismatches.
func TestCompareMatchesShape(t *testing.T) {
	exp := Expected{
		Status: "success",
		ResponseShape: map[string]any{
			"statusCode": float64(200),
			"body":       "hello",
		},
		Headers: map[string]string{"x-cs": "1"},
	}
	out := runtime.ExecutionOutput{
		Status: "success",
		Result: &api.FunctionResponse{
			StatusCode: 200,
			Headers:    map[string]string{"x-cs": "1"},
			Body:       "hello",
		},
	}
	if miss := Compare(exp, out); len(miss) != 0 {
		t.Fatalf("unexpected mismatches: %+v", miss)
	}
}

// TestCompareFlagsDrift asserts every miss is reported, not just the
// first one. This is critical for the parity report: engineers should
// see all drifts in one CI run instead of fixing them one-by-one.
func TestCompareFlagsDrift(t *testing.T) {
	exp := Expected{
		Status: "success",
		ResponseShape: map[string]any{
			"statusCode": float64(200),
			"body":       "hello",
		},
		Headers:         map[string]string{"x-cs": "1"},
		ErrorSubstrings: []string{"won't appear"},
	}
	out := runtime.ExecutionOutput{
		Status: "error",
		Result: &api.FunctionResponse{
			StatusCode: 500,
			Headers:    map[string]string{},
			Body:       "boom",
		},
		Error: &api.InvocationError{Message: "other thing happened"},
	}
	miss := Compare(exp, out)
	if len(miss) < 4 {
		t.Fatalf("expected >= 4 mismatches, got %d: %+v", len(miss), miss)
	}
}

// TestCompareLogSubstrings asserts presence-only semantics: logs may
// carry extra noise but must contain every expected substring.
func TestCompareLogSubstrings(t *testing.T) {
	exp := Expected{LogSubstrings: []string{"started", "done"}}
	out := runtime.ExecutionOutput{
		Status: "success",
		Result: &api.FunctionResponse{StatusCode: 200},
		Logs:   []string{"info: started", "info: did some work", "info: done"},
	}
	if miss := Compare(exp, out); len(miss) != 0 {
		t.Fatalf("unexpected mismatches: %+v", miss)
	}
}

// TestCompareResolvedCode locks the typed-error contract: a runtime
// that returns the right HTTP status but the wrong CSError code is
// still a parity failure. This is the guard for the "negative scenarios
// produce the same typed cserrors.Code regardless of runtime"
// acceptance criterion.
func TestCompareResolvedCode(t *testing.T) {
	exp := Expected{ResolvedCode: "CS_RUNTIME_CAPABILITY_DENIED"}
	out := runtime.ExecutionOutput{ResolvedCode: "CS_RUNTIME_EXCEPTION"}
	miss := Compare(exp, out)
	if len(miss) != 1 || miss[0].Field != "resolvedCode" {
		t.Fatalf("want one resolvedCode mismatch, got %+v", miss)
	}
}
