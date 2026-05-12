package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParentContextRoundTrip(t *testing.T) {
	if got := ParentFrom(context.Background()); got != "" {
		t.Fatalf("expected empty parent on bare ctx, got %q", got)
	}
	if got := ParentFrom(nil); got != "" { //nolint:staticcheck // intentional nil to exercise guard
		t.Fatalf("expected empty parent on nil ctx, got %q", got)
	}
	ctx := WithParent(context.Background(), "act-parent-1")
	if got := ParentFrom(ctx); got != "act-parent-1" {
		t.Fatalf("round-trip mismatch, got %q", got)
	}
	// Empty activation IDs should not create a stray key on the context.
	bare := WithParent(context.Background(), "")
	if got := ParentFrom(bare); got != "" {
		t.Fatalf("empty parent unexpectedly stored, got %q", got)
	}
}

func TestInjectParentHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/v1/web/t/ns/fn/prod", nil)
	// No parent on context — header must remain unset.
	InjectParentHeader(req, context.Background())
	if got := req.Header.Get(ParentActivationHeader); got != "" {
		t.Fatalf("header unexpectedly set without parent, got %q", got)
	}
	// With parent the header must match the stored value byte-for-byte.
	ctx := WithParent(context.Background(), "act-42")
	InjectParentHeader(req, ctx)
	if got := req.Header.Get(ParentActivationHeader); got != "act-42" {
		t.Fatalf("header mismatch: got %q want act-42", got)
	}
	// Nil request and nil context are safe to call.
	InjectParentHeader(nil, ctx)
	InjectParentHeader(req, nil)
	// Idempotent re-set: header is still set to the parent activation.
	if got := req.Header.Get(ParentActivationHeader); got != "act-42" {
		t.Fatalf("header lost after re-injection, got %q", got)
	}
}
