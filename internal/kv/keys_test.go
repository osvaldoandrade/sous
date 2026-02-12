package kv

import "testing"

func TestKeys(t *testing.T) {
	if got := FunctionMetaKey("t_abc123", "payments", "reconcile"); got != "cs:fn:t_abc123:payments:reconcile:meta" {
		t.Fatalf("unexpected key: %s", got)
	}
	if got := VersionMetaKey("t_abc123", "payments", "reconcile", 17); got != "cs:fn:t_abc123:payments:reconcile:ver:17:meta" {
		t.Fatalf("unexpected key: %s", got)
	}
}

func TestCursorHelpers(t *testing.T) {
	if got := ParseCursor("a"); got != 0 {
		t.Fatalf("expected 0 got %d", got)
	}
	if got := EncodeCursor(10); got != "10" {
		t.Fatalf("expected 10 got %s", got)
	}
}
