package kv

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/limits"
)

// newTestStoreWithLimits builds a miniredis-backed store and installs a
// fixed Limits value so size/TTL caps are exercised deterministically.
func newTestStoreWithLimits(t *testing.T, l limits.Limits) (*Store, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	store := NewStore(mr.Addr(), "")
	t.Cleanup(func() { _ = store.Close() })
	store.SetLimits(l)
	if err := store.Ping(context.Background()); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return store, mr
}

func TestEnforceResultCapTruncatesAndFlags(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStoreWithLimits(t, limits.Limits{
		MaxResultBytes:       16,
		MaxLogBytes:          128,
		ActivationTTLSeconds: 60,
	})

	rec := api.ActivationRecord{
		ActivationID: "act-result",
		Tenant:       "t_abc123",
		Namespace:    "ns",
		Function:     "fn",
		Status:       "running",
		StartMS:      1,
		Result: &api.FunctionResponse{
			StatusCode: 200,
			Body:       strings.Repeat("a", 64),
		},
	}
	if err := store.PutActivationRunning(ctx, rec, time.Hour); err != nil {
		t.Fatalf("put running: %v", err)
	}
	got, err := store.GetActivation(ctx, rec.Tenant, rec.ActivationID)
	if err != nil {
		t.Fatalf("get activation: %v", err)
	}
	if !got.ResultTruncated {
		t.Fatalf("expected ResultTruncated=true, got record=%+v", got)
	}
	if got.Result == nil || len(got.Result.Body) != 16 {
		t.Fatalf("expected body truncated to 16 bytes, got=%d body=%q", len(got.Result.Body), got.Result.Body)
	}
}

func TestEnforceResultCapPreservesSmallResults(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStoreWithLimits(t, limits.Defaults())

	rec := api.ActivationRecord{
		ActivationID: "act-small",
		Tenant:       "t_abc123",
		Status:       "success",
		Result:       &api.FunctionResponse{StatusCode: 200, Body: "hello"},
	}
	if err := store.PutActivationRunning(ctx, rec, time.Hour); err != nil {
		t.Fatalf("put running: %v", err)
	}
	got, err := store.GetActivation(ctx, rec.Tenant, rec.ActivationID)
	if err != nil {
		t.Fatalf("get activation: %v", err)
	}
	if got.ResultTruncated {
		t.Fatalf("expected no truncation, got=%+v", got)
	}
	if got.Result.Body != "hello" {
		t.Fatalf("body mutated: %q", got.Result.Body)
	}
}

func TestAppendLogChunkTruncatesAndEmitsSentinel(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStoreWithLimits(t, limits.Limits{
		MaxLogBytes:          12,
		MaxResultBytes:       1024,
		ActivationTTLSeconds: 60,
	})

	// First chunk fits inside the cap.
	if err := store.AppendLogChunk(ctx, "t_abc123", "act-log", 0, []byte("hello"), time.Hour); err != nil {
		t.Fatalf("append chunk 0: %v", err)
	}
	// Second chunk would push us past the 12-byte cap (5 + 20 = 25); expect
	// truncation + sentinel.
	if err := store.AppendLogChunk(ctx, "t_abc123", "act-log", 1, []byte("xxxxxxxxxxxxxxxxxxxx"), time.Hour); err != nil {
		t.Fatalf("append chunk 1: %v", err)
	}
	// Third chunk should become a strict no-op (sentinel already present).
	if err := store.AppendLogChunk(ctx, "t_abc123", "act-log", 2, []byte("dropped"), time.Hour); err != nil {
		t.Fatalf("append chunk 2: %v", err)
	}

	truncated, err := store.LogTruncated(ctx, "t_abc123", "act-log")
	if err != nil {
		t.Fatalf("log truncated: %v", err)
	}
	if !truncated {
		t.Fatalf("expected truncated=true")
	}

	chunks, next, err := store.ListLogChunks(ctx, "t_abc123", "act-log", 0, 50)
	if err != nil {
		t.Fatalf("list chunks: %v", err)
	}
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks (hello, truncated-payload, sentinel), got=%d chunks=%v", len(chunks), chunks)
	}
	if next != 3 {
		t.Fatalf("expected next=3, got %d", next)
	}
	// The last chunk must be the sentinel JSON.
	var sentinel map[string]any
	if err := json.Unmarshal([]byte(chunks[len(chunks)-1]), &sentinel); err != nil {
		t.Fatalf("sentinel not valid JSON: %v body=%q", err, chunks[len(chunks)-1])
	}
	if sentinel["truncated"] != true {
		t.Fatalf("sentinel missing truncated=true: %+v", sentinel)
	}
	if sentinel["reason"] != LogTruncationReason {
		t.Fatalf("sentinel reason=%v want=%s", sentinel["reason"], LogTruncationReason)
	}
	if int(sentinel["limit_bytes"].(float64)) != 12 {
		t.Fatalf("sentinel limit_bytes mismatch: %+v", sentinel)
	}
}

func TestAppendLogChunkOversizedFirstWriteHitsSentinel(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStoreWithLimits(t, limits.Limits{MaxLogBytes: 4, ActivationTTLSeconds: 60})

	if err := store.AppendLogChunk(ctx, "t_abc123", "act-big", 0, []byte("longer than cap"), time.Hour); err != nil {
		t.Fatalf("append: %v", err)
	}
	truncated, err := store.LogTruncated(ctx, "t_abc123", "act-big")
	if err != nil || !truncated {
		t.Fatalf("expected truncated, err=%v truncated=%v", err, truncated)
	}
	chunks, _, err := store.ListLogChunks(ctx, "t_abc123", "act-big", 0, 50)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("expected truncated payload + sentinel, got=%v", chunks)
	}
}

func TestGetActivationReturnsTTLExpiredAfterEviction(t *testing.T) {
	ctx := context.Background()
	store, mr := newTestStoreWithLimits(t, limits.Defaults())

	rec := api.ActivationRecord{
		ActivationID: "act-expired",
		Tenant:       "t_abc123",
		Status:       "running",
		StartMS:      1,
	}
	if err := store.PutActivationRunning(ctx, rec, 2*time.Second); err != nil {
		t.Fatalf("put running: %v", err)
	}
	// Force the meta/status keys to expire while leaving the tombstone in
	// place. miniredis' FastForward only moves the clock by the duration
	// supplied; the tombstone has 4x TTL so it survives.
	mr.FastForward(3 * time.Second)

	_, err := store.GetActivation(ctx, rec.Tenant, rec.ActivationID)
	if err == nil {
		t.Fatal("expected expiry error")
	}
	var csErr *cserrors.CSError
	if !errors.As(err, &csErr) || csErr.Code != cserrors.CSActivationTTLExpired {
		t.Fatalf("expected CS_ACTIVATION_TTL_EXPIRED, got %v", err)
	}
}

func TestGetActivationDistinguishesNotFoundFromExpired(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStoreWithLimits(t, limits.Defaults())

	_, err := store.GetActivation(ctx, "t_abc123", "never-existed")
	if err == nil {
		t.Fatal("expected not-found error")
	}
	var csErr *cserrors.CSError
	if !errors.As(err, &csErr) || csErr.Code != cserrors.CSValidationFailed {
		t.Fatalf("expected CS_VALIDATION_FAILED for missing activation, got %v", err)
	}
}

func TestTombstoneSurvivesActivationTTLOverride(t *testing.T) {
	ctx := context.Background()
	store, mr := newTestStoreWithLimits(t, limits.Defaults())

	rec := api.ActivationRecord{
		ActivationID: "act-cfg",
		Tenant:       "t_abc123",
		Status:       "running",
		StartMS:      1,
	}
	// Use a custom 60-second TTL (issue mandates configurable overrides).
	if err := store.PutActivationRunning(ctx, rec, 60*time.Second); err != nil {
		t.Fatalf("put: %v", err)
	}
	mr.FastForward(70 * time.Second)
	_, err := store.GetActivation(ctx, rec.Tenant, rec.ActivationID)
	var csErr *cserrors.CSError
	if !errors.As(err, &csErr) || csErr.Code != cserrors.CSActivationTTLExpired {
		t.Fatalf("expected 410 after override expiry, got %v", err)
	}
	// Forward past the tombstone window too — now we should see 404.
	mr.FastForward(5 * time.Minute)
	_, err = store.GetActivation(ctx, rec.Tenant, rec.ActivationID)
	if !errors.As(err, &csErr) || csErr.Code != cserrors.CSValidationFailed {
		t.Fatalf("expected 404 after tombstone expiry, got %v", err)
	}
}

func TestTruncateUTF8Boundaries(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		maxByte int
		want    string
	}{
		{"empty cap", "abc", 0, ""},
		{"already short", "abc", 10, "abc"},
		{"ascii cut", "abcdef", 3, "abc"},
		// "é" is two bytes (0xC3 0xA9). A maxByte=1 must drop the partial rune.
		{"utf8 boundary", "é", 1, ""},
		{"utf8 keeps rune", "aé", 3, "aé"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := truncateUTF8(tc.input, tc.maxByte); got != tc.want {
				t.Fatalf("truncateUTF8(%q, %d) = %q want %q", tc.input, tc.maxByte, got, tc.want)
			}
		})
	}
}
