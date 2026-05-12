package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// TestActivationLogsPaginationAndStreaming exercises the new pagination,
// truncation header, ndjson streaming, and SSE variants of
// GET .../activations/{id}/logs landed for issue #3.
func TestActivationLogsPaginationAndStreaming(t *testing.T) {
	pageOne := []string{"[info] one", "[info] two"}
	pageTwo := []string{"[info] three"}
	store := &testutil.FakePersistence{
		ListLogChunksFn: func(_ context.Context, _ string, _ string, cursor, limit int64) ([]string, int64, error) {
			_ = limit
			if cursor == 0 {
				return pageOne, int64(len(pageOne)), nil
			}
			return pageTwo, cursor + int64(len(pageTwo)), nil
		},
		LogTruncatedFn: func(context.Context, string, string) (bool, error) { return true, nil },
	}
	s := newControlServer(store, &testutil.FakeMessaging{})

	// First page: limit=2 matches page size, so next_cursor is non-empty.
	w := httptest.NewRecorder()
	req := controlRequest(http.MethodGet, "/activation/logs?limit=2", "", map[string]string{
		"tenant": "t_abc123", "activation_id": "act-x",
	}, principalWith("t_abc123", "cs:activation:read"))
	s.getActivationLogs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("page one status=%d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("X-CS-Truncated"); got != "logs" {
		t.Fatalf("expected X-CS-Truncated=logs, got %q", got)
	}
	var pageOneResp struct {
		Chunks     []string `json:"chunks"`
		NextCursor string   `json:"next_cursor"`
		Cursor     string   `json:"cursor"`
		Truncated  bool     `json:"truncated"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &pageOneResp); err != nil {
		t.Fatalf("decode page one: %v body=%s", err, w.Body.String())
	}
	if len(pageOneResp.Chunks) != 2 {
		t.Fatalf("page one chunks mismatch: %+v", pageOneResp)
	}
	if pageOneResp.NextCursor != "2" {
		t.Fatalf("page one next_cursor=%q want=2", pageOneResp.NextCursor)
	}
	if pageOneResp.Cursor != "2" {
		t.Fatalf("page one cursor=%q (legacy) want=2", pageOneResp.Cursor)
	}
	if !pageOneResp.Truncated {
		t.Fatalf("page one truncated flag not surfaced: %+v", pageOneResp)
	}

	// Second page: page size 1 < limit so next_cursor must be empty (EOF).
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/activation/logs?limit=2&cursor=2", "", map[string]string{
		"tenant": "t_abc123", "activation_id": "act-x",
	}, principalWith("t_abc123", "cs:activation:read"))
	s.getActivationLogs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("page two status=%d body=%s", w.Code, w.Body.String())
	}
	var pageTwoResp struct {
		Chunks     []string `json:"chunks"`
		NextCursor string   `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &pageTwoResp); err != nil {
		t.Fatalf("decode page two: %v body=%s", err, w.Body.String())
	}
	if len(pageTwoResp.Chunks) != 1 {
		t.Fatalf("page two chunks mismatch: %+v", pageTwoResp)
	}
	if pageTwoResp.NextCursor != "" {
		t.Fatalf("page two expected EOF (empty next_cursor), got %q", pageTwoResp.NextCursor)
	}

	// ndjson streaming via Accept: each chunk on its own line plus a trailer.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/activation/logs?limit=2", "", map[string]string{
		"tenant": "t_abc123", "activation_id": "act-x",
	}, principalWith("t_abc123", "cs:activation:read"))
	req.Header.Set("Accept", "application/x-ndjson")
	s.getActivationLogs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("ndjson status=%d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/x-ndjson" {
		t.Fatalf("ndjson content-type=%q", ct)
	}
	lines := splitLines(w.Body.String())
	if len(lines) != 3 {
		t.Fatalf("ndjson expected 3 lines (2 chunks + trailer), got %d body=%q", len(lines), w.Body.String())
	}
	var trailer map[string]any
	if err := json.Unmarshal([]byte(lines[2]), &trailer); err != nil {
		t.Fatalf("trailer not JSON: %v line=%q", err, lines[2])
	}
	if trailer["eof"] != true {
		t.Fatalf("trailer missing eof: %+v", trailer)
	}
	if trailer["next_cursor"] != "2" {
		t.Fatalf("trailer next_cursor=%v want=2", trailer["next_cursor"])
	}

	// SSE variant via ?format=sse query knob.
	w = httptest.NewRecorder()
	req = controlRequest(http.MethodGet, "/activation/logs?format=sse&limit=2", "", map[string]string{
		"tenant": "t_abc123", "activation_id": "act-x",
	}, principalWith("t_abc123", "cs:activation:read"))
	s.getActivationLogs(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("sse status=%d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("sse content-type=%q", ct)
	}
	if !strings.Contains(w.Body.String(), "event: log") {
		t.Fatalf("sse body missing log event: %q", w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "event: eof") {
		t.Fatalf("sse body missing eof event: %q", w.Body.String())
	}
}

// TestActivationGetSurfacesResultTruncationHeader makes sure the
// X-CS-Truncated: result header travels with GET /activations/{id} when the
// persisted record was clipped to MaxResultBytes on the write path.
func TestActivationGetSurfacesResultTruncationHeader(t *testing.T) {
	store := &testutil.FakePersistence{
		GetActivationFn: func(context.Context, string, string) (api.ActivationRecord, error) {
			return api.ActivationRecord{
				ActivationID:    "act-trunc",
				Tenant:          "t_abc123",
				Status:          "success",
				ResultTruncated: true,
				Result:          &api.FunctionResponse{StatusCode: 200, Body: "clipped"},
			}, nil
		},
	}
	s := newControlServer(store, &testutil.FakeMessaging{})

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodGet, "/activation", "", map[string]string{
		"tenant": "t_abc123", "activation_id": "act-trunc",
	}, principalWith("t_abc123", "cs:activation:read"))
	s.getActivation(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("X-CS-Truncated"); got != "result" {
		t.Fatalf("X-CS-Truncated=%q want=result", got)
	}
}

// TestActivationGetMapsExpiryTo410 verifies cs-control surfaces the persistence
// layer's CS_ACTIVATION_TTL_EXPIRED as HTTP 410.
func TestActivationGetMapsExpiryTo410(t *testing.T) {
	store := &testutil.FakePersistence{
		GetActivationFn: func(context.Context, string, string) (api.ActivationRecord, error) {
			return api.ActivationRecord{}, cserrors.New(cserrors.CSActivationTTLExpired, "activation expired")
		},
	}
	s := newControlServer(store, &testutil.FakeMessaging{})

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodGet, "/activation", "", map[string]string{
		"tenant": "t_abc123", "activation_id": "act-old",
	}, principalWith("t_abc123", "cs:activation:read"))
	s.getActivation(w, req)
	if w.Code != http.StatusGone {
		t.Fatalf("status=%d body=%s want=410", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSActivationTTLExpired {
		t.Fatalf("error code=%s want=%s", code, cserrors.CSActivationTTLExpired)
	}
}

func splitLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}
