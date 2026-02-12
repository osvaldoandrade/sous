package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRequestIDHelpersAndMiddleware(t *testing.T) {
	ctx := WithRequestID(context.Background(), "req_1")
	if got := RequestIDFromContext(ctx); got != "req_1" {
		t.Fatalf("unexpected request id: %s", got)
	}

	h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if RequestIDFromContext(r.Context()) == "" {
			t.Fatal("request id missing in context")
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", w.Code)
	}
	if w.Header().Get("X-Request-Id") == "" {
		t.Fatal("expected response request id")
	}
}

func TestLoggerWritesJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter("svc", &buf)
	ctx := WithRequestID(context.Background(), "req_1")
	logger.Info(ctx, "hello")
	if buf.Len() == 0 {
		t.Fatal("expected log output")
	}
	var entry Entry
	line := bytes.TrimSpace(buf.Bytes())
	if err := json.Unmarshal(line, &entry); err != nil {
		t.Fatalf("invalid json log: %v", err)
	}
	if entry.Service != "svc" || entry.RequestID != "req_1" || entry.Level != "info" {
		t.Fatalf("unexpected log entry: %+v", entry)
	}
}

func TestMetricsHandler(t *testing.T) {
	h := MetricsHandler(prometheus.NewRegistry())
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status: %d", w.Code)
	}
}

func TestLoggerWarnErrorAndMetricsNilRegistry(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLoggerWithWriter("svc", &buf)
	logger.Warn(context.Background(), "warn")
	logger.Error(context.Background(), "err")
	if buf.Len() == 0 {
		t.Fatal("expected warn/error log output")
	}

	// NewLogger should build a usable logger with stdout backend.
	stdoutLogger := NewLogger("svc-stdout")
	stdoutLogger.Info(context.Background(), "ok")

	// Nil registry branch.
	h := MetricsHandler(nil)
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status for nil-registry metrics handler: %d", w.Code)
	}
}
