package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// fakeLogServer returns an httptest.Server that serves activation-logs
// requests by handing out successive pages of fixture chunks. Pages are
// returned in order; once exhausted, subsequent requests return an empty
// chunk list (simulating a live tail with no new data).
type fakeLogServer struct {
	t        *testing.T
	pages    [][]string
	calls    int32
	mu       sync.Mutex
	gotPaths []string
}

func newFakeLogServer(t *testing.T, pages [][]string) (*fakeLogServer, *httptest.Server) {
	t.Helper()
	fs := &fakeLogServer{t: t, pages: pages}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fs.mu.Lock()
		fs.gotPaths = append(fs.gotPaths, r.URL.Path)
		fs.mu.Unlock()
		switch {
		case strings.HasSuffix(r.URL.Path, "/logs"):
			idx := int(atomic.AddInt32(&fs.calls, 1)) - 1
			var chunks []string
			if idx < len(fs.pages) {
				chunks = fs.pages[idx]
			}
			var cur int64
			if c := r.URL.Query().Get("cursor"); c != "" {
				_, _ = fmt.Sscanf(c, "%d", &cur)
			}
			next := cur + int64(len(chunks))
			payload := map[string]any{
				"chunks": chunks,
				"cursor": fmt.Sprintf("%d", next),
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(payload)
		default:
			// activation lookup endpoint
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"activation_id":"act_1","tenant":"t_test","status":"success","start_ms":1}`))
		}
	}))
	return fs, srv
}

// captureSignals replaces logsSignalNotify with a no-op that records the
// registered channel into out. Tests can then send synthetic signals onto
// the captured channel to drive cancellation.
func captureSignals(out chan<- chan<- os.Signal) func() {
	orig := logsSignalNotify
	logsSignalNotify = func(c chan<- os.Signal, sigs ...os.Signal) {
		select {
		case out <- c:
		default:
		}
	}
	return func() { logsSignalNotify = orig }
}

func TestFnLogs_OneShotPrintsAllChunks(t *testing.T) {
	setupConfigHome(t)
	_, srv := newFakeLogServer(t, [][]string{{"[info] hello", "[error] boom"}})
	defer srv.Close()
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t_test", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var buf bytes.Buffer
	origOut, origNow := logsStdout, logsNow
	defer func() { logsStdout, logsNow = origOut, origNow }()
	logsStdout = &buf
	logsNow = func() time.Time { return time.UnixMilli(1700000000000).UTC() }

	if err := fnLogs([]string{
		"--activation", "act_1",
		"--format", "compact",
	}); err != nil {
		t.Fatalf("fnLogs: %v", err)
	}
	got := buf.String()
	for _, want := range []string{"[info] hello", "[error] boom"} {
		if !strings.Contains(got, want) {
			t.Fatalf("output missing %q; got:\n%s", want, got)
		}
	}
	if strings.Index(got, "hello") > strings.Index(got, "boom") {
		t.Fatalf("output ordering wrong:\n%s", got)
	}
}

func TestFnLogs_JSONFormatEmitsNDJSON(t *testing.T) {
	setupConfigHome(t)
	_, srv := newFakeLogServer(t, [][]string{{"[warn] careful", "no prefix line"}})
	defer srv.Close()
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t_test", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var buf bytes.Buffer
	origOut := logsStdout
	defer func() { logsStdout = origOut }()
	logsStdout = &buf

	if err := fnLogs([]string{
		"--activation", "act_1",
		"--format", "json",
	}); err != nil {
		t.Fatalf("fnLogs: %v", err)
	}
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 NDJSON lines, got %d:\n%s", len(lines), buf.String())
	}
	var first logsChunk
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("decode first line: %v\nline: %s", err, lines[0])
	}
	if first.Level != "warn" || first.Message != "careful" || first.ActivationID != "act_1" {
		t.Fatalf("first decoded mismatch: %+v", first)
	}
	if first.Sequence != 0 {
		t.Fatalf("first sequence = %d, want 0", first.Sequence)
	}
	var second logsChunk
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatalf("decode second line: %v\nline: %s", err, lines[1])
	}
	if second.Level != "info" || second.Message != "no prefix line" {
		t.Fatalf("second decoded mismatch: %+v", second)
	}
	if second.Sequence != 1 {
		t.Fatalf("second sequence = %d, want 1", second.Sequence)
	}
}

func TestFnLogs_PrettyFormatNoColourWhenNotTTY(t *testing.T) {
	setupConfigHome(t)
	_, srv := newFakeLogServer(t, [][]string{{"[info] greetings"}})
	defer srv.Close()
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t_test", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var buf bytes.Buffer
	origOut, origTTY := logsStdout, logsIsTTY
	defer func() { logsStdout, logsIsTTY = origOut, origTTY }()
	logsStdout = &buf
	logsIsTTY = func() bool { return false }

	if err := fnLogs([]string{"--activation", "act_1"}); err != nil {
		t.Fatalf("fnLogs: %v", err)
	}
	out := buf.String()
	if strings.Contains(out, "\x1b[") {
		t.Fatalf("pretty output should not contain ANSI escapes when not a TTY: %q", out)
	}
	if !strings.Contains(out, "INFO") || !strings.Contains(out, "greetings") {
		t.Fatalf("pretty output missing fields: %q", out)
	}
}

func TestFnLogs_PrettyFormatANSIWhenTTY(t *testing.T) {
	setupConfigHome(t)
	_, srv := newFakeLogServer(t, [][]string{{"[error] kaboom"}})
	defer srv.Close()
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t_test", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var buf bytes.Buffer
	origOut, origTTY := logsStdout, logsIsTTY
	defer func() { logsStdout, logsIsTTY = origOut, origTTY }()
	logsStdout = &buf
	logsIsTTY = func() bool { return true }

	if err := fnLogs([]string{"--activation", "act_1"}); err != nil {
		t.Fatalf("fnLogs: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, ansiRed) || !strings.Contains(out, ansiReset) {
		t.Fatalf("expected ANSI colour codes for error level, got: %q", out)
	}
}

func TestFnLogs_FollowExitsCleanlyOnSIGINT(t *testing.T) {
	setupConfigHome(t)
	_, srv := newFakeLogServer(t, [][]string{{"[info] first"}})
	defer srv.Close()
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t_test", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var buf bytes.Buffer
	origOut := logsStdout
	defer func() { logsStdout = origOut }()
	logsStdout = &buf

	captured := make(chan chan<- os.Signal, 1)
	restore := captureSignals(captured)
	defer restore()

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- fnLogs([]string{
			"--activation", "act_1",
			"--follow",
			"--poll-interval", "30ms",
			"--format", "compact",
		})
	}()

	var sigCh chan<- os.Signal
	select {
	case sigCh = <-captured:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for signal-notify registration")
	}
	// Give the poller time to make at least one request.
	time.Sleep(150 * time.Millisecond)
	sigCh <- syscall.SIGINT

	select {
	case err := <-doneCh:
		if err != nil {
			t.Fatalf("--follow returned error on SIGINT: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("fnLogs --follow did not exit within 3s of SIGINT")
	}
	if !strings.Contains(buf.String(), "first") {
		t.Fatalf("expected first chunk in output, got:\n%s", buf.String())
	}
}

func TestFnLogs_FollowDrainsMultiplePages(t *testing.T) {
	setupConfigHome(t)
	fakeSrv, srv := newFakeLogServer(t, [][]string{{"[info] a"}, {"[info] b"}, {"[info] c"}})
	defer srv.Close()
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t_test", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var buf bytes.Buffer
	origOut := logsStdout
	defer func() { logsStdout = origOut }()
	logsStdout = &buf

	captured := make(chan chan<- os.Signal, 1)
	restore := captureSignals(captured)
	defer restore()

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- fnLogs([]string{
			"--activation", "act_1",
			"--follow",
			"--poll-interval", "20ms",
			"--format", "compact",
		})
	}()
	var sigCh chan<- os.Signal
	select {
	case sigCh = <-captured:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for signal-notify registration")
	}
	time.Sleep(250 * time.Millisecond)
	sigCh <- syscall.SIGINT
	select {
	case err := <-doneCh:
		if err != nil {
			t.Fatalf("follow error: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fnLogs --follow did not exit")
	}

	for _, want := range []string{"a", "b", "c"} {
		if !strings.Contains(buf.String(), want) {
			t.Fatalf("expected %q in output:\n%s", want, buf.String())
		}
	}
	if got := atomic.LoadInt32(&fakeSrv.calls); got < 3 {
		t.Fatalf("expected at least 3 polls, got %d", got)
	}
}

func TestFnLogs_SinceFiltersOlderActivations(t *testing.T) {
	setupConfigHome(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/logs") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"chunks":["[info] stale"],"cursor":"1"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"activation_id":"act_1","start_ms":1}`))
	}))
	defer srv.Close()
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t_test", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var buf bytes.Buffer
	origOut, origNow := logsStdout, logsNow
	defer func() { logsStdout, logsNow = origOut, origNow }()
	logsStdout = &buf
	logsNow = func() time.Time { return time.UnixMilli(10_000_000) }

	if err := fnLogs([]string{
		"--activation", "act_1",
		"--since", "5m",
		"--format", "compact",
	}); err != nil {
		t.Fatalf("fnLogs: %v", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("expected no output (activation outside --since window), got:\n%s", buf.String())
	}
}

func TestFnLogs_SinceAllowsRecentActivations(t *testing.T) {
	setupConfigHome(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/logs") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"chunks":["[info] recent"],"cursor":"1"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// start_ms ~ now in our clock
		_, _ = w.Write([]byte(`{"activation_id":"act_1","start_ms":9999000}`))
	}))
	defer srv.Close()
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t_test", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}
	var buf bytes.Buffer
	origOut, origNow := logsStdout, logsNow
	defer func() { logsStdout, logsNow = origOut, origNow }()
	logsStdout = &buf
	logsNow = func() time.Time { return time.UnixMilli(10_000_000) }
	if err := fnLogs([]string{
		"--activation", "act_1",
		"--since", "1m",
		"--format", "compact",
	}); err != nil {
		t.Fatalf("fnLogs: %v", err)
	}
	if !strings.Contains(buf.String(), "recent") {
		t.Fatalf("expected recent chunk in output, got:\n%s", buf.String())
	}
}

func TestFnLogs_InvalidFlags(t *testing.T) {
	setupConfigHome(t)
	if err := saveAuthConfig(authConfig{APIURL: "http://localhost", Tenant: "t_test", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}
	cases := []struct {
		name string
		args []string
	}{
		{"missing function and activation", []string{"--format", "compact"}},
		{"invalid format", []string{"--activation", "a", "--format", "yaml"}},
		{"invalid since", []string{"--activation", "a", "--since", "not-a-duration"}},
		{"non-positive poll-interval", []string{"--activation", "a", "--poll-interval", "0s"}},
		{"invalid limit", []string{"--activation", "a", "--limit", "0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := fnLogs(tc.args); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestFnLogs_ServerErrorSurfaced(t *testing.T) {
	setupConfigHome(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"code":"CS_AUTHZ_DENIED","message":"role missing"}}`, http.StatusForbidden)
	}))
	defer srv.Close()
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t_test", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}
	err := fnLogs([]string{"--activation", "act_1"})
	if err == nil {
		t.Fatal("expected error on 403 response")
	}
	if !strings.Contains(err.Error(), "server error") {
		t.Fatalf("expected wrapped server error, got %v", err)
	}
}

func TestFnLogs_FunctionOnlyWithoutActivationFails(t *testing.T) {
	setupConfigHome(t)
	if err := saveAuthConfig(authConfig{APIURL: "http://localhost", Tenant: "t_test", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}
	err := fnLogs([]string{"--function", "fn@prod"})
	if err == nil {
		t.Fatal("expected helpful error when --function lacks --activation")
	}
	if !strings.Contains(err.Error(), "next step:") {
		t.Fatalf("expected 'next step:' hint in error, got: %v", err)
	}
}

func TestStreamLogs_ContextCancelExitsClean(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"chunks":[],"cursor":"0"}`))
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := streamLogs(ctx, authConfig{APIURL: srv.URL, Tenant: "t_test", Token: "tok"}, logsOptions{
		activation:   "act_1",
		follow:       true,
		format:       "compact",
		pollInterval: 10 * time.Millisecond,
		limit:        10,
	}, io.Discard)
	if err != nil {
		t.Fatalf("streamLogs after cancelled context should be nil, got %v", err)
	}
}

func TestParseLogLine(t *testing.T) {
	cases := []struct {
		raw     string
		wantLvl string
		wantMsg string
	}{
		{"[info] hello", "info", "hello"},
		{"[ERROR] boom", "error", "boom"},
		{"no prefix", "info", "no prefix"},
		{"[malformed", "info", "[malformed"},
		{"[]", "info", "[]"},
		{"[debug]   spacey", "debug", "  spacey"},
	}
	for _, tc := range cases {
		got := parseLogLine(tc.raw, "act", 0)
		if got.Level != tc.wantLvl || got.Message != tc.wantMsg {
			t.Fatalf("parseLogLine(%q) = {%q, %q}; want {%q, %q}",
				tc.raw, got.Level, got.Message, tc.wantLvl, tc.wantMsg)
		}
	}
}

func TestNextBackoff(t *testing.T) {
	const cap = 2 * time.Second
	cur := 250 * time.Millisecond
	for i := 0; i < 5; i++ {
		next := nextBackoff(cur, cap)
		if next < cur || next > cap {
			t.Fatalf("nextBackoff broke monotonicity at step %d: cur=%s next=%s", i, cur, next)
		}
		cur = next
	}
	if cur != cap {
		t.Fatalf("backoff did not reach cap; final=%s", cur)
	}
}

func TestLevelColour(t *testing.T) {
	cases := map[string]string{
		"error": ansiRed,
		"err":   ansiRed,
		"fatal": ansiRed,
		"warn":  ansiYellow,
		"info":  ansiCyan,
		"debug": ansiGrey,
		"other": ansiCyan,
	}
	for level, want := range cases {
		if got := levelColour(level); got != want {
			t.Fatalf("levelColour(%q) = %q, want %q", level, got, want)
		}
	}
}
