package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
)

// Overridable hooks for tests. These default to the production wiring but
// can be swapped per-test to drive deterministic behaviour without touching
// the real OS signal or HTTP machinery.
var (
	logsNow                    = time.Now
	logsSignalNotify           = func(c chan<- os.Signal, sigs ...os.Signal) { signal.Notify(c, sigs...) }
	logsStdout       io.Writer = os.Stdout
	logsIsTTY                  = defaultIsTTY
)

func defaultIsTTY() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

// logsOptions captures the parsed flags for `cs fn logs`.
type logsOptions struct {
	function     string
	activation   string
	namespace    string
	follow       bool
	since        time.Duration
	format       string
	pollInterval time.Duration
	limit        int64
	apiURL       string
}

// logsChunk is one log line returned by the control plane.
type logsChunk struct {
	Sequence     int64  `json:"sequence"`
	ActivationID string `json:"activation_id"`
	Level        string `json:"level"`
	Message      string `json:"message"`
	Raw          string `json:"raw"`
}

// fnLogs is the entry-point for `cs fn logs`. It parses arguments, loads the
// activation pointed at by --activation (or --function if also given as a
// namespacing hint), and prints log chunks until exit. With --follow it polls
// the control plane until the activation is terminal or the user issues
// SIGINT.
func fnLogs(args []string) error {
	opts, err := parseLogsArgs(args)
	if err != nil {
		return err
	}
	cfg, err := loadAuthConfig()
	if err != nil {
		return err
	}
	if opts.apiURL != "" {
		cfg.APIURL = opts.apiURL
	}
	if opts.activation == "" {
		return errors.New("--activation is required (control plane does not yet expose per-function activation listing)\nnext step: run `cs fn invoke` first and pass the returned activation_id via --activation")
	}

	ctx, cancel := newLogsContext()
	defer cancel()

	return streamLogs(ctx, cfg, opts, logsStdout)
}

// newLogsContext wires SIGINT/SIGTERM to a cancel func.
func newLogsContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	logsSignalNotify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-ch:
			cancel()
		case <-ctx.Done():
		}
		signal.Stop(ch)
	}()
	return ctx, cancel
}

// parseLogsArgs parses the flag set for `cs fn logs`.
func parseLogsArgs(args []string) (logsOptions, error) {
	fs := flag.NewFlagSet("fn logs", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	var (
		function     string
		activation   string
		namespace    string
		follow       bool
		sinceRaw     string
		format       string
		pollInterval time.Duration
		limit        int64
		apiURL       string
	)
	fs.StringVar(&function, "function", "", "Function reference (name or name@alias)")
	fs.StringVar(&activation, "activation", "", "Activation ID")
	fs.StringVar(&namespace, "namespace", "default", "Namespace")
	fs.BoolVar(&follow, "follow", false, "Tail new log chunks as they arrive")
	fs.StringVar(&sinceRaw, "since", "", "Only show logs from activations newer than the given duration (e.g. 5m)")
	fs.StringVar(&format, "format", "pretty", "Output format: pretty|json|compact")
	fs.DurationVar(&pollInterval, "poll-interval", time.Second, "Polling interval for --follow (default 1s)")
	fs.Int64Var(&limit, "limit", 100, "Maximum chunks per poll (1-500)")
	fs.StringVar(&apiURL, "api-url", "", "Override control-plane URL (defaults to auth config)")

	if err := fs.Parse(reorderFlags(fs, args)); err != nil {
		return logsOptions{}, err
	}

	switch format {
	case "pretty", "json", "compact":
	default:
		return logsOptions{}, fmt.Errorf("invalid --format %q (must be pretty|json|compact)", format)
	}
	if pollInterval <= 0 {
		return logsOptions{}, errors.New("--poll-interval must be positive")
	}
	if limit <= 0 || limit > 500 {
		return logsOptions{}, errors.New("--limit must be between 1 and 500")
	}

	var since time.Duration
	if sinceRaw != "" {
		dur, err := time.ParseDuration(sinceRaw)
		if err != nil {
			return logsOptions{}, fmt.Errorf("invalid --since %q: %w\nnext step: pass a Go duration such as 30s, 5m, or 1h", sinceRaw, err)
		}
		if dur < 0 {
			return logsOptions{}, errors.New("--since must be non-negative")
		}
		since = dur
	}

	if function == "" && activation == "" {
		return logsOptions{}, errors.New("one of --function or --activation is required\nnext step: pass --activation <id> to tail a specific activation")
	}

	return logsOptions{
		function:     function,
		activation:   activation,
		namespace:    namespace,
		follow:       follow,
		since:        since,
		format:       format,
		pollInterval: pollInterval,
		limit:        limit,
		apiURL:       apiURL,
	}, nil
}

// streamLogs drives the cursor-based fetch loop, writing rendered chunks to
// out as they arrive. It returns nil on clean exit (including context
// cancellation in --follow mode) and a non-nil error only on hard failures.
func streamLogs(ctx context.Context, cfg authConfig, opts logsOptions, out io.Writer) error {
	if opts.since > 0 {
		rec, err := fetchActivation(ctx, cfg, opts.activation)
		if err != nil {
			return err
		}
		threshold := logsNow().Add(-opts.since).UnixMilli()
		if rec.StartMS > 0 && rec.StartMS < threshold {
			return nil
		}
	}

	render := newLogsRenderer(opts.format, logsIsTTY())

	const maxBackoff = 2 * time.Second
	var cursor int64
	backoff := opts.pollInterval

	for {
		if ctx.Err() != nil {
			return nil
		}
		chunks, next, err := fetchLogChunks(ctx, cfg, opts.activation, cursor, opts.limit)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		for i, raw := range chunks {
			ch := parseLogLine(raw, opts.activation, cursor+int64(i))
			if _, err := fmt.Fprintln(out, render(ch)); err != nil {
				return err
			}
		}
		cursor = next

		if !opts.follow {
			return nil
		}
		if len(chunks) > 0 {
			backoff = opts.pollInterval
		} else {
			backoff = nextBackoff(backoff, maxBackoff)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
	}
}

func nextBackoff(current, max time.Duration) time.Duration {
	if current*2 > max {
		return max
	}
	return current * 2
}

// getAuthed performs an authenticated GET against the control plane and
// decodes the JSON body into v. It is shared by fetchActivation and
// fetchLogChunks to avoid duplicating header wiring and error mapping.
func getAuthed(ctx context.Context, cfg authConfig, endpoint string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("server error (%d): %s", resp.StatusCode, string(body))
	}
	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// fetchActivation reads the activation record so --since can be evaluated
// against the server-side StartMS rather than the chunk index (which has no
// timestamp).
func fetchActivation(ctx context.Context, cfg authConfig, activationID string) (api.ActivationRecord, error) {
	var rec api.ActivationRecord
	endpoint := fmt.Sprintf("%s/v1/tenants/%s/activations/%s",
		strings.TrimRight(cfg.APIURL, "/"),
		url.PathEscape(cfg.Tenant),
		url.PathEscape(activationID),
	)
	return rec, getAuthed(ctx, cfg, endpoint, &rec)
}

// fetchLogChunks performs a single GET against the activation-logs endpoint.
func fetchLogChunks(ctx context.Context, cfg authConfig, activationID string, cursor, limit int64) ([]string, int64, error) {
	endpoint := fmt.Sprintf("%s/v1/tenants/%s/activations/%s/logs?cursor=%d&limit=%d",
		strings.TrimRight(cfg.APIURL, "/"),
		url.PathEscape(cfg.Tenant),
		url.PathEscape(activationID),
		cursor,
		limit,
	)
	var payload struct {
		Chunks []string `json:"chunks"`
		Cursor string   `json:"cursor"`
	}
	if err := getAuthed(ctx, cfg, endpoint, &payload); err != nil {
		return nil, cursor, err
	}
	next := cursor + int64(len(payload.Chunks))
	if v, err := strconv.ParseInt(payload.Cursor, 10, 64); err == nil && v > next {
		next = v
	}
	return payload.Chunks, next, nil
}

// parseLogLine splits a chunk like "[info] message text" into structured
// fields. Lines that do not match the prefix shape are returned with
// level="info" and the full text as message.
func parseLogLine(raw, activationID string, sequence int64) logsChunk {
	ch := logsChunk{Sequence: sequence, ActivationID: activationID, Raw: raw, Level: "info", Message: raw}
	if !strings.HasPrefix(raw, "[") {
		return ch
	}
	end := strings.Index(raw, "]")
	if end <= 1 {
		return ch
	}
	level := strings.TrimSpace(raw[1:end])
	rest := strings.TrimPrefix(raw[end+1:], " ")
	if level == "" {
		return ch
	}
	ch.Level = strings.ToLower(level)
	ch.Message = rest
	return ch
}

// newLogsRenderer returns a function that converts a logsChunk into a printable
// string in the requested format.
func newLogsRenderer(format string, colour bool) func(logsChunk) string {
	switch format {
	case "json":
		return func(ch logsChunk) string {
			raw, _ := json.Marshal(ch)
			return string(raw)
		}
	case "compact":
		return func(ch logsChunk) string {
			return fmt.Sprintf("[%s] %s", ch.Level, ch.Message)
		}
	default: // pretty
		return func(ch logsChunk) string {
			ts := logsNow().UTC().Format(time.RFC3339Nano)
			level := strings.ToUpper(ch.Level)
			if colour {
				return fmt.Sprintf("%s %s%-5s%s %s %s", ts, levelColour(ch.Level), level, ansiReset, shortID(ch.ActivationID), ch.Message)
			}
			return fmt.Sprintf("%s %-5s %s %s", ts, level, shortID(ch.ActivationID), ch.Message)
		}
	}
}

const (
	ansiReset  = "\x1b[0m"
	ansiRed    = "\x1b[31m"
	ansiYellow = "\x1b[33m"
	ansiCyan   = "\x1b[36m"
	ansiGrey   = "\x1b[90m"
)

// shortID truncates a UUID-style activation id to its first 8 characters
// for compact display while keeping output recognisable.
func shortID(id string) string {
	return safePrefix(id, 8)
}

func levelColour(level string) string {
	switch strings.ToLower(level) {
	case "error", "err", "fatal":
		return ansiRed
	case "warn", "warning":
		return ansiYellow
	case "debug", "trace":
		return ansiGrey
	default:
		return ansiCyan
	}
}
