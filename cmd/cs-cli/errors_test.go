package main

import (
	"errors"
	"strings"
	"testing"
)

func TestCLIErrorFormatsCauseAndHint(t *testing.T) {
	underlying := errors.New("dial tcp 127.0.0.1:65535: connect: connection refused")
	e := newServerError(
		"control plane unreachable at http://localhost:65535",
		"start `./bin/cs-control` or pass `--api-url`",
		underlying,
	)
	msg := e.Error()
	for _, want := range []string{
		"control plane unreachable at http://localhost:65535",
		"cause: dial tcp 127.0.0.1:65535: connect: connection refused",
		"next step: start `./bin/cs-control` or pass `--api-url`",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error message missing %q\ngot:\n%s", want, msg)
		}
	}
}

func TestCLIErrorOmitsRedundantCauseLine(t *testing.T) {
	// When the wrapped error message equals the cause, we shouldn't print it
	// twice.
	underlying := errors.New("missing tenant")
	e := newClientError("missing tenant", "run `cs auth login`", underlying)
	msg := e.Error()
	if strings.Count(msg, "missing tenant") != 1 {
		t.Fatalf("expected cause to appear once, got:\n%s", msg)
	}
	if !strings.Contains(msg, "next step: run `cs auth login`") {
		t.Fatalf("expected next-step line, got:\n%s", msg)
	}
}

func TestExitCodeFor(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"nil error", nil, 0},
		{"client cliError", newClientError("missing flag", "pass --foo", nil), 1},
		{"server cliError", newServerError("server down", "start it", nil), 2},
		{"runtime cliError", newRuntimeError("runtime exception", "rebuild", nil), 3},
		{"usage cliError", usageError("usage: cs foo"), 1},
		{"legacy runtime substring", errors.New("runtime error: oops"), 3},
		{"legacy server substring", errors.New("server error (500)"), 2},
		{"legacy fallback", errors.New("something else"), 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := exitCodeFor(tt.err); got != tt.want {
				t.Fatalf("exitCodeFor(%v) = %d, want %d", tt.err, got, tt.want)
			}
		})
	}
}

func TestCLIErrorUnwrap(t *testing.T) {
	sentinel := errors.New("inner")
	e := newClientError("outer", "next", sentinel)
	if !errors.Is(e, sentinel) {
		t.Fatalf("errors.Is should walk to sentinel; got %v", e)
	}
}

func TestUsageErrorIsClient(t *testing.T) {
	e := usageError("usage: cs foo")
	var cli *cliError
	if !errors.As(e, &cli) {
		t.Fatalf("usageError should produce *cliError")
	}
	if cli.Kind != kindClient {
		t.Fatalf("usageError kind = %v, want kindClient", cli.Kind)
	}
	if cli.ExitCode() != 1 {
		t.Fatalf("usageError exit code = %d, want 1", cli.ExitCode())
	}
}

func TestNilCLIErrorMethods(t *testing.T) {
	var nilErr *cliError
	if nilErr.Error() != "" {
		t.Fatalf("nil cliError.Error should return empty string")
	}
	if nilErr.Unwrap() != nil {
		t.Fatalf("nil cliError.Unwrap should return nil")
	}
	if nilErr.ExitCode() != 0 {
		t.Fatalf("nil cliError.ExitCode should return 0")
	}
}
