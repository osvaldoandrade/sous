package main

import (
	"errors"
	"fmt"
	"strings"
)

// cliErrorKind classifies CLI errors for exit-code mapping. It lets callers
// pick the exit-code bucket explicitly instead of relying on the legacy
// `strings.Contains` substring match on the error message.
type cliErrorKind int

const (
	// kindClient indicates a usage or configuration problem on the local
	// side (missing flags, missing config, parse errors). Exits with 1.
	kindClient cliErrorKind = iota + 1
	// kindServer indicates a remote control-plane failure (5xx, network
	// unreachable, transport error). Exits with 2.
	kindServer
	// kindRuntime indicates a local `cs-js` runtime failure. Exits with 3.
	kindRuntime
)

// cliError is the canonical error type surfaced to users from the CLI. It
// carries the original cause, a short cause description, and an actionable
// next step suggesting how to recover.
type cliError struct {
	Kind  cliErrorKind
	Cause string
	Hint  string
	Err   error
}

// Error renders the wrapped error in three lines:
//
//	<cause>
//	cause: <original error>
//	next step: <hint>
//
// The first line is the human-readable summary; subsequent lines stay
// machine-parseable so agents can grep them.
func (e *cliError) Error() string {
	if e == nil {
		return ""
	}
	var b strings.Builder
	if e.Cause != "" {
		b.WriteString(e.Cause)
	}
	if e.Err != nil && e.Err.Error() != e.Cause {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "cause: %s", e.Err.Error())
	}
	if e.Hint != "" {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		fmt.Fprintf(&b, "next step: %s", e.Hint)
	}
	return b.String()
}

// Unwrap exposes the underlying error so `errors.Is` / `errors.As` works.
func (e *cliError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ExitCode returns the process exit code associated with the error's kind.
func (e *cliError) ExitCode() int {
	if e == nil {
		return 0
	}
	switch e.Kind {
	case kindServer:
		return 2
	case kindRuntime:
		return 3
	default:
		return 1
	}
}

// newClientError wraps a user/config error (exit 1).
func newClientError(cause, hint string, err error) *cliError {
	return &cliError{Kind: kindClient, Cause: cause, Hint: hint, Err: err}
}

// newServerError wraps a control-plane error (exit 2).
func newServerError(cause, hint string, err error) *cliError {
	return &cliError{Kind: kindServer, Cause: cause, Hint: hint, Err: err}
}

// newRuntimeError wraps a local runtime error (exit 3).
func newRuntimeError(cause, hint string, err error) *cliError {
	return &cliError{Kind: kindRuntime, Cause: cause, Hint: hint, Err: err}
}

// usageError is shorthand for an exit-1 error with no underlying cause; the
// `cause` text doubles as the message printed to the user.
func usageError(msg string) *cliError {
	return &cliError{Kind: kindClient, Cause: msg}
}

// exitCodeFor returns the exit code for an error. cliError values drive their
// own code; otherwise we fall back to the legacy substring heuristic used by
// pre-existing callers so adopting the helper is incremental and safe.
func exitCodeFor(err error) int {
	if err == nil {
		return 0
	}
	var cli *cliError
	if errors.As(err, &cli) {
		return cli.ExitCode()
	}
	msg := err.Error()
	if strings.Contains(msg, "runtime") {
		return 3
	}
	if strings.Contains(msg, "server") {
		return 2
	}
	return 1
}
