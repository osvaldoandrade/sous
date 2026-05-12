package codeq

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

// SubscriptionConsumer is the contract a codeQ-backed transport must satisfy
// to drive an event subscription. The Kafka transport implements this directly
// through its ConsumeTopic method; tests inject in-memory fakes.
//
// The handler receives the raw envelope plus the JSON-decoded body. Returning
// an error stops the consumer; returning nil (after handler completion)
// is the signal to advance the offset for that message.
type SubscriptionConsumer interface {
	ConsumeTopic(ctx context.Context, topic, groupID string, handler func(Envelope) error) error
}

// Filter parses the minimal subscription expression grammar:
//
//	<path> == "<value>"
//	<path> != "<value>"
//
// where <path> is a dot-separated lookup into either the envelope (top-level
// keys "tenant" and "type") or the JSON body (any nested path).
//
// Empty filter matches everything. Returning a parse error surfaces an
// invalid expression at binding creation time, which is what the issue's
// "syntax errors rejected at create time" acceptance criteria requires.
//
// A richer expression language (AND/OR, comparison, JSONPath) is explicitly
// deferred per docs/07-codeq-protocol.md ("Subscription triggers — filter
// grammar").
type Filter struct {
	raw      string
	path     []string
	op       string
	value    string
	matchAll bool
}

var filterRE = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_.]*)\s*(==|!=)\s*"((?:[^"\\]|\\.)*)"\s*$`)

// ParseFilter compiles raw into a Filter. An empty raw means "match all".
func ParseFilter(raw string) (*Filter, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return &Filter{raw: "", matchAll: true}, nil
	}
	m := filterRE.FindStringSubmatch(trimmed)
	if m == nil {
		return nil, fmt.Errorf("invalid filter expression: %q (expected `field == \"value\"` or `field != \"value\"`)", raw)
	}
	pathParts := strings.Split(m[1], ".")
	// Unescape "\\\"" -> "\"" and "\\\\" -> "\\".
	v, err := strconv.Unquote(`"` + m[3] + `"`)
	if err != nil {
		v = m[3]
	}
	return &Filter{raw: trimmed, path: pathParts, op: m[2], value: v}, nil
}

// Matches evaluates the filter against the supplied envelope. Unknown fields
// resolve to the empty string, so `field == ""` and `field != "x"` both fire
// for missing values.
func (f *Filter) Matches(env Envelope) bool {
	if f == nil || f.matchAll {
		return true
	}
	got := lookup(env, f.path)
	switch f.op {
	case "==":
		return got == f.value
	case "!=":
		return got != f.value
	}
	return false
}

// String returns the canonical text form of the filter (trimmed source).
func (f *Filter) String() string {
	if f == nil {
		return ""
	}
	return f.raw
}

func lookup(env Envelope, path []string) string {
	if len(path) == 0 {
		return ""
	}
	switch path[0] {
	case "event":
		// "event.<key>" addresses top-level envelope fields. We surface the
		// commonly-needed fields (tenant, type, id, schema) without exposing
		// the entire envelope; this keeps the grammar narrow and predictable.
		if len(path) != 2 {
			return ""
		}
		return envelopeField(env, path[1])
	case "tenant":
		return env.Tenant
	case "type":
		return env.Type
	case "body":
		return jsonLookup(env.Body, path[1:])
	default:
		// Treat unprefixed paths as body fields so a CLI can write
		// `kind == "x"` without the verbose `body.kind`.
		return jsonLookup(env.Body, path)
	}
}

func envelopeField(env Envelope, key string) string {
	switch key {
	case "tenant":
		return env.Tenant
	case "type":
		return env.Type
	case "id":
		return env.ID
	case "schema":
		return env.Schema
	}
	return ""
}

func jsonLookup(raw json.RawMessage, path []string) string {
	if len(raw) == 0 || len(path) == 0 {
		return ""
	}
	var cur any
	if err := json.Unmarshal(raw, &cur); err != nil {
		return ""
	}
	for _, p := range path {
		obj, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur, ok = obj[p]
		if !ok {
			return ""
		}
	}
	switch v := cur.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case nil:
		return ""
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// SubscriptionHandler is invoked once per matching envelope. Implementations
// publish an InvocationRequest to the invoke pipeline. Returning an error
// halts the subscription's consumer loop (the caller is responsible for
// surfacing the failure or recreating the consumer).
type SubscriptionHandler func(ctx context.Context, env Envelope) error

// RunSubscription dispatches matching envelopes from a topic to handler
// according to mode. The function blocks until ctx is cancelled or the
// underlying consumer returns an error.
//
//   - mode "ordered" (or anything that is not "unordered") runs a single
//     goroutine that invokes handler synchronously, preserving per-partition
//     order. The consumer only advances after handler returns, so a slow
//     handler applies back-pressure.
//   - mode "unordered" launches a worker pool of size max(maxConcurrency, 1).
//     The dispatcher fans matched envelopes out via a buffered channel; order
//     is not preserved across workers but per-partition order is preserved if
//     the codeQ broker delivers a single partition per consumer goroutine
//     (the v0.1 in-memory and Kafka backends both satisfy that).
//
// Non-matching envelopes are silently skipped (consumed but not dispatched),
// matching the codeQ at-least-once + dedup contract: the offset still
// advances so the broker stops redelivering.
func RunSubscription(
	ctx context.Context,
	consumer SubscriptionConsumer,
	topic, groupID, mode string,
	maxConcurrency int,
	filter *Filter,
	handler SubscriptionHandler,
) error {
	if consumer == nil {
		return fmt.Errorf("codeq: subscription consumer is nil")
	}
	if handler == nil {
		return fmt.Errorf("codeq: subscription handler is nil")
	}
	if mode != "unordered" {
		// Ordered path: handler runs inline so the consumer's commit only
		// fires after the invocation request has been published.
		return consumer.ConsumeTopic(ctx, topic, groupID, func(env Envelope) error {
			if !filter.Matches(env) {
				return nil
			}
			return handler(ctx, env)
		})
	}
	// Unordered path.
	if maxConcurrency < 1 {
		maxConcurrency = 1
	}
	type task struct{ env Envelope }
	q := make(chan task, maxConcurrency)
	wg := &sync.WaitGroup{}
	errCh := make(chan error, maxConcurrency)
	for i := 0; i < maxConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range q {
				if err := handler(ctx, t.env); err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
			}
		}()
	}
	consumeErr := consumer.ConsumeTopic(ctx, topic, groupID, func(env Envelope) error {
		if !filter.Matches(env) {
			return nil
		}
		select {
		case q <- task{env: env}:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		}
	})
	close(q)
	wg.Wait()
	if consumeErr != nil {
		return consumeErr
	}
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
