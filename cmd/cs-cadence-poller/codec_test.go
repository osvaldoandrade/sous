package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/cadence"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// runActivityWithCodec drives a single Cadence activity end-to-end through
// the poller's pollLoop + consumeResults pair using the supplied binding
// (which pins the input/output codec). It returns the base64-encoded
// payload the fake cadence client received on RespondActivityTaskCompleted
// along with the InvocationRequest published to the broker — both are
// what every codec assertion in this file actually checks.
func runActivityWithCodec(t *testing.T, binding api.WorkerBinding, fnResp *api.FunctionResponse, inputBase64 string) (completedPayload []byte, published api.InvocationRequest) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	invocationCh := make(chan api.InvocationRequest, 1)
	capturedCh := make(chan api.InvocationRequest, 1)
	publishedCh := make(chan struct{}, 1)
	completedCh := make(chan []byte, 1)
	var taskReturned atomic.Bool

	store := &testutil.FakePersistence{
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
			return 1, nil
		},
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				Config: api.VersionConfig{
					TimeoutMS: 500,
					Authz:     api.VersionAuthz{InvokeCadenceRoles: []string{"role:cadence"}},
				},
			}, nil, nil
		},
		PutCadenceTaskMappingFn:    func(context.Context, string, string, string, string, time.Duration) error { return nil },
		DeleteCadenceTaskMappingFn: func(context.Context, string, string, string) error { return nil },
	}
	broker := &testutil.FakeMessaging{
		PublishInvocationFn: func(_ context.Context, req api.InvocationRequest) error {
			invocationCh <- req
			select {
			case capturedCh <- req:
			default:
			}
			select {
			case publishedCh <- struct{}{}:
			default:
			}
			return nil
		},
		ConsumeResultsFn: func(_ context.Context, _ string, handler func(messaging.Envelope, api.InvocationResult) error) error {
			req := <-invocationCh
			return handler(messaging.Envelope{}, api.InvocationResult{
				ActivationID: req.ActivationID,
				RequestID:    req.RequestID,
				Status:       "success",
				Result:       fnResp,
			})
		},
	}
	client := &fakeCadenceClient{
		pollFn: func(ctx context.Context, _, _, _ string) (*cadence.ActivityTask, error) {
			if taskReturned.Load() {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			taskReturned.Store(true)
			return &cadence.ActivityTask{
				TaskToken:    "tok-codec",
				Domain:       binding.Domain,
				Tasklist:     binding.Tasklist,
				ActivityType: "ActivityA",
				InputBase64:  inputBase64,
			}, nil
		},
		completedFn: func(_ context.Context, _ string, payload []byte) error {
			completedCh <- payload
			cancel()
			return nil
		},
	}

	p := newTestPoller(store, broker, client)
	sem := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.pollLoop(ctx, binding, sem)
	}()

	select {
	case <-publishedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("publish invocation was not called")
	}

	p.consumeResults(context.Background())
	<-done

	select {
	case completedPayload = <-completedCh:
	case <-time.After(time.Second):
		t.Fatal("RespondActivityTaskCompleted was not called")
	}
	select {
	case published = <-capturedCh:
	default:
	}
	return completedPayload, published
}

func bindingForCodec(name, inputCodec, outputCodec string) api.WorkerBinding {
	return api.WorkerBinding{
		Tenant:    "t1",
		Namespace: "n1",
		Name:      name,
		Domain:    "d-codec",
		Tasklist:  "tl-codec",
		WorkerID:  "w-codec",
		ActivityMap: map[string]api.WorkerBindingRef{
			"ActivityA": {Function: "fn1", Alias: "prod"},
		},
		InputCodec:  inputCodec,
		OutputCodec: outputCodec,
	}
}

func TestCadencePollerJSONCodec(t *testing.T) {
	resp := &api.FunctionResponse{StatusCode: 200, Body: "ok-json"}
	payload, _ := runActivityWithCodec(t, bindingForCodec("w-json", "", ""), resp, "aA==")

	raw, err := base64.StdEncoding.DecodeString(string(payload))
	if err != nil {
		t.Fatalf("expected base64 payload, got %q: %v", payload, err)
	}
	if !json.Valid(raw) {
		t.Fatalf("expected JSON payload, got %q", raw)
	}
	var out api.FunctionResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.Body != "ok-json" || out.StatusCode != 200 {
		t.Fatalf("round-trip mismatch: %+v", out)
	}
}

func TestCadencePollerMsgpackCodec(t *testing.T) {
	resp := &api.FunctionResponse{StatusCode: 201, Body: "ok-msgpack"}
	payload, published := runActivityWithCodec(t, bindingForCodec("w-msgpack", "msgpack", "msgpack"), resp, "aA==")

	raw, err := base64.StdEncoding.DecodeString(string(payload))
	if err != nil {
		t.Fatalf("expected base64 payload, got %q: %v", payload, err)
	}
	// Msgpack payloads must not be valid JSON for this shape — guards the
	// regression where the poller silently falls back to JSON.
	if json.Valid(raw) {
		t.Fatalf("expected msgpack payload, got valid JSON: %q", raw)
	}
	var out api.FunctionResponse
	if err := msgpack.Unmarshal(raw, &out); err != nil {
		t.Fatalf("msgpack decode: %v", err)
	}
	if out.Body != "ok-msgpack" || out.StatusCode != 201 {
		t.Fatalf("round-trip mismatch: %+v", out)
	}

	// The event.input envelope carries the codec hint so the runtime can
	// route the decoder.
	event, _ := published.Event.(map[string]any)
	input, _ := event["input"].(map[string]any)
	if input["codec"] != "msgpack" {
		t.Fatalf("expected event.input.codec=msgpack, got %v", input)
	}
	if input["content_type"] != "application/msgpack" {
		t.Fatalf("expected content_type=application/msgpack, got %v", input)
	}
}

func TestCadencePollerRawCodec(t *testing.T) {
	// Raw codec ships res.Body verbatim. We mark the body as base64 so
	// it survives transit through FunctionResponse (a UTF-8 string field)
	// even when the underlying payload is opaque binary.
	rawBytes := []byte{0x00, 0x01, 0xfe, 0xff, 'h', 'i'}
	resp := &api.FunctionResponse{
		StatusCode:      200,
		Body:            base64.StdEncoding.EncodeToString(rawBytes),
		IsBase64Encoded: true,
	}
	payload, _ := runActivityWithCodec(t, bindingForCodec("w-raw", "raw", "raw"), resp, "")

	// The poller wraps the codec output in another base64 to fit the
	// existing Cadence RespondActivityTaskCompleted contract.
	got, err := base64.StdEncoding.DecodeString(string(payload))
	if err != nil {
		t.Fatalf("expected base64 payload, got %q: %v", payload, err)
	}
	if string(got) != string(rawBytes) {
		t.Fatalf("raw round-trip mismatch: got %v want %v", got, rawBytes)
	}
}

func TestEncodeActivityPayloadFallbackOnUnknownCodec(t *testing.T) {
	// Unknown codec names must fall back to JSON rather than panicking
	// or returning an error — this is the safety net behind LookupCodec.
	resp := &api.FunctionResponse{StatusCode: 200, Body: "fallback"}
	payload, err := encodeActivityPayload("not-a-codec", resp)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var out api.FunctionResponse
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatalf("expected JSON fallback, got %q: %v", payload, err)
	}
	if out.Body != "fallback" {
		t.Fatalf("fallback round-trip mismatch: %+v", out)
	}
}

func TestBuildEventInputAnnotation(t *testing.T) {
	// Empty codec name resolves to "json" with the JSON content type.
	got := buildEventInput("ZA==", "")
	if got["codec"] != "json" || got["content_type"] != "application/json" || got["raw_base64"] != "ZA==" {
		t.Fatalf("default codec envelope wrong: %v", got)
	}
	// Unknown codec names downshift to json so the runtime is never
	// asked to decode an unknown wire format.
	got = buildEventInput("ZA==", "bogus")
	if got["codec"] != "json" {
		t.Fatalf("expected json fallback for unknown codec, got %v", got)
	}
	// Known binary codec keeps its name and content type.
	got = buildEventInput("ZA==", "msgpack")
	if got["codec"] != "msgpack" || got["content_type"] != "application/msgpack" {
		t.Fatalf("msgpack envelope wrong: %v", got)
	}
}
