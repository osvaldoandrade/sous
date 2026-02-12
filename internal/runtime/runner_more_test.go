package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dop251/goja"
	"github.com/google/uuid"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

type recordingKV struct {
	mu   sync.Mutex
	data map[string]string
	ops  []string
}

func (k *recordingKV) Get(ctx context.Context, key string) (string, error) {
	_ = ctx
	k.mu.Lock()
	defer k.mu.Unlock()
	k.ops = append(k.ops, "get:"+key)
	return k.data[key], nil
}

func (k *recordingKV) Set(ctx context.Context, key, value string, ttlSeconds int) error {
	_ = ctx
	_ = ttlSeconds
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.data == nil {
		k.data = make(map[string]string)
	}
	k.ops = append(k.ops, "set:"+key)
	k.data[key] = value
	return nil
}

func (k *recordingKV) Del(ctx context.Context, key string) error {
	_ = ctx
	k.mu.Lock()
	defer k.mu.Unlock()
	k.ops = append(k.ops, "del:"+key)
	delete(k.data, key)
	return nil
}

type publishCall struct {
	topic   string
	payload any
}

type recordingCodeQ struct {
	mu    sync.Mutex
	calls []publishCall
}

func (c *recordingCodeQ) Publish(ctx context.Context, topic string, payload any) error {
	_ = ctx
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, publishCall{topic: topic, payload: payload})
	return nil
}

func baseManifest() api.FunctionManifest {
	return api.FunctionManifest{
		Schema:  "cs.function.script.v1",
		Runtime: "cs-js",
		Entry:   "function.js",
		Handler: "default",
		Limits: api.ManifestLimits{
			TimeoutMS:      200,
			MemoryMB:       64,
			MaxConcurrency: 1,
		},
		Capabilities: api.ManifestCapabilities{
			KV:    api.ManifestKVCaps{Prefixes: []string{"ctr:"}, Ops: []string{"get", "set", "del"}},
			CodeQ: api.ManifestCodeQCaps{PublishTopics: []string{"jobs.*"}},
			HTTP:  api.ManifestHTTPCaps{AllowHosts: []string{"example.com"}, TimeoutMS: 500},
		},
	}
}

func buildBundle(t *testing.T, manifest api.FunctionManifest, functionJS string) []byte {
	t.Helper()
	manifestRaw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	b, _, _, err := bundle.BuildCanonical(map[string][]byte{
		"function.js":   []byte(functionJS),
		"manifest.json": manifestRaw,
	})
	if err != nil {
		t.Fatalf("build bundle: %v", err)
	}
	return b
}

func invocationReq(trigger string, event map[string]any, deadline time.Time) api.InvocationRequest {
	return api.InvocationRequest{
		ActivationID: uuid.NewString(),
		RequestID:    "req_" + uuid.NewString(),
		Tenant:       "t_abc123",
		Namespace:    "ns1",
		Ref:          api.FunctionRef{Function: "fn1", Version: 1},
		Trigger:      api.Trigger{Type: trigger, Source: map[string]any{}},
		Principal:    api.Principal{Sub: "u1", Roles: []string{"role:app", "role:worker"}},
		DeadlineMS:   deadline.UnixMilli(),
		Event:        event,
	}
}

func TestRunnerExecuteBundleAndManifestFailures(t *testing.T) {
	r := NewRunner(NewMemoryKV(), NopCodeQ{}, 0, 0, 0)

	out := r.Execute(t.Context(), []byte("not-a-tar"), invocationReq("api", map[string]any{}, time.Now().Add(time.Second)))
	if out.Status != "error" || out.Error == nil || out.Error.Type != "BundleError" || out.ResolvedCode != cserrors.CSValidationManifest {
		t.Fatalf("unexpected bundle error output: %+v", out)
	}

	raw, _, _, err := bundle.BuildCanonical(map[string][]byte{
		"function.js":   []byte(`export default function() { return {statusCode:200, body:"ok"} }`),
		"manifest.json": []byte(`{`),
	})
	if err != nil {
		t.Fatalf("build invalid-manifest tar: %v", err)
	}
	out = r.Execute(t.Context(), raw, invocationReq("api", map[string]any{}, time.Now().Add(time.Second)))
	if out.Status != "error" || out.Error == nil || out.Error.Type != "ManifestError" || out.ResolvedCode != cserrors.CSValidationManifest {
		t.Fatalf("unexpected manifest error output: %+v", out)
	}
}

func TestRunnerAwaitValueStates(t *testing.T) {
	r := NewRunner(NewMemoryKV(), NopCodeQ{}, 0, 0, 0)
	rt := goja.New()

	pending, _, _ := rt.NewPromise()
	if _, err := r.awaitValue(context.Background(), rt, rt.ToValue(pending)); err == nil || !strings.Contains(err.Error(), "pending promise") {
		t.Fatalf("pending promise should fail, got: %v", err)
	}

	rejected, _, reject := rt.NewPromise()
	reject(rt.ToValue("boom"))
	if _, err := r.awaitValue(context.Background(), rt, rt.ToValue(rejected)); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("rejected promise should return rejection reason, got: %v", err)
	}

	fulfilled, resolve, _ := rt.NewPromise()
	resolve(rt.ToValue(map[string]any{"ok": true}))
	v, err := r.awaitValue(context.Background(), rt, rt.ToValue(fulfilled))
	if err != nil {
		t.Fatalf("fulfilled promise returned error: %v", err)
	}
	if got, ok := v.Export().(map[string]any); !ok || got["ok"] != true {
		t.Fatalf("unexpected fulfilled value: %#v", v.Export())
	}
}

func TestRunnerExecuteTimeoutAndValidationFailure(t *testing.T) {
	manifest := baseManifest()
	manifest.Limits.TimeoutMS = 20
	r := NewRunner(NewMemoryKV(), NopCodeQ{}, 0, 0, 0)

	timeoutBundle := buildBundle(t, manifest, `export default function() { while (true) {} }`)
	out := r.Execute(t.Context(), timeoutBundle, invocationReq("api", map[string]any{}, time.Now().Add(25*time.Millisecond)))
	if out.Status != "timeout" || out.ResolvedCode != cserrors.CSRuntimeTimeout {
		t.Fatalf("expected timeout output, got %+v", out)
	}

	manifest = baseManifest()
	invalidShapeBundle := buildBundle(t, manifest, `export default function() { return { statusCode: 700, body: "bad" } }`)
	out = r.Execute(t.Context(), invalidShapeBundle, invocationReq("api", map[string]any{}, time.Now().Add(time.Second)))
	if out.Status != "error" || out.Error == nil || out.Error.Type != "ValidationError" {
		t.Fatalf("expected validation error output, got %+v", out)
	}
}

func TestRunnerExecuteKVCodeQHTTPAndCadence(t *testing.T) {
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		if got := r.Header.Get("X-Test"); got != "yes" {
			t.Fatalf("expected X-Test header, got %q", got)
		}
		_, _ = w.Write([]byte("pong"))
	}))
	defer httpSrv.Close()

	host := strings.Split(strings.TrimPrefix(httpSrv.URL, "http://"), ":")[0]
	manifest := baseManifest()
	manifest.Capabilities.HTTP.AllowHosts = []string{host}

	kv := &recordingKV{data: map[string]string{}}
	codeq := &recordingCodeQ{}
	r := NewRunner(kv, codeq, 0, 0, 128)
	r.allowPrivateIPCheck = false

	js := `
export default async function handle(event, ctx) {
  cs.log.info({ activation_id: ctx.activation_id })
  cs.kv.set("ctr:key", { value: 10 }, { ttlSeconds: 2 })
  const got = cs.kv.get("ctr:key")
  cs.kv.del("ctr:key")
  cs.codeq.publish("jobs.run", { tenant: "t_abc123", got })
  const resp = cs.http.fetch(event.url, { method: "POST", body: "x", headers: {"X-Test": "yes"}, timeoutMs: 25 })
  cs.cadence.heartbeat({ step: 1 })
  return {
    statusCode: 201,
    headers: {"content-type":"application/json"},
    body: JSON.stringify({ kv: got, status: resp.status, body: resp.body })
  }
}
`
	b := buildBundle(t, manifest, js)
	out := r.Execute(t.Context(), b, invocationReq("cadence", map[string]any{"url": httpSrv.URL}, time.Now().Add(time.Second)))
	if out.Status != "success" || out.Result == nil || out.Result.StatusCode != 201 {
		t.Fatalf("unexpected execution output: %+v", out)
	}
	if len(out.Logs) == 0 {
		t.Fatal("expected runtime logs")
	}
	if out.Result.Headers["content-type"] != "application/json" {
		t.Fatalf("unexpected headers: %+v", out.Result.Headers)
	}

	var body map[string]any
	if err := json.Unmarshal([]byte(out.Result.Body), &body); err != nil {
		t.Fatalf("result body is not json: %v", err)
	}
	if int(body["status"].(float64)) != 200 {
		t.Fatalf("expected http status 200 in response payload, got %#v", body["status"])
	}
	if _, ok := body["kv"]; !ok {
		t.Fatalf("expected kv field in result body: %#v", body)
	}

	kv.mu.Lock()
	ops := append([]string(nil), kv.ops...)
	kv.mu.Unlock()
	if len(ops) < 3 || ops[0] != "set:ctr:key" || ops[1] != "get:ctr:key" || ops[2] != "del:ctr:key" {
		t.Fatalf("unexpected kv ops: %#v", ops)
	}

	codeq.mu.Lock()
	callCount := len(codeq.calls)
	codeq.mu.Unlock()
	if callCount != 1 {
		t.Fatalf("expected 1 codeq publish, got %d", callCount)
	}
}

func TestRunnerHTTPAndCadenceDenied(t *testing.T) {
	manifest := baseManifest()
	manifest.Capabilities.HTTP.AllowHosts = []string{"example.com"}
	r := NewRunner(NewMemoryKV(), NopCodeQ{}, 0, 0, 0)

	// Not allowed host.
	notAllowedBundle := buildBundle(t, manifest, `
export default function(event) {
  cs.http.fetch(event.url)
  return { statusCode: 200, body: "ok" }
}
`)
	out := r.Execute(t.Context(), notAllowedBundle, invocationReq("api", map[string]any{"url": "http://not-allowed.local"}, time.Now().Add(time.Second)))
	if out.Status != "error" || out.Error == nil || !strings.Contains(strings.ToLower(out.Error.Message), "not allowed") {
		t.Fatalf("expected host denied error, got %+v", out)
	}

	// Cadence heartbeat is denied outside cadence trigger.
	cadBundle := buildBundle(t, manifest, `
export default function() {
  cs.cadence.heartbeat({ hello: "world" })
  return { statusCode: 200, body: "ok" }
}
`)
	out = r.Execute(t.Context(), cadBundle, invocationReq("api", map[string]any{}, time.Now().Add(time.Second)))
	if out.Status != "error" || out.Error == nil || !strings.Contains(strings.ToLower(out.Error.Message), "cadence heartbeat") {
		t.Fatalf("expected cadence denied error, got %+v", out)
	}
}

func TestRunnerStringReturnAndLogTruncation(t *testing.T) {
	manifest := baseManifest()
	js := `export default function() { return 7 }`
	b := buildBundle(t, manifest, js)

	r := NewRunner(NewMemoryKV(), NopCodeQ{}, 0, 0, 1)
	out := r.Execute(t.Context(), b, invocationReq("api", map[string]any{}, time.Now().Add(time.Second)))
	if out.Status != "success" || out.Result == nil || out.Result.Body != "7" {
		t.Fatalf("unexpected scalar return output: %+v", out)
	}

	c := &logCollector{maxBytes: 4}
	c.Append("info", "abcdef")
	if !c.Truncated() {
		t.Fatal("expected truncated logs")
	}
	if len(c.Logs()) == 0 {
		t.Fatal("expected at least one truncated line")
	}
}

func TestTransformESModuleAndPrivateHostHelpers(t *testing.T) {
	noExport := transformESModule(`function x() { return 1 }`)
	if !strings.Contains(noExport, "__cs_default") {
		t.Fatalf("no-export transform missing default assignment: %s", noExport)
	}
	named := transformESModule(`export default function handle(){ return 1 }`)
	if !strings.Contains(named, "globalThis.__cs_default = handle;") {
		t.Fatalf("named export transform failed: %s", named)
	}
	anon := transformESModule(`export default async function(){ return 1 }`)
	if !strings.Contains(anon, "globalThis.__cs_default = __cs_default;") {
		t.Fatalf("anon export transform failed: %s", anon)
	}

	denied, err := denyPrivateHost("127.0.0.1")
	if err != nil || !denied {
		t.Fatalf("127.0.0.1 should be denied, got denied=%v err=%v", denied, err)
	}
	denied, err = denyPrivateHost("example.invalid")
	if err == nil {
		t.Fatalf("expected lookup error for invalid host, got denied=%v", denied)
	}

	if !isPrivateIP(net.ParseIP("10.0.0.1")) {
		t.Fatal("10.0.0.1 should be private")
	}
	if isPrivateIP(net.ParseIP("8.8.8.8")) {
		t.Fatal("8.8.8.8 should not be private")
	}
}

func TestRunnerBase64DecodeFallbackInFunctionResponse(t *testing.T) {
	manifest := baseManifest()
	js := `
export default function() {
  return { statusCode: 200, headers: {}, body: "not-base64", isBase64Encoded: true }
}
`
	b := buildBundle(t, manifest, js)
	r := NewRunner(NewMemoryKV(), NopCodeQ{}, 0, 0, 0)
	out := r.Execute(t.Context(), b, invocationReq("api", map[string]any{}, time.Now().Add(time.Second)))
	if out.Status != "success" || out.Result == nil {
		t.Fatalf("unexpected output: %+v", out)
	}
	// Runtime does not decode function response body; this check protects accidental behavior changes.
	if _, err := base64.StdEncoding.DecodeString(out.Result.Body); err == nil {
		t.Fatalf("expected non-base64 output body, got decodable value: %q", out.Result.Body)
	}
}
