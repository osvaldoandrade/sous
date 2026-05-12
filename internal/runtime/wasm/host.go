package wasm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	apipkg "github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/observability"
	rt "github.com/osvaldoandrade/sous/internal/runtime"
)

// hostBridge is the per-invocation state shared by every host function
// exported into the "env" module. It pins the capability set declared
// by the manifest (no mutation after instantiation), the providers
// for kv/codeq/http, the per-call deadline-bearing context, and a log
// collector that mirrors the cs-js runner's collector contract.
//
// A fresh bridge is built for every Execute call; the lifecycle is
// strictly request-scoped and the bridge is discarded with the
// wazero.Runtime when Execute returns.
type hostBridge struct {
	caps      apipkg.ManifestCapabilities
	kv        rt.KVProvider
	codeq     rt.CodeQProvider
	http      *http.Client
	denyLocal bool
	logger    *logBuffer

	// ctx carries the per-call deadline. It is rebound on every entry
	// into the bridge from a wazero host function so user-controlled
	// host calls can be cancelled mid-flight along with the wasm
	// guest. See wireBridge for the rebinding contract.
	mu  sync.RWMutex
	ctx context.Context

	// allocFn is the guest-exported cs_alloc function. We can't look
	// it up until the guest is instantiated, so the bridge defers the
	// lookup to the moment the first host call needs to return a
	// payload to the guest.
	allocFn api.Function
}

// hostName is the wasm module name the guest imports from. We always
// publish host calls under "env" because that's the convention WASI
// targets, AssemblyScript, and TinyGo all default to. Guests that
// want a different name can still bind explicitly with --import-name.
const hostName = "env"

// Canonical export names. Mirrors the cs-js surface so a tool authored
// against either runtime sees the same operations.
const (
	exportHTTPFetch    = "cs_http_fetch"
	exportKVGet        = "cs_kv_get"
	exportKVSet        = "cs_kv_set"
	exportKVDel        = "cs_kv_del"
	exportCodeQPublish = "cs_codeq_publish"
	exportLogWrite     = "cs_log_write"
)

// registerHostModule installs every cs_* host function under the
// "env" module in the wazero runtime. The bridge captures the
// per-invocation state; the wazero builder hands every host function
// a pointer back to it via closure capture.
//
// The set of functions exposed here is exhaustive: the cs-wasm
// runtime is allowlist-first. WASI snapshot_preview1 imports are
// deliberately NOT registered — modules that need them will fail to
// instantiate, which is the documented "deny fs / proc / sock"
// invariant from docs/15-security.md.
func registerHostModule(ctx context.Context, runtime wazero.Runtime, bridge *hostBridge) error {
	builder := runtime.NewHostModuleBuilder(hostName)

	builder.NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(bridge.kvGet), []api.ValueType{
			api.ValueTypeI32, api.ValueTypeI32,
		}, []api.ValueType{api.ValueTypeI64}).
		WithParameterNames("key_ptr", "key_len").
		WithResultNames("packed_value").
		Export(exportKVGet)

	builder.NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(bridge.kvSet), []api.ValueType{
			api.ValueTypeI32, api.ValueTypeI32,
			api.ValueTypeI32, api.ValueTypeI32,
			api.ValueTypeI32,
		}, []api.ValueType{api.ValueTypeI32}).
		WithParameterNames("key_ptr", "key_len", "val_ptr", "val_len", "ttl_seconds").
		WithResultNames("status").
		Export(exportKVSet)

	builder.NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(bridge.kvDel), []api.ValueType{
			api.ValueTypeI32, api.ValueTypeI32,
		}, []api.ValueType{api.ValueTypeI32}).
		WithParameterNames("key_ptr", "key_len").
		WithResultNames("status").
		Export(exportKVDel)

	builder.NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(bridge.codeqPublish), []api.ValueType{
			api.ValueTypeI32, api.ValueTypeI32,
			api.ValueTypeI32, api.ValueTypeI32,
		}, []api.ValueType{api.ValueTypeI32}).
		WithParameterNames("topic_ptr", "topic_len", "payload_ptr", "payload_len").
		WithResultNames("status").
		Export(exportCodeQPublish)

	builder.NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(bridge.httpFetch), []api.ValueType{
			api.ValueTypeI32, api.ValueTypeI32,
		}, []api.ValueType{api.ValueTypeI64}).
		WithParameterNames("req_ptr", "req_len").
		WithResultNames("packed_response").
		Export(exportHTTPFetch)

	builder.NewFunctionBuilder().
		WithGoModuleFunction(api.GoModuleFunc(bridge.logWrite), []api.ValueType{
			api.ValueTypeI32,
			api.ValueTypeI32, api.ValueTypeI32,
		}, []api.ValueType{api.ValueTypeI32}).
		WithParameterNames("level", "msg_ptr", "msg_len").
		WithResultNames("status").
		Export(exportLogWrite)

	_, err := builder.Instantiate(ctx)
	return err
}

// withCallContext rebinds the bridge's request context for the
// duration of a single host call. wazero passes a fresh ctx into
// every Go host function, but we want host operations (kv, http) to
// observe the per-call deadline carried by the bridge — wazero's
// own ctx is the runtime ctx, not the per-invocation one. The
// returned cleanup must run after the host body completes.
func (b *hostBridge) withCallContext(callCtx context.Context) context.Context {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ctx == nil {
		return callCtx
	}
	return mergeDeadlines(b.ctx, callCtx)
}

// mergeDeadlines returns a context derived from b whose deadline is
// the earlier of a.Deadline() and b.Deadline(). When a carries no
// deadline the original b is returned untouched. We accept the small
// cost of an unused cancel by routing it through context.AfterFunc
// on b — that way the merged context is finalised when either parent
// cancels and we don't leak the timer goroutine.
func mergeDeadlines(a, b context.Context) context.Context {
	da, aok := a.Deadline()
	if !aok {
		return b
	}
	db, bok := b.Deadline()
	if bok && !da.Before(db) {
		return b
	}
	ctx, cancel := context.WithDeadline(b, da)
	// Free the timer when a finishes early; the deadline goroutine
	// inside ctx already covers the deadline-elapsed case.
	context.AfterFunc(a, cancel)
	return ctx
}

// SetContext lets the runner pin the per-invocation context onto the
// bridge before user code runs. Wazero plumbs its own ctx through
// host calls; this lets every kv/http operation honour the same
// deadline the guest is bounded by.
func (b *hostBridge) SetContext(ctx context.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ctx = ctx
}

// SetAlloc records the guest's cs_alloc export so output-bearing
// host calls (kvGet, httpFetch) can return payloads to the guest.
func (b *hostBridge) SetAlloc(fn api.Function) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.allocFn = fn
}

func (b *hostBridge) alloc() api.Function {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.allocFn
}

// kvGet implements cs.kv.get. Reads the key from guest memory, calls
// the KV provider, and writes the (JSON-encoded) value back into a
// fresh guest allocation. Returns 0 when the key is missing or when
// the capability gate denies the call. The latter is observable via
// the StatusErrCapDenied surfaced through cs_log_write side-effects
// in tests; v0.1 guests are expected to treat 0 as "absent or
// denied" and recover.
func (b *hostBridge) kvGet(ctx context.Context, mod api.Module, stack []uint64) {
	keyPtr := uint32(stack[0])
	keyLen := uint32(stack[1])
	stack[0] = 0

	mem := mod.Memory()
	key, err := ReadString(mem, keyPtr, keyLen)
	if err != nil {
		return
	}
	if err := b.checkKVCap("get", key); err != nil {
		return
	}

	value, err := b.kv.Get(b.withCallContext(ctx), key)
	if err != nil || value == "" {
		return
	}
	packed, err := WriteToGuest(b.withCallContext(ctx), b.alloc(), mem, []byte(value))
	if err != nil {
		return
	}
	stack[0] = packed
}

// kvSet implements cs.kv.set. Stores value bytes under key with an
// optional TTL.
func (b *hostBridge) kvSet(ctx context.Context, mod api.Module, stack []uint64) {
	keyPtr := uint32(stack[0])
	keyLen := uint32(stack[1])
	valPtr := uint32(stack[2])
	valLen := uint32(stack[3])
	ttl := int32(stack[4])

	mem := mod.Memory()
	key, err := ReadString(mem, keyPtr, keyLen)
	if err != nil {
		stack[0] = uint64(EncodeStatusU32(StatusErrInvalidArg))
		return
	}
	value, err := ReadString(mem, valPtr, valLen)
	if err != nil {
		stack[0] = uint64(EncodeStatusU32(StatusErrInvalidArg))
		return
	}
	if err := b.checkKVCap("set", key); err != nil {
		stack[0] = uint64(EncodeStatusU32(StatusErrCapDenied))
		return
	}
	if err := b.kv.Set(b.withCallContext(ctx), key, value, int(ttl)); err != nil {
		stack[0] = uint64(EncodeStatusU32(StatusErrHostFailure))
		return
	}
	stack[0] = uint64(EncodeStatusU32(StatusOK))
}

// kvDel implements cs.kv.del.
func (b *hostBridge) kvDel(ctx context.Context, mod api.Module, stack []uint64) {
	keyPtr := uint32(stack[0])
	keyLen := uint32(stack[1])

	mem := mod.Memory()
	key, err := ReadString(mem, keyPtr, keyLen)
	if err != nil {
		stack[0] = uint64(EncodeStatusU32(StatusErrInvalidArg))
		return
	}
	if err := b.checkKVCap("del", key); err != nil {
		stack[0] = uint64(EncodeStatusU32(StatusErrCapDenied))
		return
	}
	if err := b.kv.Del(b.withCallContext(ctx), key); err != nil {
		stack[0] = uint64(EncodeStatusU32(StatusErrHostFailure))
		return
	}
	stack[0] = uint64(EncodeStatusU32(StatusOK))
}

// codeqPublish implements cs.codeq.publish. The payload bytes are
// passed through as a JSON-encoded value when possible; on failure
// the bytes are forwarded as a plain string so guests written in
// languages without a native JSON encoder still get something
// readable on the wire.
func (b *hostBridge) codeqPublish(ctx context.Context, mod api.Module, stack []uint64) {
	topicPtr := uint32(stack[0])
	topicLen := uint32(stack[1])
	payloadPtr := uint32(stack[2])
	payloadLen := uint32(stack[3])

	mem := mod.Memory()
	topic, err := ReadString(mem, topicPtr, topicLen)
	if err != nil {
		stack[0] = uint64(EncodeStatusU32(StatusErrInvalidArg))
		return
	}
	if !b.codeqTopicAllowed(topic) {
		stack[0] = uint64(EncodeStatusU32(StatusErrCapDenied))
		return
	}
	payload, err := ReadMemory(mem, payloadPtr, payloadLen)
	if err != nil {
		stack[0] = uint64(EncodeStatusU32(StatusErrInvalidArg))
		return
	}

	var decoded any
	if len(payload) > 0 && json.Unmarshal(payload, &decoded) != nil {
		decoded = string(payload)
	}
	if err := b.codeq.Publish(b.withCallContext(ctx), topic, decoded); err != nil {
		stack[0] = uint64(EncodeStatusU32(StatusErrHostFailure))
		return
	}
	stack[0] = uint64(EncodeStatusU32(StatusOK))
}

// httpFetch implements cs.http.fetch. The request is encoded by the
// guest as a JSON document with the same shape as the cs-js
// `{ url, method, headers, body, timeoutMs }` options object.
// The response, when present, is encoded back as JSON matching the
// cs-js `{ status, headers, body, isBase64Encoded }` envelope.
func (b *hostBridge) httpFetch(ctx context.Context, mod api.Module, stack []uint64) {
	reqPtr := uint32(stack[0])
	reqLen := uint32(stack[1])
	stack[0] = 0

	mem := mod.Memory()
	raw, err := ReadMemory(mem, reqPtr, reqLen)
	if err != nil {
		return
	}
	var req wasmHTTPRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return
	}
	u, err := url.Parse(req.URL)
	if err != nil {
		return
	}
	if !b.httpHostAllowed(u.Hostname()) {
		return
	}
	if b.denyLocal {
		if denied, err := denyPrivateHost(u.Hostname()); err != nil || denied {
			return
		}
	}

	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	timeoutMS := b.caps.HTTP.TimeoutMS
	if req.TimeoutMS > 0 && req.TimeoutMS < timeoutMS {
		timeoutMS = req.TimeoutMS
	}

	callCtx := b.withCallContext(ctx)
	reqCtx, cancel := context.WithTimeout(callCtx, time.Duration(timeoutMS)*time.Millisecond)
	defer cancel()

	body := req.Body
	if req.IsBase64Encoded {
		decoded, decErr := base64.StdEncoding.DecodeString(body)
		if decErr == nil {
			body = string(decoded)
		}
	}

	httpReq, err := http.NewRequestWithContext(reqCtx, strings.ToUpper(method), req.URL, strings.NewReader(body))
	if err != nil {
		return
	}
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}
	observability.InjectParentHeader(httpReq, callCtx)
	resp, err := b.http.Do(httpReq)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	bodyBytes, _ := io.ReadAll(resp.Body)
	hdr := map[string]string{}
	for k, values := range resp.Header {
		if len(values) > 0 {
			hdr[strings.ToLower(k)] = strings.Join(values, ",")
		}
	}
	envelope := map[string]any{
		"status":          resp.StatusCode,
		"headers":         hdr,
		"body":            base64.StdEncoding.EncodeToString(bodyBytes),
		"isBase64Encoded": true,
	}
	out, err := json.Marshal(envelope)
	if err != nil {
		return
	}
	packed, err := WriteToGuest(callCtx, b.alloc(), mem, out)
	if err != nil {
		return
	}
	stack[0] = packed
}

// logWrite implements cs.log.{info,warn,error}. Level is encoded as
// 0=info / 1=warn / 2=error to keep the ABI numeric. Anything else
// is treated as info.
func (b *hostBridge) logWrite(ctx context.Context, mod api.Module, stack []uint64) {
	_ = ctx
	level := levelName(int32(stack[0]))
	msgPtr := uint32(stack[1])
	msgLen := uint32(stack[2])

	mem := mod.Memory()
	msg, err := ReadString(mem, msgPtr, msgLen)
	if err != nil {
		stack[0] = uint64(EncodeStatusU32(StatusErrInvalidArg))
		return
	}
	b.logger.append(level, msg)
	stack[0] = uint64(EncodeStatusU32(StatusOK))
}

// wasmHTTPRequest mirrors the shape cs-js publishers pass to
// cs.http.fetch's options bag, with the addition of an explicit url
// field. The guest constructs the JSON; the host validates it.
type wasmHTTPRequest struct {
	URL             string            `json:"url"`
	Method          string            `json:"method,omitempty"`
	Headers         map[string]string `json:"headers,omitempty"`
	Body            string            `json:"body,omitempty"`
	IsBase64Encoded bool              `json:"isBase64Encoded,omitempty"`
	TimeoutMS       int               `json:"timeoutMs,omitempty"`
}

// checkKVCap mirrors the cs-js bindKV check: deny if the op is not
// in the manifest's KV.Ops, or if the key does not match any of the
// declared prefixes.
func (b *hostBridge) checkKVCap(op, key string) error {
	ops := map[string]struct{}{}
	for _, o := range b.caps.KV.Ops {
		ops[o] = struct{}{}
	}
	if _, ok := ops[op]; !ok {
		return fmt.Errorf("kv op not allowed")
	}
	prefixes := append([]string(nil), b.caps.KV.Prefixes...)
	sort.Strings(prefixes)
	for _, p := range prefixes {
		if strings.HasPrefix(key, p) {
			return nil
		}
	}
	return fmt.Errorf("kv prefix not allowed")
}

func (b *hostBridge) codeqTopicAllowed(topic string) bool {
	allowed := append([]string(nil), b.caps.CodeQ.PublishTopics...)
	sort.Strings(allowed)
	for _, allow := range allowed {
		if strings.HasSuffix(allow, "*") {
			if strings.HasPrefix(topic, strings.TrimSuffix(allow, "*")) {
				return true
			}
			continue
		}
		if topic == allow {
			return true
		}
	}
	return false
}

func (b *hostBridge) httpHostAllowed(host string) bool {
	for _, h := range b.caps.HTTP.AllowHosts {
		if strings.EqualFold(host, h) {
			return true
		}
	}
	return false
}

func levelName(level int32) string {
	switch level {
	case 1:
		return "warn"
	case 2:
		return "error"
	default:
		return "info"
	}
}

// logBuffer is a request-scoped collector that mirrors the cs-js
// runner's logCollector but keeps the type local to this package so
// the wasm runtime never depends on the goja-flavoured original.
type logBuffer struct {
	mu        sync.Mutex
	maxBytes  int
	bytesUsed int
	truncated bool
	lines     []string
}

func newLogBuffer(maxBytes int) *logBuffer {
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	return &logBuffer{maxBytes: maxBytes}
}

func (l *logBuffer) append(level, msg string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	line := fmt.Sprintf("[%s] %s", level, msg)
	if l.bytesUsed+len(line) > l.maxBytes {
		remaining := l.maxBytes - l.bytesUsed
		if remaining > 0 {
			line = line[:remaining]
			l.lines = append(l.lines, line)
			l.bytesUsed += len(line)
		}
		l.truncated = true
		return
	}
	l.lines = append(l.lines, line)
	l.bytesUsed += len(line)
}

func (l *logBuffer) snapshot() ([]string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.lines))
	copy(out, l.lines)
	return out, l.truncated
}

// denyPrivateHost mirrors the cs-js runner's private-IP guard. Kept
// local so the wasm runtime does not need to import the runtime
// package's helpers, which would introduce a cycle.
func denyPrivateHost(hostname string) (bool, error) {
	if ip := net.ParseIP(hostname); ip != nil {
		return isPrivateIP(ip), nil
	}
	ips, err := net.LookupIP(hostname)
	if err != nil {
		return false, err
	}
	for _, ip := range ips {
		if isPrivateIP(ip) {
			return true, nil
		}
	}
	return false, nil
}

func isPrivateIP(ip net.IP) bool {
	cidrs := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"127.0.0.0/8",
		"::1/128",
		"fc00::/7",
	}
	for _, c := range cidrs {
		_, block, _ := net.ParseCIDR(c)
		if block.Contains(ip) {
			return true
		}
	}
	return false
}
