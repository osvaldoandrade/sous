package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

var (
	exportDefaultNamedRegex = regexp.MustCompile(`export\s+default\s+(async\s+)?function\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(`)
	exportDefaultAnonRegex  = regexp.MustCompile(`export\s+default\s+(async\s+)?function\s*\(`)
)

type Runner struct {
	kv                  KVProvider
	codeq               CodeQProvider
	httpClient          *http.Client
	maxResultBytes      int
	maxErrorBytes       int
	maxLogBytes         int
	allowPrivateIPCheck bool
}

type ExecutionOutput struct {
	Status       string
	Result       *api.FunctionResponse
	Error        *api.InvocationError
	Logs         []string
	Truncated    bool
	DurationMS   int64
	ResolvedCode cserrors.Code
}

func NewRunner(kv KVProvider, codeq CodeQProvider, maxResultBytes, maxErrorBytes, maxLogBytes int) *Runner {
	if kv == nil {
		kv = NewMemoryKV()
	}
	if codeq == nil {
		codeq = NopCodeQ{}
	}
	if maxResultBytes <= 0 {
		maxResultBytes = 256 * 1024
	}
	if maxErrorBytes <= 0 {
		maxErrorBytes = 64 * 1024
	}
	if maxLogBytes <= 0 {
		maxLogBytes = 1 * 1024 * 1024
	}
	return &Runner{
		kv:                  kv,
		codeq:               codeq,
		httpClient:          &http.Client{Timeout: 5 * time.Second},
		maxResultBytes:      maxResultBytes,
		maxErrorBytes:       maxErrorBytes,
		maxLogBytes:         maxLogBytes,
		allowPrivateIPCheck: true,
	}
}

func (r *Runner) Execute(ctx context.Context, bundleBytes []byte, request api.InvocationRequest) ExecutionOutput {
	start := time.Now()
	out := ExecutionOutput{Status: "error"}

	files, err := bundle.ExtractTar(bundleBytes)
	if err != nil {
		out.Error = &api.InvocationError{Type: "BundleError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSValidationManifest
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}
	manifest, err := api.ParseManifest(files["manifest.json"])
	if err != nil {
		out.Error = &api.InvocationError{Type: "ManifestError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSValidationManifest
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}

	deadline := time.UnixMilli(request.DeadlineMS)
	if deadline.IsZero() || deadline.Before(time.Now()) {
		deadline = time.Now().Add(time.Duration(manifest.Limits.TimeoutMS) * time.Millisecond)
	}
	runCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	collector := &logCollector{maxBytes: r.maxLogBytes}

	res, execErr := r.runJS(runCtx, files["function.js"], manifest, request, collector)
	out.Logs = collector.Logs()
	out.Truncated = collector.Truncated()
	if execErr != nil {
		mappedCode := cserrors.CSRuntimeException
		status := "error"
		errType := "Exception"
		if errors.Is(execErr, context.DeadlineExceeded) || strings.Contains(execErr.Error(), "timeout") {
			mappedCode = cserrors.CSRuntimeTimeout
			status = "timeout"
			errType = "Timeout"
		}
		msg := execErr.Error()
		if len(msg) > r.maxErrorBytes {
			msg = msg[:r.maxErrorBytes]
			out.Truncated = true
		}
		out.Status = status
		out.Error = &api.InvocationError{Type: errType, Message: msg}
		out.ResolvedCode = mappedCode
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}

	if err := api.ValidateResultShape(res); err != nil {
		out.Status = "error"
		out.Error = &api.InvocationError{Type: "ValidationError", Message: err.Error()}
		out.ResolvedCode = cserrors.CSRuntimeException
		out.DurationMS = time.Since(start).Milliseconds()
		return out
	}
	resultRaw, _ := json.Marshal(res)
	if len(resultRaw) > r.maxResultBytes {
		resultRaw = resultRaw[:r.maxResultBytes]
		out.Truncated = true
		var truncated api.FunctionResponse
		if err := json.Unmarshal(resultRaw, &truncated); err == nil {
			res = &truncated
		}
	}
	out.Status = "success"
	out.Result = res
	out.ResolvedCode = ""
	out.DurationMS = time.Since(start).Milliseconds()
	return out
}

func (r *Runner) runJS(ctx context.Context, code []byte, manifest api.FunctionManifest, request api.InvocationRequest, logs *logCollector) (*api.FunctionResponse, error) {
	rt := goja.New()

	timeout := time.Until(time.UnixMilli(request.DeadlineMS))
	if timeout <= 0 {
		timeout = time.Duration(manifest.Limits.TimeoutMS) * time.Millisecond
	}
	timer := time.AfterFunc(timeout, func() {
		rt.Interrupt("runtime timeout")
	})
	defer timer.Stop()
	defer rt.ClearInterrupt()

	csObj := rt.NewObject()
	if err := r.bindLog(rt, csObj, logs); err != nil {
		return nil, err
	}
	if err := r.bindKV(ctx, rt, csObj, manifest.Capabilities.KV); err != nil {
		return nil, err
	}
	if err := r.bindCodeQ(ctx, rt, csObj, manifest.Capabilities.CodeQ); err != nil {
		return nil, err
	}
	if err := r.bindHTTP(ctx, rt, csObj, manifest.Capabilities.HTTP); err != nil {
		return nil, err
	}
	if err := r.bindCadence(rt, csObj, request); err != nil {
		return nil, err
	}
	if err := rt.Set("cs", csObj); err != nil {
		return nil, err
	}
	if err := rt.Set("exports", map[string]any{}); err != nil {
		return nil, err
	}

	compiled := transformESModule(string(code))
	if _, err := rt.RunString(compiled); err != nil {
		return nil, err
	}

	h := rt.Get("__cs_default")
	if goja.IsUndefined(h) || goja.IsNull(h) {
		exportsObj := rt.Get("exports")
		if obj := exportsObj.ToObject(rt); obj != nil {
			h = obj.Get("default")
		}
	}
	handler, ok := goja.AssertFunction(h)
	if !ok {
		return nil, fmt.Errorf("default handler not found")
	}

	ctxObj := map[string]any{
		"activation_id": request.ActivationID,
		"deadline_ms":   request.DeadlineMS,
		"tenant":        request.Tenant,
		"namespace":     request.Namespace,
		"function":      request.Ref.Function,
		"ref": map[string]any{
			"alias":   request.Ref.Alias,
			"version": request.Ref.Version,
		},
		"trigger": map[string]any{"type": request.Trigger.Type},
		"principal": map[string]any{
			"sub":   request.Principal.Sub,
			"roles": request.Principal.Roles,
		},
	}

	val, err := handler(goja.Undefined(), rt.ToValue(request.Event), rt.ToValue(ctxObj))
	if err != nil {
		return nil, err
	}

	resolved, err := r.awaitValue(ctx, rt, val)
	if err != nil {
		return nil, err
	}

	if goja.IsUndefined(resolved) || goja.IsNull(resolved) {
		return &api.FunctionResponse{StatusCode: 200, Headers: map[string]string{}, Body: ""}, nil
	}

	if obj := resolved.ToObject(rt); obj != nil && obj.ClassName() == "Object" {
		exported := resolved.Export()
		raw, err := json.Marshal(exported)
		if err != nil {
			return nil, err
		}
		var res api.FunctionResponse
		if err := json.Unmarshal(raw, &res); err != nil {
			return nil, err
		}
		if res.StatusCode == 0 {
			res.StatusCode = 200
		}
		if res.Headers == nil {
			res.Headers = map[string]string{}
		}
		return &res, nil
	}

	body := JSONString(resolved.Export())
	return &api.FunctionResponse{StatusCode: 200, Headers: map[string]string{}, Body: body}, nil
}

func (r *Runner) awaitValue(ctx context.Context, rt *goja.Runtime, val goja.Value) (goja.Value, error) {
	prom, ok := val.Export().(*goja.Promise)
	if !ok {
		return val, nil
	}
	_ = rt
	if prom.State() == goja.PromiseStatePending {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return nil, fmt.Errorf("pending promise is not supported")
		}
	}
	if prom.State() == goja.PromiseStateRejected {
		return nil, fmt.Errorf("%v", prom.Result().Export())
	}
	return prom.Result(), nil
}

func (r *Runner) bindLog(rt *goja.Runtime, csObj *goja.Object, logs *logCollector) error {
	logObj := rt.NewObject()
	for _, level := range []string{"info", "warn", "error"} {
		lvl := level
		if err := logObj.Set(level, func(call goja.FunctionCall) goja.Value {
			var v any
			if len(call.Arguments) > 0 {
				v = call.Argument(0).Export()
			}
			logs.Append(lvl, v)
			return goja.Undefined()
		}); err != nil {
			return err
		}
	}
	return csObj.Set("log", logObj)
}

func (r *Runner) bindKV(ctx context.Context, rt *goja.Runtime, csObj *goja.Object, caps api.ManifestKVCaps) error {
	ops := map[string]struct{}{}
	for _, op := range caps.Ops {
		ops[op] = struct{}{}
	}
	allowedPrefixes := append([]string(nil), caps.Prefixes...)
	sort.Strings(allowedPrefixes)

	check := func(op, key string) error {
		if _, ok := ops[op]; !ok {
			return cserrors.New(cserrors.CSRuntimeCapDenied, "kv op not allowed")
		}
		for _, p := range allowedPrefixes {
			if strings.HasPrefix(key, p) {
				return nil
			}
		}
		return cserrors.New(cserrors.CSRuntimeCapDenied, "kv prefix not allowed")
	}

	kvObj := rt.NewObject()
	if err := kvObj.Set("get", func(call goja.FunctionCall) goja.Value {
		key := call.Argument(0).String()
		if err := check("get", key); err != nil {
			panic(rt.NewGoError(err))
		}
		value, err := r.kv.Get(ctx, key)
		if err != nil {
			panic(rt.NewGoError(err))
		}
		if value == "" {
			return goja.Null()
		}
		var decoded any
		if json.Unmarshal([]byte(value), &decoded) == nil {
			return rt.ToValue(decoded)
		}
		return rt.ToValue(value)
	}); err != nil {
		return err
	}
	if err := kvObj.Set("set", func(call goja.FunctionCall) goja.Value {
		key := call.Argument(0).String()
		if err := check("set", key); err != nil {
			panic(rt.NewGoError(err))
		}
		value := JSONString(call.Argument(1).Export())
		ttl := 0
		if len(call.Arguments) >= 3 {
			if opt := call.Argument(2).ToObject(rt); opt != nil {
				ttl = int(opt.Get("ttlSeconds").ToInteger())
			}
		}
		if err := r.kv.Set(ctx, key, value, ttl); err != nil {
			panic(rt.NewGoError(err))
		}
		return goja.Undefined()
	}); err != nil {
		return err
	}
	if err := kvObj.Set("del", func(call goja.FunctionCall) goja.Value {
		key := call.Argument(0).String()
		if err := check("del", key); err != nil {
			panic(rt.NewGoError(err))
		}
		if err := r.kv.Del(ctx, key); err != nil {
			panic(rt.NewGoError(err))
		}
		return goja.Undefined()
	}); err != nil {
		return err
	}
	return csObj.Set("kv", kvObj)
}

func (r *Runner) bindCodeQ(ctx context.Context, rt *goja.Runtime, csObj *goja.Object, caps api.ManifestCodeQCaps) error {
	codeqObj := rt.NewObject()
	allowed := append([]string(nil), caps.PublishTopics...)
	sort.Strings(allowed)

	matches := func(topic string) bool {
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

	if err := codeqObj.Set("publish", func(call goja.FunctionCall) goja.Value {
		topic := call.Argument(0).String()
		if !matches(topic) {
			panic(rt.NewGoError(cserrors.New(cserrors.CSRuntimeCapDenied, "codeq topic not allowed")))
		}
		var payload any
		if len(call.Arguments) > 1 {
			payload = call.Argument(1).Export()
		}
		if err := r.codeq.Publish(ctx, topic, payload); err != nil {
			panic(rt.NewGoError(err))
		}
		return goja.Undefined()
	}); err != nil {
		return err
	}
	return csObj.Set("codeq", codeqObj)
}

func (r *Runner) bindHTTP(ctx context.Context, rt *goja.Runtime, csObj *goja.Object, caps api.ManifestHTTPCaps) error {
	httpObj := rt.NewObject()
	allow := append([]string(nil), caps.AllowHosts...)
	sort.Strings(allow)

	isAllowedHost := func(host string) bool {
		for _, h := range allow {
			if strings.EqualFold(host, h) {
				return true
			}
		}
		return false
	}

	if err := httpObj.Set("fetch", func(call goja.FunctionCall) goja.Value {
		rawURL := call.Argument(0).String()
		u, err := url.Parse(rawURL)
		if err != nil {
			panic(rt.NewGoError(err))
		}
		hostname := u.Hostname()
		if !isAllowedHost(hostname) {
			panic(rt.NewGoError(cserrors.New(cserrors.CSRuntimeCapDenied, "http host not allowed")))
		}
		if r.allowPrivateIPCheck {
			if denied, err := denyPrivateHost(hostname); err != nil {
				panic(rt.NewGoError(err))
			} else if denied {
				panic(rt.NewGoError(cserrors.New(cserrors.CSRuntimeCapDenied, "private ip denied")))
			}
		}

		method := http.MethodGet
		headers := map[string]string{}
		body := ""
		timeoutMS := caps.TimeoutMS
		if len(call.Arguments) > 1 {
			opt := call.Argument(1).ToObject(rt)
			if opt != nil {
				if m := opt.Get("method"); !goja.IsUndefined(m) {
					method = strings.ToUpper(m.String())
				}
				if b := opt.Get("body"); !goja.IsUndefined(b) && !goja.IsNull(b) {
					body = b.String()
				}
				if h := opt.Get("headers"); !goja.IsUndefined(h) && !goja.IsNull(h) {
					if exported, ok := h.Export().(map[string]any); ok {
						for k, v := range exported {
							headers[k] = fmt.Sprintf("%v", v)
						}
					}
				}
				if t := opt.Get("timeoutMs"); !goja.IsUndefined(t) && !goja.IsNull(t) {
					value := int(t.ToInteger())
					if value > 0 && value < timeoutMS {
						timeoutMS = value
					}
				}
			}
		}

		reqCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMS)*time.Millisecond)
		defer cancel()
		req, err := http.NewRequestWithContext(reqCtx, method, rawURL, strings.NewReader(body))
		if err != nil {
			panic(rt.NewGoError(err))
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := r.httpClient.Do(req)
		if err != nil {
			panic(rt.NewGoError(err))
		}
		defer resp.Body.Close()
		bytes, _ := io.ReadAll(resp.Body)
		hdr := map[string]string{}
		for k, values := range resp.Header {
			if len(values) > 0 {
				hdr[strings.ToLower(k)] = strings.Join(values, ",")
			}
		}
		ret := map[string]any{
			"status":          resp.StatusCode,
			"headers":         hdr,
			"body":            base64.StdEncoding.EncodeToString(bytes),
			"isBase64Encoded": true,
		}
		return rt.ToValue(ret)
	}); err != nil {
		return err
	}

	return csObj.Set("http", httpObj)
}

func (r *Runner) bindCadence(rt *goja.Runtime, csObj *goja.Object, req api.InvocationRequest) error {
	cad := rt.NewObject()
	if err := cad.Set("heartbeat", func(call goja.FunctionCall) goja.Value {
		if req.Trigger.Type != "cadence" {
			panic(rt.NewGoError(cserrors.New(cserrors.CSRuntimeCapDenied, "cadence heartbeat only for cadence trigger")))
		}
		return goja.Undefined()
	}); err != nil {
		return err
	}
	return csObj.Set("cadence", cad)
}

func transformESModule(src string) string {
	trimmed := strings.TrimSpace(src)
	if !strings.Contains(trimmed, "export default") {
		return src + "\nif (typeof exports !== 'undefined' && exports.default) { globalThis.__cs_default = exports.default; }\n"
	}
	if match := exportDefaultNamedRegex.FindStringSubmatch(src); len(match) > 2 {
		name := match[2]
		replaced := exportDefaultNamedRegex.ReplaceAllString(src, "${1}function "+name+"(")
		return replaced + "\nglobalThis.__cs_default = " + name + ";\n"
	}
	if exportDefaultAnonRegex.MatchString(src) {
		replaced := exportDefaultAnonRegex.ReplaceAllString(src, "const __cs_default = ${1}function(")
		return replaced + "\nglobalThis.__cs_default = __cs_default;\n"
	}
	replaced := strings.Replace(src, "export default", "const __cs_default =", 1)
	return replaced + "\nglobalThis.__cs_default = __cs_default;\n"
}

type logCollector struct {
	mu        sync.Mutex
	maxBytes  int
	bytesUsed int
	truncated bool
	lines     []string
}

func (l *logCollector) Append(level string, value any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	line := fmt.Sprintf("[%s] %s", level, JSONString(value))
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

func (l *logCollector) Logs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.lines))
	copy(out, l.lines)
	return out
}

func (l *logCollector) Truncated() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.truncated
}

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
	privateCIDRs := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"127.0.0.0/8",
		"::1/128",
		"fc00::/7",
	}
	for _, cidr := range privateCIDRs {
		_, block, _ := net.ParseCIDR(cidr)
		if block.Contains(ip) {
			return true
		}
	}
	return false
}
