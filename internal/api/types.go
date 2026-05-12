package api

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

var (
	tenantPattern         = regexp.MustCompile(`^t_[a-z0-9]{6,32}$`)
	namespacePattern      = regexp.MustCompile(`^[a-z][a-z0-9_-]{2,63}$`)
	functionPattern       = regexp.MustCompile(`^[a-z][a-z0-9_-]{2,63}$`)
	aliasPattern          = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,31}$`)
	entryPattern          = regexp.MustCompile(`^[a-zA-Z0-9._/-]+$`)
	runtimeVersionPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9._@:+-]{0,63}$`)
)

// Canonical runtime identifiers accepted by the manifest schema. The
// concrete adapters live in internal/runtime; the control plane consults
// runtime.DefaultRegistry at publish time to verify that an adapter is
// actually installed (and rejects with CS_RUNTIME_UNSUPPORTED otherwise).
const (
	RuntimeCSJS     = "cs-js"
	RuntimeCSPython = "cs-python"
	RuntimeCSWASM   = "cs-wasm"
)

// SupportedManifestRuntimes is the closed set of runtime identifiers the
// manifest validator accepts. Append-only: removing a value is a breaking
// change to the public manifest contract.
var SupportedManifestRuntimes = []string{
	RuntimeCSJS,
	RuntimeCSPython,
	RuntimeCSWASM,
}

// IsKnownRuntime reports whether s is one of SupportedManifestRuntimes. The
// empty string is also accepted because v1 manifests without a "runtime"
// field implicitly target cs-js; callers that need to materialise the
// default should use NormalizeRuntime.
func IsKnownRuntime(s string) bool {
	if s == "" {
		return true
	}
	for _, r := range SupportedManifestRuntimes {
		if r == s {
			return true
		}
	}
	return false
}

// NormalizeRuntime resolves the implicit cs-js default for empty runtime
// fields. It does not validate the input; callers should pair it with
// IsKnownRuntime.
func NormalizeRuntime(s string) string {
	if s == "" {
		return RuntimeCSJS
	}
	return s
}

// IsValidRuntimeVersion reports whether v matches the runtime version
// format. The empty string is accepted because RuntimeVersion is optional.
func IsValidRuntimeVersion(v string) bool {
	if v == "" {
		return true
	}
	return runtimeVersionPattern.MatchString(v)
}

type FunctionRef struct {
	Function string `json:"function"`
	Alias    string `json:"alias,omitempty"`
	Version  int64  `json:"version,omitempty"`
}

type Principal struct {
	Sub   string   `json:"sub"`
	Roles []string `json:"roles"`
}

type Trigger struct {
	Type   string         `json:"type"`
	Source map[string]any `json:"source"`
	// Sampling is the optional per-trigger sampling policy introduced by
	// E7.02. nil (or zero-value) preserves the pre-E7.02 behaviour of
	// recording every activation; see internal/api/sampling.go for the
	// policy shape and internal/observability for the Decider contract
	// that consumes it.
	Sampling *SamplingPolicy `json:"sampling,omitempty"`
}

type InvocationRequest struct {
	ActivationID string      `json:"activation_id"`
	RequestID    string      `json:"request_id"`
	Tenant       string      `json:"tenant"`
	Namespace    string      `json:"namespace"`
	Ref          FunctionRef `json:"ref"`
	Trigger      Trigger     `json:"trigger"`
	Principal    Principal   `json:"principal"`
	DeadlineMS   int64       `json:"deadline_ms"`
	Event        any         `json:"event"`
}

type InvocationError struct {
	Type    string `json:"type,omitempty"`
	Message string `json:"message,omitempty"`
	Stack   string `json:"stack,omitempty"`
}

type FunctionResponse struct {
	StatusCode      int               `json:"statusCode"`
	Headers         map[string]string `json:"headers,omitempty"`
	Body            string            `json:"body,omitempty"`
	IsBase64Encoded bool              `json:"isBase64Encoded,omitempty"`
}

type InvocationResult struct {
	ActivationID string            `json:"activation_id"`
	RequestID    string            `json:"request_id"`
	Status       string            `json:"status"`
	DurationMS   int64             `json:"duration_ms"`
	Result       *FunctionResponse `json:"result,omitempty"`
	Error        *InvocationError  `json:"error,omitempty"`
}

type ManifestLimits struct {
	TimeoutMS      int `json:"timeoutMs"`
	MemoryMB       int `json:"memoryMb"`
	MaxConcurrency int `json:"maxConcurrency"`
}

type ManifestKVCaps struct {
	Prefixes []string `json:"prefixes"`
	Ops      []string `json:"ops"`
}

type ManifestCodeQCaps struct {
	PublishTopics []string `json:"publishTopics"`
}

type ManifestHTTPCaps struct {
	AllowHosts []string `json:"allowHosts"`
	TimeoutMS  int      `json:"timeoutMs"`
}

type ManifestCapabilities struct {
	KV    ManifestKVCaps    `json:"kv"`
	CodeQ ManifestCodeQCaps `json:"codeq"`
	HTTP  ManifestHTTPCaps  `json:"http"`
}

// ManifestImport declares a single JavaScript dependency that the publisher
// wants frozen into the bundle. Exactly one of URL or Path must be set:
//
//   - URL: an http(s) URL pointing at a curated mirror. The control plane
//     fetches the bytes at publish time, verifies them, and freezes them
//     into the bundle under deps/. The host must appear in the configured
//     allowlist (see publish.imports.allowed_mirrors in
//     docs/20-config-reference.md).
//   - Path: a path inside the uploaded bundle (e.g. "lib/zod.js"). The
//     control plane copies the bytes into deps/ unchanged.
//
// Integrity is optional but, when present, must be a SubResource Integrity
// hash of the form "sha256-<base64>" or "sha384-<base64>". When the
// publisher omits Integrity the control plane fills it in from the bytes
// it actually fetched and freezes the result. The runtime never trusts a
// remote fetch at invoke time — only the frozen import map is consulted.
type ManifestImport struct {
	URL       string `json:"url,omitempty"`
	Path      string `json:"path,omitempty"`
	Integrity string `json:"integrity,omitempty"`
}

type FunctionManifest struct {
	Schema       string               `json:"schema"`
	Runtime      string               `json:"runtime"`
	Entry        string               `json:"entry"`
	Handler      string               `json:"handler"`
	Limits       ManifestLimits       `json:"limits"`
	Capabilities ManifestCapabilities `json:"capabilities"`
	// Optional pin for the runtime adapter version, e.g. "cs-js@1",
	// "node20-goja", "python3.12", "wasi-preview1". When empty the control
	// plane picks the latest installed version of the declared runtime.
	// Appended at the bottom of the struct so existing v1 manifests that
	// lack the field round-trip with byte-identical JSON encoding.
	RuntimeVersion string `json:"runtimeVersion,omitempty"`
	// Imports is the publisher-declared JS dependency map: bare specifier
	// → ManifestImport. Resolution runs once in cs-control at publish
	// time and is frozen into the bundle; the cs-js runtime resolves
	// import statements against the frozen copy with zero network egress.
	// An optional field — when nil or empty the bundle has no
	// dependencies and behaves exactly like a v0.1 single-file function.
	// See docs/08-runtime-cs-js.md and roadmap task E5.01.
	Imports map[string]ManifestImport `json:"imports,omitempty"`
	// Cadence is the optional Cadence-aware section of the manifest. When
	// nil the function defaults to Cadence Activity semantics (the v0.1
	// behaviour). When Cadence.Kind == "workflow" the control plane runs
	// the publish-time static determinism linter from
	// internal/cadence/determinism and rejects any banned API usage with
	// CS_WORKFLOW_NON_DETERMINISTIC. See docs/12-cadence-integration.md
	// "Determinism rules" and roadmap task E8.03.
	Cadence *CadenceConfig `json:"cadence,omitempty"`
}

type VersionAuthz struct {
	InvokeHTTPRoles     []string `json:"invoke_http_roles"`
	InvokeScheduleRoles []string `json:"invoke_schedule_roles"`
	InvokeCadenceRoles  []string `json:"invoke_cadence_roles"`
}

type VersionConfig struct {
	TimeoutMS      int               `json:"timeout_ms"`
	MemoryMB       int               `json:"memory_mb"`
	MaxConcurrency int               `json:"max_concurrency"`
	Env            map[string]string `json:"env"`
	Capabilities   map[string]any    `json:"capabilities"`
	Authz          VersionAuthz      `json:"authz"`
	// Secrets is the list of secret references the function wants
	// injected at activation start. Each entry is a wire-friendly
	// string parsed by internal/plugins/secrets.ParseRef; accepted
	// forms are:
	//
	//   "NAME"                              -> path == name
	//   "NAME=provider/path"                -> explicit path
	//   "NAME=provider/path#json-field:key" -> extract one field
	//
	// The cs-invoker-pool asks the configured secrets provider
	// (E6.01) to resolve each reference and exposes the values to
	// user code via cs.env.get(NAME). Secret material is never
	// persisted in the bundle, KVRocks, or activation results — see
	// docs/15-security.md "Secrets" and internal/plugins/secrets.
	Secrets []string `json:"secrets,omitempty"`
}

type FunctionRecord struct {
	Tenant      string `json:"tenant"`
	Namespace   string `json:"namespace"`
	Name        string `json:"name"`
	Runtime     string `json:"runtime"`
	Entry       string `json:"entry"`
	Handler     string `json:"handler"`
	CreatedAtMS int64  `json:"created_at_ms"`
	DeletedAtMS *int64 `json:"deleted_at_ms"`
}

type DraftRecord struct {
	DraftID     string            `json:"draft_id"`
	SHA256      string            `json:"sha256"`
	Files       map[string]string `json:"files"`
	CreatedAtMS int64             `json:"created_at_ms"`
	ExpiresAtMS int64             `json:"expires_at_ms"`
	Consumed    bool              `json:"consumed,omitempty"`
}

type VersionRecord struct {
	Version       int64         `json:"version"`
	SHA256        string        `json:"sha256"`
	Config        VersionConfig `json:"config"`
	PublishedAtMS int64         `json:"published_at_ms"`
	// Signature is the optional E5.02 publish-time signature record.
	// Present when the publisher supplied a valid signature (or when
	// plugins.signing.required is true; the publish handler rejects
	// missing signatures in that mode). nil for legacy versions
	// published before E5.02 — the invoker enforces re-verification
	// only when this field is non-nil, so the roll-out is backward
	// compatible. See docs/15-security.md.
	Signature *BundleSignature `json:"signature,omitempty"`
}

type AliasRecord struct {
	Alias       string `json:"alias"`
	Version     int64  `json:"version"`
	UpdatedAtMS int64  `json:"updated_at_ms"`
}

type ScheduleRef struct {
	Function string `json:"function"`
	Alias    string `json:"alias,omitempty"`
	Version  int64  `json:"version,omitempty"`
}

type ScheduleRecord struct {
	Tenant        string      `json:"tenant"`
	Namespace     string      `json:"namespace"`
	Name          string      `json:"name"`
	EverySeconds  int         `json:"every_seconds"`
	OverlapPolicy string      `json:"overlap_policy"`
	Ref           ScheduleRef `json:"ref"`
	Payload       any         `json:"payload,omitempty"`
	Enabled       bool        `json:"enabled"`
	CreatedAtMS   int64       `json:"created_at_ms"`
	// E4.01: cron schedule trigger. New fields are append-at-bottom and
	// optional so existing interval schedules round-trip unchanged.
	// Kind is "interval" (default, backward-compatible) or "cron".
	Kind string `json:"kind,omitempty"`
	// Cron is a 5-field CRON expression (minute hour day-of-month month
	// day-of-week). Ignored when Kind is empty or "interval".
	Cron string `json:"cron,omitempty"`
	// TZ is an IANA timezone (e.g. "America/Sao_Paulo"). Defaults to
	// "UTC" when empty. Used to resolve wall-clock semantics for cron
	// schedules across DST transitions.
	TZ string `json:"tz,omitempty"`
	// JitterMs spreads tick fan-out by adding a deterministic offset
	// in [0, JitterMs) milliseconds to the next computed fire time.
	// Optional; 0 disables jitter.
	JitterMs int64 `json:"jitter_ms,omitempty"`
}

type ScheduleState struct {
	NextTickMS int64 `json:"next_tick_ms"`
	TickSeq    int64 `json:"tick_seq"`
}

type WorkerBindingRef struct {
	Function string `json:"function"`
	Alias    string `json:"alias,omitempty"`
	Version  int64  `json:"version,omitempty"`
}

type WorkerBinding struct {
	Tenant      string                      `json:"tenant"`
	Namespace   string                      `json:"namespace"`
	Name        string                      `json:"name"`
	Domain      string                      `json:"domain"`
	Tasklist    string                      `json:"tasklist"`
	WorkerID    string                      `json:"worker_id"`
	ActivityMap map[string]WorkerBindingRef `json:"activity_map"`
	Pollers     struct {
		Activity int `json:"activity"`
	} `json:"pollers"`
	Limits struct {
		MaxInflightTasks int `json:"max_inflight_tasks"`
	} `json:"limits"`
	Enabled bool `json:"enabled"`
	// InputCodec selects the codec used to decode Activity input bytes
	// before the function sees them. Empty defaults to JSON for backward
	// compatibility — bindings created before E8.02 keep behaving as if
	// "json" was set. Recognised values: "json", "msgpack", "raw".
	InputCodec string `json:"input_codec,omitempty"`
	// OutputCodec selects the codec used to encode the function's
	// FunctionResponse before shipping it to RespondActivityTaskCompleted.
	// Empty defaults to JSON. Recognised values: "json", "msgpack", "raw".
	OutputCodec string `json:"output_codec,omitempty"`
}

type ActivationRecord struct {
	ActivationID    string            `json:"activation_id"`
	Tenant          string            `json:"tenant"`
	Namespace       string            `json:"namespace"`
	Function        string            `json:"function"`
	Ref             FunctionRef       `json:"ref"`
	Trigger         Trigger           `json:"trigger"`
	Status          string            `json:"status"`
	StartMS         int64             `json:"start_ms"`
	EndMS           int64             `json:"end_ms,omitempty"`
	DurationMS      int64             `json:"duration_ms,omitempty"`
	ResultTruncated bool              `json:"result_truncated"`
	Error           *InvocationError  `json:"error,omitempty"`
	Result          *FunctionResponse `json:"result,omitempty"`
	RequestID       string            `json:"request_id,omitempty"`
	ResolvedVersion int64             `json:"resolved_version,omitempty"`
	// ParentActivationID links a child activation back to the activation
	// whose user code triggered it. Empty for root activations (i.e. the
	// first hop of a call chain). Propagated via the X-CS-Parent-Activation
	// header injected by the runtime egress shim. See docs/14-observability.md.
	ParentActivationID string `json:"parent_activation_id,omitempty"`
	// RootActivationID identifies the top of the call tree this activation
	// belongs to. Roots have RootActivationID == ActivationID; children
	// inherit it from their parent. Used by the /tree endpoint to bound
	// graph traversal to a single decision tree.
	RootActivationID string `json:"root_activation_id,omitempty"`
	// SamplingDecision records the reason this activation was retained or
	// reduced to a skeleton row. Populated by the E7.02 sampler; empty for
	// pre-E7.02 records and for triggers using the default always-on
	// policy. Values are one of the SamplingDecision* constants.
	SamplingDecision string `json:"sampling_decision,omitempty"`
}

type CreateFunctionRequest struct {
	Name    string `json:"name"`
	Runtime string `json:"runtime"`
	Entry   string `json:"entry"`
	Handler string `json:"handler"`
}

type UploadDraftRequest struct {
	Files map[string]string `json:"files"`
}

type PublishVersionRequest struct {
	DraftID string        `json:"draft_id"`
	Config  VersionConfig `json:"config"`
	Alias   string        `json:"alias,omitempty"`
}

type SetAliasRequest struct {
	Version int64 `json:"version"`
}

type InvokeAPIRequest struct {
	Ref struct {
		Alias   string `json:"alias,omitempty"`
		Version int64  `json:"version,omitempty"`
	} `json:"ref"`
	Mode  string `json:"mode"`
	Event any    `json:"event"`
}

type CreateScheduleRequest struct {
	Name          string      `json:"name"`
	EverySeconds  int         `json:"every_seconds"`
	OverlapPolicy string      `json:"overlap_policy"`
	Ref           ScheduleRef `json:"ref"`
	Payload       any         `json:"payload"`
	// E4.01: optional cron trigger inputs. Mixing Cron with EverySeconds
	// is rejected by validateScheduleRequest in cs-control. Kind defaults
	// to "interval" when EverySeconds is set and "cron" when Cron is set.
	Kind     string `json:"kind,omitempty"`
	Cron     string `json:"cron,omitempty"`
	TZ       string `json:"tz,omitempty"`
	JitterMs int64  `json:"jitter_ms,omitempty"`
}

type CreateWorkerBindingRequest struct {
	Name        string                      `json:"name"`
	Domain      string                      `json:"domain"`
	Tasklist    string                      `json:"tasklist"`
	WorkerID    string                      `json:"worker_id"`
	ActivityMap map[string]WorkerBindingRef `json:"activity_map"`
	Pollers     struct {
		Activity int `json:"activity"`
	} `json:"pollers"`
	Limits struct {
		MaxInflightTasks int `json:"max_inflight_tasks"`
	} `json:"limits"`
	// InputCodec / OutputCodec mirror WorkerBinding fields and let a
	// caller pin per-tasklist payload codecs at binding-create time.
	// Validated against the codec registry — unknown values are rejected
	// with CS_VALIDATION_UNSUPPORTED_CODEC.
	InputCodec  string `json:"input_codec,omitempty"`
	OutputCodec string `json:"output_codec,omitempty"`
}

func ValidateTenant(v string) error {
	if !tenantPattern.MatchString(v) {
		return fmt.Errorf("invalid tenant")
	}
	return nil
}

func ValidateNamespace(v string) error {
	if !namespacePattern.MatchString(v) {
		return fmt.Errorf("invalid namespace")
	}
	return nil
}

func ValidateFunction(v string) error {
	if !functionPattern.MatchString(v) {
		return fmt.Errorf("invalid function")
	}
	return nil
}

func ValidateAlias(v string) error {
	if v == "" {
		return nil
	}
	if !aliasPattern.MatchString(v) {
		return fmt.Errorf("invalid alias")
	}
	return nil
}

func (m FunctionManifest) Validate() error {
	if m.Schema != "cs.function.script.v1" {
		return fmt.Errorf("unsupported schema")
	}
	if !IsKnownRuntime(m.Runtime) {
		return fmt.Errorf("unsupported runtime")
	}
	if !IsValidRuntimeVersion(m.RuntimeVersion) {
		return fmt.Errorf("runtimeVersion has invalid format")
	}
	if strings.TrimSpace(m.Entry) == "" {
		return fmt.Errorf("entry is required")
	}
	if !entryPattern.MatchString(m.Entry) {
		return fmt.Errorf("entry has invalid characters")
	}
	if m.Handler != "default" {
		return fmt.Errorf("handler must be default")
	}
	if m.Limits.TimeoutMS < 1 || m.Limits.TimeoutMS > 900000 {
		return fmt.Errorf("timeoutMs out of range")
	}
	if m.Limits.MemoryMB < 16 || m.Limits.MemoryMB > 4096 {
		return fmt.Errorf("memoryMb out of range")
	}
	if m.Limits.MaxConcurrency < 1 || m.Limits.MaxConcurrency > 100 {
		return fmt.Errorf("maxConcurrency out of range")
	}
	if m.Capabilities.KV.Prefixes == nil || m.Capabilities.KV.Ops == nil {
		return fmt.Errorf("kv capabilities are required")
	}
	if len(m.Capabilities.KV.Prefixes) > 64 {
		return fmt.Errorf("kv prefixes exceed limit")
	}
	for _, p := range m.Capabilities.KV.Prefixes {
		if len(p) < 1 || len(p) > 256 {
			return fmt.Errorf("kv prefix length out of range")
		}
	}
	if len(m.Capabilities.KV.Ops) > 3 {
		return fmt.Errorf("kv ops exceed limit")
	}
	for _, op := range m.Capabilities.KV.Ops {
		switch op {
		case "get", "set", "del":
		default:
			return fmt.Errorf("kv op not allowed")
		}
	}
	if m.Capabilities.CodeQ.PublishTopics == nil {
		return fmt.Errorf("codeq capabilities are required")
	}
	if len(m.Capabilities.CodeQ.PublishTopics) > 64 {
		return fmt.Errorf("codeq publishTopics exceed limit")
	}
	for _, topic := range m.Capabilities.CodeQ.PublishTopics {
		if len(topic) < 1 || len(topic) > 256 {
			return fmt.Errorf("codeq publishTopic length out of range")
		}
	}
	if m.Capabilities.HTTP.AllowHosts == nil {
		return fmt.Errorf("http capabilities are required")
	}
	if len(m.Capabilities.HTTP.AllowHosts) > 128 {
		return fmt.Errorf("http allowHosts exceed limit")
	}
	for _, host := range m.Capabilities.HTTP.AllowHosts {
		if len(host) < 1 || len(host) > 253 {
			return fmt.Errorf("http allowHost length out of range")
		}
	}
	if m.Capabilities.HTTP.TimeoutMS < 1 || m.Capabilities.HTTP.TimeoutMS > 30000 {
		return fmt.Errorf("http timeoutMs out of range")
	}
	// Imports validation. Optional; when present, each entry must declare
	// exactly one source (url or path) and the specifier must be non-empty.
	// Integrity, when set, must use an algorithm prefix we support.
	if len(m.Imports) > 128 {
		return fmt.Errorf("imports exceed limit")
	}
	for spec, imp := range m.Imports {
		if strings.TrimSpace(spec) == "" {
			return fmt.Errorf("imports specifier is empty")
		}
		if len(spec) > 256 {
			return fmt.Errorf("imports specifier too long")
		}
		hasURL := strings.TrimSpace(imp.URL) != ""
		hasPath := strings.TrimSpace(imp.Path) != ""
		if hasURL == hasPath {
			return fmt.Errorf("imports[%q]: exactly one of url or path is required", spec)
		}
		if imp.Integrity != "" {
			if !strings.HasPrefix(imp.Integrity, "sha256-") && !strings.HasPrefix(imp.Integrity, "sha384-") {
				return fmt.Errorf("imports[%q]: integrity must use sha256-/sha384- prefix", spec)
			}
		}
	}
	return nil
}

func ParseManifest(raw []byte) (FunctionManifest, error) {
	var m FunctionManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, err
	}
	return m, m.Validate()
}

func ValidateResultShape(res *FunctionResponse) error {
	if res == nil {
		return nil
	}
	if res.StatusCode < 100 || res.StatusCode > 599 {
		return fmt.Errorf("statusCode out of range")
	}
	for k := range res.Headers {
		if strings.TrimSpace(k) == "" {
			return fmt.Errorf("header key is empty")
		}
	}
	return nil
}

func IntersectsRoles(required []string, principal Principal) bool {
	if len(required) == 0 {
		return false
	}
	set := make(map[string]struct{}, len(principal.Roles))
	for _, role := range principal.Roles {
		set[role] = struct{}{}
	}
	for _, role := range required {
		if _, ok := set[role]; ok {
			return true
		}
	}
	return false
}
