package api

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

var (
	tenantPattern    = regexp.MustCompile(`^t_[a-z0-9]{6,32}$`)
	namespacePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{2,63}$`)
	functionPattern  = regexp.MustCompile(`^[a-z][a-z0-9_-]{2,63}$`)
	aliasPattern     = regexp.MustCompile(`^[a-z][a-z0-9_-]{1,31}$`)
	entryPattern     = regexp.MustCompile(`^[a-zA-Z0-9._/-]+$`)
)

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

type FunctionManifest struct {
	Schema       string               `json:"schema"`
	Runtime      string               `json:"runtime"`
	Entry        string               `json:"entry"`
	Handler      string               `json:"handler"`
	Limits       ManifestLimits       `json:"limits"`
	Capabilities ManifestCapabilities `json:"capabilities"`
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
	Secrets        []string          `json:"secrets,omitempty"`
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
	if m.Runtime != "cs-js" {
		return fmt.Errorf("unsupported runtime")
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
