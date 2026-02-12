package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	"github.com/osvaldoandrade/sous/internal/runtime"
)

type authConfig struct {
	APIURL string `json:"api_url"`
	Tenant string `json:"tenant"`
	Token  string `json:"token"`
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	var err error
	switch os.Args[1] {
	case "auth":
		err = handleAuth(os.Args[2:])
	case "fn":
		err = handleFunction(os.Args[2:])
	case "http":
		err = handleHTTP(os.Args[2:])
	case "schedule":
		err = handleSchedule(os.Args[2:])
	case "cadence":
		err = handleCadence(os.Args[2:])
	default:
		usage()
		err = fmt.Errorf("unknown command: %s", os.Args[1])
	}
	if err == nil {
		os.Exit(0)
	}
	fmt.Fprintln(os.Stderr, "error:", err)
	if strings.Contains(err.Error(), "runtime") {
		os.Exit(3)
	}
	if strings.Contains(err.Error(), "server") {
		os.Exit(2)
	}
	os.Exit(1)
}

func usage() {
	fmt.Println("cs <auth|fn|http|schedule|cadence> ...")
}

func handleAuth(args []string) error {
	if len(args) < 1 {
		return errors.New("auth subcommand required: login|whoami")
	}
	switch args[0] {
	case "login":
		fs := flag.NewFlagSet("auth login", flag.ContinueOnError)
		var tiktiURL, tenant, token, apiURL string
		fs.StringVar(&tiktiURL, "tikti-url", "", "Tikti URL")
		fs.StringVar(&tenant, "tenant", "", "Tenant")
		fs.StringVar(&token, "token", "", "Bearer token")
		fs.StringVar(&apiURL, "api-url", "http://localhost:8080", "Control plane URL")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		_ = tiktiURL
		if token == "" {
			token = os.Getenv("CS_TOKEN")
		}
		if token == "" || tenant == "" {
			return errors.New("--tenant and token (--token or CS_TOKEN) are required")
		}
		cfg := authConfig{APIURL: apiURL, Tenant: tenant, Token: token}
		return saveAuthConfig(cfg)
	case "whoami":
		cfg, err := loadAuthConfig()
		if err != nil {
			return err
		}
		fmt.Printf("tenant=%s api_url=%s token_prefix=%s\n", cfg.Tenant, cfg.APIURL, safePrefix(cfg.Token, 8))
		return nil
	default:
		return fmt.Errorf("unknown auth subcommand: %s", args[0])
	}
}

func handleFunction(args []string) error {
	if len(args) < 1 {
		return errors.New("fn subcommand required")
	}
	switch args[0] {
	case "init":
		if len(args) < 2 {
			return errors.New("usage: cs fn init <name>")
		}
		return fnInit(args[1])
	case "create":
		return fnCreate(args[1:])
	case "test":
		return fnTest(args[1:])
	case "draft":
		if len(args) >= 2 && args[1] == "upload" {
			return fnDraftUpload(args[2:])
		}
		return errors.New("usage: cs fn draft upload <name>")
	case "publish":
		return fnPublish(args[1:])
	case "alias":
		if len(args) >= 2 && args[1] == "set" {
			return fnAliasSet(args[2:])
		}
		return errors.New("usage: cs fn alias set <name> <alias> --version <n>")
	case "invoke":
		return fnInvoke(args[1:])
	default:
		return fmt.Errorf("unknown fn subcommand: %s", args[0])
	}
}

func fnInit(name string) error {
	dir := name
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	functionJS := `export default async function handle(event, ctx) {
  cs.log.info({ event, activation_id: ctx.activation_id })
  return { statusCode: 200, headers: {"content-type":"application/json"}, body: JSON.stringify({ ok: true }), isBase64Encoded: false }
}
`
	manifest := `{
  "schema": "cs.function.script.v1",
  "runtime": "cs-js",
  "entry": "function.js",
  "handler": "default",
  "limits": { "timeoutMs": 3000, "memoryMb": 64, "maxConcurrency": 1 },
  "capabilities": {
    "kv": { "prefixes": ["ctr:"], "ops": ["get", "set", "del"] },
    "codeq": { "publishTopics": ["jobs.*"] },
    "http": { "allowHosts": ["api.example.com"], "timeoutMs": 1500 }
  }
}
`
	if err := os.WriteFile(filepath.Join(dir, "function.js"), []byte(functionJS), 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(manifest), 0o644); err != nil {
		return err
	}
	fmt.Println("initialized", dir)
	return nil
}

func fnCreate(args []string) error {
	fs := flag.NewFlagSet("fn create", flag.ContinueOnError)
	var namespace, runtimeName string
	fs.StringVar(&namespace, "namespace", "default", "Namespace")
	fs.StringVar(&runtimeName, "runtime", "cs-js", "Runtime")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("usage: cs fn create <name> --namespace <ns>")
	}
	name := fs.Arg(0)
	cfg, err := loadAuthConfig()
	if err != nil {
		return err
	}
	body := api.CreateFunctionRequest{Name: name, Runtime: runtimeName, Entry: "function.js", Handler: "default"}
	url := fmt.Sprintf("%s/v1/tenants/%s/namespaces/%s/functions", cfg.APIURL, cfg.Tenant, namespace)
	respBody, err := doJSON(cfg.Token, http.MethodPost, url, body)
	if err != nil {
		return err
	}
	fmt.Println(string(respBody))
	return nil
}

func fnTest(args []string) error {
	fs := flag.NewFlagSet("fn test", flag.ContinueOnError)
	var eventPath, path string
	fs.StringVar(&eventPath, "event", "", "Path to event JSON")
	fs.StringVar(&path, "path", ".", "Bundle path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	functionJS, err := os.ReadFile(filepath.Join(path, "function.js"))
	if err != nil {
		return err
	}
	manifestRaw, err := os.ReadFile(filepath.Join(path, "manifest.json"))
	if err != nil {
		return err
	}
	files := map[string][]byte{"function.js": functionJS, "manifest.json": manifestRaw}
	bundleBytes, _, _, err := bundle.BuildCanonical(files)
	if err != nil {
		return err
	}
	event := map[string]any{}
	if eventPath != "" {
		eventRaw, err := os.ReadFile(eventPath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(eventRaw, &event); err != nil {
			return err
		}
	}
	r := runtime.NewRunner(runtime.NewMemoryKV(), runtime.NopCodeQ{}, 256*1024, 64*1024, 1024*1024)
	out := r.Execute(context.Background(), bundleBytes, api.InvocationRequest{
		ActivationID: uuid.NewString(),
		RequestID:    "req_" + uuid.NewString(),
		Tenant:       "t_local",
		Namespace:    "local",
		Ref:          api.FunctionRef{Function: "local", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		Principal:    api.Principal{Sub: "cli", Roles: []string{"role:app"}},
		DeadlineMS:   time.Now().Add(3 * time.Second).UnixMilli(),
		Event:        event,
	})
	raw, _ := json.MarshalIndent(out, "", "  ")
	fmt.Println(string(raw))
	if out.Status != "success" {
		return fmt.Errorf("runtime error: %s", out.Status)
	}
	return nil
}

func fnDraftUpload(args []string) error {
	fs := flag.NewFlagSet("fn draft upload", flag.ContinueOnError)
	var namespace, path string
	fs.StringVar(&namespace, "namespace", "default", "Namespace")
	fs.StringVar(&path, "path", ".", "Bundle path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("usage: cs fn draft upload <name> --path .")
	}
	name := fs.Arg(0)
	cfg, err := loadAuthConfig()
	if err != nil {
		return err
	}
	functionJS, err := os.ReadFile(filepath.Join(path, "function.js"))
	if err != nil {
		return err
	}
	manifestRaw, err := os.ReadFile(filepath.Join(path, "manifest.json"))
	if err != nil {
		return err
	}
	body := api.UploadDraftRequest{Files: map[string]string{
		"function.js":   base64.StdEncoding.EncodeToString(functionJS),
		"manifest.json": base64.StdEncoding.EncodeToString(manifestRaw),
	}}
	url := fmt.Sprintf("%s/v1/tenants/%s/namespaces/%s/functions/%s/draft", cfg.APIURL, cfg.Tenant, namespace, name)
	respBody, err := doJSON(cfg.Token, http.MethodPut, url, body)
	if err != nil {
		return err
	}
	fmt.Println(string(respBody))
	return nil
}

func fnPublish(args []string) error {
	fs := flag.NewFlagSet("fn publish", flag.ContinueOnError)
	var namespace, draftID, alias string
	var timeoutMS, memoryMB int
	fs.StringVar(&namespace, "namespace", "default", "Namespace")
	fs.StringVar(&draftID, "draft", "", "Draft ID")
	fs.StringVar(&alias, "alias", "", "Alias to set")
	fs.IntVar(&timeoutMS, "timeout-ms", 3000, "Timeout ms")
	fs.IntVar(&memoryMB, "memory-mb", 64, "Memory MB")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 || draftID == "" {
		return errors.New("usage: cs fn publish <name> --draft <id>")
	}
	name := fs.Arg(0)
	cfg, err := loadAuthConfig()
	if err != nil {
		return err
	}
	body := api.PublishVersionRequest{DraftID: draftID, Alias: alias, Config: api.VersionConfig{TimeoutMS: timeoutMS, MemoryMB: memoryMB, MaxConcurrency: 1}}
	url := fmt.Sprintf("%s/v1/tenants/%s/namespaces/%s/functions/%s/versions", cfg.APIURL, cfg.Tenant, namespace, name)
	respBody, err := doJSON(cfg.Token, http.MethodPost, url, body)
	if err != nil {
		return err
	}
	fmt.Println(string(respBody))
	return nil
}

func fnAliasSet(args []string) error {
	fs := flag.NewFlagSet("fn alias set", flag.ContinueOnError)
	var namespace string
	var version int64
	fs.StringVar(&namespace, "namespace", "default", "Namespace")
	fs.Int64Var(&version, "version", 0, "Version")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 2 || version <= 0 {
		return errors.New("usage: cs fn alias set <name> <alias> --version <n>")
	}
	name := fs.Arg(0)
	alias := fs.Arg(1)
	cfg, err := loadAuthConfig()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/v1/tenants/%s/namespaces/%s/functions/%s/aliases/%s", cfg.APIURL, cfg.Tenant, namespace, name, alias)
	respBody, err := doJSON(cfg.Token, http.MethodPut, url, api.SetAliasRequest{Version: version})
	if err != nil {
		return err
	}
	fmt.Println(string(respBody))
	return nil
}

func fnInvoke(args []string) error {
	fs := flag.NewFlagSet("fn invoke", flag.ContinueOnError)
	var namespace, eventPath, mode string
	fs.StringVar(&namespace, "namespace", "default", "Namespace")
	fs.StringVar(&eventPath, "event", "", "Path to event JSON")
	fs.StringVar(&mode, "mode", "sync", "sync|async")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("usage: cs fn invoke <function@alias|function@version>")
	}
	fnRef := fs.Arg(0)
	parts := strings.Split(fnRef, "@")
	if len(parts) != 2 {
		return errors.New("ref must be function@alias or function@version")
	}
	name := parts[0]
	ref := parts[1]
	cfg, err := loadAuthConfig()
	if err != nil {
		return err
	}
	event := map[string]any{}
	if eventPath != "" {
		raw, err := os.ReadFile(eventPath)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(raw, &event); err != nil {
			return err
		}
	}
	body := api.InvokeAPIRequest{Mode: mode, Event: event}
	if v, err := strconv.ParseInt(ref, 10, 64); err == nil {
		body.Ref.Version = v
	} else {
		body.Ref.Alias = ref
	}
	url := fmt.Sprintf("%s/v1/tenants/%s/namespaces/%s/functions/%s:invoke", cfg.APIURL, cfg.Tenant, namespace, name)
	respBody, err := doJSON(cfg.Token, http.MethodPost, url, body)
	if err != nil {
		return err
	}
	fmt.Println(string(respBody))
	return nil
}

func handleHTTP(args []string) error {
	if len(args) < 1 || args[0] != "invoke" {
		return errors.New("usage: cs http invoke <path> [-X METHOD] [-d @file]")
	}
	fs := flag.NewFlagSet("http invoke", flag.ContinueOnError)
	var method, data string
	fs.StringVar(&method, "X", http.MethodPost, "HTTP method")
	fs.StringVar(&data, "d", "", "Body or @file")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return errors.New("missing path")
	}
	path := fs.Arg(0)
	cfg, err := loadAuthConfig()
	if err != nil {
		return err
	}
	body := []byte{}
	if data != "" {
		if strings.HasPrefix(data, "@") {
			body, err = os.ReadFile(strings.TrimPrefix(data, "@"))
			if err != nil {
				return err
			}
		} else {
			body = []byte(data)
		}
	}
	req, err := http.NewRequest(method, strings.TrimRight(cfg.APIURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	fmt.Printf("status=%d\n%s\n", resp.StatusCode, string(respBody))
	if resp.StatusCode >= 400 {
		return errors.New("server error")
	}
	return nil
}

func handleSchedule(args []string) error {
	if len(args) < 1 {
		return errors.New("schedule subcommand required: create|delete")
	}
	cfg, err := loadAuthConfig()
	if err != nil {
		return err
	}
	switch args[0] {
	case "create":
		fs := flag.NewFlagSet("schedule create", flag.ContinueOnError)
		var namespace, functionRef, payloadPath string
		var every int
		fs.StringVar(&namespace, "namespace", "default", "Namespace")
		fs.StringVar(&functionRef, "fn", "", "function@alias|function@version")
		fs.StringVar(&payloadPath, "payload", "", "Path to JSON payload")
		fs.IntVar(&every, "every", 30, "Interval in seconds")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() < 1 || functionRef == "" {
			return errors.New("usage: cs schedule create <name> --every 30 --fn reconcile@prod")
		}
		name := fs.Arg(0)
		refParts := strings.Split(functionRef, "@")
		if len(refParts) != 2 {
			return errors.New("--fn must be function@alias|version")
		}
		body := api.CreateScheduleRequest{Name: name, EverySeconds: every, OverlapPolicy: "skip", Ref: api.ScheduleRef{Function: refParts[0]}}
		if v, err := strconv.ParseInt(refParts[1], 10, 64); err == nil {
			body.Ref.Version = v
		} else {
			body.Ref.Alias = refParts[1]
		}
		if payloadPath != "" {
			raw, err := os.ReadFile(payloadPath)
			if err != nil {
				return err
			}
			var payload any
			if err := json.Unmarshal(raw, &payload); err != nil {
				return err
			}
			body.Payload = payload
		}
		url := fmt.Sprintf("%s/v1/tenants/%s/namespaces/%s/schedules", cfg.APIURL, cfg.Tenant, namespace)
		respBody, err := doJSON(cfg.Token, http.MethodPost, url, body)
		if err != nil {
			return err
		}
		fmt.Println(string(respBody))
		return nil
	case "delete":
		fs := flag.NewFlagSet("schedule delete", flag.ContinueOnError)
		var namespace string
		fs.StringVar(&namespace, "namespace", "default", "Namespace")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if fs.NArg() < 1 {
			return errors.New("usage: cs schedule delete <name>")
		}
		name := fs.Arg(0)
		url := fmt.Sprintf("%s/v1/tenants/%s/namespaces/%s/schedules/%s", cfg.APIURL, cfg.Tenant, namespace, name)
		respBody, err := doJSON(cfg.Token, http.MethodDelete, url, nil)
		if err != nil {
			return err
		}
		fmt.Println(string(respBody))
		return nil
	default:
		return fmt.Errorf("unknown schedule subcommand: %s", args[0])
	}
}

func handleCadence(args []string) error {
	if len(args) < 2 || args[0] != "worker" || args[1] != "create" {
		return errors.New("usage: cs cadence worker create <name> ...")
	}
	fs := flag.NewFlagSet("cadence worker create", flag.ContinueOnError)
	var namespace, domain, tasklist, workerID, activity string
	fs.StringVar(&namespace, "namespace", "default", "Namespace")
	fs.StringVar(&domain, "domain", "", "Cadence domain")
	fs.StringVar(&tasklist, "tasklist", "", "Cadence tasklist")
	fs.StringVar(&workerID, "worker-id", "", "Worker ID")
	fs.StringVar(&activity, "activity", "", "ActivityType=function@alias")
	if err := fs.Parse(args[2:]); err != nil {
		return err
	}
	if fs.NArg() < 1 || domain == "" || tasklist == "" || workerID == "" || activity == "" {
		return errors.New("usage: cs cadence worker create <name> --domain d --tasklist t --worker-id w --activity Type=function@prod")
	}
	name := fs.Arg(0)
	activityParts := strings.Split(activity, "=")
	if len(activityParts) != 2 {
		return errors.New("--activity must be ActivityType=function@alias|version")
	}
	refParts := strings.Split(activityParts[1], "@")
	if len(refParts) != 2 {
		return errors.New("activity ref must be function@alias|version")
	}
	ref := api.WorkerBindingRef{Function: refParts[0]}
	if v, err := strconv.ParseInt(refParts[1], 10, 64); err == nil {
		ref.Version = v
	} else {
		ref.Alias = refParts[1]
	}
	body := api.CreateWorkerBindingRequest{
		Name:        name,
		Domain:      domain,
		Tasklist:    tasklist,
		WorkerID:    workerID,
		ActivityMap: map[string]api.WorkerBindingRef{activityParts[0]: ref},
	}
	body.Pollers.Activity = 8
	body.Limits.MaxInflightTasks = 256

	cfg, err := loadAuthConfig()
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/v1/tenants/%s/namespaces/%s/cadence/workers", cfg.APIURL, cfg.Tenant, namespace)
	respBody, err := doJSON(cfg.Token, http.MethodPost, url, body)
	if err != nil {
		return err
	}
	fmt.Println(string(respBody))
	return nil
}

func doJSON(token, method, url string, body any) ([]byte, error) {
	var payload io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		payload = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("server error (%d): %s", resp.StatusCode, string(raw))
	}
	return raw, nil
}

func authPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "code-sous", "auth.json")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	return path, nil
}

func saveAuthConfig(cfg authConfig) error {
	path, err := authPath()
	if err != nil {
		return err
	}
	raw, _ := json.MarshalIndent(cfg, "", "  ")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return err
	}
	fmt.Println("saved auth config to", path)
	return nil
}

func loadAuthConfig() (authConfig, error) {
	var cfg authConfig
	path, err := authPath()
	if err != nil {
		return cfg, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, err
	}
	if cfg.APIURL == "" || cfg.Token == "" || cfg.Tenant == "" {
		return cfg, errors.New("invalid auth config")
	}
	return cfg, nil
}

func safePrefix(v string, n int) string {
	if len(v) <= n {
		return v
	}
	return v[:n]
}
