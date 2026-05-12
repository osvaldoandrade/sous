package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	"github.com/osvaldoandrade/sous/internal/cli/templates"
	"github.com/osvaldoandrade/sous/internal/runtime"
)

// checkStatus is the per-probe outcome surfaced to the user / JSON output.
type checkStatus string

const (
	statusPass checkStatus = "pass"
	statusWarn checkStatus = "warn"
	statusFail checkStatus = "fail"
)

// doctorCheck is one probe result.
type doctorCheck struct {
	Name   string      `json:"name"`
	Status checkStatus `json:"status"`
	Detail string      `json:"detail,omitempty"`
	Hint   string      `json:"hint,omitempty"`
}

// doctorOptions holds the inputs to the doctor command, factored out so the
// tests can drive probes without touching the global flag set. The
// unexported function-typed fields exist so tests can inject fakes; nil
// values resolve to the production defaults.
type doctorOptions struct {
	APIURL       string
	JSON         bool
	Timeout      time.Duration
	APIClient    *http.Client
	loader       func() (authConfig, error)
	runtimeProbe func(ctx context.Context) error
}

// doctorReport bundles the result of all probes for JSON serialisation.
type doctorReport struct {
	Checks []doctorCheck `json:"checks"`
}

// handleDoctor is the dispatcher entry point. It parses flags then runs the
// probes against the resolved options.
func handleDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	var apiURL string
	var jsonOut bool
	var timeoutMS int
	fs.StringVar(&apiURL, "api-url", "", "Override the control-plane URL (defaults to auth config)")
	fs.BoolVar(&jsonOut, "json", false, "Emit results as JSON")
	fs.IntVar(&timeoutMS, "timeout-ms", 2000, "HTTP probe timeout in milliseconds")
	if err := fs.Parse(args); err != nil {
		return err
	}
	opts := doctorOptions{
		APIURL:  apiURL,
		JSON:    jsonOut,
		Timeout: time.Duration(timeoutMS) * time.Millisecond,
	}
	return runDoctor(opts, os.Stdout)
}

// runDoctor executes the probe set and writes a report to w. It returns a
// `cliError` when one or more checks fail so `main` exits non-zero with a
// clear remediation hint.
func runDoctor(opts doctorOptions, w io.Writer) error {
	if opts.Timeout <= 0 {
		opts.Timeout = 2 * time.Second
	}
	if opts.APIClient == nil {
		opts.APIClient = &http.Client{Timeout: opts.Timeout}
	}
	if opts.loader == nil {
		opts.loader = loadAuthConfig
	}
	if opts.runtimeProbe == nil {
		opts.runtimeProbe = runDoctorRuntimeProbe
	}

	report := doctorReport{}

	// Probe 1: auth config + token shape.
	authCheck, cfg := probeAuth(opts.loader)
	report.Checks = append(report.Checks, authCheck)

	// Resolve effective API URL: flag wins, then auth config, then default.
	apiURL := opts.APIURL
	if apiURL == "" {
		apiURL = cfg.APIURL
	}
	if apiURL == "" {
		apiURL = "http://localhost:8080"
	}

	// Probe 2: control-plane reachability (GET /healthz).
	report.Checks = append(report.Checks, probeControlPlane(opts.APIClient, apiURL))

	// Probe 3: cs-js runtime parity smoke check.
	report.Checks = append(report.Checks, probeRuntime(opts.runtimeProbe))

	// Probe 4: writable config dir.
	report.Checks = append(report.Checks, probeConfigDir())

	if opts.JSON {
		raw, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return newClientError("failed to render doctor report", "rerun without --json", err)
		}
		fmt.Fprintln(w, string(raw))
	} else {
		writeDoctorTable(w, report.Checks)
	}

	// Any failure produces a non-zero exit. Pick the most-specific kind based
	// on which probe failed so exit-code semantics line up with the rest of
	// the CLI.
	for _, c := range report.Checks {
		if c.Status != statusFail {
			continue
		}
		switch c.Name {
		case "control-plane":
			return newServerError("doctor: control-plane probe failed", c.Hint, nil)
		case "runtime":
			return newRuntimeError("doctor: runtime probe failed", c.Hint, nil)
		default:
			return newClientError("doctor: one or more checks failed", c.Hint, nil)
		}
	}
	return nil
}

// writeDoctorTable prints a fixed-width status table.
func writeDoctorTable(w io.Writer, checks []doctorCheck) {
	nameW := len("check")
	for _, c := range checks {
		if len(c.Name) > nameW {
			nameW = len(c.Name)
		}
	}
	fmt.Fprintf(w, "%-*s  %-6s  %s\n", nameW, "check", "status", "detail")
	for _, c := range checks {
		detail := c.Detail
		if c.Status != statusPass && c.Hint != "" {
			if detail != "" {
				detail = detail + " — " + c.Hint
			} else {
				detail = c.Hint
			}
		}
		fmt.Fprintf(w, "%-*s  %-6s  %s\n", nameW, c.Name, string(c.Status), detail)
	}
}

// probeAuth verifies the auth config is parseable and contains the required
// fields. It returns both the check and the (possibly partial) config so
// downstream probes can reuse the resolved API URL.
func probeAuth(loader func() (authConfig, error)) (doctorCheck, authConfig) {
	cfg, err := loader()
	if err != nil {
		return doctorCheck{
			Name:   "auth",
			Status: statusFail,
			Detail: err.Error(),
			Hint:   "run `cs auth login --tenant <tenant_id> --token <token>` to create the auth config",
		}, cfg
	}
	if cfg.Tenant == "" {
		return doctorCheck{
			Name:   "auth",
			Status: statusFail,
			Detail: "missing tenant in auth config",
			Hint:   "run `cs auth login --tenant <tenant_id> --token <token>`",
		}, cfg
	}
	if cfg.Token == "" {
		return doctorCheck{
			Name:   "auth",
			Status: statusFail,
			Detail: "missing bearer token in auth config",
			Hint:   "set CS_TOKEN or pass --token to `cs auth login`",
		}, cfg
	}
	return doctorCheck{
		Name:   "auth",
		Status: statusPass,
		Detail: fmt.Sprintf("tenant=%s api_url=%s", cfg.Tenant, cfg.APIURL),
	}, cfg
}

// probeControlPlane issues GET /healthz against the resolved API URL.
func probeControlPlane(client *http.Client, apiURL string) doctorCheck {
	healthURL := strings.TrimRight(apiURL, "/") + "/healthz"
	req, err := http.NewRequest(http.MethodGet, healthURL, nil)
	if err != nil {
		return doctorCheck{
			Name:   "control-plane",
			Status: statusFail,
			Detail: err.Error(),
			Hint:   fmt.Sprintf("verify --api-url is a valid URL (got %q)", apiURL),
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return doctorCheck{
			Name:   "control-plane",
			Status: statusFail,
			Detail: err.Error(),
			Hint:   fmt.Sprintf("control plane unreachable at %s — start `./bin/cs-control` or pass `--api-url`", apiURL),
		}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 400 {
		return doctorCheck{
			Name:   "control-plane",
			Status: statusFail,
			Detail: fmt.Sprintf("GET %s returned %d", healthURL, resp.StatusCode),
			Hint:   "check `cs-control` logs; the service is up but rejecting healthz",
		}
	}
	return doctorCheck{
		Name:   "control-plane",
		Status: statusPass,
		Detail: fmt.Sprintf("GET %s -> %d", healthURL, resp.StatusCode),
	}
}

// probeRuntime invokes the cs-js parity smoke check.
func probeRuntime(probe func(ctx context.Context) error) doctorCheck {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := probe(ctx); err != nil {
		return doctorCheck{
			Name:   "runtime",
			Status: statusFail,
			Detail: err.Error(),
			Hint:   "verify the `cs-js` runtime build; rebuild with `make build`",
		}
	}
	return doctorCheck{
		Name:   "runtime",
		Status: statusPass,
		Detail: "cs-js canary executed successfully",
	}
}

// probeConfigDir checks that the auth config directory is writable. It
// resolves the path via authPath() so it stays in sync with `cs auth login`.
func probeConfigDir() doctorCheck {
	path, err := authPath()
	if err != nil {
		return doctorCheck{
			Name:   "config-dir",
			Status: statusFail,
			Detail: err.Error(),
			Hint:   "set $XDG_CONFIG_HOME or $HOME to a writable directory",
		}
	}
	csDir := filepath.Dir(path)
	// Operate directly (write+cleanup) rather than stat-then-write to avoid a
	// TOCTOU race; best-effort cleanup on success.
	sentinel := filepath.Join(csDir, ".doctor-write-probe")
	if err := os.WriteFile(sentinel, []byte("ok"), 0o600); err != nil {
		return doctorCheck{
			Name:   "config-dir",
			Status: statusFail,
			Detail: err.Error(),
			Hint:   fmt.Sprintf("ensure %s is writable", csDir),
		}
	}
	_ = os.Remove(sentinel)
	return doctorCheck{
		Name:   "config-dir",
		Status: statusPass,
		Detail: csDir,
	}
}

// runDoctorRuntimeProbe runs the http-handler scaffold through the same
// `cs-js` runtime path used by `cs fn test`. It's the closest thing to a
// parity smoke check we can do without an external dependency.
func runDoctorRuntimeProbe(ctx context.Context) error {
	files, err := templates.Render(defaultTemplate, templates.Variables{Name: "doctor"})
	if err != nil {
		return fmt.Errorf("render canary template: %w", err)
	}
	functionJS, ok := files["function.js"]
	if !ok {
		return fmt.Errorf("canary template missing function.js")
	}
	manifestRaw, ok := files["manifest.json"]
	if !ok {
		return fmt.Errorf("canary template missing manifest.json")
	}
	bundleBytes, _, _, err := bundle.BuildCanonical(map[string][]byte{
		"function.js":   functionJS,
		"manifest.json": manifestRaw,
	})
	if err != nil {
		return fmt.Errorf("build canary bundle: %w", err)
	}
	r := runtime.NewRunner(runtime.NewMemoryKV(), runtime.NopCodeQ{}, 256*1024, 64*1024, 1024*1024)
	out := r.Execute(ctx, bundleBytes, api.InvocationRequest{
		ActivationID: uuid.NewString(),
		RequestID:    "req_doctor_" + uuid.NewString(),
		Tenant:       "t_doctor",
		Namespace:    "doctor",
		Ref:          api.FunctionRef{Function: "doctor", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		Principal:    api.Principal{Sub: "doctor", Roles: []string{"role:app"}},
		DeadlineMS:   time.Now().Add(2 * time.Second).UnixMilli(),
		Event:        map[string]any{"doctor": true},
	})
	if out.Status != "success" {
		return fmt.Errorf("runtime returned status=%s", out.Status)
	}
	return nil
}
