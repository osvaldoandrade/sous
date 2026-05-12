package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeRuntimeProbe is a stand-in for the cs-js parity smoke check so tests
// don't need to run the embedded runtime on every assertion.
func fakeRuntimeProbe(err error) func(ctx context.Context) error {
	return func(ctx context.Context) error { return err }
}

func newHealthyHealthz(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
}

func newDegradedHealthz(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
}

func TestProbeAuthMissingConfig(t *testing.T) {
	check, _ := probeAuth(func() (authConfig, error) {
		return authConfig{}, errors.New("auth config not found")
	})
	if check.Status != statusFail {
		t.Fatalf("status = %v, want fail", check.Status)
	}
	if !strings.Contains(check.Hint, "cs auth login") {
		t.Fatalf("hint should mention cs auth login, got %q", check.Hint)
	}
}

func TestProbeAuthValidConfig(t *testing.T) {
	check, cfg := probeAuth(func() (authConfig, error) {
		return authConfig{APIURL: "http://localhost:8080", Tenant: "t1", Token: "tok"}, nil
	})
	if check.Status != statusPass {
		t.Fatalf("status = %v, want pass", check.Status)
	}
	if cfg.Tenant != "t1" {
		t.Fatalf("returned cfg tenant = %s, want t1", cfg.Tenant)
	}
}

func TestProbeControlPlaneReachable(t *testing.T) {
	srv := newHealthyHealthz(t)
	defer srv.Close()
	check := probeControlPlane(srv.Client(), srv.URL)
	if check.Status != statusPass {
		t.Fatalf("status = %v, want pass; detail=%s hint=%s", check.Status, check.Detail, check.Hint)
	}
}

func TestProbeControlPlaneUnreachable(t *testing.T) {
	client := &http.Client{Timeout: 200 * time.Millisecond}
	// Port 1 is reserved and refuses connections — no listener should exist.
	check := probeControlPlane(client, "http://127.0.0.1:1")
	if check.Status != statusFail {
		t.Fatalf("status = %v, want fail", check.Status)
	}
	if !strings.Contains(check.Hint, "cs-control") {
		t.Fatalf("hint should reference cs-control, got %q", check.Hint)
	}
}

func TestProbeControlPlaneDegraded(t *testing.T) {
	srv := newDegradedHealthz(t)
	defer srv.Close()
	check := probeControlPlane(srv.Client(), srv.URL)
	if check.Status != statusFail {
		t.Fatalf("status = %v, want fail", check.Status)
	}
	if !strings.Contains(check.Detail, "500") {
		t.Fatalf("detail should reference HTTP status 500, got %q", check.Detail)
	}
}

func TestProbeRuntime(t *testing.T) {
	pass := probeRuntime(fakeRuntimeProbe(nil))
	if pass.Status != statusPass {
		t.Fatalf("pass status = %v", pass.Status)
	}
	fail := probeRuntime(fakeRuntimeProbe(errors.New("explode")))
	if fail.Status != statusFail {
		t.Fatalf("fail status = %v", fail.Status)
	}
	if !strings.Contains(fail.Detail, "explode") {
		t.Fatalf("detail should propagate error, got %q", fail.Detail)
	}
}

func TestProbeConfigDirWritable(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("HOME", root)
	check := probeConfigDir()
	if check.Status != statusPass {
		t.Fatalf("status = %v, want pass; detail=%s", check.Status, check.Detail)
	}
	if _, err := os.Stat(filepath.Join(root, "code-sous")); err != nil {
		t.Fatalf("config dir should exist after probe: %v", err)
	}
}

func TestRunDoctorHealthyExitsZero(t *testing.T) {
	srv := newHealthyHealthz(t)
	defer srv.Close()

	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("HOME", root)
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t1", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var out bytes.Buffer
	err := runDoctor(doctorOptions{
		APIClient:    srv.Client(),
		runtimeProbe: fakeRuntimeProbe(nil),
	}, &out)
	if err != nil {
		t.Fatalf("runDoctor: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "pass") {
		t.Fatalf("expected at least one 'pass' row; got:\n%s", out.String())
	}
}

func TestRunDoctorUnreachableControlPlaneFails(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("HOME", root)
	if err := saveAuthConfig(authConfig{APIURL: "http://127.0.0.1:1", Tenant: "t1", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var out bytes.Buffer
	err := runDoctor(doctorOptions{
		Timeout:      200 * time.Millisecond,
		runtimeProbe: fakeRuntimeProbe(nil),
	}, &out)
	if err == nil {
		t.Fatalf("expected runDoctor to fail when control plane is unreachable\noutput:\n%s", out.String())
	}
	var cli *cliError
	if !errors.As(err, &cli) {
		t.Fatalf("expected *cliError, got %T (%v)", err, err)
	}
	if cli.ExitCode() != 2 {
		t.Fatalf("expected server-error exit code (2), got %d", cli.ExitCode())
	}
	if !strings.Contains(err.Error(), "cs-control") {
		t.Fatalf("error should mention cs-control hint, got:\n%s", err.Error())
	}
}

func TestRunDoctorMissingAuthFails(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("HOME", root)

	srv := newHealthyHealthz(t)
	defer srv.Close()

	var out bytes.Buffer
	err := runDoctor(doctorOptions{
		APIURL:       srv.URL,
		APIClient:    srv.Client(),
		runtimeProbe: fakeRuntimeProbe(nil),
	}, &out)
	if err == nil {
		t.Fatalf("expected runDoctor to fail when auth is missing\noutput:\n%s", out.String())
	}
	var cli *cliError
	if !errors.As(err, &cli) {
		t.Fatalf("expected *cliError, got %T (%v)", err, err)
	}
	if cli.ExitCode() != 1 {
		t.Fatalf("expected client-error exit code (1), got %d", cli.ExitCode())
	}
}

func TestRunDoctorRuntimeFailureMapsToExitThree(t *testing.T) {
	srv := newHealthyHealthz(t)
	defer srv.Close()
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("HOME", root)
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t1", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var out bytes.Buffer
	err := runDoctor(doctorOptions{
		APIClient:    srv.Client(),
		runtimeProbe: fakeRuntimeProbe(errors.New("v8 crashed")),
	}, &out)
	if err == nil {
		t.Fatalf("expected runDoctor to fail when runtime probe fails\noutput:\n%s", out.String())
	}
	var cli *cliError
	if !errors.As(err, &cli) {
		t.Fatalf("expected *cliError, got %T (%v)", err, err)
	}
	if cli.ExitCode() != 3 {
		t.Fatalf("expected runtime-error exit code (3), got %d", cli.ExitCode())
	}
}

func TestRunDoctorJSONOutput(t *testing.T) {
	srv := newHealthyHealthz(t)
	defer srv.Close()
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("HOME", root)
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t1", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var out bytes.Buffer
	if err := runDoctor(doctorOptions{
		APIClient:    srv.Client(),
		JSON:         true,
		runtimeProbe: fakeRuntimeProbe(nil),
	}, &out); err != nil {
		t.Fatalf("runDoctor: %v\n%s", err, out.String())
	}
	var report doctorReport
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, out.String())
	}
	if len(report.Checks) < 4 {
		t.Fatalf("expected at least four checks, got %d", len(report.Checks))
	}
	want := map[string]bool{"auth": false, "control-plane": false, "runtime": false, "config-dir": false}
	for _, c := range report.Checks {
		if _, ok := want[c.Name]; ok {
			want[c.Name] = true
		}
	}
	for name, present := range want {
		if !present {
			t.Fatalf("JSON output missing check %q", name)
		}
	}
}

func TestHandleDoctorFlagParse(t *testing.T) {
	// handleDoctor should reject unknown flags so users notice typos.
	err := handleDoctor([]string{"--no-such-flag"})
	if err == nil {
		t.Fatal("expected flag parse error")
	}
}

func TestRunDoctorRuntimeProbeIntegratesReal(t *testing.T) {
	// Sanity-check the real probe at least once so future drift in the
	// embedded template surfaces in CI.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runDoctorRuntimeProbe(ctx); err != nil {
		t.Fatalf("real runtime probe failed: %v", err)
	}
}

func TestRunDoctorAPIURLOverride(t *testing.T) {
	// Use a server that returns a marker on /healthz so we can confirm the
	// override is honoured even when auth config carries a different URL.
	hit := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			hit = true
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("HOME", root)
	if err := saveAuthConfig(authConfig{APIURL: "http://example.invalid", Tenant: "t1", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	var out bytes.Buffer
	if err := runDoctor(doctorOptions{
		APIURL:       srv.URL,
		APIClient:    srv.Client(),
		runtimeProbe: fakeRuntimeProbe(nil),
	}, &out); err != nil {
		t.Fatalf("runDoctor: %v\n%s", err, out.String())
	}
	if !hit {
		t.Fatal("control-plane probe should have hit the override URL")
	}
}

func TestWriteDoctorTable(t *testing.T) {
	var buf bytes.Buffer
	writeDoctorTable(&buf, []doctorCheck{
		{Name: "auth", Status: statusPass, Detail: "tenant=t1"},
		{Name: "control-plane", Status: statusFail, Detail: "down", Hint: "start it"},
	})
	got := buf.String()
	if !strings.Contains(got, "auth") || !strings.Contains(got, "pass") {
		t.Fatalf("table missing pass row:\n%s", got)
	}
	if !strings.Contains(got, "start it") {
		t.Fatalf("table missing hint:\n%s", got)
	}
}

// Sanity check: dispatcher must surface a non-zero exit when doctor fails.
func TestDoctorRegisteredInDispatcher(t *testing.T) {
	// Seed a valid auth config + healthy /healthz so the only failure is the
	// control-plane probe; that lets us assert the dispatcher path maps to
	// a non-zero exit without needing real cs-control.
	root := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", root)
	t.Setenv("HOME", root)
	srv := newHealthyHealthz(t)
	defer srv.Close()
	if err := saveAuthConfig(authConfig{APIURL: srv.URL, Tenant: "t1", Token: "tok"}); err != nil {
		t.Fatalf("saveAuthConfig: %v", err)
	}

	// We invoke handleDoctor directly (the dispatcher branch is a one-line
	// case in main.go that simply forwards to handleDoctor). The unreachable
	// URL exercises the failure path.
	err := handleDoctor([]string{"--api-url", "http://127.0.0.1:1", "--timeout-ms", "100"})
	if err == nil {
		t.Fatal("expected unreachable doctor to fail")
	}
	code := exitCodeFor(err)
	if code == 0 {
		t.Fatalf("expected non-zero exit code, got %d", code)
	}
	if !strings.Contains(err.Error(), "control-plane probe failed") {
		t.Fatalf("error should mention control-plane diagnosis: %s", err.Error())
	}
}
