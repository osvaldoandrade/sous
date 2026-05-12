package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/bundle"
	"github.com/osvaldoandrade/sous/internal/cli/templates"
	"github.com/osvaldoandrade/sous/internal/runtime"
)

func TestTemplatesListAndDescriptions(t *testing.T) {
	names := templates.Names()
	want := []string{"cadence-activity", "codeq-consumer", "http-handler", "scheduled-job"}
	if len(names) != len(want) {
		t.Fatalf("got %v want %v", names, want)
	}
	for i, n := range want {
		if names[i] != n {
			t.Fatalf("Names[%d]=%q want %q", i, names[i], n)
		}
		if d := templates.Description(n); d == "" {
			t.Fatalf("template %q missing description", n)
		}
	}
	if templates.Exists("does-not-exist") {
		t.Fatal("unexpected template exists")
	}
}

func TestFnInitListFlag(t *testing.T) {
	setupConfigHome(t)
	stdout := captureStdout(t, func() {
		if err := fnInit([]string{"--list"}); err != nil {
			t.Fatalf("fnInit --list: %v", err)
		}
	})
	for _, want := range []string{"http-handler", "scheduled-job", "cadence-activity", "codeq-consumer"} {
		if !strings.Contains(stdout, want) {
			t.Fatalf("--list output missing %q: %s", want, stdout)
		}
	}
}

func TestFnInitUnknownTemplate(t *testing.T) {
	setupConfigHome(t)
	dir := filepath.Join(t.TempDir(), "fn")
	err := fnInit([]string{"--template", "nope", dir})
	if err == nil {
		t.Fatal("expected unknown template error")
	}
	if !strings.Contains(err.Error(), "unknown template") {
		t.Fatalf("error %v missing 'unknown template'", err)
	}
	if !strings.Contains(err.Error(), "http-handler") {
		t.Fatalf("error %v should list available templates", err)
	}
}

func TestFnInitDefaultTemplateMatchesLegacyShape(t *testing.T) {
	setupConfigHome(t)
	dir := filepath.Join(t.TempDir(), "fn_default")
	if err := fnInit([]string{dir}); err != nil {
		t.Fatalf("fnInit default: %v", err)
	}
	for _, name := range []string{"function.js", "manifest.json", "README.md"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"name": "fn_default"`) {
		t.Fatalf("manifest missing substituted name: %s", string(raw))
	}
	if strings.Contains(string(raw), "{{.Name}}") {
		t.Fatalf("manifest still has unsubstituted token: %s", string(raw))
	}
}

// TestFnInitFlagsAfterPositional checks the acceptance criterion that
// `cs fn init <name> --template <name>` parses identically to flags-first.
func TestFnInitFlagsAfterPositional(t *testing.T) {
	setupConfigHome(t)
	dir := filepath.Join(t.TempDir(), "fn_after")
	if err := fnInit([]string{dir, "--template", "scheduled-job"}); err != nil {
		t.Fatalf("fnInit: %v", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"jobs.scheduled.*"`) {
		t.Fatalf("expected scheduled-job manifest, got: %s", string(raw))
	}
}

// TestEveryTemplateBundleAndRun walks every embedded template through
// bundle.BuildCanonical and the in-process runner with a representative event
// and asserts status=success plus the manifest snapshot fields.
func TestEveryTemplateBundleAndRun(t *testing.T) {
	for _, name := range templates.Names() {
		name := name
		t.Run(name, func(t *testing.T) {
			rendered, err := templates.Render(name, templates.Variables{Name: name})
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			files := map[string][]byte{
				"function.js":   rendered["function.js"],
				"manifest.json": rendered["manifest.json"],
			}
			if len(files["function.js"]) == 0 || len(files["manifest.json"]) == 0 {
				t.Fatalf("template %q missing required files", name)
			}

			// Snapshot manifest shape.
			manifest, err := api.ParseManifest(files["manifest.json"])
			if err != nil {
				t.Fatalf("ParseManifest: %v", err)
			}
			if manifest.Runtime != "cs-js" {
				t.Fatalf("runtime=%s", manifest.Runtime)
			}
			if manifest.Entry != "function.js" {
				t.Fatalf("entry=%s", manifest.Entry)
			}
			if manifest.Handler != "default" {
				t.Fatalf("handler=%s", manifest.Handler)
			}
			if manifest.Limits.TimeoutMS < 1 || manifest.Limits.MemoryMB < 16 || manifest.Limits.MaxConcurrency < 1 {
				t.Fatalf("limits invalid: %+v", manifest.Limits)
			}

			bundleBytes, _, _, err := bundle.BuildCanonical(files)
			if err != nil {
				t.Fatalf("BuildCanonical: %v", err)
			}

			r := runtime.NewRunner(runtime.NewMemoryKV(), runtime.NopCodeQ{}, 256*1024, 64*1024, 1024*1024)
			event := sampleEventForTemplate(name)
			out := r.Execute(context.Background(), bundleBytes, api.InvocationRequest{
				ActivationID: uuid.NewString(),
				RequestID:    "req_" + uuid.NewString(),
				Tenant:       "t_local",
				Namespace:    "local",
				Ref:          api.FunctionRef{Function: name, Version: 1},
				Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
				Principal:    api.Principal{Sub: "cli", Roles: []string{"role:app"}},
				DeadlineMS:   time.Now().Add(5 * time.Second).UnixMilli(),
				Event:        event,
			})
			if out.Status != "success" {
				raw, _ := json.MarshalIndent(out, "", "  ")
				t.Fatalf("status=%s expected success: %s", out.Status, string(raw))
			}
		})
	}
}

func sampleEventForTemplate(name string) map[string]any {
	switch name {
	case "cadence-activity":
		return map[string]any{
			"domain":       "payments",
			"tasklist":     "payments-activities",
			"workflowId":   "wf_1",
			"runId":        "run_1",
			"activityType": "SousInvokeActivity",
			"input":        map[string]any{"id": "o_1"},
		}
	case "codeq-consumer":
		return map[string]any{"id": "msg_1", "payload": map[string]any{"k": "v"}}
	case "scheduled-job":
		return map[string]any{"every_seconds": 30}
	default:
		return map[string]any{"method": "POST", "path": "/x", "body": map[string]any{"k": "v"}}
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	rPipe, wPipe, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = wPipe
	done := make(chan string)
	go func() {
		buf := make([]byte, 4096)
		var collected strings.Builder
		for {
			n, err := rPipe.Read(buf)
			if n > 0 {
				collected.Write(buf[:n])
			}
			if err != nil {
				break
			}
		}
		done <- collected.String()
	}()
	fn()
	_ = wPipe.Close()
	os.Stdout = old
	return <-done
}
