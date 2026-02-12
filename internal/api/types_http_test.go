package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidationHelpers(t *testing.T) {
	if err := ValidateTenant("t_abc123"); err != nil {
		t.Fatalf("tenant should be valid: %v", err)
	}
	if err := ValidateTenant("bad"); err == nil {
		t.Fatal("expected invalid tenant")
	}
	if err := ValidateNamespace("payments"); err != nil {
		t.Fatalf("namespace should be valid: %v", err)
	}
	if err := ValidateNamespace("1bad"); err == nil {
		t.Fatal("expected invalid namespace")
	}
	if err := ValidateFunction("reconcile"); err != nil {
		t.Fatalf("function should be valid: %v", err)
	}
	if err := ValidateFunction("A"); err == nil {
		t.Fatal("expected invalid function")
	}
	if err := ValidateAlias("prod"); err != nil {
		t.Fatalf("alias should be valid: %v", err)
	}
	if err := ValidateAlias("BAD ALIAS"); err == nil {
		t.Fatal("expected invalid alias")
	}
	if err := ValidateAlias(""); err != nil {
		t.Fatalf("empty alias should be accepted: %v", err)
	}
}

func TestManifestParseValidate(t *testing.T) {
	okManifest := []byte(`{
		"schema":"cs.function.script.v1",
		"runtime":"cs-js",
		"entry":"function.js",
		"handler":"default",
		"limits":{"timeoutMs":1000,"memoryMb":64,"maxConcurrency":1},
		"capabilities":{
			"kv":{"prefixes":["ctr:"],"ops":["get"]},
			"codeq":{"publishTopics":["jobs.*"]},
			"http":{"allowHosts":["example.com"],"timeoutMs":1000}
		}
	}`)
	if _, err := ParseManifest(okManifest); err != nil {
		t.Fatalf("expected valid manifest: %v", err)
	}

	badSchema := bytes.ReplaceAll(okManifest, []byte("cs.function.script.v1"), []byte("x"))
	if _, err := ParseManifest(badSchema); err == nil {
		t.Fatal("expected schema error")
	}

	badRuntime := bytes.ReplaceAll(okManifest, []byte("cs-js"), []byte("x"))
	if _, err := ParseManifest(badRuntime); err == nil {
		t.Fatal("expected runtime error")
	}

	badHandler := bytes.ReplaceAll(okManifest, []byte("default"), []byte("x"))
	if _, err := ParseManifest(badHandler); err == nil {
		t.Fatal("expected handler error")
	}

	badEntry := bytes.ReplaceAll(okManifest, []byte("function.js"), []byte("function bad.js"))
	if _, err := ParseManifest(badEntry); err == nil {
		t.Fatal("expected invalid entry path error")
	}

	noCapabilities := []byte(`{
		"schema":"cs.function.script.v1",
		"runtime":"cs-js",
		"entry":"function.js",
		"handler":"default",
		"limits":{"timeoutMs":1000,"memoryMb":64,"maxConcurrency":1}
	}`)
	if _, err := ParseManifest(noCapabilities); err == nil {
		t.Fatal("expected missing capabilities error")
	}

	badKVOp := bytes.ReplaceAll(okManifest, []byte(`"ops":["get"]`), []byte(`"ops":["exec"]`))
	if _, err := ParseManifest(badKVOp); err == nil {
		t.Fatal("expected invalid kv op error")
	}

	badHTTPTimeout := bytes.ReplaceAll(okManifest, []byte(`"timeoutMs":1000`), []byte(`"timeoutMs":0`))
	if _, err := ParseManifest(badHTTPTimeout); err == nil {
		t.Fatal("expected invalid http timeout error")
	}
}

func TestValidateResultShape(t *testing.T) {
	if err := ValidateResultShape(nil); err != nil {
		t.Fatalf("nil should be accepted: %v", err)
	}
	if err := ValidateResultShape(&FunctionResponse{StatusCode: 200, Headers: map[string]string{"x": "1"}}); err != nil {
		t.Fatalf("expected valid response: %v", err)
	}
	if err := ValidateResultShape(&FunctionResponse{StatusCode: 99}); err == nil {
		t.Fatal("expected status error")
	}
	if err := ValidateResultShape(&FunctionResponse{StatusCode: 200, Headers: map[string]string{"": "1"}}); err == nil {
		t.Fatal("expected header key error")
	}
}

func TestIntersectsRoles(t *testing.T) {
	principal := Principal{Roles: []string{"role:a", "role:b"}}
	if !IntersectsRoles([]string{"role:c", "role:b"}, principal) {
		t.Fatal("expected true intersection")
	}
	if IntersectsRoles([]string{"role:c"}, principal) {
		t.Fatal("expected false intersection")
	}
	if IntersectsRoles(nil, principal) {
		t.Fatal("empty required should be false")
	}
}

func TestReadWriteJSON(t *testing.T) {
	reqBody := bytes.NewBufferString(`{"x":1}`)
	req := httptest.NewRequest(http.MethodPost, "/", reqBody)
	var payload map[string]any
	if err := ReadJSON(req, &payload); err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if payload["x"].(float64) != 1 {
		t.Fatalf("unexpected payload: %+v", payload)
	}

	badReq := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(`{"x":1,"y":2}`))
	var strict struct {
		X int `json:"x"`
	}
	if err := ReadJSON(badReq, &strict); err == nil {
		t.Fatal("expected unknown field error")
	}

	rr := httptest.NewRecorder()
	WriteJSON(rr, http.StatusCreated, map[string]any{"ok": true})
	if rr.Code != http.StatusCreated {
		t.Fatalf("unexpected status: %d", rr.Code)
	}
	var out map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out["ok"] != true {
		t.Fatalf("unexpected body: %+v", out)
	}
}
