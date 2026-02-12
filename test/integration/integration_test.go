//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

func TestPublishAndInvokeAPI(t *testing.T) {
	baseURL := requiredEnv(t, "CS_INTEGRATION_API_URL")
	tenant := requiredEnv(t, "CS_INTEGRATION_TENANT")
	token := requiredEnv(t, "CS_INTEGRATION_TOKEN")
	namespace := requiredEnv(t, "CS_INTEGRATION_NAMESPACE")

	fnName := fmt.Sprintf("it_fn_%d", time.Now().UnixNano())
	createURL := fmt.Sprintf("%s/v1/tenants/%s/namespaces/%s/functions", baseURL, tenant, namespace)
	createBody := map[string]any{"name": fnName, "runtime": "cs-js", "entry": "function.js", "handler": "default"}
	doJSON(t, token, http.MethodPost, createURL, createBody)
}

func requiredEnv(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Fatalf("missing env: %s", key)
	}
	return v
}

func doJSON(t *testing.T, token, method, url string, body any) []byte {
	t.Helper()
	raw, _ := json.Marshal(body)
	req, err := http.NewRequest(method, url, bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	respRaw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		t.Fatalf("request failed status=%d body=%s", resp.StatusCode, string(respRaw))
	}
	return respRaw
}
