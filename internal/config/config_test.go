package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadRejectsMissingRequiredFields(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("cluster_name: cs\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestLoadWithEnvOverride(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := `
cluster_name: cs
environment: prod
kvrocks:
  addr: kvrocks:6666
  auth:
    mode: none
    password: ""
codeq:
  brokers: ["codeq:9092"]
  topics:
    invoke: cs.invoke
    results: cs.results
    dlq_invoke: cs.dlq.invoke
    dlq_results: cs.dlq.results
tikti:
  introspection_url: https://tikti.example.com/introspect
  cache_ttl_seconds: 60
cs_control:
  http:
    addr: :8080
  limits:
    max_bundle_bytes: 16777216
    draft_ttl_seconds: 86400
    activation_ttl_seconds: 604800
cs_http_gateway:
  http:
    addr: :8081
  limits:
    max_body_bytes: 1
    max_header_bytes: 1
    max_query_bytes: 1
  rate_limits:
    tenant_rps: 1
    function_rps: 1
cs_invoker_pool:
  http:
    addr: :8082
  workers:
    threads: 1
    max_inflight: 1
  cache:
    bundles_max: 1
    bytes_max: 1
  limits:
    max_result_bytes: 1
    max_error_bytes: 1
    max_log_bytes: 1
cs_scheduler:
  tick_ms: 1000
  max_catchup_ticks: 60
  leader_election:
    enabled: true
    lease_name: cs-scheduler
cs_cadence_poller:
  cadence:
    addr: code-flow:7933
  refresh_seconds: 10
  heartbeat:
    max_per_second: 2
  limits:
    max_inflight_tasks_default: 256
`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CS_KVROCKS_ADDR", "127.0.0.1:6666")
	t.Setenv("CS_TIKTI_API_KEY", "tikti_api_key_env")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.KVRocks.Addr != "127.0.0.1:6666" {
		t.Fatalf("env override did not apply, got %q", cfg.KVRocks.Addr)
	}
	if cfg.Plugins.Persistence.KVRocks.Addr != "127.0.0.1:6666" {
		t.Fatalf("plugin env override did not apply, got %q", cfg.Plugins.Persistence.KVRocks.Addr)
	}
	if cfg.Plugins.AuthN.Driver != "tikti" {
		t.Fatalf("expected authn driver tikti, got %q", cfg.Plugins.AuthN.Driver)
	}
	if cfg.Tikti.APIKey != "tikti_api_key_env" || cfg.Plugins.AuthN.Tikti.APIKey != "tikti_api_key_env" {
		t.Fatalf("expected tikti api key override, got %+v %+v", cfg.Tikti, cfg.Plugins.AuthN.Tikti)
	}
}

func TestLoadPluginOnlyConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	data := `
cluster_name: cs
environment: prod
plugins:
  authn:
    driver: tikti
    tikti:
      introspection_url: https://tikti.example.com/introspect
      cache_ttl_seconds: 60
  persistence:
    driver: kvrocks
    kvrocks:
      addr: kvrocks:6666
      auth:
        mode: none
        password: ""
  messaging:
    driver: codeq
    codeq:
      brokers: ["codeq:9092"]
      topics:
        invoke: cs.invoke
        results: cs.results
        dlq_invoke: cs.dlq.invoke
        dlq_results: cs.dlq.results
cs_control:
  http:
    addr: :8080
  limits:
    max_bundle_bytes: 16777216
    draft_ttl_seconds: 86400
    activation_ttl_seconds: 604800
cs_http_gateway:
  http:
    addr: :8081
  limits:
    max_body_bytes: 1
    max_header_bytes: 1
    max_query_bytes: 1
  rate_limits:
    tenant_rps: 1
    function_rps: 1
cs_invoker_pool:
  http:
    addr: :8082
  workers:
    threads: 1
    max_inflight: 1
  cache:
    bundles_max: 1
    bytes_max: 1
  limits:
    max_result_bytes: 1
    max_error_bytes: 1
    max_log_bytes: 1
cs_scheduler:
  tick_ms: 1000
  max_catchup_ticks: 60
  leader_election:
    enabled: true
    lease_name: cs-scheduler
cs_cadence_poller:
  cadence:
    addr: code-flow:7933
  refresh_seconds: 10
  heartbeat:
    max_per_second: 2
  limits:
    max_inflight_tasks_default: 256
`
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Tikti.IntrospectionURL == "" || cfg.KVRocks.Addr == "" || len(cfg.CodeQ.Brokers) == 0 {
		t.Fatal("expected legacy fields to be hydrated from plugin config")
	}
}
