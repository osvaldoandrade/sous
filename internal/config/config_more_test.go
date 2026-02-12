package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validConfigForTests() Config {
	var cfg Config
	cfg.ClusterName = "cs"
	cfg.Environment = "prod"

	cfg.Plugins.AuthN.Driver = "tikti"
	cfg.Plugins.AuthN.Tikti.IntrospectionURL = "https://tikti.example.com/introspect"
	cfg.Plugins.AuthN.Tikti.CacheTTLSeconds = 60

	cfg.Plugins.Persistence.Driver = "kvrocks"
	cfg.Plugins.Persistence.KVRocks.Addr = "kvrocks:6666"
	cfg.Plugins.Persistence.KVRocks.Auth.Mode = "none"

	cfg.Plugins.Messaging.Driver = "codeq"
	cfg.Plugins.Messaging.CodeQ.Brokers = []string{"codeq:9092"}
	cfg.Plugins.Messaging.CodeQ.Topics.Invoke = "cs.invoke"
	cfg.Plugins.Messaging.CodeQ.Topics.Results = "cs.results"

	cfg.CSControl.HTTP.Addr = ":8080"
	cfg.CSHTTPGateway.HTTP.Addr = ":8081"
	cfg.CSInvokerPool.HTTP.Addr = ":8082"
	cfg.CSControl.Limits.MaxBundleBytes = 1024
	cfg.CSControl.Limits.DraftTTLSeconds = 60
	cfg.CSControl.Limits.ActTTLSeconds = 120
	return cfg
}

func TestValidateBranches(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Config)
		want string
	}{
		{"missing cluster", func(c *Config) { c.ClusterName = "" }, "cluster_name is required"},
		{"missing environment", func(c *Config) { c.Environment = "" }, "environment is required"},
		{"unsupported authn", func(c *Config) { c.Plugins.AuthN.Driver = "x" }, "unsupported authn plugin driver"},
		{"missing tikti url", func(c *Config) { c.Plugins.AuthN.Tikti.IntrospectionURL = "" }, "plugins.authn.tikti.introspection_url is required"},
		{"unsupported persistence", func(c *Config) { c.Plugins.Persistence.Driver = "x" }, "unsupported persistence plugin driver"},
		{"missing kv addr", func(c *Config) { c.Plugins.Persistence.KVRocks.Addr = "" }, "plugins.persistence.kvrocks.addr is required"},
		{"unsupported messaging", func(c *Config) { c.Plugins.Messaging.Driver = "x" }, "unsupported messaging plugin driver"},
		{"missing brokers", func(c *Config) { c.Plugins.Messaging.CodeQ.Brokers = nil }, "plugins.messaging.codeq.brokers is required"},
		{"missing topics", func(c *Config) { c.Plugins.Messaging.CodeQ.Topics.Invoke = "" }, "plugins.messaging.codeq.topics.invoke and results are required"},
		{"missing cs_control addr", func(c *Config) { c.CSControl.HTTP.Addr = "" }, "cs_control.http.addr is required"},
		{"missing cs_http_gateway addr", func(c *Config) { c.CSHTTPGateway.HTTP.Addr = "" }, "cs_http_gateway.http.addr is required"},
		{"missing cs_invoker_pool addr", func(c *Config) { c.CSInvokerPool.HTTP.Addr = "" }, "cs_invoker_pool.http.addr is required"},
		{"invalid max bundle", func(c *Config) { c.CSControl.Limits.MaxBundleBytes = 0 }, "max_bundle_bytes must be > 0"},
		{"invalid draft ttl", func(c *Config) { c.CSControl.Limits.DraftTTLSeconds = 0 }, "draft_ttl_seconds must be > 0"},
		{"invalid act ttl", func(c *Config) { c.CSControl.Limits.ActTTLSeconds = 0 }, "activation_ttl_seconds must be > 0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConfigForTests()
			tc.edit(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate error = %v, want contains %q", err, tc.want)
			}
		})
	}

	cfg := validConfigForTests()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid config should pass, got %v", err)
	}
}

func TestOverrideEnvAndSyncPluginConfig(t *testing.T) {
	cfg := Config{}
	cfg.ClusterName = "orig"
	cfg.Environment = "dev"
	cfg.KVRocks.Addr = "kvrocks:1"
	cfg.KVRocks.Auth.Password = "p1"
	cfg.CodeQ.Brokers = []string{"b1"}
	cfg.Tikti.IntrospectionURL = "http://tikti-old"
	cfg.Tikti.CacheTTLSeconds = 30

	t.Setenv("CS_CLUSTER_NAME", "from-env-cluster")
	t.Setenv("CS_ENVIRONMENT", "from-env-env")
	t.Setenv("CS_KVROCKS_ADDR", "127.0.0.1:6666")
	t.Setenv("CS_KVROCKS_PASSWORD", "secret")
	t.Setenv("CS_CODEQ_BROKERS", "b2, b3")
	t.Setenv("CS_TIKTI_INTROSPECTION_URL", "https://tikti.example.com/introspect")
	t.Setenv("CS_TIKTI_CACHE_TTL_SECONDS", "90")
	overrideEnv(&cfg)

	if cfg.ClusterName != "from-env-cluster" || cfg.Environment != "from-env-env" {
		t.Fatalf("unexpected cluster/env override: %+v", cfg)
	}
	if cfg.KVRocks.Addr != "127.0.0.1:6666" || cfg.Plugins.Persistence.KVRocks.Addr != "127.0.0.1:6666" {
		t.Fatalf("unexpected kvrocks override: %+v", cfg.Plugins.Persistence.KVRocks)
	}
	if cfg.KVRocks.Auth.Password != "secret" || cfg.Plugins.Persistence.KVRocks.Auth.Password != "secret" {
		t.Fatalf("unexpected kvrocks password override")
	}
	if len(cfg.CodeQ.Brokers) != 2 || cfg.CodeQ.Brokers[0] != "b2" || len(cfg.Plugins.Messaging.CodeQ.Brokers) != 2 {
		t.Fatalf("unexpected brokers override: %+v %+v", cfg.CodeQ.Brokers, cfg.Plugins.Messaging.CodeQ.Brokers)
	}
	if cfg.Tikti.IntrospectionURL == "" || cfg.Plugins.AuthN.Tikti.IntrospectionURL == "" {
		t.Fatalf("unexpected tikti override: %+v %+v", cfg.Tikti, cfg.Plugins.AuthN.Tikti)
	}
	if cfg.Tikti.CacheTTLSeconds != 90 || cfg.Plugins.AuthN.Tikti.CacheTTLSeconds != 90 {
		t.Fatalf("unexpected tikti ttl override: %+v %+v", cfg.Tikti, cfg.Plugins.AuthN.Tikti)
	}

	// Invalid env TTL should be ignored.
	t.Setenv("CS_TIKTI_CACHE_TTL_SECONDS", "nope")
	cfg.Tikti.CacheTTLSeconds = 45
	cfg.Plugins.AuthN.Tikti.CacheTTLSeconds = 45
	overrideEnv(&cfg)
	if cfg.Tikti.CacheTTLSeconds != 45 || cfg.Plugins.AuthN.Tikti.CacheTTLSeconds != 45 {
		t.Fatalf("invalid env TTL should be ignored, got %+v %+v", cfg.Tikti, cfg.Plugins.AuthN.Tikti)
	}

	// Sync plugin config should fill defaults and hydrate legacy fields back.
	cfg = Config{}
	cfg.ClusterName = "cs"
	cfg.Environment = "prod"
	cfg.Tikti.IntrospectionURL = "https://tikti.example.com/introspect"
	cfg.Tikti.CacheTTLSeconds = 70
	cfg.KVRocks.Addr = "kvrocks:6666"
	cfg.KVRocks.Auth.Mode = "none"
	cfg.KVRocks.Auth.Password = "pw"
	cfg.CodeQ.Brokers = []string{"codeq:9092"}
	cfg.CodeQ.Topics.Invoke = "cs.invoke"
	cfg.CodeQ.Topics.Results = "cs.results"
	cfg.CodeQ.Topics.DLQInvoke = "cs.dlq.invoke"
	cfg.CodeQ.Topics.DLQResult = "cs.dlq.results"

	syncPluginConfig(&cfg)
	if cfg.Plugins.AuthN.Driver != "tikti" || cfg.Plugins.Persistence.Driver != "kvrocks" || cfg.Plugins.Messaging.Driver != "codeq" {
		t.Fatalf("default drivers not set: %+v", cfg.Plugins)
	}
	if cfg.Plugins.AuthN.Tikti.IntrospectionURL == "" || cfg.Plugins.Persistence.KVRocks.Addr == "" || len(cfg.Plugins.Messaging.CodeQ.Brokers) == 0 {
		t.Fatalf("plugin hydration failed: %+v", cfg.Plugins)
	}
}

func TestLoadErrors(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("expected read error for missing config")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("{bad_yaml"), 0o600); err != nil {
		t.Fatalf("write bad yaml: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected yaml unmarshal error")
	}
}
