package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ClusterName string `yaml:"cluster_name"`
	Environment string `yaml:"environment"`

	// Legacy fields kept for backward compatibility.
	KVRocks struct {
		Addr string `yaml:"addr"`
		Auth struct {
			Mode     string `yaml:"mode"`
			Password string `yaml:"password"`
		} `yaml:"auth"`
	} `yaml:"kvrocks"`

	CodeQ struct {
		Brokers []string `yaml:"brokers"`
		Topics  struct {
			Invoke    string `yaml:"invoke"`
			Results   string `yaml:"results"`
			DLQInvoke string `yaml:"dlq_invoke"`
			DLQResult string `yaml:"dlq_results"`
		} `yaml:"topics"`
	} `yaml:"codeq"`

	Tikti struct {
		IntrospectionURL string `yaml:"introspection_url"`
		CacheTTLSeconds  int    `yaml:"cache_ttl_seconds"`
	} `yaml:"tikti"`

	Plugins struct {
		AuthN struct {
			Driver string `yaml:"driver"`
			Tikti  struct {
				IntrospectionURL string `yaml:"introspection_url"`
				CacheTTLSeconds  int    `yaml:"cache_ttl_seconds"`
			} `yaml:"tikti"`
		} `yaml:"authn"`
		Persistence struct {
			Driver  string `yaml:"driver"`
			KVRocks struct {
				Addr string `yaml:"addr"`
				Auth struct {
					Mode     string `yaml:"mode"`
					Password string `yaml:"password"`
				} `yaml:"auth"`
			} `yaml:"kvrocks"`
		} `yaml:"persistence"`
		Messaging struct {
			Driver string `yaml:"driver"`
			CodeQ  struct {
				Brokers []string `yaml:"brokers"`
				Topics  struct {
					Invoke    string `yaml:"invoke"`
					Results   string `yaml:"results"`
					DLQInvoke string `yaml:"dlq_invoke"`
					DLQResult string `yaml:"dlq_results"`
				} `yaml:"topics"`
			} `yaml:"codeq"`
		} `yaml:"messaging"`
	} `yaml:"plugins"`

	CSControl struct {
		HTTP struct {
			Addr string `yaml:"addr"`
		} `yaml:"http"`
		Limits struct {
			MaxBundleBytes  int `yaml:"max_bundle_bytes"`
			DraftTTLSeconds int `yaml:"draft_ttl_seconds"`
			ActTTLSeconds   int `yaml:"activation_ttl_seconds"`
		} `yaml:"limits"`
	} `yaml:"cs_control"`

	CSHTTPGateway struct {
		HTTP struct {
			Addr string `yaml:"addr"`
		} `yaml:"http"`
		Limits struct {
			MaxBodyBytes   int `yaml:"max_body_bytes"`
			MaxHeaderBytes int `yaml:"max_header_bytes"`
			MaxQueryBytes  int `yaml:"max_query_bytes"`
		} `yaml:"limits"`
		RateLimits struct {
			TenantRPS   int `yaml:"tenant_rps"`
			FunctionRPS int `yaml:"function_rps"`
		} `yaml:"rate_limits"`
	} `yaml:"cs_http_gateway"`

	CSInvokerPool struct {
		HTTP struct {
			Addr string `yaml:"addr"`
		} `yaml:"http"`
		Workers struct {
			Threads     int `yaml:"threads"`
			MaxInflight int `yaml:"max_inflight"`
		} `yaml:"workers"`
		Cache struct {
			BundlesMax int `yaml:"bundles_max"`
			BytesMax   int `yaml:"bytes_max"`
		} `yaml:"cache"`
		Limits struct {
			MaxResultBytes int `yaml:"max_result_bytes"`
			MaxErrorBytes  int `yaml:"max_error_bytes"`
			MaxLogBytes    int `yaml:"max_log_bytes"`
		} `yaml:"limits"`
	} `yaml:"cs_invoker_pool"`

	CSScheduler struct {
		TickMS         int `yaml:"tick_ms"`
		MaxCatchupTick int `yaml:"max_catchup_ticks"`
		LeaderElection struct {
			Enabled   bool   `yaml:"enabled"`
			LeaseName string `yaml:"lease_name"`
		} `yaml:"leader_election"`
	} `yaml:"cs_scheduler"`

	CSCadencePoller struct {
		Cadence struct {
			Addr string `yaml:"addr"`
		} `yaml:"cadence"`
		RefreshSeconds int `yaml:"refresh_seconds"`
		Heartbeat      struct {
			MaxPerSecond int `yaml:"max_per_second"`
		} `yaml:"heartbeat"`
		Limits struct {
			MaxInflightTasksDefault int `yaml:"max_inflight_tasks_default"`
		} `yaml:"limits"`
	} `yaml:"cs_cadence_poller"`
}

func Load(path string) (Config, error) {
	var cfg Config
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, err
	}
	overrideEnv(&cfg)
	syncPluginConfig(&cfg)
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func overrideEnv(cfg *Config) {
	if v := os.Getenv("CS_CLUSTER_NAME"); v != "" {
		cfg.ClusterName = v
	}
	if v := os.Getenv("CS_ENVIRONMENT"); v != "" {
		cfg.Environment = v
	}
	if v := os.Getenv("CS_KVROCKS_ADDR"); v != "" {
		cfg.KVRocks.Addr = v
		cfg.Plugins.Persistence.KVRocks.Addr = v
	}
	if v := os.Getenv("CS_KVROCKS_PASSWORD"); v != "" {
		cfg.KVRocks.Auth.Password = v
		cfg.Plugins.Persistence.KVRocks.Auth.Password = v
	}
	if v := os.Getenv("CS_CODEQ_BROKERS"); v != "" {
		parts := strings.Split(v, ",")
		cfg.CodeQ.Brokers = cfg.CodeQ.Brokers[:0]
		cfg.Plugins.Messaging.CodeQ.Brokers = cfg.Plugins.Messaging.CodeQ.Brokers[:0]
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" {
				cfg.CodeQ.Brokers = append(cfg.CodeQ.Brokers, trimmed)
				cfg.Plugins.Messaging.CodeQ.Brokers = append(cfg.Plugins.Messaging.CodeQ.Brokers, trimmed)
			}
		}
	}
	if v := os.Getenv("CS_TIKTI_INTROSPECTION_URL"); v != "" {
		cfg.Tikti.IntrospectionURL = v
		cfg.Plugins.AuthN.Tikti.IntrospectionURL = v
	}
	if v := os.Getenv("CS_TIKTI_CACHE_TTL_SECONDS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			cfg.Tikti.CacheTTLSeconds = parsed
			cfg.Plugins.AuthN.Tikti.CacheTTLSeconds = parsed
		}
	}
}

func syncPluginConfig(cfg *Config) {
	if cfg.Plugins.AuthN.Driver == "" {
		cfg.Plugins.AuthN.Driver = "tikti"
	}
	if cfg.Plugins.Persistence.Driver == "" {
		cfg.Plugins.Persistence.Driver = "kvrocks"
	}
	if cfg.Plugins.Messaging.Driver == "" {
		cfg.Plugins.Messaging.Driver = "codeq"
	}

	if cfg.Plugins.AuthN.Tikti.IntrospectionURL == "" {
		cfg.Plugins.AuthN.Tikti.IntrospectionURL = cfg.Tikti.IntrospectionURL
	}
	if cfg.Plugins.AuthN.Tikti.CacheTTLSeconds == 0 {
		cfg.Plugins.AuthN.Tikti.CacheTTLSeconds = cfg.Tikti.CacheTTLSeconds
	}

	if cfg.Plugins.Persistence.KVRocks.Addr == "" {
		cfg.Plugins.Persistence.KVRocks.Addr = cfg.KVRocks.Addr
	}
	if cfg.Plugins.Persistence.KVRocks.Auth.Mode == "" {
		cfg.Plugins.Persistence.KVRocks.Auth.Mode = cfg.KVRocks.Auth.Mode
	}
	if cfg.Plugins.Persistence.KVRocks.Auth.Password == "" {
		cfg.Plugins.Persistence.KVRocks.Auth.Password = cfg.KVRocks.Auth.Password
	}

	if len(cfg.Plugins.Messaging.CodeQ.Brokers) == 0 {
		cfg.Plugins.Messaging.CodeQ.Brokers = append(cfg.Plugins.Messaging.CodeQ.Brokers, cfg.CodeQ.Brokers...)
	}
	if cfg.Plugins.Messaging.CodeQ.Topics.Invoke == "" {
		cfg.Plugins.Messaging.CodeQ.Topics.Invoke = cfg.CodeQ.Topics.Invoke
	}
	if cfg.Plugins.Messaging.CodeQ.Topics.Results == "" {
		cfg.Plugins.Messaging.CodeQ.Topics.Results = cfg.CodeQ.Topics.Results
	}
	if cfg.Plugins.Messaging.CodeQ.Topics.DLQInvoke == "" {
		cfg.Plugins.Messaging.CodeQ.Topics.DLQInvoke = cfg.CodeQ.Topics.DLQInvoke
	}
	if cfg.Plugins.Messaging.CodeQ.Topics.DLQResult == "" {
		cfg.Plugins.Messaging.CodeQ.Topics.DLQResult = cfg.CodeQ.Topics.DLQResult
	}

	// Keep legacy fields hydrated for transitional compatibility.
	if cfg.Tikti.IntrospectionURL == "" {
		cfg.Tikti.IntrospectionURL = cfg.Plugins.AuthN.Tikti.IntrospectionURL
	}
	if cfg.Tikti.CacheTTLSeconds == 0 {
		cfg.Tikti.CacheTTLSeconds = cfg.Plugins.AuthN.Tikti.CacheTTLSeconds
	}
	if cfg.KVRocks.Addr == "" {
		cfg.KVRocks.Addr = cfg.Plugins.Persistence.KVRocks.Addr
	}
	if cfg.KVRocks.Auth.Mode == "" {
		cfg.KVRocks.Auth.Mode = cfg.Plugins.Persistence.KVRocks.Auth.Mode
	}
	if cfg.KVRocks.Auth.Password == "" {
		cfg.KVRocks.Auth.Password = cfg.Plugins.Persistence.KVRocks.Auth.Password
	}
	if len(cfg.CodeQ.Brokers) == 0 {
		cfg.CodeQ.Brokers = append(cfg.CodeQ.Brokers, cfg.Plugins.Messaging.CodeQ.Brokers...)
	}
	if cfg.CodeQ.Topics.Invoke == "" {
		cfg.CodeQ.Topics.Invoke = cfg.Plugins.Messaging.CodeQ.Topics.Invoke
	}
	if cfg.CodeQ.Topics.Results == "" {
		cfg.CodeQ.Topics.Results = cfg.Plugins.Messaging.CodeQ.Topics.Results
	}
	if cfg.CodeQ.Topics.DLQInvoke == "" {
		cfg.CodeQ.Topics.DLQInvoke = cfg.Plugins.Messaging.CodeQ.Topics.DLQInvoke
	}
	if cfg.CodeQ.Topics.DLQResult == "" {
		cfg.CodeQ.Topics.DLQResult = cfg.Plugins.Messaging.CodeQ.Topics.DLQResult
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.ClusterName) == "" {
		return errors.New("cluster_name is required")
	}
	if strings.TrimSpace(c.Environment) == "" {
		return errors.New("environment is required")
	}
	if c.Plugins.AuthN.Driver == "" {
		return errors.New("plugins.authn.driver is required")
	}
	if c.Plugins.Persistence.Driver == "" {
		return errors.New("plugins.persistence.driver is required")
	}
	if c.Plugins.Messaging.Driver == "" {
		return errors.New("plugins.messaging.driver is required")
	}

	switch c.Plugins.AuthN.Driver {
	case "tikti":
		if strings.TrimSpace(c.Plugins.AuthN.Tikti.IntrospectionURL) == "" {
			return errors.New("plugins.authn.tikti.introspection_url is required")
		}
	default:
		return fmt.Errorf("unsupported authn plugin driver: %s", c.Plugins.AuthN.Driver)
	}

	switch c.Plugins.Persistence.Driver {
	case "kvrocks":
		if strings.TrimSpace(c.Plugins.Persistence.KVRocks.Addr) == "" {
			return errors.New("plugins.persistence.kvrocks.addr is required")
		}
	default:
		return fmt.Errorf("unsupported persistence plugin driver: %s", c.Plugins.Persistence.Driver)
	}

	switch c.Plugins.Messaging.Driver {
	case "codeq":
		if len(c.Plugins.Messaging.CodeQ.Brokers) == 0 {
			return errors.New("plugins.messaging.codeq.brokers is required")
		}
		if c.Plugins.Messaging.CodeQ.Topics.Invoke == "" || c.Plugins.Messaging.CodeQ.Topics.Results == "" {
			return errors.New("plugins.messaging.codeq.topics.invoke and results are required")
		}
	default:
		return fmt.Errorf("unsupported messaging plugin driver: %s", c.Plugins.Messaging.Driver)
	}

	if c.CSControl.HTTP.Addr == "" {
		return errors.New("cs_control.http.addr is required")
	}
	if c.CSHTTPGateway.HTTP.Addr == "" {
		return errors.New("cs_http_gateway.http.addr is required")
	}
	if c.CSInvokerPool.HTTP.Addr == "" {
		return errors.New("cs_invoker_pool.http.addr is required")
	}
	if c.CSControl.Limits.MaxBundleBytes <= 0 {
		return fmt.Errorf("cs_control.limits.max_bundle_bytes must be > 0")
	}
	if c.CSControl.Limits.DraftTTLSeconds <= 0 {
		return fmt.Errorf("cs_control.limits.draft_ttl_seconds must be > 0")
	}
	if c.CSControl.Limits.ActTTLSeconds <= 0 {
		return fmt.Errorf("cs_control.limits.activation_ttl_seconds must be > 0")
	}
	return nil
}
