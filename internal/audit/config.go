package audit

import (
	"fmt"
	"io"
)

// Config is the slice of the global config relevant to the audit
// subsystem. Pass the loaded value from config.Config.Plugins.Audit
// to NewSinkFromConfig.
type Config struct {
	Sink         string
	TopicPrefix  string
	WebhookURL   string
	HMACSecret   string
	HistoryLimit int
}

// NewSinkFromConfig builds the configured Sink. The codeqPublisher is
// only consulted when cfg.Sink == "codeq"; pass nil if the caller is
// running without messaging available (e.g., CLI-only flows). The
// stdoutWriter is required when cfg.Sink == "stdout"; pass os.Stdout
// from main.
func NewSinkFromConfig(cfg Config, codeqPublisher CodeQPublisher, stdoutWriter io.Writer) (Sink, error) {
	switch cfg.Sink {
	case "", "stdout":
		if stdoutWriter == nil {
			return nil, fmt.Errorf("audit sink stdout: writer is nil")
		}
		return NewStdoutJSONSink(stdoutWriter), nil
	case "codeq":
		if codeqPublisher == nil {
			return nil, fmt.Errorf("audit sink codeq: publisher is nil")
		}
		return NewCodeQSink(codeqPublisher, cfg.TopicPrefix), nil
	case "webhook":
		if cfg.WebhookURL == "" {
			return nil, fmt.Errorf("audit sink webhook: webhook_url is required")
		}
		return NewWebhookSink(cfg.WebhookURL, cfg.HMACSecret, nil), nil
	default:
		return nil, fmt.Errorf("audit sink: unsupported driver %q", cfg.Sink)
	}
}
