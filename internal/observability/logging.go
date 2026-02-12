package observability

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

type Logger struct {
	service string
	base    *log.Logger
	mu      sync.Mutex
}

type Entry struct {
	TsMS         int64  `json:"ts_ms"`
	Level        string `json:"level"`
	Service      string `json:"service"`
	Message      string `json:"message"`
	RequestID    string `json:"request_id,omitempty"`
	Tenant       string `json:"tenant,omitempty"`
	Namespace    string `json:"namespace,omitempty"`
	Function     string `json:"function,omitempty"`
	ActivationID string `json:"activation_id,omitempty"`
}

func NewLogger(service string) *Logger {
	return &Logger{service: service, base: log.New(os.Stdout, "", 0)}
}

func NewLoggerWithWriter(service string, w io.Writer) *Logger {
	return &Logger{service: service, base: log.New(w, "", 0)}
}

func (l *Logger) Info(ctx context.Context, message string) {
	l.write(ctx, "info", message)
}

func (l *Logger) Warn(ctx context.Context, message string) {
	l.write(ctx, "warn", message)
}

func (l *Logger) Error(ctx context.Context, message string) {
	l.write(ctx, "error", message)
}

func (l *Logger) write(ctx context.Context, level, message string) {
	entry := Entry{
		TsMS:      time.Now().UnixMilli(),
		Level:     level,
		Service:   l.service,
		Message:   message,
		RequestID: RequestIDFromContext(ctx),
	}
	b, err := json.Marshal(entry)
	if err != nil {
		l.base.Printf(`{"ts_ms":%d,"level":"error","service":"%s","message":"failed to marshal log"}`,
			time.Now().UnixMilli(), l.service)
		return
	}
	l.mu.Lock()
	l.base.Println(string(b))
	l.mu.Unlock()
}
