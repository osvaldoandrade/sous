package codeq

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/config"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
)

func TestNewFromConfigValidation(t *testing.T) {
	cfg := config.Config{}
	cfg.Plugins.Messaging.CodeQ.Topics.Invoke = "cs.invoke"
	cfg.Plugins.Messaging.CodeQ.Topics.Results = "cs.results"
	if _, err := NewFromConfig(cfg); err == nil {
		t.Fatal("expected missing transport error")
	}

	cfg.Plugins.Messaging.CodeQ.BaseURL = "http://127.0.0.1:8080"
	if _, err := NewFromConfig(cfg); err == nil {
		t.Fatal("expected missing producer token error")
	}

	cfg.Plugins.Messaging.CodeQ.ProducerToken = "producer"
	provider, err := NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	if _, ok := provider.(*HTTPProvider); !ok {
		t.Fatalf("expected HTTPProvider, got %T", provider)
	}

	legacy := config.Config{}
	legacy.Plugins.Messaging.CodeQ.Brokers = []string{"localhost:9092"}
	legacy.Plugins.Messaging.CodeQ.Topics.Invoke = "cs.invoke"
	legacy.Plugins.Messaging.CodeQ.Topics.Results = "cs.results"
	legacyProvider, err := NewFromConfig(legacy)
	if err != nil {
		t.Fatalf("legacy provider: %v", err)
	}
	if _, ok := legacyProvider.(*Provider); !ok {
		t.Fatalf("expected kafka Provider, got %T", legacyProvider)
	}
}

func TestHTTPProviderPublishAndWaitForResult(t *testing.T) {
	type queueTask struct {
		ID      string
		Command string
		Payload string
	}

	var (
		mu            sync.Mutex
		counter       int
		tasksByID     = map[string]queueTask{}
		tasksByCmd    = map[string][]string{}
		resultByTask  = map[string]submitResultRequest{}
		producerToken = "producer-token"
		workerToken   = "worker-token"
	)

	nextID := func() string {
		mu.Lock()
		defer mu.Unlock()
		counter++
		return "task-" + strings.TrimSpace(time.Unix(int64(counter), 0).UTC().Format("150405"))
	}

	popTask := func(command string) (queueTask, bool) {
		mu.Lock()
		defer mu.Unlock()
		ids := tasksByCmd[command]
		if len(ids) == 0 {
			return queueTask{}, false
		}
		id := ids[0]
		tasksByCmd[command] = ids[1:]
		task, ok := tasksByID[id]
		return task, ok
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/codeq/tasks":
			if r.Header.Get("Authorization") != "Bearer "+producerToken {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"bad token"}`))
				return
			}
			var req createTaskRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			rawPayload, _ := json.Marshal(req.Payload)
			id := nextID()
			task := queueTask{ID: id, Command: req.Command, Payload: string(rawPayload)}
			mu.Lock()
			tasksByID[id] = task
			tasksByCmd[req.Command] = append(tasksByCmd[req.Command], id)
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":      task.ID,
				"command": task.Command,
				"payload": task.Payload,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/v1/codeq/tasks/claim":
			if r.Header.Get("Authorization") != "Bearer "+workerToken {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"bad token"}`))
				return
			}
			var req claimTaskRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if len(req.Commands) == 0 {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			task, ok := popTask(req.Commands[0])
			if !ok {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id":      task.ID,
				"command": task.Command,
				"payload": task.Payload,
			})
		case strings.HasPrefix(r.URL.Path, "/v1/codeq/tasks/") && strings.HasSuffix(r.URL.Path, "/result") && r.Method == http.MethodPost:
			if r.Header.Get("Authorization") != "Bearer "+workerToken {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"bad token"}`))
				return
			}
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/codeq/tasks/"), "/")
			taskID := parts[0]
			var req submitResultRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mu.Lock()
			resultByTask[taskID] = req
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"taskId": taskID, "status": req.Status, "result": req.Result, "error": req.Error})
		case strings.HasPrefix(r.URL.Path, "/v1/codeq/tasks/") && strings.HasSuffix(r.URL.Path, "/result") && r.Method == http.MethodGet:
			if r.Header.Get("Authorization") != "Bearer "+producerToken {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"bad token"}`))
				return
			}
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/v1/codeq/tasks/"), "/")
			taskID := parts[0]
			mu.Lock()
			task, taskOK := tasksByID[taskID]
			res, resultOK := resultByTask[taskID]
			mu.Unlock()
			if !taskOK || !resultOK {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"error":"result not found"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"task": map[string]any{
					"id":      task.ID,
					"command": task.Command,
					"payload": task.Payload,
				},
				"result": map[string]any{
					"taskId": task.ID,
					"status": res.Status,
					"result": res.Result,
					"error":  res.Error,
				},
			})
		case strings.HasPrefix(r.URL.Path, "/v1/codeq/tasks/") && strings.HasSuffix(r.URL.Path, "/nack"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"requeued"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":"not found"}`)
		}
	}))
	defer server.Close()

	cfg := config.Config{}
	cfg.Plugins.Messaging.CodeQ.BaseURL = server.URL
	cfg.Plugins.Messaging.CodeQ.ProducerToken = producerToken
	cfg.Plugins.Messaging.CodeQ.WorkerToken = workerToken
	cfg.Plugins.Messaging.CodeQ.Topics.Invoke = "cs.invoke"
	cfg.Plugins.Messaging.CodeQ.Topics.Results = "cs.results"

	p, err := NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("new provider: %v", err)
	}
	provider := p.(*HTTPProvider)

	req := api.InvocationRequest{
		ActivationID: "act_1",
		RequestID:    "req_1",
		Tenant:       "t_abc123",
		Namespace:    "default",
		Ref:          api.FunctionRef{Function: "fn1", Alias: "prod", Version: 1},
		Trigger:      api.Trigger{Type: "http", Source: map[string]any{"path": "/x"}},
		Principal:    api.Principal{Sub: "u1", Roles: []string{"admin"}},
		DeadlineMS:   time.Now().Add(2 * time.Second).UnixMilli(),
		Event:        map[string]any{"hello": "world"},
	}
	if err := provider.PublishInvocation(context.Background(), req); err != nil {
		t.Fatalf("PublishInvocation: %v", err)
	}

	invocationResult := api.InvocationResult{
		ActivationID: req.ActivationID,
		RequestID:    req.RequestID,
		Status:       "success",
		DurationMS:   12,
		Result: &api.FunctionResponse{
			StatusCode:      200,
			Headers:         map[string]string{"content-type": "application/json"},
			Body:            `{"ok":true}`,
			IsBase64Encoded: false,
		},
	}
	if err := provider.PublishResult(context.Background(), req.Tenant, invocationResult); err != nil {
		t.Fatalf("PublishResult: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := provider.WaitForResult(ctx, req.RequestID)
	if err != nil {
		t.Fatalf("WaitForResult: %v", err)
	}
	if got.RequestID != req.RequestID || got.ActivationID != req.ActivationID || got.Status != "success" {
		t.Fatalf("unexpected invocation result: %+v", got)
	}
	if got.Result == nil || got.Result.StatusCode != 200 || got.Result.Body != `{"ok":true}` {
		t.Fatalf("unexpected function result payload: %+v", got.Result)
	}
}

func TestHTTPProviderPublishResultRequiresMapping(t *testing.T) {
	provider := &HTTPProvider{
		baseURL:        "http://127.0.0.1:9",
		producerToken:  "p",
		workerToken:    "w",
		resultsCommand: "cs.results",
		requestToTask:  make(map[string]string),
	}
	err := provider.PublishResult(context.Background(), "t", api.InvocationResult{RequestID: "req_missing"})
	if err == nil {
		t.Fatal("expected missing task mapping error")
	}
}

func TestConsumeMappingsUseWrapper(t *testing.T) {
	// compile-time interface checks + signatures sanity
	var _ messaging.Provider = &HTTPProvider{}
}
