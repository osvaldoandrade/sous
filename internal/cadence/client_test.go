package cadence

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientMethods(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/poll-activity-task", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"task": map[string]any{"task_token": "tok", "domain": "d", "tasklist": "tl", "workflow_id": "wid", "run_id": "rid", "activity_id": "aid", "activity_type": "Sous", "attempt": 1, "input_base64": "aA=="}})
	})
	mux.HandleFunc("/v1/respond-activity-completed", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/v1/respond-activity-failed", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("/v1/record-activity-heartbeat", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	ts := httptest.NewServer(mux)
	defer ts.Close()

	c := NewHTTPClient(ts.URL)
	task, err := c.PollActivityTask(context.Background(), "d", "tl", "w")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if task == nil || task.TaskToken != "tok" {
		t.Fatalf("unexpected task: %+v", task)
	}
	if err := c.RespondActivityCompleted(context.Background(), "tok", []byte("abc")); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := c.RespondActivityFailed(context.Background(), "tok", "err", []byte("abc")); err != nil {
		t.Fatalf("failed: %v", err)
	}
	if err := c.RecordActivityHeartbeat(context.Background(), "tok", []byte("abc")); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
}

func TestClientErrorStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer ts.Close()

	c := NewHTTPClient(ts.URL)
	if _, err := c.PollActivityTask(context.Background(), "d", "tl", "w"); err == nil {
		t.Fatal("expected error for non-2xx")
	}
}
