package cadence

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type ActivityTask struct {
	TaskToken    string `json:"task_token"`
	Domain       string `json:"domain"`
	Tasklist     string `json:"tasklist"`
	WorkflowID   string `json:"workflow_id"`
	RunID        string `json:"run_id"`
	ActivityID   string `json:"activity_id"`
	ActivityType string `json:"activity_type"`
	Attempt      int    `json:"attempt"`
	InputBase64  string `json:"input_base64"`
}

type Client interface {
	PollActivityTask(ctx context.Context, domain, tasklist, workerID string) (*ActivityTask, error)
	RespondActivityCompleted(ctx context.Context, taskToken string, payload []byte) error
	RespondActivityFailed(ctx context.Context, taskToken, reason string, details []byte) error
	RecordActivityHeartbeat(ctx context.Context, taskToken string, details []byte) error
}

type HTTPClient struct {
	baseURL string
	http    *http.Client
}

func NewHTTPClient(addr string) *HTTPClient {
	baseURL := addr
	if !strings.HasPrefix(baseURL, "http") {
		baseURL = "http://" + baseURL
	}
	return &HTTPClient{baseURL: strings.TrimRight(baseURL, "/"), http: &http.Client{Timeout: 20 * time.Second}}
}

func (c *HTTPClient) PollActivityTask(ctx context.Context, domain, tasklist, workerID string) (*ActivityTask, error) {
	payload := map[string]any{"domain": domain, "tasklist": tasklist, "worker_id": workerID}
	var resp struct {
		Task *ActivityTask `json:"task"`
	}
	if err := c.postJSON(ctx, "/v1/poll-activity-task", payload, &resp); err != nil {
		return nil, err
	}
	return resp.Task, nil
}

func (c *HTTPClient) RespondActivityCompleted(ctx context.Context, taskToken string, payload []byte) error {
	return c.postJSON(ctx, "/v1/respond-activity-completed", map[string]any{
		"task_token":     taskToken,
		"payload_base64": string(payload),
	}, nil)
}

func (c *HTTPClient) RespondActivityFailed(ctx context.Context, taskToken, reason string, details []byte) error {
	return c.postJSON(ctx, "/v1/respond-activity-failed", map[string]any{
		"task_token":     taskToken,
		"reason":         reason,
		"details_base64": string(details),
	}, nil)
}

func (c *HTTPClient) RecordActivityHeartbeat(ctx context.Context, taskToken string, details []byte) error {
	return c.postJSON(ctx, "/v1/record-activity-heartbeat", map[string]any{
		"task_token":     taskToken,
		"details_base64": string(details),
	}, nil)
}

func (c *HTTPClient) postJSON(ctx context.Context, path string, payload any, out any) error {
	raw, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("cadence endpoint %s returned %d", path, resp.StatusCode)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
