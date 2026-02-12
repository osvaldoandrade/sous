package codeq

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/config"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
)

const (
	defaultClaimLeaseSeconds = 300
	defaultClaimWaitSeconds  = 1
	defaultPollInterval      = 100 * time.Millisecond
)

type HTTPProvider struct {
	baseURL       string
	producerToken string
	workerToken   string
	workerID      string
	client        *http.Client

	invokeCommand    string
	resultsCommand   string
	dlqInvokeCommand string
	dlqResultCommand string

	mu            sync.RWMutex
	requestToTask map[string]string
}

type createTaskRequest struct {
	Command        string `json:"command"`
	Payload        any    `json:"payload"`
	Priority       int    `json:"priority,omitempty"`
	IdempotencyKey string `json:"idempotencyKey,omitempty"`
}

type claimTaskRequest struct {
	Commands     []string `json:"commands,omitempty"`
	LeaseSeconds int      `json:"leaseSeconds,omitempty"`
	WaitSeconds  int      `json:"waitSeconds,omitempty"`
	WorkerID     string   `json:"workerId,omitempty"`
}

type submitResultRequest struct {
	Status string         `json:"status"`
	Result map[string]any `json:"result,omitempty"`
	Error  string         `json:"error,omitempty"`
}

type codeQTask struct {
	ID      string `json:"id"`
	Command string `json:"command"`
	Payload string `json:"payload"`
}

type codeQResultEnvelope struct {
	Task   codeQTask `json:"task"`
	Result struct {
		TaskID string         `json:"taskId"`
		Status string         `json:"status"`
		Result map[string]any `json:"result,omitempty"`
		Error  string         `json:"error,omitempty"`
	} `json:"result"`
}

func NewHTTPProviderFromConfig(cfg config.Config) (*HTTPProvider, error) {
	baseURL := strings.TrimSpace(cfg.Plugins.Messaging.CodeQ.BaseURL)
	if baseURL == "" {
		return nil, fmt.Errorf("plugins.messaging.codeq.base_url is required")
	}
	producerToken := strings.TrimSpace(cfg.Plugins.Messaging.CodeQ.ProducerToken)
	if producerToken == "" {
		return nil, fmt.Errorf("plugins.messaging.codeq.producer_token is required")
	}
	workerToken := strings.TrimSpace(cfg.Plugins.Messaging.CodeQ.WorkerToken)
	if workerToken == "" {
		workerToken = producerToken
	}
	topics := cfg.Plugins.Messaging.CodeQ.Topics
	if topics.Invoke == "" || topics.Results == "" {
		return nil, fmt.Errorf("plugins.messaging.codeq.topics.invoke and results are required")
	}
	return &HTTPProvider{
		baseURL:          normalizeBaseURL(baseURL),
		producerToken:    producerToken,
		workerToken:      workerToken,
		workerID:         defaultWorkerID(),
		client:           &http.Client{Timeout: 15 * time.Second},
		invokeCommand:    topics.Invoke,
		resultsCommand:   topics.Results,
		dlqInvokeCommand: topics.DLQInvoke,
		dlqResultCommand: topics.DLQResult,
		requestToTask:    make(map[string]string),
	}, nil
}

func normalizeBaseURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(trimmed, "http://") || strings.HasPrefix(trimmed, "https://") {
		return strings.TrimRight(trimmed, "/")
	}
	return "http://" + strings.TrimRight(trimmed, "/")
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || strings.TrimSpace(host) == "" {
		host = "unknown-host"
	}
	return strings.Join([]string{
		"sous",
		host,
		strconv.Itoa(os.Getpid()),
		uuid.NewString(),
	}, "-")
}

func (p *HTTPProvider) Close() error {
	return nil
}

func (p *HTTPProvider) Publish(ctx context.Context, topic, tenant, typ string, body any) error {
	rawBody, err := json.Marshal(body)
	if err != nil {
		return err
	}
	env := messaging.Envelope{
		Schema: "cs.envelope.v1",
		ID:     "msg_" + uuid.NewString(),
		TSMS:   time.Now().UnixMilli(),
		Tenant: tenant,
		Type:   typ,
		Body:   rawBody,
	}
	_, err = p.enqueue(ctx, p.producerToken, topic, env, "")
	if err != nil {
		return cserrors.Wrap(cserrors.CSCodeQPublishFailed, fmt.Sprintf("failed to publish to topic %s", topic), err)
	}
	return nil
}

func (p *HTTPProvider) PublishInvocation(ctx context.Context, req api.InvocationRequest) error {
	rawReq, err := json.Marshal(req)
	if err != nil {
		return err
	}
	env := messaging.Envelope{
		Schema: "cs.envelope.v1",
		ID:     "msg_" + uuid.NewString(),
		TSMS:   time.Now().UnixMilli(),
		Tenant: req.Tenant,
		Type:   "InvocationRequest",
		Body:   rawReq,
	}
	taskID, err := p.enqueue(ctx, p.producerToken, p.invokeCommand, env, req.RequestID)
	if err != nil {
		return cserrors.Wrap(cserrors.CSCodeQPublishFailed, fmt.Sprintf("failed to publish to topic %s", p.invokeCommand), err)
	}
	if req.RequestID != "" {
		p.rememberTask(req.RequestID, taskID)
	}
	return nil
}

func (p *HTTPProvider) PublishResult(ctx context.Context, tenant string, result api.InvocationResult) error {
	taskID, ok := p.taskByRequestID(result.RequestID)
	if !ok {
		return cserrors.New(cserrors.CSCodeQPublishFailed, "missing codeQ task mapping for invocation result")
	}
	if err := p.submitInvocationResult(ctx, taskID, result); err != nil {
		return cserrors.Wrap(cserrors.CSCodeQPublishFailed, "failed to submit invocation result to codeQ", err)
	}
	// Keep result stream semantics for components that consume cs.results.
	if err := p.Publish(ctx, p.resultsCommand, tenant, "InvocationResult", result); err != nil {
		return err
	}
	return nil
}

func (p *HTTPProvider) PublishDLQInvoke(ctx context.Context, tenant string, req api.InvocationRequest) error {
	if strings.TrimSpace(p.dlqInvokeCommand) == "" {
		return nil
	}
	return p.Publish(ctx, p.dlqInvokeCommand, tenant, "InvocationRequest", req)
}

func (p *HTTPProvider) PublishDLQResult(ctx context.Context, tenant string, result api.InvocationResult) error {
	if strings.TrimSpace(p.dlqResultCommand) == "" {
		return nil
	}
	return p.Publish(ctx, p.dlqResultCommand, tenant, "InvocationResult", result)
}

func (p *HTTPProvider) ConsumeInvocations(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationRequest) error) error {
	for {
		task, ok, err := p.claimTask(ctx, p.invokeCommand, groupID)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return cserrors.Wrap(cserrors.CSCodeQSubFailed, "failed to claim invocation task", err)
		}
		if !ok {
			if ctx.Err() != nil {
				return nil
			}
			time.Sleep(defaultPollInterval)
			continue
		}

		env, req, err := parseInvocationTask(task)
		if err != nil {
			_ = p.nackTask(ctx, task.ID, 1, "invalid invocation payload")
			continue
		}
		p.rememberTask(req.RequestID, task.ID)

		if err := handler(env, req); err != nil {
			_ = p.nackTask(ctx, task.ID, 1, "handler failed")
			return err
		}
	}
}

func (p *HTTPProvider) ConsumeResults(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationResult) error) error {
	return p.consumeTopicTasks(ctx, p.resultsCommand, groupID, func(task codeQTask, env messaging.Envelope) error {
		var out api.InvocationResult
		if err := json.Unmarshal(env.Body, &out); err != nil {
			return err
		}
		return handler(env, out)
	})
}

func (p *HTTPProvider) ConsumeTopic(ctx context.Context, topic, groupID string, handler func(messaging.Envelope) error) error {
	return p.consumeTopicTasks(ctx, topic, groupID, func(task codeQTask, env messaging.Envelope) error {
		return handler(env)
	})
}

func (p *HTTPProvider) WaitForResult(ctx context.Context, requestID string) (api.InvocationResult, error) {
	taskID, ok := p.taskByRequestID(requestID)
	if !ok {
		return api.InvocationResult{}, cserrors.New(cserrors.CSCodeQTimeout, "request mapping not found")
	}
	ticker := time.NewTicker(defaultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return api.InvocationResult{}, cserrors.New(cserrors.CSCodeQTimeout, "result wait timed out")
		case <-ticker.C:
			res, ready, err := p.fetchInvocationResult(ctx, taskID)
			if err != nil {
				return api.InvocationResult{}, err
			}
			if ready {
				return res, nil
			}
		}
	}
}

func (p *HTTPProvider) consumeTopicTasks(ctx context.Context, command, groupID string, handler func(codeQTask, messaging.Envelope) error) error {
	for {
		task, ok, err := p.claimTask(ctx, command, groupID)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return cserrors.Wrap(cserrors.CSCodeQSubFailed, "failed to claim codeQ task", err)
		}
		if !ok {
			if ctx.Err() != nil {
				return nil
			}
			time.Sleep(defaultPollInterval)
			continue
		}

		env, err := parseEnvelopeTask(task)
		if err != nil {
			_ = p.nackTask(ctx, task.ID, 1, "invalid envelope payload")
			continue
		}
		if err := handler(task, env); err != nil {
			_ = p.nackTask(ctx, task.ID, 1, "handler failed")
			return err
		}
		if err := p.completeTask(ctx, task.ID); err != nil {
			return cserrors.Wrap(cserrors.CSCodeQSubFailed, "failed to complete consumed codeQ task", err)
		}
	}
}

func parseInvocationTask(task codeQTask) (messaging.Envelope, api.InvocationRequest, error) {
	env, err := parseEnvelopeTask(task)
	if err != nil {
		return messaging.Envelope{}, api.InvocationRequest{}, err
	}
	var req api.InvocationRequest
	if err := json.Unmarshal(env.Body, &req); err != nil {
		return messaging.Envelope{}, api.InvocationRequest{}, err
	}
	return env, req, nil
}

func parseEnvelopeTask(task codeQTask) (messaging.Envelope, error) {
	var env messaging.Envelope
	if err := json.Unmarshal([]byte(task.Payload), &env); err != nil {
		return messaging.Envelope{}, err
	}
	return env, nil
}

func (p *HTTPProvider) enqueue(ctx context.Context, token, command string, payload any, idempotencyKey string) (string, error) {
	req := createTaskRequest{
		Command:        command,
		Payload:        payload,
		Priority:       0,
		IdempotencyKey: strings.TrimSpace(idempotencyKey),
	}
	status, body, err := p.doJSON(ctx, token, http.MethodPost, "/v1/codeq/tasks", req)
	if err != nil {
		return "", err
	}
	if status != http.StatusAccepted {
		return "", fmt.Errorf("codeq create task failed: status=%d body=%s", status, string(body))
	}
	var out codeQTask
	if err := json.Unmarshal(body, &out); err != nil {
		return "", err
	}
	if out.ID == "" {
		return "", fmt.Errorf("codeq create task returned empty id")
	}
	return out.ID, nil
}

func (p *HTTPProvider) claimTask(ctx context.Context, command, groupID string) (codeQTask, bool, error) {
	workerID := strings.TrimSpace(p.workerID)
	if workerID == "" {
		workerID = defaultWorkerID()
	}
	if trimmedGroup := strings.TrimSpace(groupID); trimmedGroup != "" {
		workerID = workerID + "::" + trimmedGroup
	}
	req := claimTaskRequest{
		Commands:     []string{command},
		LeaseSeconds: defaultClaimLeaseSeconds,
		WaitSeconds:  defaultClaimWaitSeconds,
		WorkerID:     workerID,
	}
	status, body, err := p.doJSON(ctx, p.workerToken, http.MethodPost, "/v1/codeq/tasks/claim", req)
	if err != nil {
		return codeQTask{}, false, err
	}
	if status == http.StatusNoContent {
		return codeQTask{}, false, nil
	}
	if status != http.StatusOK {
		return codeQTask{}, false, fmt.Errorf("codeq claim failed: status=%d body=%s", status, string(body))
	}
	var out codeQTask
	if err := json.Unmarshal(body, &out); err != nil {
		return codeQTask{}, false, err
	}
	if out.ID == "" {
		return codeQTask{}, false, fmt.Errorf("codeq claim returned empty id")
	}
	return out, true, nil
}

func (p *HTTPProvider) completeTask(ctx context.Context, taskID string) error {
	status, body, err := p.doJSON(ctx, p.workerToken, http.MethodPost, fmt.Sprintf("/v1/codeq/tasks/%s/result", taskID), submitResultRequest{
		Status: "COMPLETED",
		Result: map[string]any{"ok": true},
	})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("codeq complete task failed: status=%d body=%s", status, string(body))
	}
	return nil
}

func (p *HTTPProvider) nackTask(ctx context.Context, taskID string, delaySeconds int, reason string) error {
	status, body, err := p.doJSON(ctx, p.workerToken, http.MethodPost, fmt.Sprintf("/v1/codeq/tasks/%s/nack", taskID), map[string]any{
		"delaySeconds": delaySeconds,
		"reason":       reason,
	})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("codeq nack failed: status=%d body=%s", status, string(body))
	}
	return nil
}

func (p *HTTPProvider) submitInvocationResult(ctx context.Context, taskID string, result api.InvocationResult) error {
	payload, err := toMap(result)
	if err != nil {
		return err
	}
	req := submitResultRequest{
		Status: "COMPLETED",
		Result: payload,
	}
	if result.Status != "success" {
		req.Status = "FAILED"
		req.Result = nil
		msg := "runtime failure"
		if result.Error != nil && strings.TrimSpace(result.Error.Message) != "" {
			msg = result.Error.Message
		}
		req.Error = msg
	}
	status, body, err := p.doJSON(ctx, p.workerToken, http.MethodPost, fmt.Sprintf("/v1/codeq/tasks/%s/result", taskID), req)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("codeq submit result failed: status=%d body=%s", status, string(body))
	}
	return nil
}

func (p *HTTPProvider) fetchInvocationResult(ctx context.Context, taskID string) (api.InvocationResult, bool, error) {
	status, body, err := p.doJSON(ctx, p.producerToken, http.MethodGet, fmt.Sprintf("/v1/codeq/tasks/%s/result", taskID), nil)
	if err != nil {
		return api.InvocationResult{}, false, err
	}
	if status == http.StatusNotFound {
		return api.InvocationResult{}, false, nil
	}
	if status != http.StatusOK {
		return api.InvocationResult{}, false, fmt.Errorf("codeq get result failed: status=%d body=%s", status, string(body))
	}
	var out codeQResultEnvelope
	if err := json.Unmarshal(body, &out); err != nil {
		return api.InvocationResult{}, false, err
	}
	if len(out.Result.Result) > 0 {
		raw, _ := json.Marshal(out.Result.Result)
		var inv api.InvocationResult
		if err := json.Unmarshal(raw, &inv); err == nil && inv.RequestID != "" {
			return inv, true, nil
		}
	}

	// Fallback reconstruction for FAILED results that may only include an error string.
	env, err := parseEnvelopeTask(out.Task)
	if err != nil {
		return api.InvocationResult{}, false, err
	}
	var req api.InvocationRequest
	if err := json.Unmarshal(env.Body, &req); err != nil {
		return api.InvocationResult{}, false, err
	}
	rebuilt := api.InvocationResult{
		ActivationID: req.ActivationID,
		RequestID:    req.RequestID,
		Status:       "error",
	}
	if strings.EqualFold(out.Result.Status, "COMPLETED") {
		rebuilt.Status = "success"
	}
	if strings.TrimSpace(out.Result.Error) != "" {
		rebuilt.Error = &api.InvocationError{Message: out.Result.Error}
	}
	return rebuilt, true, nil
}

func (p *HTTPProvider) taskByRequestID(requestID string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	taskID, ok := p.requestToTask[requestID]
	return taskID, ok
}

func (p *HTTPProvider) rememberTask(requestID, taskID string) {
	if requestID == "" || taskID == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.requestToTask[requestID] = taskID
}

func (p *HTTPProvider) doJSON(ctx context.Context, token, method, path string, body any) (int, []byte, error) {
	var reqBody io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		reqBody = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.baseURL+path, reqBody)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, out, nil
}

func toMap(v any) (map[string]any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}
