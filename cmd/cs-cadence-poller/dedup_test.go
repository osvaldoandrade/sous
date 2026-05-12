package main

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/cadence"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// TestPollLoopReusesActivationIDForSameTaskToken verifies the dedup
// contract: two Cadence deliveries of the same task_token must resolve to
// the same activation_id so heartbeats and respond callbacks line up.
func TestPollLoopReusesActivationIDForSameTaskToken(t *testing.T) {
	publishes := make(chan api.InvocationRequest, 2)
	store := &testutil.FakePersistence{
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) { return 1, nil },
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				Config: api.VersionConfig{
					TimeoutMS: 200,
					Authz: api.VersionAuthz{
						InvokeCadenceRoles: []string{"role:cadence"},
					},
				},
			}, nil, nil
		},
		PutCadenceTaskMappingFn: func(context.Context, string, string, string, string, time.Duration) error { return nil },
	}
	broker := &testutil.FakeMessaging{
		PublishInvocationFn: func(ctx context.Context, req api.InvocationRequest) error {
			_ = ctx
			publishes <- req
			return nil
		},
	}
	calls := int32(0)
	client := &fakeCadenceClient{
		pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
			n := atomic.AddInt32(&calls, 1)
			if n > 2 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return &cadence.ActivityTask{
				TaskToken:    "token-X",
				Domain:       "d",
				Tasklist:     "tl",
				ActivityType: "A",
				Attempt:      int(n),
			}, nil
		},
	}
	p := newTestPoller(store, broker, client)
	binding := api.WorkerBinding{
		Tenant:      "t1",
		Namespace:   "n1",
		Name:        "w1",
		ActivityMap: map[string]api.WorkerBindingRef{"A": {Function: "fn", Alias: "prod"}},
	}
	sem := make(chan struct{}, 2)
	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		defer close(done)
		p.pollLoop(ctx, binding, sem)
	}()
	first := <-publishes
	second := <-publishes
	cancel()
	<-done

	if first.ActivationID == "" || first.ActivationID != second.ActivationID {
		t.Fatalf("retried task_token must reuse activation_id; first=%q second=%q",
			first.ActivationID, second.ActivationID)
	}
}

// TestPollLoopReplaysCachedCompletionForCompletedToken verifies the second
// half of the contract: once a task has produced a terminal result, a
// redelivery of the same task_token must respond to Cadence with the cached
// completion rather than re-publishing the invocation.
func TestPollLoopReplaysCachedCompletionForCompletedToken(t *testing.T) {
	store := &testutil.FakePersistence{
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) { return 1, nil },
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				Config: api.VersionConfig{
					TimeoutMS: 200,
					Authz: api.VersionAuthz{
						InvokeCadenceRoles: []string{"role:cadence"},
					},
				},
			}, nil, nil
		},
		DeleteCadenceTaskMappingFn: func(context.Context, string, string, string) error { return nil },
	}
	publishes := int32(0)
	broker := &testutil.FakeMessaging{
		PublishInvocationFn: func(context.Context, api.InvocationRequest) error {
			atomic.AddInt32(&publishes, 1)
			return nil
		},
	}
	var completedToken string
	completedPayload := make(chan []byte, 1)
	client := &fakeCadenceClient{
		pollFn: func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
		completedFn: func(ctx context.Context, taskToken string, payload []byte) error {
			_ = ctx
			completedToken = taskToken
			select {
			case completedPayload <- payload:
			default:
			}
			return nil
		},
	}
	p := newTestPoller(store, broker, client)

	// Pre-seed the dedup store with a terminal result keyed by the token
	// hash, exactly as consumeResults would after a successful activation.
	tokenHash := hashToken("token-replay")
	cached, _ := json.Marshal(cadenceCachedResult{
		Status: "success",
		Result: &api.FunctionResponse{StatusCode: 201, Body: "cached"},
	})
	if _, err := p.idemStore.Reserve(context.Background(), tokenHash, "act_cached", tokenHash, time.Hour); err != nil {
		t.Fatalf("seed reserve: %v", err)
	}
	if err := p.idemStore.Commit(context.Background(), tokenHash, tokenHash, cached, ""); err != nil {
		t.Fatalf("seed commit: %v", err)
	}

	// Drive the pollLoop synchronously by handing it a single task.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivered := false
	client.pollFn = func(ctx context.Context, domain, tasklist, workerID string) (*cadence.ActivityTask, error) {
		if delivered {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		delivered = true
		return &cadence.ActivityTask{TaskToken: "token-replay", ActivityType: "A"}, nil
	}
	binding := api.WorkerBinding{
		Tenant:      "t1",
		Namespace:   "n1",
		Name:        "w1",
		ActivityMap: map[string]api.WorkerBindingRef{"A": {Function: "fn", Alias: "prod"}},
	}
	sem := make(chan struct{}, 1)
	go func() {
		// Wait for the replay to happen then cancel the loop.
		select {
		case <-completedPayload:
		case <-time.After(2 * time.Second):
		}
		cancel()
	}()
	p.pollLoop(ctx, binding, sem)

	if atomic.LoadInt32(&publishes) != 0 {
		t.Fatalf("dedup-hit token must not be re-published; publishes=%d", publishes)
	}
	if completedToken != "token-replay" {
		t.Fatalf("expected cached completion to be sent for token-replay, got %q", completedToken)
	}
}

// TestConsumeResultsPersistsTerminalDedupRecord verifies the dual: once a
// result arrives, the dedup store is updated so subsequent redeliveries can
// replay (see TestPollLoopReplaysCachedCompletionForCompletedToken).
func TestConsumeResultsPersistsTerminalDedupRecord(t *testing.T) {
	store := &testutil.FakePersistence{
		DeleteCadenceTaskMappingFn: func(context.Context, string, string, string) error { return nil },
	}
	broker := &testutil.FakeMessaging{
		ConsumeResultsFn: func(ctx context.Context, groupID string, handler func(messaging.Envelope, api.InvocationResult) error) error {
			_ = ctx
			_ = groupID
			return handler(messaging.Envelope{}, api.InvocationResult{
				ActivationID: "act-success",
				Status:       "success",
				Result:       &api.FunctionResponse{StatusCode: 200, Body: "ok"},
			})
		},
	}
	client := &fakeCadenceClient{}
	p := newTestPoller(store, broker, client)

	tokenHash := "hash-success"
	if _, err := p.idemStore.Reserve(context.Background(), tokenHash, "act-success", tokenHash, time.Hour); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	sem := make(chan struct{}, 1)
	sem <- struct{}{}
	p.activation["act-success"] = taskInfo{
		Tenant:    "t1",
		Namespace: "n1",
		TaskToken: "token-success",
		TaskHash:  tokenHash,
		Semaphore: sem,
	}
	p.consumeResults(context.Background())
	rec, ok := p.idemStore.Get(context.Background(), tokenHash)
	if !ok || !rec.Terminal {
		t.Fatalf("expected terminal dedup record after success; got rec=%+v ok=%v", rec, ok)
	}
	var cached cadenceCachedResult
	if err := json.Unmarshal(rec.Result, &cached); err != nil {
		t.Fatalf("decode cached result: %v", err)
	}
	if cached.Status != "success" || cached.Result == nil || cached.Result.StatusCode != 200 {
		t.Fatalf("unexpected cached result: %+v", cached)
	}
}
