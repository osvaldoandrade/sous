package main

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/codeq"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
)

// subscriptionRefreshDefault is the fallback reconciliation interval for the
// in-process subscription runner. It is overridable via
// cs_control.subscriptions.refresh_seconds.
const subscriptionRefreshDefault = 10 * time.Second

// runSubscriptions reconciles the set of enabled SubscriptionBindings against
// running consumer goroutines on a refresh tick. New bindings spawn a
// goroutine; bindings that disappear from persistence (delete / disable) have
// their consumer context cancelled. The loop exits when ctx is cancelled.
func (s *server) runSubscriptions(ctx context.Context) {
	interval := subscriptionRefreshDefault
	if v := s.cfg.CSControl.Subscriptions.RefreshSeconds; v > 0 {
		interval = time.Duration(v) * time.Second
	}
	mu := &sync.Mutex{}
	active := map[string]context.CancelFunc{}

	startBinding := func(b api.SubscriptionBinding) {
		bindCtx, cancel := context.WithCancel(ctx)
		key := subscriptionKey(b)
		mu.Lock()
		active[key] = cancel
		mu.Unlock()
		go func() {
			defer func() {
				mu.Lock()
				delete(active, key)
				mu.Unlock()
			}()
			if err := s.runOneSubscription(bindCtx, b); err != nil && bindCtx.Err() == nil {
				s.logger.Error(bindCtx, "subscription consumer exited: "+err.Error())
			}
		}()
	}

	reconcile := func() {
		bindings, err := s.store.ListAllSubscriptionBindings(ctx)
		if err != nil {
			s.logger.Error(ctx, "failed to list subscriptions: "+err.Error())
			return
		}
		want := map[string]api.SubscriptionBinding{}
		for _, b := range bindings {
			if !b.Enabled {
				continue
			}
			want[subscriptionKey(b)] = b
		}
		mu.Lock()
		for k, cancel := range active {
			if _, ok := want[k]; !ok {
				cancel()
			}
		}
		// Capture the set of currently-running keys before unlocking so we
		// can start new goroutines outside the lock.
		runningKeys := make(map[string]struct{}, len(active))
		for k := range active {
			runningKeys[k] = struct{}{}
		}
		mu.Unlock()
		for k, b := range want {
			if _, ok := runningKeys[k]; ok {
				continue
			}
			startBinding(b)
		}
	}

	reconcile()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			mu.Lock()
			for _, cancel := range active {
				cancel()
			}
			mu.Unlock()
			return
		case <-ticker.C:
			reconcile()
		}
	}
}

// runOneSubscription wires a single binding to the broker. It builds a
// codeq.SubscriptionConsumer adapter that re-shapes the broker's Envelope into
// codeq.Envelope so the shared filter/dispatcher machinery works regardless of
// the messaging plugin.
func (s *server) runOneSubscription(ctx context.Context, b api.SubscriptionBinding) error {
	filter, err := codeq.ParseFilter(b.Filter)
	if err != nil {
		// We already rejected invalid filters at create time, but a stale
		// binding (e.g. left over from an older schema) is still possible.
		s.logger.Error(ctx, "skipping subscription with invalid filter: "+err.Error())
		return nil
	}
	consumer := &brokerConsumerAdapter{broker: s.broker}
	handler := s.subscriptionHandler(b)
	return codeq.RunSubscription(ctx, consumer, b.Topic, b.GroupID, b.Mode, b.MaxConcurrency, filter, handler)
}

// subscriptionHandler returns the per-binding codeq.SubscriptionHandler. The
// closure builds an InvocationRequest with `trigger.type = "subscription"`
// (matching docs/07-codeq-protocol.md) and publishes it through the same
// broker path used by HTTP/schedule/cadence triggers so observability,
// capability, and IAM checks are reused without duplication.
func (s *server) subscriptionHandler(b api.SubscriptionBinding) codeq.SubscriptionHandler {
	return func(ctx context.Context, env codeq.Envelope) error {
		activationID := uuid.NewString()
		reqID := "req_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		inv := api.InvocationRequest{
			ActivationID: activationID,
			RequestID:    reqID,
			Tenant:       b.Tenant,
			Namespace:    b.Namespace,
			Ref: api.FunctionRef{
				Function: b.FunctionRef.Function,
				Alias:    b.FunctionRef.Alias,
				Version:  b.FunctionRef.Version,
			},
			Trigger: api.Trigger{
				Type: "subscription",
				Source: map[string]any{
					"binding":     b.Name,
					"topic":       b.Topic,
					"envelope_id": env.ID,
					"group_id":    b.GroupID,
				},
			},
			Principal:  api.Principal{Sub: "subscription:" + b.Name, Roles: []string{"role:subscription"}},
			DeadlineMS: time.Now().Add(30 * time.Second).UnixMilli(),
			Event:      env.Body,
		}
		return s.broker.PublishInvocation(ctx, inv)
	}
}

// subscriptionKey is the in-memory identity used to decide whether a running
// goroutine still satisfies a binding after reconcile. Changing the topic,
// mode, filter, or function ref produces a new key so the consumer is
// restarted with the fresh config.
func subscriptionKey(b api.SubscriptionBinding) string {
	return b.Tenant + "/" + b.Namespace + "/" + b.Name + "@" + b.Topic + "|" + b.Mode + "|" + b.Filter
}

// brokerConsumerAdapter bridges messaging.Provider.ConsumeTopic to the
// codeq.SubscriptionConsumer contract.
type brokerConsumerAdapter struct {
	broker messaging.Provider
}

func (a *brokerConsumerAdapter) ConsumeTopic(ctx context.Context, topic, groupID string, handler func(codeq.Envelope) error) error {
	return a.broker.ConsumeTopic(ctx, topic, groupID, func(env messaging.Envelope) error {
		return handler(codeq.Envelope{
			Schema: env.Schema,
			ID:     env.ID,
			TSMS:   env.TSMS,
			Tenant: env.Tenant,
			Type:   env.Type,
			Body:   env.Body,
		})
	})
}
