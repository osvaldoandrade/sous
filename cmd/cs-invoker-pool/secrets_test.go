package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	secretsmemory "github.com/osvaldoandrade/sous/internal/plugins/secrets/memory"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// buildSecretAwareBundle constructs a bundle whose handler echoes the
// resolved secret back through the response body so the test can assert
// that runtime.WithEnv plumbed it through cs.env.get correctly.
func buildSecretAwareBundle(t *testing.T) ([]byte, string) {
	t.Helper()
	manifest := defaultInvokerManifest()
	script := `export default function(event, ctx) {
		const v = cs.env.get("STRIPE_KEY");
		const names = cs.env.list();
		return {
			statusCode: 200,
			headers: { "x-name": names.join(",") },
			body: JSON.stringify({ stripe: v, names })
		};
	}`
	return buildInvokerBundleWithManifest(t, manifest, script)
}

func TestActivationInjectsSecretsAsEnv(t *testing.T) {
	bundleBytes, sha := buildSecretAwareBundle(t)

	store := &testutil.FakePersistence{
		IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
			return false, api.ActivationRecord{}, nil
		},
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
			return 1, nil
		},
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				SHA256: sha,
				Config: api.VersionConfig{
					TimeoutMS:      300,
					MaxConcurrency: 1,
					Secrets:        []string{"STRIPE_KEY=payments/stripe"},
				},
			}, bundleBytes, nil
		},
		PutActivationRunningFn:  func(context.Context, api.ActivationRecord, time.Duration) error { return nil },
		CompleteActivationCASFn: func(context.Context, api.ActivationRecord, time.Duration) (bool, error) { return true, nil },
		SaveResultByRequestIDFn: func(context.Context, string, string, api.InvocationResult, time.Duration) error { return nil },
	}

	var published api.InvocationResult
	broker := &testutil.FakeMessaging{
		PublishResultFn: func(_ context.Context, _ string, res api.InvocationResult) error {
			published = res
			return nil
		},
	}
	i := newTestInvoker(store, broker)
	// Swap in a memory secrets provider seeded with the value the
	// manifest reference points at. The reference name (STRIPE_KEY) is
	// what the function sees via cs.env.get; the path (payments/stripe)
	// is what the driver looks up.
	mem := secretsmemory.New(map[string]string{
		"payments/stripe": "sk_live_TEST",
	})
	i.secretsProvider = mem

	err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act-secret-1",
		RequestID:    "req-secret-1",
		Tenant:       "t_acme",
		Namespace:    "payments",
		Ref:          api.FunctionRef{Function: "checkout", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		DeadlineMS:   time.Now().Add(time.Second).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("handleInvocation: %v", err)
	}
	if published.Status != "success" {
		t.Fatalf("status: want success, got %s err=%+v", published.Status, published.Error)
	}
	if published.Result == nil {
		t.Fatalf("nil result")
	}

	var body struct {
		Stripe string   `json:"stripe"`
		Names  []string `json:"names"`
	}
	if err := json.Unmarshal([]byte(published.Result.Body), &body); err != nil {
		t.Fatalf("decode body %q: %v", published.Result.Body, err)
	}
	if body.Stripe != "sk_live_TEST" {
		t.Fatalf("body.stripe: want sk_live_TEST, got %q", body.Stripe)
	}
	if len(body.Names) != 1 || body.Names[0] != "STRIPE_KEY" {
		t.Fatalf("body.names: want [STRIPE_KEY], got %v", body.Names)
	}
}

func TestActivationFailsWhenSecretMissing(t *testing.T) {
	manifest := defaultInvokerManifest()
	script := `export default function() { return { statusCode: 200, body: "ok" } }`
	bundleBytes, sha := buildInvokerBundleWithManifest(t, manifest, script)

	store := &testutil.FakePersistence{
		IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
			return false, api.ActivationRecord{}, nil
		},
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) { return 1, nil },
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				SHA256: sha,
				Config: api.VersionConfig{
					TimeoutMS:      300,
					MaxConcurrency: 1,
					Secrets:        []string{"X=missing/path"},
				},
			}, bundleBytes, nil
		},
		PutActivationRunningFn:  func(context.Context, api.ActivationRecord, time.Duration) error { return nil },
		CompleteActivationCASFn: func(context.Context, api.ActivationRecord, time.Duration) (bool, error) { return true, nil },
	}
	i := newTestInvoker(store, &testutil.FakeMessaging{})
	i.secretsProvider = secretsmemory.New(nil)

	err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act-secret-2",
		RequestID:    "req-secret-2",
		Tenant:       "t_acme",
		Namespace:    "payments",
		Ref:          api.FunctionRef{Function: "checkout", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		DeadlineMS:   time.Now().Add(time.Second).UnixMilli(),
	})
	if err == nil {
		t.Fatal("expected CS_SECRET_NOT_FOUND error")
	}
	var cserr *cserrors.CSError
	if !errors.As(err, &cserr) {
		t.Fatalf("expected *CSError, got %T (%v)", err, err)
	}
	if cserr.Code != cserrors.CSSecretNotFound {
		t.Fatalf("expected CS_SECRET_NOT_FOUND, got %s", cserr.Code)
	}
}

func TestActivationWithoutSecretsRunsCleanly(t *testing.T) {
	bundleBytes, sha := buildInvokerBundle(t)
	store := &testutil.FakePersistence{
		IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
			return false, api.ActivationRecord{}, nil
		},
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) { return 1, nil },
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				SHA256: sha,
				Config: api.VersionConfig{TimeoutMS: 300, MaxConcurrency: 1},
			}, bundleBytes, nil
		},
		PutActivationRunningFn:  func(context.Context, api.ActivationRecord, time.Duration) error { return nil },
		CompleteActivationCASFn: func(context.Context, api.ActivationRecord, time.Duration) (bool, error) { return true, nil },
		SaveResultByRequestIDFn: func(context.Context, string, string, api.InvocationResult, time.Duration) error { return nil },
	}
	var published api.InvocationResult
	broker := &testutil.FakeMessaging{
		PublishResultFn: func(_ context.Context, _ string, res api.InvocationResult) error {
			published = res
			return nil
		},
	}
	i := newTestInvoker(store, broker)
	// Even with a nil-seeded memory provider, an empty Secrets list
	// should bypass resolution and not raise CS_SECRET_NOT_FOUND.
	i.secretsProvider = secretsmemory.New(nil)
	err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act-clean",
		RequestID:    "req-clean",
		Tenant:       "t_acme",
		Namespace:    "payments",
		Ref:          api.FunctionRef{Function: "checkout", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		DeadlineMS:   time.Now().Add(time.Second).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("handleInvocation: %v", err)
	}
	if published.Status != "success" {
		t.Fatalf("status: want success, got %s", published.Status)
	}
}
