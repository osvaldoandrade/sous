package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// invokeWithStore is a small helper shared across egress tests: it
// stands up a fake persistence and broker, runs a single activation,
// and returns the published InvocationResult so tests can assert on
// the wire shape (status, error code) that downstream callers will
// observe.
func invokeWithStore(t *testing.T, manifest api.FunctionManifest, script string, store *testutil.FakePersistence, policy *api.EgressPolicy) api.InvocationResult {
	t.Helper()
	bundleBytes, sha := buildInvokerBundleWithManifest(t, manifest, script)
	var published api.InvocationResult

	store.IsActivationTerminalFn = func(context.Context, string, string) (bool, api.ActivationRecord, error) {
		return false, api.ActivationRecord{}, nil
	}
	store.ResolveVersionFn = func(context.Context, string, string, string, string, int64) (int64, error) {
		return 1, nil
	}
	store.GetVersionFn = func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
		return api.VersionRecord{
			SHA256: sha,
			Config: api.VersionConfig{TimeoutMS: manifest.Limits.TimeoutMS, MaxConcurrency: 1},
		}, bundleBytes, nil
	}
	store.PutActivationRunningFn = func(context.Context, api.ActivationRecord, time.Duration) error { return nil }
	store.CompleteActivationCASFn = func(context.Context, api.ActivationRecord, time.Duration) (bool, error) {
		return true, nil
	}
	store.SaveResultByRequestIDFn = func(context.Context, string, string, api.InvocationResult, time.Duration) error { return nil }
	if policy != nil {
		p := *policy
		store.GetEgressPolicyFn = func(context.Context, string) (api.EgressPolicy, error) {
			return p, nil
		}
	}

	broker := &testutil.FakeMessaging{
		PublishResultFn: func(_ context.Context, _ string, result api.InvocationResult) error {
			published = result
			return nil
		},
	}

	i := newTestInvoker(store, broker)
	if err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act-egress",
		RequestID:    "req-egress",
		Tenant:       "t_abc123",
		Namespace:    "ns",
		Ref:          api.FunctionRef{Function: "fn", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		DeadlineMS:   time.Now().Add(2 * time.Second).UnixMilli(),
		Event:        map[string]any{},
	}); err != nil {
		t.Fatalf("handleInvocation: %v", err)
	}
	return published
}

func TestEgressPolicyDeniesUnlistedHost(t *testing.T) {
	// Run a fake target so cs.http.fetch resolves against a known
	// address; the policy rejects it before the dial happens.
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("forbidden"))
	}))
	defer target.Close()

	manifest := defaultInvokerManifest()
	// Manifest allows the host so the only gate left is the tenant
	// egress matcher — without it the call would succeed.
	manifest.Capabilities.HTTP.AllowHosts = []string{"127.0.0.1"}

	store := &testutil.FakePersistence{}
	policy := &api.EgressPolicy{
		AllowedHosts: []string{"good.example.com"},
		DefaultDeny:  true,
	}

	script := `export default function(event){
  cs.http.fetch(event.url)
  return { statusCode: 200, body: "ok" }
}`
	manifest.Capabilities.HTTP.AllowHosts = []string{"127.0.0.1", "good.example.com"}

	result := invokeWithStore(t, manifest, script, store, policy)
	if result.Status != "error" || result.Error == nil {
		t.Fatalf("expected error result, got %+v", result)
	}
	if !strings.Contains(result.Error.Message, string(cserrors.CSEgressDenied)) {
		t.Fatalf("expected CS_EGRESS_DENIED in error, got %q", result.Error.Message)
	}
	if result.Error.Type != "EgressDenied" {
		t.Fatalf("expected error Type=EgressDenied, got %q", result.Error.Type)
	}
}

func TestEgressPolicyAllowsListedHost(t *testing.T) {
	// Pre-populate matcher with the test server's host so the egress
	// allowlist permits the call and the fetch round-trips.
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer target.Close()

	manifest := defaultInvokerManifest()
	host := strings.TrimPrefix(target.URL, "http://")
	host = strings.Split(host, ":")[0]
	manifest.Capabilities.HTTP.AllowHosts = []string{host}

	store := &testutil.FakePersistence{}
	policy := &api.EgressPolicy{
		AllowedHosts: []string{host},
		DefaultDeny:  true,
	}

	script := `export default function(event){
  const r = cs.http.fetch(event.url)
  return { statusCode: 200, body: String(r.status) }
}`

	bundleBytes, sha := buildInvokerBundleWithManifest(t, manifest, script)
	store.IsActivationTerminalFn = func(context.Context, string, string) (bool, api.ActivationRecord, error) {
		return false, api.ActivationRecord{}, nil
	}
	store.ResolveVersionFn = func(context.Context, string, string, string, string, int64) (int64, error) {
		return 1, nil
	}
	store.GetVersionFn = func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
		return api.VersionRecord{
			SHA256: sha,
			Config: api.VersionConfig{TimeoutMS: manifest.Limits.TimeoutMS, MaxConcurrency: 1},
		}, bundleBytes, nil
	}
	store.PutActivationRunningFn = func(context.Context, api.ActivationRecord, time.Duration) error { return nil }
	store.CompleteActivationCASFn = func(context.Context, api.ActivationRecord, time.Duration) (bool, error) {
		return true, nil
	}
	store.SaveResultByRequestIDFn = func(context.Context, string, string, api.InvocationResult, time.Duration) error { return nil }
	p := *policy
	store.GetEgressPolicyFn = func(context.Context, string) (api.EgressPolicy, error) { return p, nil }

	var published api.InvocationResult
	broker := &testutil.FakeMessaging{
		PublishResultFn: func(_ context.Context, _ string, result api.InvocationResult) error {
			published = result
			return nil
		},
	}
	i := newTestInvoker(store, broker)
	// Lift private-IP guard so the test server's loopback target
	// passes once the egress allowlist accepts it.
	i.runner.SetAllowPrivateIPCheck(false)

	err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act-egress-ok",
		RequestID:    "req-egress-ok",
		Tenant:       "t_abc123",
		Namespace:    "ns",
		Ref:          api.FunctionRef{Function: "fn", Version: 1},
		Trigger:      api.Trigger{Type: "api", Source: map[string]any{}},
		DeadlineMS:   time.Now().Add(2 * time.Second).UnixMilli(),
		Event:        map[string]any{"url": target.URL},
	})
	if err != nil {
		t.Fatalf("handleInvocation: %v", err)
	}
	if published.Status != "success" {
		t.Fatalf("expected success, got status=%q error=%+v", published.Status, published.Error)
	}
}

func TestEgressPolicyMissingFallsThroughToLegacyAllowHosts(t *testing.T) {
	// No tenant policy installed: the runner must fall back to the
	// manifest http.allowHosts behaviour. A host not in the manifest
	// list should still be rejected as CS_RUNTIME_CAPABILITY_DENIED.
	manifest := defaultInvokerManifest()
	manifest.Capabilities.HTTP.AllowHosts = []string{"good.example.com"}

	store := &testutil.FakePersistence{}
	// Default behavior of GetEgressPolicy in testutil is "not found",
	// so the matcher load returns nil.

	script := `export default function(event){
  cs.http.fetch("http://other.example.com")
  return { statusCode: 200, body: "ok" }
}`

	result := invokeWithStore(t, manifest, script, store, nil)
	if result.Status != "error" || result.Error == nil {
		t.Fatalf("expected error, got %+v", result)
	}
	// With no egress policy, the deny path is the legacy manifest
	// allowHosts check (CSRuntimeCapDenied), not CS_EGRESS_DENIED.
	if strings.Contains(result.Error.Message, string(cserrors.CSEgressDenied)) {
		t.Fatalf("expected legacy capability-denied error, got egress denied: %s", result.Error.Message)
	}
	if !strings.Contains(result.Error.Message, "host not allowed") {
		t.Fatalf("expected host-not-allowed legacy message, got %q", result.Error.Message)
	}
}

func TestLoadEgressMatcherBadStoredPolicyFallsThrough(t *testing.T) {
	store := &testutil.FakePersistence{
		GetEgressPolicyFn: func(context.Context, string) (api.EgressPolicy, error) {
			return api.EgressPolicy{AllowedCIDRs: []string{"not-a-cidr"}}, nil
		},
	}
	i := newTestInvoker(store, &testutil.FakeMessaging{})
	if got := i.loadEgressMatcher(context.Background(), "t_abc123"); got != nil {
		t.Fatal("malformed stored policy must not yield a matcher")
	}
}

func TestLoadEgressMatcherEmptyTenant(t *testing.T) {
	i := newTestInvoker(&testutil.FakePersistence{}, &testutil.FakeMessaging{})
	if got := i.loadEgressMatcher(context.Background(), ""); got != nil {
		t.Fatal("empty tenant must not yield a matcher")
	}
}
