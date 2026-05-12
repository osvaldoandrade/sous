package main

// E5.02 invoker-side signature verification tests.

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"strings"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/kv"
	"github.com/osvaldoandrade/sous/internal/plugins/messaging"
	"github.com/osvaldoandrade/sous/internal/signing"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

type signedVersionFixture struct {
	tenant    string
	namespace string
	function  string
	bundle    []byte
	sha       string
	keyPair   signing.KeyPair
	keyRecRaw []byte
	signature *api.BundleSignature
}

func newSignedVersionFixture(t *testing.T) signedVersionFixture {
	t.Helper()
	tenant, namespace, function := "t_abc123", "ns1", "fn1"
	b, sha := buildInvokerBundle(t)
	pair := signing.Generate()
	payload, err := signing.CanonicalPayloadFromHexSHA(sha, tenant, namespace, function, 0)
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	sig := signing.Sign(payload, pair.Private)
	keyRec := api.TenantSigningKey{
		KID:         "kid_inv0001",
		Algorithm:   signing.Algorithm,
		PublicKey:   pair.Public,
		CreatedAtMS: time.Now().UnixMilli(),
	}
	raw, err := json.Marshal(keyRec)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	return signedVersionFixture{
		tenant:    tenant,
		namespace: namespace,
		function:  function,
		bundle:    b,
		sha:       sha,
		keyPair:   pair,
		keyRecRaw: raw,
		signature: &api.BundleSignature{
			KID:       keyRec.KID,
			Algorithm: signing.Algorithm,
			Sig:       sig,
			SignedAt:  time.Now().UnixMilli(),
		},
	}
}

func (f signedVersionFixture) newStore(t *testing.T, persistedBundle []byte) *testutil.FakePersistence {
	t.Helper()
	return &testutil.FakePersistence{
		KVGetFn: func(_ context.Context, key string) (string, error) {
			if key == kv.TenantSigningKeysKey(f.tenant) {
				return string(f.keyRecRaw), nil
			}
			return "", cserrors.New(cserrors.CSValidationFailed, "missing key")
		},
		IsActivationTerminalFn: func(context.Context, string, string) (bool, api.ActivationRecord, error) {
			return false, api.ActivationRecord{}, nil
		},
		ResolveVersionFn: func(context.Context, string, string, string, string, int64) (int64, error) {
			return 7, nil
		},
		GetVersionFn: func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
			return api.VersionRecord{
				SHA256:    f.sha,
				Config:    api.VersionConfig{TimeoutMS: 300, MaxConcurrency: 1},
				Signature: f.signature,
			}, persistedBundle, nil
		},
		PutActivationRunningFn:  func(context.Context, api.ActivationRecord, time.Duration) error { return nil },
		CompleteActivationCASFn: func(context.Context, api.ActivationRecord, time.Duration) (bool, error) { return true, nil },
	}
}

func TestInvokerVerifyValidSignatureAllowsExecution(t *testing.T) {
	fx := newSignedVersionFixture(t)
	store := fx.newStore(t, fx.bundle)
	i := newTestInvoker(store, &testutil.FakeMessaging{})

	err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act_ok",
		RequestID:    "req_ok",
		Tenant:       fx.tenant,
		Namespace:    fx.namespace,
		Ref:          api.FunctionRef{Function: fx.function, Alias: "prod"},
		DeadlineMS:   time.Now().Add(time.Second).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("valid signature should not have failed: %v", err)
	}
}

func TestInvokerTamperedBundleIsRejected(t *testing.T) {
	fx := newSignedVersionFixture(t)
	tampered := append([]byte(nil), fx.bundle...)
	if len(tampered) == 0 {
		t.Fatal("empty bundle")
	}
	tampered[0] ^= 0x55
	store := fx.newStore(t, tampered)
	i := newTestInvoker(store, &testutil.FakeMessaging{})

	err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act_bad",
		RequestID:    "req_bad",
		Tenant:       fx.tenant,
		Namespace:    fx.namespace,
		Ref:          api.FunctionRef{Function: fx.function, Alias: "prod"},
		DeadlineMS:   time.Now().Add(time.Second).UnixMilli(),
	})
	if err == nil {
		t.Fatal("tampered bundle should have been rejected")
	}
	if !strings.Contains(err.Error(), "sha mismatch") && !isSignatureErr(err) {
		t.Fatalf("expected sha or signature failure, got %v", err)
	}
}

func TestInvokerSignatureMismatchAfterKeyRotation(t *testing.T) {
	fx := newSignedVersionFixture(t)
	rotated := signing.Generate()
	newKey := api.TenantSigningKey{
		KID:         "kid_rotated_1",
		Algorithm:   signing.Algorithm,
		PublicKey:   rotated.Public,
		CreatedAtMS: time.Now().UnixMilli(),
	}
	raw, err := json.Marshal(newKey)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	fx.keyRecRaw = raw

	store := fx.newStore(t, fx.bundle)
	i := newTestInvoker(store, &testutil.FakeMessaging{})

	err = i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act_rot",
		RequestID:    "req_rot",
		Tenant:       fx.tenant,
		Namespace:    fx.namespace,
		Ref:          api.FunctionRef{Function: fx.function, Alias: "prod"},
		DeadlineMS:   time.Now().Add(time.Second).UnixMilli(),
	})
	if err == nil {
		t.Fatal("rotated key should have invalidated the signature")
	}
	if !isSignatureErr(err) {
		t.Fatalf("expected CSSignatureInvalid, got %v", err)
	}
}

func TestInvokerLegacyVersionWithoutSignatureExecutes(t *testing.T) {
	fx := newSignedVersionFixture(t)
	store := fx.newStore(t, fx.bundle)
	store.GetVersionFn = func(context.Context, string, string, string, int64) (api.VersionRecord, []byte, error) {
		return api.VersionRecord{
			SHA256: fx.sha,
			Config: api.VersionConfig{TimeoutMS: 300, MaxConcurrency: 1},
		}, fx.bundle, nil
	}
	i := newTestInvoker(store, &testutil.FakeMessaging{})
	err := i.handleInvocation(context.Background(), messaging.Envelope{}, api.InvocationRequest{
		ActivationID: "act_legacy",
		RequestID:    "req_legacy",
		Tenant:       fx.tenant,
		Namespace:    fx.namespace,
		Ref:          api.FunctionRef{Function: fx.function, Alias: "prod"},
		DeadlineMS:   time.Now().Add(time.Second).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("legacy unsigned version should execute, got %v", err)
	}
}

func isSignatureErr(err error) bool {
	var cs *cserrors.CSError
	if stderrors.As(err, &cs) {
		return cs.Code == cserrors.CSSignatureInvalid
	}
	return false
}
