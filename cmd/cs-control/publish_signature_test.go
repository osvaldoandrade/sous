package main

// Publish-time signature gate tests for E5.02. These tests cover the
// three exit points from verifyPublishSignature():
//
//   - missing header + required=true  -> CS_SIGNATURE_MISSING (400)
//   - bad signature                   -> CS_SIGNATURE_INVALID (400)
//   - valid signature                 -> 200 + Signature persisted

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/kv"
	"github.com/osvaldoandrade/sous/internal/signing"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

type signatureFixture struct {
	draft      api.DraftRecord
	bundleSHA  string
	keyPair    signing.KeyPair
	keyRec     api.TenantSigningKey
	storage    map[string]string
	signature  []byte
	signatureB string
}

func newSignatureFixture(t *testing.T, tenant, namespace, function string) signatureFixture {
	t.Helper()
	files := map[string][]byte{
		"function.js":   []byte(`export default function(){ return { statusCode: 200, body: "ok" } }`),
		"manifest.json": manifestWithImports(t, nil),
	}
	draft, sha := freezeDraftBundle(t, files)
	pair := signing.Generate()
	rec := api.TenantSigningKey{
		KID:         "kid_test1234",
		Algorithm:   signing.Algorithm,
		PublicKey:   pair.Public,
		CreatedAtMS: time.Now().UnixMilli(),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	storage := map[string]string{
		kv.TenantSigningKeysKey(tenant): string(raw),
	}
	payload, err := signing.CanonicalPayloadFromHexSHA(sha, tenant, namespace, function, 0)
	if err != nil {
		t.Fatalf("canonical payload: %v", err)
	}
	sig := signing.Sign(payload, pair.Private)
	return signatureFixture{
		draft:      draft,
		bundleSHA:  sha,
		keyPair:    pair,
		keyRec:     rec,
		storage:    storage,
		signature:  sig,
		signatureB: base64.StdEncoding.EncodeToString(sig),
	}
}

func (f signatureFixture) newStore(draft api.DraftRecord) *testutil.FakePersistence {
	storage := f.storage
	return &testutil.FakePersistence{
		KVGetFn: func(_ context.Context, key string) (string, error) {
			if v, ok := storage[key]; ok {
				return v, nil
			}
			return "", cserrors.New(cserrors.CSValidationFailed, "key not found")
		},
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
		PublishVersionFn: func(context.Context, string, string, string, api.VersionRecord, []byte, string) (int64, error) {
			return 1, nil
		},
		MarkDraftConsumedFn: func(context.Context, string, string, string, string, time.Duration) error { return nil },
	}
}

func TestPublishVersionSignatureMissingWhenRequired(t *testing.T) {
	tenant, namespace, function := "t_abc123", "ns1", "fn1"
	fx := newSignatureFixture(t, tenant, namespace, function)
	store := fx.newStore(fx.draft)
	s := newControlServer(store, &testutil.FakeMessaging{})
	s.cfg.Plugins.Signing.Required = true

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": tenant, "namespace": namespace, "name": function,
	}, principalWith(tenant, "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSSignatureMissing {
		t.Fatalf("expected CSSignatureMissing, got %s", code)
	}
}

func TestPublishVersionSignatureMissingToleratedWhenNotRequired(t *testing.T) {
	tenant, namespace, function := "t_abc123", "ns1", "fn1"
	fx := newSignatureFixture(t, tenant, namespace, function)
	store := fx.newStore(fx.draft)
	var captured *api.BundleSignature
	store.PublishVersionFn = func(_ context.Context, _, _, _ string, rec api.VersionRecord, _ []byte, _ string) (int64, error) {
		captured = rec.Signature
		return 1, nil
	}
	s := newControlServer(store, &testutil.FakeMessaging{})
	s.cfg.Plugins.Signing.Required = false

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": tenant, "namespace": namespace, "name": function,
	}, principalWith(tenant, "cs:function:publish"))
	s.publishVersion(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", w.Code, w.Body.String())
	}
	if captured != nil {
		t.Fatalf("did not expect a signature on the version record: %+v", captured)
	}
}

func TestPublishVersionSignatureInvalid(t *testing.T) {
	tenant, namespace, function := "t_abc123", "ns1", "fn1"
	fx := newSignatureFixture(t, tenant, namespace, function)
	store := fx.newStore(fx.draft)
	s := newControlServer(store, &testutil.FakeMessaging{})
	s.cfg.Plugins.Signing.Required = true

	other := signing.Generate()
	payload, err := signing.CanonicalPayloadFromHexSHA(fx.bundleSHA, tenant, namespace, function, 0)
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	badSig := signing.Sign(payload, other.Private)

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": tenant, "namespace": namespace, "name": function,
	}, principalWith(tenant, "cs:function:publish"))
	req.Header.Set(SignatureHeader, base64.StdEncoding.EncodeToString(badSig))
	s.publishVersion(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSSignatureInvalid {
		t.Fatalf("expected CSSignatureInvalid, got %s", code)
	}
}

func TestPublishVersionSignatureMalformedBase64(t *testing.T) {
	tenant, namespace, function := "t_abc123", "ns1", "fn1"
	fx := newSignatureFixture(t, tenant, namespace, function)
	store := fx.newStore(fx.draft)
	s := newControlServer(store, &testutil.FakeMessaging{})

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": tenant, "namespace": namespace, "name": function,
	}, principalWith(tenant, "cs:function:publish"))
	req.Header.Set(SignatureHeader, "not!base64@@")
	s.publishVersion(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d body=%s", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSSignatureInvalid {
		t.Fatalf("expected CSSignatureInvalid, got %s", code)
	}
}

func TestPublishVersionSignatureKeyNotFound(t *testing.T) {
	tenant, namespace, function := "t_abc123", "ns1", "fn1"
	files := map[string][]byte{
		"function.js":   []byte(`export default function(){ return { statusCode: 200, body: "ok" } }`),
		"manifest.json": manifestWithImports(t, nil),
	}
	draft, sha := freezeDraftBundle(t, files)
	pair := signing.Generate()
	payload, err := signing.CanonicalPayloadFromHexSHA(sha, tenant, namespace, function, 0)
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	sig := signing.Sign(payload, pair.Private)
	store := &testutil.FakePersistence{
		KVGetFn: func(context.Context, string) (string, error) {
			return "", cserrors.New(cserrors.CSValidationFailed, "key not found")
		},
		GetDraftFn: func(context.Context, string, string, string, string) (api.DraftRecord, error) {
			return draft, nil
		},
	}
	s := newControlServer(store, &testutil.FakeMessaging{})

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": tenant, "namespace": namespace, "name": function,
	}, principalWith(tenant, "cs:function:publish"))
	req.Header.Set(SignatureHeader, base64.StdEncoding.EncodeToString(sig))
	s.publishVersion(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body=%s", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSSignatureKeyNotFound {
		t.Fatalf("expected CSSignatureKeyNotFound, got %s", code)
	}
}

func TestPublishVersionValidSignaturePersisted(t *testing.T) {
	tenant, namespace, function := "t_abc123", "ns1", "fn1"
	fx := newSignatureFixture(t, tenant, namespace, function)
	store := fx.newStore(fx.draft)
	var captured *api.BundleSignature
	store.PublishVersionFn = func(_ context.Context, _, _, _ string, rec api.VersionRecord, _ []byte, _ string) (int64, error) {
		captured = rec.Signature
		return 1, nil
	}
	s := newControlServer(store, &testutil.FakeMessaging{})
	s.cfg.Plugins.Signing.Required = true

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodPost, "/publish", `{"draft_id":"drf_1"}`, map[string]string{
		"tenant": tenant, "namespace": namespace, "name": function,
	}, principalWith(tenant, "cs:function:publish"))
	req.Header.Set(SignatureHeader, fx.signatureB)
	s.publishVersion(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", w.Code, w.Body.String())
	}
	if captured == nil {
		t.Fatal("publish handler did not persist a signature record")
	}
	if captured.KID != fx.keyRec.KID {
		t.Fatalf("kid=%s want=%s", captured.KID, fx.keyRec.KID)
	}
	if captured.Algorithm != signing.Algorithm {
		t.Fatalf("algorithm=%s", captured.Algorithm)
	}
	if string(captured.Sig) != string(fx.signature) {
		t.Fatal("persisted signature bytes drift from input")
	}
	payload, err := signing.CanonicalPayloadFromHexSHA(fx.bundleSHA, tenant, namespace, function, 0)
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	if !signing.Verify(payload, captured.Sig, fx.keyPair.Public) {
		t.Fatal("persisted signature no longer verifies")
	}
}
