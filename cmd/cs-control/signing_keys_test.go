package main

// Tests for the E5.02 signing-key management surface. The handler is
// exercised with the same FakePersistence pattern used by
// publish_imports_test.go: a tiny in-memory KV map stands in for
// kvrocks so rotate/fetch round-trips run without a real backend.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/kv"
	"github.com/osvaldoandrade/sous/internal/signing"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

type inMemKV struct {
	mu sync.Mutex
	m  map[string]string
}

func newInMemKV() *inMemKV { return &inMemKV{m: make(map[string]string)} }

func (k *inMemKV) get(_ context.Context, key string) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	v, ok := k.m[key]
	if !ok {
		return "", cserrors.New(cserrors.CSValidationFailed, "key not found")
	}
	return v, nil
}

func (k *inMemKV) set(_ context.Context, key, value string, _ time.Duration) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m[key] = value
	return nil
}

func newSigningTestServer(t *testing.T) (*server, *inMemKV) {
	t.Helper()
	store := newInMemKV()
	fp := &testutil.FakePersistence{
		KVGetFn: store.get,
		KVSetFn: store.set,
	}
	return newControlServer(fp, &testutil.FakeMessaging{}), store
}

func TestRotateSigningKeyReturnsPrivateKeyOnce(t *testing.T) {
	s, store := newSigningTestServer(t)
	w := httptest.NewRecorder()
	r := controlRequest(http.MethodPost,
		"/v1/tenants/t_abc123/signing-keys/rotate", "",
		map[string]string{"tenant": "t_abc123"},
		principalWith("t_abc123", "cs:tenant:signing-key:rotate"))
	s.rotateSigningKey(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var resp rotateSigningKeyResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}
	if resp.Algorithm != signing.Algorithm {
		t.Fatalf("algorithm=%s want=%s", resp.Algorithm, signing.Algorithm)
	}
	if len(resp.PublicKey) == 0 || len(resp.PrivateKey) == 0 {
		t.Fatalf("expected non-empty key bytes: pub=%d priv=%d",
			len(resp.PublicKey), len(resp.PrivateKey))
	}
	if resp.KID == "" {
		t.Fatal("KID should be non-empty")
	}
	stored, ok := store.m[kv.TenantSigningKeysKey("t_abc123")]
	if !ok {
		t.Fatal("rotate did not persist the public key under the documented kv key")
	}
	var rec api.TenantSigningKey
	if err := json.Unmarshal([]byte(stored), &rec); err != nil {
		t.Fatalf("kv record decode: %v", err)
	}
	if string(rec.PublicKey) != string(resp.PublicKey) {
		t.Fatal("persisted public key differs from response")
	}
	if strings.Contains(stored, "private_key") {
		t.Fatalf("kv record leaks private key: %s", stored)
	}
}

func TestGetActiveSigningKeyReturnsPublicAfterRotate(t *testing.T) {
	s, _ := newSigningTestServer(t)
	w := httptest.NewRecorder()
	r := controlRequest(http.MethodPost,
		"/v1/tenants/t_abc123/signing-keys/rotate", "",
		map[string]string{"tenant": "t_abc123"},
		principalWith("t_abc123", "cs:tenant:signing-key:rotate"))
	s.rotateSigningKey(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("rotate failed: %s", w.Body.String())
	}
	var rotated rotateSigningKeyResponse
	if err := json.Unmarshal(w.Body.Bytes(), &rotated); err != nil {
		t.Fatalf("decode rotate: %v", err)
	}

	w2 := httptest.NewRecorder()
	r2 := controlRequest(http.MethodGet,
		"/v1/tenants/t_abc123/signing-keys/active", "",
		map[string]string{"tenant": "t_abc123"},
		principalWith("t_abc123", "cs:tenant:signing-key:read"))
	s.getActiveSigningKey(w2, r2)
	if w2.Code != http.StatusOK {
		t.Fatalf("fetch status=%d body=%s", w2.Code, w2.Body.String())
	}
	var fetched api.TenantSigningKey
	if err := json.Unmarshal(w2.Body.Bytes(), &fetched); err != nil {
		t.Fatalf("decode fetch: %v body=%s", err, w2.Body.String())
	}
	if fetched.KID != rotated.KID {
		t.Fatalf("kid mismatch fetch=%s rotated=%s", fetched.KID, rotated.KID)
	}
	if string(fetched.PublicKey) != string(rotated.PublicKey) {
		t.Fatal("public key drift between rotate and fetch")
	}
	if strings.Contains(w2.Body.String(), "private_key") {
		t.Fatalf("fetch leaks private key: %s", w2.Body.String())
	}
}

func TestGetActiveSigningKeyNotFound(t *testing.T) {
	s, _ := newSigningTestServer(t)
	w := httptest.NewRecorder()
	r := controlRequest(http.MethodGet,
		"/v1/tenants/t_abc123/signing-keys/active", "",
		map[string]string{"tenant": "t_abc123"},
		principalWith("t_abc123", "cs:tenant:signing-key:read"))
	s.getActiveSigningKey(w, r)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body=%s", w.Code, w.Body.String())
	}
	if code := parseErrorCode(t, w.Body.Bytes()); code != cserrors.CSSignatureKeyNotFound {
		t.Fatalf("expected CSSignatureKeyNotFound, got %s", code)
	}
}

func TestRotateSigningKeyAuthzDenied(t *testing.T) {
	s, _ := newSigningTestServer(t)
	w := httptest.NewRecorder()
	r := controlRequest(http.MethodPost,
		"/v1/tenants/t_abc123/signing-keys/rotate", "",
		map[string]string{"tenant": "t_abc123"},
		principalWith("t_abc123", "role:user"))
	s.rotateSigningKey(w, r)
	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", w.Code, w.Body.String())
	}
}
