package authz

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

type fakeProvider struct {
	principal Principal
	err       error
	name      string
}

func (f fakeProvider) Name() string { return f.name }

func (f fakeProvider) Introspect(ctx context.Context, token string) (Principal, error) {
	if f.err != nil {
		return Principal{}, f.err
	}
	return f.principal, nil
}

func TestPrincipalContext(t *testing.T) {
	p := Principal{Sub: "u1"}
	ctx := WithPrincipal(context.Background(), p)
	got, ok := PrincipalFromContext(ctx)
	if !ok || got.Sub != "u1" {
		t.Fatalf("unexpected principal: %+v ok=%v", got, ok)
	}
}

func TestCheckAction(t *testing.T) {
	if !CheckAction(Principal{Roles: []string{"admin"}}, "x") {
		t.Fatal("admin should pass")
	}
	if !CheckAction(Principal{Roles: []string{"action:cs:function:create"}}, "cs:function:create") {
		t.Fatal("action role should pass")
	}
	if CheckAction(Principal{Roles: []string{"role:x"}}, "cs:function:create") {
		t.Fatal("should not pass")
	}
}

func TestRequireRoleIntersection(t *testing.T) {
	if err := RequireRoleIntersection(nil, Principal{Roles: []string{"a"}}); err == nil {
		t.Fatal("expected empty allowlist error")
	}
	if err := RequireRoleIntersection([]string{"a"}, Principal{Roles: []string{"a"}}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := RequireRoleIntersection([]string{"x"}, Principal{Roles: []string{"a"}}); err == nil {
		t.Fatal("expected role missing error")
	}
}

func TestAuthnMiddleware(t *testing.T) {
	okProvider := fakeProvider{principal: Principal{Sub: "u1", Tenant: "t_abc123", Roles: []string{"admin"}}, name: "fake"}
	h := AuthnMiddleware(okProvider)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFromContext(r.Context())
		if !ok || p.Sub != "u1" {
			t.Fatalf("principal not propagated")
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Authorization", "Bearer token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusNoContent {
		t.Fatalf("unexpected status: %d", w.Code)
	}

	r2 := httptest.NewRequest(http.MethodGet, "/", nil)
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r2)
	if w2.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w2.Code)
	}

	errProvider := fakeProvider{err: cserrors.New(cserrors.CSAuthnInvalidToken, "bad"), name: "fake"}
	hErr := AuthnMiddleware(errProvider)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	r3 := httptest.NewRequest(http.MethodGet, "/", nil)
	r3.Header.Set("Authorization", "Bearer token")
	w3 := httptest.NewRecorder()
	hErr.ServeHTTP(w3, r3)
	if w3.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w3.Code)
	}
}

func TestRequireAction(t *testing.T) {
	mw := RequireAction("cs:function:create", func(r *http.Request) string { return "res" })
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	// Missing principal
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", w.Code)
	}

	// Denied principal
	r2 := httptest.NewRequest(http.MethodGet, "/", nil)
	r2 = r2.WithContext(WithPrincipal(r2.Context(), Principal{Roles: []string{"role:user"}}))
	w2 := httptest.NewRecorder()
	h.ServeHTTP(w2, r2)
	if w2.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w2.Code)
	}

	// Allowed principal
	r3 := httptest.NewRequest(http.MethodGet, "/", nil)
	r3 = r3.WithContext(WithPrincipal(r3.Context(), Principal{Roles: []string{"action:cs:function:create"}}))
	w3 := httptest.NewRecorder()
	h.ServeHTTP(w3, r3)
	if w3.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", w3.Code)
	}
}

func TestAuthnProviderInterfaceCompile(t *testing.T) {
	var _ Provider = fakeProvider{name: "x"}
	if errors.Is(nil, nil) {
		// keep linter quiet for imported errors package
	}
}
