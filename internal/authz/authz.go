package authz

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

type Principal struct {
	Sub    string   `json:"sub"`
	Tenant string   `json:"tenant"`
	Roles  []string `json:"roles"`
	Exp    int64    `json:"exp"`
}

type Provider interface {
	Name() string
	Introspect(ctx context.Context, token string) (Principal, error)
}

type contextKey string

const principalKey contextKey = "principal"

func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey).(Principal)
	return p, ok
}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey, principal)
}

func AuthnMiddleware(provider Provider) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if !strings.HasPrefix(authHeader, "Bearer ") {
				cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthnMissingToken, "missing bearer token"), "")
				return
			}
			token := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
			principal, err := provider.Introspect(r.Context(), token)
			if err != nil {
				cserrors.WriteHTTP(w, err, "")
				return
			}
			next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), principal)))
		})
	}
}

func CheckAction(principal Principal, action string) bool {
	for _, role := range principal.Roles {
		if role == "role:admin" || role == "admin" {
			return true
		}
		if role == action || role == "action:"+action {
			return true
		}
	}
	return false
}

func RequireAction(action string, resourceFn func(*http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			principal, ok := PrincipalFromContext(r.Context())
			if !ok {
				cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthnInvalidToken, "principal missing in context"), "")
				return
			}
			if !CheckAction(principal, action) {
				msg := fmt.Sprintf("action denied: %s resource=%s", action, resourceFn(r))
				cserrors.WriteHTTP(w, cserrors.New(cserrors.CSAuthzDenied, msg), "")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func RequireRoleIntersection(required []string, principal Principal) error {
	if len(required) == 0 {
		return cserrors.New(cserrors.CSAuthzRoleMissing, "role allowlist is empty")
	}
	set := make(map[string]struct{}, len(principal.Roles))
	for _, role := range principal.Roles {
		set[role] = struct{}{}
	}
	for _, role := range required {
		if _, ok := set[role]; ok {
			return nil
		}
	}
	return cserrors.New(cserrors.CSAuthzRoleMissing, "role missing")
}
