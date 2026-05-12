package main

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/runtime/egress"
)

// egressRoutes wires the per-tenant egress allowlist CRUD surface
// (roadmap E6.02). Routes mirror the other tenant-scoped, non-namespaced
// resources (e.g. signing keys) so the API surface stays uniform — see
// docs/04-api-rest.md "Egress policy".
func (s *server) egressRoutes(pr chi.Router) {
	pr.Get("/v1/tenants/{tenant}/egress-policy", s.getEgressPolicy)
	pr.Put("/v1/tenants/{tenant}/egress-policy", s.putEgressPolicy)
}

func (s *server) getEgressPolicy(w http.ResponseWriter, r *http.Request) {
	_, tenant, _, _, ok := s.authorize(w, r, "cs:egress:policy:read")
	if !ok {
		return
	}
	if err := api.ValidateTenant(tenant); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationName, err.Error()), requestID(r))
		return
	}
	policy, err := s.store.GetEgressPolicy(r.Context(), tenant)
	if err != nil {
		// Missing policy is steady-state for new tenants. Return the
		// implicit default-deny stub so CLI callers can render
		// "no policy installed" without a dedicated 404 path.
		var csErr *cserrors.CSError
		if errors.As(err, &csErr) && csErr.Code == cserrors.CSValidationFailed {
			api.WriteJSON(w, http.StatusOK, api.EgressPolicy{DefaultDeny: true})
			return
		}
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, policy)
}

func (s *server) putEgressPolicy(w http.ResponseWriter, r *http.Request) {
	principal, tenant, _, _, ok := s.authorize(w, r, "cs:egress:policy:write")
	if !ok {
		return
	}
	if err := api.ValidateTenant(tenant); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationName, err.Error()), requestID(r))
		return
	}
	var policy api.EgressPolicy
	if err := api.ReadJSON(r, &policy); err != nil {
		cserrors.WriteHTTP(w, cserrors.Wrap(cserrors.CSValidationFailed, "invalid request body", err), requestID(r))
		return
	}
	// Defensive: clients are not allowed to forge UpdatedAtMS; the
	// store stamps wall-clock authority server-side.
	policy.UpdatedAtMS = 0
	if err := egress.Validate(egress.Policy{
		AllowedHosts: policy.AllowedHosts,
		AllowedCIDRs: policy.AllowedCIDRs,
		DeniedHosts:  policy.DeniedHosts,
		DefaultDeny:  policy.DefaultDeny,
	}); err != nil {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "invalid egress policy: "+err.Error()), requestID(r))
		return
	}
	if err := s.store.PutEgressPolicy(r.Context(), tenant, policy); err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	// Re-read so the response carries the server-stamped UpdatedAtMS.
	stored, err := s.store.GetEgressPolicy(r.Context(), tenant)
	if err != nil {
		// Treat read-after-write failure as a soft error: the write
		// landed, but we couldn't echo the timestamp. Return the
		// caller's payload as best-effort.
		stored = policy
	}
	s.auditAfterCommit(r, principal, "egress.policy.write",
		fmt.Sprintf("egress://%s", tenant),
		map[string]any{
			"allowed_hosts": len(stored.AllowedHosts),
			"allowed_cidrs": len(stored.AllowedCIDRs),
			"denied_hosts":  len(stored.DeniedHosts),
			"default_deny":  stored.DefaultDeny,
		})
	api.WriteJSON(w, http.StatusOK, stored)
}
