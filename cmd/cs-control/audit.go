package main

// Audit replay endpoint for cs-control. The wire contract is documented
// in docs/14-observability.md ("Audit") and wiki/Enabled-Services-Tikti-IAM.md
// ("Resource x action matrix": cs:audit:read).
//
// Tenant scoping is enforced by the existing authorize() helper which
// rejects principals whose Tikti tenant claim does not match the URL
// tenant. The audit history itself is held in a per-tenant ring
// buffer in KVRocks (see internal/audit/recorder.go).

import (
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
)

// auditListLimitFallback is the default page size when ?limit= is
// absent. auditListLimitMax is the per-request cap; clients wanting
// more must page via ?since=.
const (
	auditListLimitFallback = 100
	auditListLimitMax      = 500
)

func (s *server) getAuditHistory(w http.ResponseWriter, r *http.Request) {
	_, _, _, _, ok := s.authorize(w, r, "cs:audit:read")
	if !ok {
		return
	}
	tenant := chi.URLParam(r, "tenant")
	if tenant == "" {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "tenant required"), requestID(r))
		return
	}
	if s.recorder == nil {
		api.WriteJSON(w, http.StatusOK, map[string]any{"events": []any{}})
		return
	}

	q := r.URL.Query()
	since := parseInt64Query(q.Get("since"), 0)
	actor := q.Get("actor")
	action := q.Get("action")
	limit := auditListLimitFallback
	if raw := q.Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= auditListLimitMax {
			limit = parsed
		}
	}

	events, err := s.recorder.ReadHistory(r.Context(), tenant, since, actor, action, limit)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{"events": events})
}

// parseInt64Query parses a query-string int64, returning fallback for
// empty or invalid input.
func parseInt64Query(v string, fallback int64) int64 {
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}
