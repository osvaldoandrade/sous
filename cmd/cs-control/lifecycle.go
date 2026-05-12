package main

// Function lifecycle CRUD semantics for cs-control.
//
// This file groups the helpers that lock the contract between the HTTP
// handlers in main.go and the persistence layer. The behavior here is
// covered by lifecycle_contract_test.go and documented in
// docs/19-entity-state-machines.md ("Lifecycle invariants").
//
// Invariants implemented here:
//
//   - Create is idempotent on identical (runtime, entry, handler). A
//     re-create with conflicting attributes returns
//     CS_IDEMPOTENCY_CONFLICT (HTTP 409).
//   - Draft uploads are independent. Concurrent uploads for the same
//     function return distinct draft IDs and each carries its own TTL
//     window (defaulting to limits.DefaultDraftTTLSeconds).
//   - Publish atomically allocates a monotonic version and writes meta
//     and bundle in a single transaction (see kv.Store.PublishVersion).
//   - Alias mutations target a single key; readers observe either the
//     pre or post version, never a torn state.
//
// Errors raised by this file always use the typed CS_* codes from
// internal/errors so that HTTP status mapping is uniform.

import (
	"context"
	"errors"
	"time"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
)

// functionIdentityMatches returns true when two function records describe
// the same logical function. It is used to make create-function
// idempotent.
func functionIdentityMatches(a, b api.FunctionRecord) bool {
	return a.Tenant == b.Tenant &&
		a.Namespace == b.Namespace &&
		a.Name == b.Name &&
		a.Runtime == b.Runtime &&
		a.Entry == b.Entry &&
		a.Handler == b.Handler
}

// createFunctionIdempotent enforces the "same ref returns same record"
// contract from docs/19-entity-state-machines.md.
//
// Outcomes:
//   - rec did not exist: created and (rec, true, nil) is returned.
//   - rec already exists with identical identity: (existing, false, nil).
//   - rec already exists with conflicting identity: returns
//     CS_IDEMPOTENCY_CONFLICT.
//   - rec exists but is soft-deleted: returns CS_IDEMPOTENCY_CONFLICT.
func createFunctionIdempotent(ctx context.Context, store persistence.Provider, rec api.FunctionRecord) (api.FunctionRecord, bool, error) {
	if err := store.CreateFunction(ctx, rec); err == nil {
		return rec, true, nil
	} else if !isAlreadyExists(err) {
		return api.FunctionRecord{}, false, err
	}

	existing, err := store.GetFunction(ctx, rec.Tenant, rec.Namespace, rec.Name)
	if err != nil {
		return api.FunctionRecord{}, false, err
	}
	if existing.DeletedAtMS != nil {
		return api.FunctionRecord{}, false, cserrors.New(
			cserrors.CSIdempotencyConflict,
			"function exists in deleted state; soft-deleted functions cannot be re-created",
		)
	}
	if !functionIdentityMatches(existing, rec) {
		return api.FunctionRecord{}, false, cserrors.New(
			cserrors.CSIdempotencyConflict,
			"function already exists with a different runtime, entry, or handler",
		)
	}
	return existing, false, nil
}

// isAlreadyExists matches the sentinel raised by kv.Store.CreateFunction
// when SetNX fails (function already present). The KV layer wraps it
// as CSValidationFailed with the "function already exists" message;
// we detect it by message to avoid a public sentinel.
func isAlreadyExists(err error) bool {
	var csErr *cserrors.CSError
	if !errors.As(err, &csErr) {
		return false
	}
	return csErr.Code == cserrors.CSValidationFailed && csErr.Message == "function already exists"
}

// draftExpired reports whether a draft has crossed its TTL boundary.
// The 24-hour default lives in internal/limits.DefaultDraftTTLSeconds;
// the value used at runtime is whatever cs-control was configured with.
func draftExpired(d api.DraftRecord, now time.Time) bool {
	return now.UnixMilli() > d.ExpiresAtMS
}
