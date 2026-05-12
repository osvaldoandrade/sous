// Package idempotency defines the dedup-store contract used by the v0.1
// invocation paths (HTTP gateway, codeQ consumer, Cadence poller) to collapse
// retried deliveries into a single activation.
//
// The contract is intentionally narrow:
//
//   1. The caller derives an activation_id from (tenant, function, ref,
//      idempotency_key) using UUIDv5 — see deriveActivationID in
//      cmd/cs-http-gateway/main.go. The same input MUST always produce the
//      same activation_id.
//
//   2. Before executing a function the caller invokes Reserve(ctx, key, body).
//      If no record exists, Reserve persists a sentinel and returns
//      Reserved{Created:true}. If a record already exists with the same
//      body fingerprint, it returns the cached record. If a record exists
//      with a different fingerprint, Reserve returns ErrConflict so the
//      caller can surface CS_IDEMPOTENCY_CONFLICT (HTTP 409).
//
//   3. When a terminal result is available, the executor calls Commit so a
//      second delivery can replay the cached response instead of executing
//      the function again. Commit is idempotent.
//
// The in-memory implementation in this package is suitable for unit tests
// and single-node dev. Production deployments back this contract with the
// KVRocks-resident `idem:{tenant}:{hash}` keys described in roadmap task
// E1.03 (issue #5).
package idempotency

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"
)

// ErrConflict is returned by Reserve when a record with the same key but a
// different body fingerprint already exists. Callers MUST map this to the
// CS_IDEMPOTENCY_CONFLICT error code (HTTP 409).
var ErrConflict = errors.New("idempotency: body fingerprint conflict")

// Record is the persisted view of a single (tenant, idempotency_key) pair.
// Result and Error are nil until the executor calls Commit.
type Record struct {
	ActivationID    string
	BodyFingerprint string
	Result          []byte
	Err             string
	Terminal        bool
	CreatedAt       time.Time
	ExpiresAt       time.Time
}

// Reserved is the outcome of a Reserve call.
type Reserved struct {
	// Created is true when this Reserve call inserted the sentinel; false
	// when it observed an existing record.
	Created bool
	Record  Record
}

// Store is the dedup contract. Implementations MUST be safe for concurrent
// use by multiple goroutines.
type Store interface {
	// Reserve attempts to reserve an activation slot under key. The
	// fingerprint is the canonical hash of the invocation body; if a record
	// already exists with a different fingerprint, Reserve returns
	// ErrConflict. ttl is the wall-clock TTL after which the entry may be
	// evicted.
	Reserve(ctx context.Context, key, activationID, fingerprint string, ttl time.Duration) (Reserved, error)

	// Commit attaches a terminal result/error to an existing reservation.
	// Subsequent Reserve calls observe Record.Terminal==true and can replay
	// Result/Err.
	Commit(ctx context.Context, key, fingerprint string, result []byte, errMsg string) error

	// Get returns the current record for key, if any.
	Get(ctx context.Context, key string) (Record, bool)
}

// Fingerprint returns the canonical body fingerprint used for conflict
// detection. It is a hex-encoded SHA-256 so it is safe to embed in logs and
// audit records.
func Fingerprint(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

// NewMemoryStore returns an in-memory implementation of Store. now is
// injectable for tests; pass nil to use time.Now.
func NewMemoryStore(now func() time.Time) Store {
	if now == nil {
		now = time.Now
	}
	return &memoryStore{now: now, records: map[string]Record{}}
}

type memoryStore struct {
	mu      sync.Mutex
	now     func() time.Time
	records map[string]Record
}

func (m *memoryStore) Reserve(ctx context.Context, key, activationID, fingerprint string, ttl time.Duration) (Reserved, error) {
	if ctx.Err() != nil {
		return Reserved{}, ctx.Err()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	if rec, ok := m.records[key]; ok && rec.ExpiresAt.After(now) {
		if rec.BodyFingerprint != fingerprint {
			return Reserved{Record: rec}, ErrConflict
		}
		return Reserved{Created: false, Record: rec}, nil
	}
	rec := Record{
		ActivationID:    activationID,
		BodyFingerprint: fingerprint,
		CreatedAt:       now,
		ExpiresAt:       now.Add(ttl),
	}
	m.records[key] = rec
	return Reserved{Created: true, Record: rec}, nil
}

func (m *memoryStore) Commit(ctx context.Context, key, fingerprint string, result []byte, errMsg string) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[key]
	if !ok || rec.ExpiresAt.Before(m.now()) {
		return errors.New("idempotency: no reservation to commit")
	}
	if rec.BodyFingerprint != fingerprint {
		return ErrConflict
	}
	rec.Result = append([]byte(nil), result...)
	rec.Err = errMsg
	rec.Terminal = true
	m.records[key] = rec
	return nil
}

func (m *memoryStore) Get(ctx context.Context, key string) (Record, bool) {
	if ctx.Err() != nil {
		return Record{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[key]
	if !ok || rec.ExpiresAt.Before(m.now()) {
		return Record{}, false
	}
	return rec, true
}
