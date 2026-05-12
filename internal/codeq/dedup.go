// Package codeq dedup helpers used by the consumer paths to collapse
// at-least-once redeliveries into a single activation.
//
// Producers compute a deterministic envelope ID of the form
//
//	msg = sha256(tenant + "|" + activation_id + "|" + seq)
//
// so the same logical message produced more than once (e.g. due to a producer
// retry that previously succeeded) carries the same envelope.ID. Consumers
// running through a SeenSet ignore the second delivery.
//
// The contract is intentionally narrow: SeenSet is an in-memory set of
// envelope IDs with a wall-clock TTL. Production deployments should swap in
// a KVRocks-backed implementation; the in-memory version is suitable for
// single-node dev, unit tests, and the codeQ broker fanout where one
// consumer goroutine owns the dedup state.
package codeq

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"sync"
	"time"
)

// DeterministicMessageID returns the envelope ID a producer should set on
// the Nth message it emits for a given (tenant, activation_id) pair so a
// duplicate delivery on the consumer side can be detected and skipped.
//
// The prefix "msg_" matches the legacy random ID format so downstream
// observers (logs, dashboards) continue to render the value consistently.
func DeterministicMessageID(tenant, activationID string, seq int) string {
	h := sha256.New()
	h.Write([]byte(tenant))
	h.Write([]byte{'|'})
	h.Write([]byte(activationID))
	h.Write([]byte{'|'})
	h.Write([]byte(strconv.Itoa(seq)))
	return "msg_" + hex.EncodeToString(h.Sum(nil))
}

// SeenSet is the consumer-side dedup contract. Implementations MUST be safe
// for concurrent use by multiple goroutines.
type SeenSet interface {
	// MarkSeen records id with the supplied TTL. It returns true the first
	// time an id is observed and false on every subsequent call within the
	// TTL window — callers MUST treat a false return value as a duplicate
	// delivery and refrain from re-executing the message.
	MarkSeen(id string, ttl time.Duration) bool
}

// NewMemorySeenSet returns an in-memory SeenSet. Pass nil for `now` to use
// time.Now (production), or a fake clock for deterministic tests.
func NewMemorySeenSet(now func() time.Time) SeenSet {
	if now == nil {
		now = time.Now
	}
	return &memorySeenSet{now: now, seen: map[string]time.Time{}}
}

type memorySeenSet struct {
	mu   sync.Mutex
	now  func() time.Time
	seen map[string]time.Time
}

func (m *memorySeenSet) MarkSeen(id string, ttl time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	if expires, ok := m.seen[id]; ok && expires.After(now) {
		return false
	}
	m.seen[id] = now.Add(ttl)
	// Best-effort sweep: bounded so a long-lived process does not retain
	// expired IDs forever. The cost is O(n) per call so we keep the map
	// small in practice; production should replace this with KVRocks
	// where TTL eviction is a primitive.
	if len(m.seen) > 1024 {
		for k, exp := range m.seen {
			if !exp.After(now) {
				delete(m.seen, k)
			}
		}
	}
	return true
}
