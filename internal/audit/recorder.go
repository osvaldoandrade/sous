package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// IndexStore is the KV contract the Recorder uses to materialise a
// tenant-scoped history for the GET /v1/audit replay endpoint. It is
// satisfied by persistence.Provider (KVGet/KVSet/KVDel) without an
// adapter — the methods match by signature.
type IndexStore interface {
	KVGet(ctx context.Context, key string) (string, error)
	KVSet(ctx context.Context, key, value string, ttl time.Duration) error
}

// Logger captures the small slice of internal/observability.Logger
// the Recorder needs to emit SinkLag warnings.
type Logger interface {
	Warn(ctx context.Context, message string)
}

// defaultHistoryLimit caps the per-tenant ring buffer so a single
// chatty tenant cannot exhaust the KV with audit history. Anything
// older drops off the tail; the codeQ topic remains the
// long-retention source of truth (see docs/14-observability.md).
const defaultHistoryLimit = 1000

// IndexKeyPrefix is the KVRocks namespace under which we hold the
// per-tenant audit history. Tests and runbooks may key off the same
// prefix.
const IndexKeyPrefix = "cs:audit:"

// Recorder is the integration point cs-control handlers call after a
// successful kv mutation. It fans out to the configured Sink and
// records a ring-buffered copy in the IndexStore so the replay
// endpoint can serve recent history without re-reading the topic.
type Recorder struct {
	sink         Sink
	index        IndexStore
	logger       Logger
	historyTTL   time.Duration
	historyLimit int
	mu           sync.Mutex
}

// NewRecorder wires a Recorder. Passing a nil Sink disables sink
// emission entirely (the index continues to be populated); this is
// useful in tests that only care about the replay endpoint.
//
// historyTTL is the TTL on the per-tenant index key. Defaults match
// the activation TTL configured for cs-control; pass 0 to use a 7-day
// fallback. historyLimit caps the number of records retained per
// tenant; pass 0 to use defaultHistoryLimit (1000).
func NewRecorder(sink Sink, index IndexStore, logger Logger, historyTTL time.Duration, historyLimit int) *Recorder {
	if historyTTL <= 0 {
		historyTTL = 7 * 24 * time.Hour
	}
	if historyLimit <= 0 {
		historyLimit = defaultHistoryLimit
	}
	return &Recorder{
		sink:         sink,
		index:        index,
		logger:       logger,
		historyTTL:   historyTTL,
		historyLimit: historyLimit,
	}
}

// AfterCommit emits the event after a successful control-plane
// mutation. Callers MUST NOT call this before the kv write completes;
// the function name is the load-bearing signal that this is post-
// commit. Returning a non-nil error is informational only — the
// mutation is already durable.
func (r *Recorder) AfterCommit(ctx context.Context, event Event) error {
	if event.EventID == "" {
		event.EventID = "evt_" + uuid.NewString()
	}
	if event.SchemaVersion == "" {
		event.SchemaVersion = SchemaVersion
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if event.Outcome == "" {
		event.Outcome = OutcomeSuccess
	}
	return r.emit(ctx, event)
}

// EmitFailure is the explicit failure-path counterpart to AfterCommit.
// Use it from handlers that need to record an authz denial or kv
// error as an audit line. The mutation must NOT have committed when
// this is called — the contract is "we tried, here's why we stopped".
func (r *Recorder) EmitFailure(ctx context.Context, event Event) error {
	if event.Outcome == OutcomeSuccess || event.Outcome == "" {
		event.Outcome = OutcomeError
	}
	return r.AfterCommit(ctx, event)
}

func (r *Recorder) emit(ctx context.Context, event Event) error {
	var sinkErr error
	if r.sink != nil {
		sinkErr = r.sink.Emit(ctx, event)
		if sinkErr != nil && r.logger != nil {
			r.logger.Warn(ctx, fmt.Sprintf("audit sink %q emit failed: %v", r.sink.Name(), sinkErr))
		}
	}
	if err := r.recordHistory(ctx, event); err != nil && r.logger != nil {
		r.logger.Warn(ctx, fmt.Sprintf("audit history write failed: %v", err))
	}
	return sinkErr
}

func (r *Recorder) recordHistory(ctx context.Context, event Event) error {
	if r.index == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	key := IndexKeyPrefix + event.Tenant
	history, err := r.loadHistory(ctx, key)
	if err != nil {
		return err
	}
	history = append(history, event)
	if len(history) > r.historyLimit {
		history = history[len(history)-r.historyLimit:]
	}
	raw, err := json.Marshal(history)
	if err != nil {
		return err
	}
	return r.index.KVSet(ctx, key, string(raw), r.historyTTL)
}

func (r *Recorder) loadHistory(ctx context.Context, key string) ([]Event, error) {
	raw, err := r.index.KVGet(ctx, key)
	if err != nil {
		// The persistence layer returns CS_KVROCKS_READ_FAILED for
		// genuine read errors and an empty string for missing keys.
		// We treat unknown errors as fatal so callers see the
		// problem rather than silently dropping history.
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var out []Event
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		// Corrupt index — rotate by starting fresh rather than
		// poisoning the next write.
		return nil, nil
	}
	return out, nil
}

// ReadHistory returns the in-order audit events held in the KV index
// for tenant, optionally filtered by sinceMS / actor / action and
// capped at limit. Callers (the replay endpoint) are expected to
// enforce tenant scoping before invoking this method.
func (r *Recorder) ReadHistory(ctx context.Context, tenant string, sinceMS int64, actor, action string, limit int) ([]Event, error) {
	if r.index == nil {
		return nil, nil
	}
	if limit <= 0 || limit > r.historyLimit {
		limit = r.historyLimit
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	history, err := r.loadHistory(ctx, IndexKeyPrefix+tenant)
	if err != nil {
		return nil, err
	}
	out := make([]Event, 0, len(history))
	for _, e := range history {
		if sinceMS > 0 && e.Timestamp.UnixMilli() < sinceMS {
			continue
		}
		if actor != "" && e.Actor != actor {
			continue
		}
		if action != "" && e.Action != action {
			continue
		}
		out = append(out, e)
	}
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out, nil
}
