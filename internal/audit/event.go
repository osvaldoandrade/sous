// Package audit implements the control-plane audit log stream described
// in docs/14-observability.md (Audit section) and Roadmap epic E6.03.
//
// The package is intentionally narrow: it defines a stable Event
// envelope, a Sink abstraction with three first-party implementations
// (stdout JSON, codeQ topic, HMAC-signed webhook), and a Recorder that
// emits events *after* a successful control-plane mutation. Emission
// only happens on the success path so we never log a phantom mutation;
// failure-paths can call EmitFailure explicitly when a typed error
// outcome ("denied" / "error") is required.
//
// Event is an append-only contract. Fields are additive; the
// SchemaVersion field exists so consumers can fan out on the wire
// schema independently from the in-process Go struct.
package audit

import (
	"encoding/json"
	"time"
)

// SchemaVersion is the current Event schema version. Bump this when
// the wire shape changes in a non-additive way (no such change is
// planned; the contract is meant to evolve additively).
const SchemaVersion = "1"

// Outcome captures the high-level result of an attempted control-plane
// mutation. Auditors and SIEMs filter on this column heavily, so we
// expose a small enumerated set instead of arbitrary strings.
type Outcome string

const (
	// OutcomeSuccess is recorded after the kv mutation commits.
	OutcomeSuccess Outcome = "success"
	// OutcomeDenied is recorded when authn/authz blocks a mutation.
	OutcomeDenied Outcome = "denied"
	// OutcomeError is recorded when a mutation reaches the persistence
	// layer but fails (validation, kv write error, etc.).
	OutcomeError Outcome = "error"
)

// Event is the canonical audit record for a control-plane mutation.
//
// The shape mirrors the docs/14-observability.md "Audit" section. The
// docs are the authoritative contract; this struct is the in-process
// view of that contract.
type Event struct {
	// SchemaVersion is the wire-schema version (currently "1"). It is
	// always populated by NewEvent.
	SchemaVersion string `json:"schema_version"`
	// EventID uniquely identifies this event. Consumers use it to
	// dedupe at-least-once delivery.
	EventID string `json:"event_id,omitempty"`
	// Timestamp is when the event was emitted (after commit).
	Timestamp time.Time `json:"ts"`
	// Tenant scopes every event. Cross-tenant reads are forbidden by
	// the replay endpoint.
	Tenant string `json:"tenant"`
	// Actor is the authenticated principal sub. Empty for the rare
	// pre-authn failure path.
	Actor string `json:"actor,omitempty"`
	// Action is the dotted action name (e.g., "function.publish").
	// One control-plane handler emits exactly one Action value.
	Action string `json:"action"`
	// Resource is a URN-style identifier for the affected object
	// (e.g., fn://tenant/ns/name@v3).
	Resource string `json:"resource"`
	// Outcome is one of OutcomeSuccess / OutcomeDenied / OutcomeError.
	Outcome Outcome `json:"outcome"`
	// RequestID propagates the X-Request-Id header so audit lines
	// correlate with API request logs.
	RequestID string `json:"request_id,omitempty"`
	// Detail is an open map for additional structured context
	// (e.g., draft_id, version, alias). Callers MUST keep this
	// allowlisted; never put secret material or bundle bytes here.
	Detail map[string]any `json:"detail,omitempty"`
}

// NewEvent populates the always-on fields (SchemaVersion, Timestamp)
// while leaving the caller in charge of the semantic fields. Use this
// rather than building Event literals so the schema version stays in
// one place.
func NewEvent(tenant, actor, action, resource string, outcome Outcome) Event {
	return Event{
		SchemaVersion: SchemaVersion,
		Timestamp:     time.Now().UTC(),
		Tenant:        tenant,
		Actor:         actor,
		Action:        action,
		Resource:      resource,
		Outcome:       outcome,
	}
}

// Marshal returns the canonical JSON encoding of the event. The codeQ
// and webhook sinks use this directly so the wire encoding is uniform.
func (e Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}
