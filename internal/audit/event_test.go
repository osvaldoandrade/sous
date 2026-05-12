package audit

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventMarshalRoundTrip(t *testing.T) {
	src := NewEvent("t_abc", "u_42", "function.publish", "fn://t_abc/ns/foo@v3", OutcomeSuccess)
	src.RequestID = "req_xyz"
	src.Detail = map[string]any{"sha256": "deadbeef"}

	raw, err := src.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got Event
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.SchemaVersion != SchemaVersion {
		t.Fatalf("schema_version: got %q want %q", got.SchemaVersion, SchemaVersion)
	}
	if got.Tenant != src.Tenant || got.Actor != src.Actor || got.Action != src.Action || got.Resource != src.Resource {
		t.Fatalf("core fields not preserved: got %+v want %+v", got, src)
	}
	if got.Outcome != src.Outcome {
		t.Fatalf("outcome: got %q want %q", got.Outcome, src.Outcome)
	}
	if got.RequestID != src.RequestID {
		t.Fatalf("request_id: got %q want %q", got.RequestID, src.RequestID)
	}
	if d, ok := got.Detail["sha256"]; !ok || d != "deadbeef" {
		t.Fatalf("detail not preserved: %+v", got.Detail)
	}
	if got.Timestamp.IsZero() {
		t.Fatalf("timestamp zero after round-trip")
	}
}

func TestNewEventDefaults(t *testing.T) {
	before := time.Now().Add(-time.Second)
	ev := NewEvent("t1", "u1", "function.create", "fn://t1/ns/f", OutcomeSuccess)
	after := time.Now().Add(time.Second)
	if ev.SchemaVersion != SchemaVersion {
		t.Fatalf("schema_version=%q want %q", ev.SchemaVersion, SchemaVersion)
	}
	if ev.Timestamp.Before(before) || ev.Timestamp.After(after) {
		t.Fatalf("timestamp out of expected window: %v", ev.Timestamp)
	}
	if ev.Outcome != OutcomeSuccess {
		t.Fatalf("outcome=%q", ev.Outcome)
	}
}

// TestEventSchemaContract guards the JSON wire keys consumers depend
// on. Adding a new key is fine; renaming/removing one breaks SIEMs.
func TestEventSchemaContract(t *testing.T) {
	ev := NewEvent("t1", "u1", "function.publish", "fn://t1/ns/f@v1", OutcomeSuccess)
	ev.Detail = map[string]any{"k": "v"}
	raw, err := ev.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var generic map[string]any
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatalf("unmarshal generic: %v", err)
	}
	mustHave := []string{"schema_version", "ts", "tenant", "action", "resource", "outcome", "detail"}
	for _, k := range mustHave {
		if _, ok := generic[k]; !ok {
			t.Fatalf("missing required key %q in marshalled event: %s", k, string(raw))
		}
	}
	if generic["schema_version"] != SchemaVersion {
		t.Fatalf("schema_version wire value=%v", generic["schema_version"])
	}
}
