package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/osvaldoandrade/sous/internal/api"
	"github.com/osvaldoandrade/sous/internal/testutil"
)

// fakeTreeStore wires a small in-memory parent->children map onto a
// FakePersistence so the /tree endpoint can be exercised end-to-end
// without spinning up KVRocks. Each method is intentionally tiny so the
// test reads as a specification of the traversal contract.
func newFakeTreeStore(records map[string]api.ActivationRecord, edges map[string][]string) *testutil.FakePersistence {
	return &testutil.FakePersistence{
		GetActivationFn: func(_ context.Context, _ string, id string) (api.ActivationRecord, error) {
			rec, ok := records[id]
			if !ok {
				return api.ActivationRecord{}, errors.New("not found")
			}
			return rec, nil
		},
		GetActivationChildrenFn: func(_ context.Context, _ string, parent string) ([]string, error) {
			return edges[parent], nil
		},
	}
}

func TestActivationTreeReturnsBoundedBFS(t *testing.T) {
	records := map[string]api.ActivationRecord{
		"root": {ActivationID: "root", Tenant: "t_abc123", Function: "fn_root", Status: "success", StartMS: 1, DurationMS: 5, Trigger: api.Trigger{Type: "http"}, RootActivationID: "root"},
		"c1":   {ActivationID: "c1", Tenant: "t_abc123", Function: "fn_c1", Status: "success", StartMS: 2, DurationMS: 3, Trigger: api.Trigger{Type: "http"}, ParentActivationID: "root", RootActivationID: "root"},
		"c2":   {ActivationID: "c2", Tenant: "t_abc123", Function: "fn_c2", Status: "success", StartMS: 3, DurationMS: 4, Trigger: api.Trigger{Type: "http"}, ParentActivationID: "root", RootActivationID: "root"},
		"c1a":  {ActivationID: "c1a", Tenant: "t_abc123", Function: "fn_c1a", Status: "success", StartMS: 4, DurationMS: 1, Trigger: api.Trigger{Type: "http"}, ParentActivationID: "c1", RootActivationID: "root"},
		"orph": {ActivationID: "orph", Tenant: "t_abc123", Function: "fn_orph", Status: "success", StartMS: 5, DurationMS: 1, Trigger: api.Trigger{Type: "http"}},
	}
	edges := map[string][]string{
		"root": {"c1", "c2"},
		"c1":   {"c1a"},
		"c2":   {"missing"}, // childID points at an expired activation; tree must still include it as a placeholder.
	}
	store := newFakeTreeStore(records, edges)
	srv := newControlServer(store, &testutil.FakeMessaging{})

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodGet, "/v1/tenants/t_abc123/activations/root/tree", "", map[string]string{
		"tenant": "t_abc123", "activation_id": "root",
	}, principalWith("t_abc123", "cs:activation:read"))
	srv.getActivationTree(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status=%d body=%s", w.Code, w.Body.String())
	}
	var resp struct {
		ActivationID string         `json:"activation_id"`
		Tree         map[string]any `json:"tree"`
		Truncated    bool           `json:"truncated"`
		MaxDepth     int            `json:"max_depth"`
		MaxNodes     int            `json:"max_nodes"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v body=%s", err, w.Body.String())
	}
	if resp.ActivationID != "root" {
		t.Fatalf("unexpected root id: %q", resp.ActivationID)
	}
	if resp.MaxDepth != activationTreeMaxDepth || resp.MaxNodes != activationTreeMaxNodes {
		t.Fatalf("caps not echoed: %+v", resp)
	}
	if resp.Truncated {
		t.Fatalf("response unexpectedly truncated: %+v", resp)
	}
	// Walk the JSON tree to assert that c1 has c1a, c2 has the "missing"
	// placeholder, and the orphan node never appears (it is not connected
	// to root via the children index).
	children, _ := resp.Tree["children"].([]any)
	if len(children) != 2 {
		t.Fatalf("expected 2 top-level children, got %d body=%s", len(children), w.Body.String())
	}
	var seenC1, seenC2, seenOrph bool
	var c1Children, c2Children []any
	for _, ch := range children {
		m, _ := ch.(map[string]any)
		switch m["activation_id"] {
		case "c1":
			seenC1 = true
			c1Children, _ = m["children"].([]any)
		case "c2":
			seenC2 = true
			c2Children, _ = m["children"].([]any)
		case "orph":
			seenOrph = true
		}
	}
	if !seenC1 || !seenC2 {
		t.Fatalf("missing direct children: c1=%v c2=%v", seenC1, seenC2)
	}
	if seenOrph {
		t.Fatalf("orphan unexpectedly present in tree")
	}
	if len(c1Children) != 1 {
		t.Fatalf("c1 should have 1 child, got %d", len(c1Children))
	}
	if len(c2Children) != 1 {
		t.Fatalf("c2 should have 1 placeholder child, got %d", len(c2Children))
	}
	// Placeholder for an expired activation must still carry the ID so
	// operators can recognise the partial view.
	placeholder, _ := c2Children[0].(map[string]any)
	if placeholder["activation_id"] != "missing" {
		t.Fatalf("placeholder ID mismatch: %+v", placeholder)
	}
}

func TestActivationTreeCycleSafe(t *testing.T) {
	records := map[string]api.ActivationRecord{
		"a": {ActivationID: "a", Tenant: "t_abc123", Function: "fa", Status: "success"},
		"b": {ActivationID: "b", Tenant: "t_abc123", Function: "fb", Status: "success"},
	}
	// Deliberate cycle a -> b -> a. The traversal must terminate without
	// inflating the response.
	edges := map[string][]string{
		"a": {"b"},
		"b": {"a"},
	}
	store := newFakeTreeStore(records, edges)
	srv := newControlServer(store, &testutil.FakeMessaging{})

	w := httptest.NewRecorder()
	req := controlRequest(http.MethodGet, "/v1/tenants/t_abc123/activations/a/tree", "", map[string]string{
		"tenant": "t_abc123", "activation_id": "a",
	}, principalWith("t_abc123", "cs:activation:read"))
	srv.getActivationTree(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	// Ensure the response decodes — a cycle that exploded the queue would
	// blow past the size cap or never return.
	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}
}
