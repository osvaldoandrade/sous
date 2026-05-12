package main

import (
	"context"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/osvaldoandrade/sous/internal/api"
	cserrors "github.com/osvaldoandrade/sous/internal/errors"
	"github.com/osvaldoandrade/sous/internal/plugins/persistence"
)

// activationTreeMaxDepth caps how deep the /tree endpoint traverses the
// children index. The cap exists to bound work for pathological agent loops
// (cycles, accidental fan-out) so a single API call cannot exhaust the
// control plane. See docs/14-observability.md.
const activationTreeMaxDepth = 10

// activationTreeMaxNodes caps the total number of nodes returned in a single
// /tree response. When the cap is hit the response sets "truncated": true so
// CLIs and dashboards can surface the partial view without misleading the
// operator. See docs/14-observability.md.
const activationTreeMaxNodes = 1000

// activationTreeNode is the JSON shape returned by the /tree endpoint. It
// is intentionally compact: only the per-node identity, lifecycle, and
// trigger fields needed to draw an ASCII / web tree. Full activation
// records remain reachable via GET /v1/tenants/{tenant}/activations/{id}.
type activationTreeNode struct {
	ActivationID       string                `json:"activation_id"`
	ParentActivationID string                `json:"parent_activation_id,omitempty"`
	RootActivationID   string                `json:"root_activation_id,omitempty"`
	Function           string                `json:"function,omitempty"`
	Version            int64                 `json:"version,omitempty"`
	TriggerType        string                `json:"trigger_type,omitempty"`
	Status             string                `json:"status,omitempty"`
	StartMS            int64                 `json:"start_ms,omitempty"`
	DurationMS         int64                 `json:"duration_ms,omitempty"`
	Children           []*activationTreeNode `json:"children,omitempty"`
}

// getActivationTree serves GET /v1/tenants/{tenant}/activations/{id}/tree.
// It walks the children index in a bounded BFS so the response cannot be
// inflated by a pathological agent loop. Missing or expired records are
// surfaced as nodes with empty function/status fields so callers can still
// see the shape of the partial tree without inferring it from logs.
func (s *server) getActivationTree(w http.ResponseWriter, r *http.Request) {
	_, tenant, _, _, ok := s.authorize(w, r, "cs:activation:read")
	if !ok {
		return
	}
	activationID := chi.URLParam(r, "activation_id")
	if activationID == "" {
		cserrors.WriteHTTP(w, cserrors.New(cserrors.CSValidationFailed, "activation_id required"), requestID(r))
		return
	}
	root, truncated, err := buildActivationTree(r.Context(), s.store, tenant, activationID)
	if err != nil {
		cserrors.WriteHTTP(w, err, requestID(r))
		return
	}
	api.WriteJSON(w, http.StatusOK, map[string]any{
		"activation_id": activationID,
		"tree":          root,
		"truncated":     truncated,
		"max_depth":     activationTreeMaxDepth,
		"max_nodes":     activationTreeMaxNodes,
	})
}

// buildActivationTree performs the bounded BFS used by getActivationTree.
// It is exported package-internal so tests can call it directly without
// going through the HTTP layer. The traversal:
//   - tracks visited activations so cycles cannot inflate the response;
//   - stops descending at activationTreeMaxDepth;
//   - stops appending nodes once activationTreeMaxNodes is reached and
//     returns truncated=true so the caller can flag the partial view.
func buildActivationTree(ctx context.Context, store persistence.Provider, tenant, rootID string) (*activationTreeNode, bool, error) {
	rec, err := store.GetActivation(ctx, tenant, rootID)
	if err != nil {
		return nil, false, err
	}
	root := nodeFromRecord(rec)
	visited := map[string]struct{}{root.ActivationID: {}}
	nodes := 1
	truncated := false

	type frame struct {
		node  *activationTreeNode
		depth int
	}
	queue := []frame{{node: root, depth: 0}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		children, err := store.GetActivationChildren(ctx, tenant, cur.node.ActivationID)
		if err != nil {
			return nil, false, err
		}
		// Depth cap fires only when there are children we are choosing not
		// to follow, so a leaf at the cap does not mislead the caller into
		// thinking the tree was truncated.
		if cur.depth >= activationTreeMaxDepth {
			if len(children) > 0 {
				truncated = true
			}
			continue
		}
		for _, childID := range children {
			if _, seen := visited[childID]; seen {
				continue
			}
			if nodes >= activationTreeMaxNodes {
				truncated = true
				break
			}
			visited[childID] = struct{}{}
			childRec, err := store.GetActivation(ctx, tenant, childID)
			var childNode *activationTreeNode
			if err != nil {
				// Activation expired or absent: surface a placeholder so the
				// tree shape is still meaningful to operators.
				childNode = &activationTreeNode{
					ActivationID:       childID,
					ParentActivationID: cur.node.ActivationID,
				}
			} else {
				childNode = nodeFromRecord(childRec)
			}
			cur.node.Children = append(cur.node.Children, childNode)
			nodes++
			queue = append(queue, frame{node: childNode, depth: cur.depth + 1})
		}
	}
	return root, truncated, nil
}

// nodeFromRecord projects the durable activation record into the compact
// /tree response shape. Keeping the projection here (rather than in the
// generic ActivationRecord type) lets the endpoint evolve its on-the-wire
// shape without forcing every consumer of ActivationRecord to follow.
func nodeFromRecord(rec api.ActivationRecord) *activationTreeNode {
	return &activationTreeNode{
		ActivationID:       rec.ActivationID,
		ParentActivationID: rec.ParentActivationID,
		RootActivationID:   rec.RootActivationID,
		Function:           rec.Function,
		Version:            rec.ResolvedVersion,
		TriggerType:        rec.Trigger.Type,
		Status:             rec.Status,
		StartMS:            rec.StartMS,
		DurationMS:         rec.DurationMS,
	}
}
