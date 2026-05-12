package api

// CadenceConfig is the optional Cadence-aware section of a FunctionManifest.
// It identifies functions that need different treatment at publish time
// because they execute under Cadence's deterministic replay model.
//
// Kind selects the manifest's Cadence role. Two values are defined:
//
//   - "activity" (default): the function executes as a Cadence Activity.
//     Activities run once per attempt, can perform IO, and may call any
//     nondeterministic API. This is the v0.1 behaviour and the default
//     when the field is omitted.
//
//   - "workflow": the function executes as a Cadence Workflow. Workflow
//     code is replayed against its history on every decision, so it must
//     be deterministic — see internal/cadence/determinism and
//     docs/12-cadence-integration.md for the banned-API list enforced
//     at publish time (E8.03).
//
// Append-only: new Cadence-aware fields land at the bottom of this struct
// so existing manifests round-trip with byte-identical JSON encoding.
type CadenceConfig struct {
	Kind string `json:"kind,omitempty"`
}

// CadenceKind* are the recognised values for CadenceConfig.Kind.
const (
	CadenceKindActivity = "activity"
	CadenceKindWorkflow = "workflow"
)
