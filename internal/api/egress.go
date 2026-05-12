package api

// EgressPolicy is the per-tenant outbound network policy persisted under
// the cs:tenant:<tenant>:egress:policy KVRocks key. The control plane
// CRUD endpoints (GET/PUT /v1/tenants/{tenant}/egress-policy) marshal
// this shape directly; the invoker hot path loads it once per
// activation and compiles it into a runtime/egress.Matcher.
//
// The struct mirrors internal/runtime/egress.Policy. The duplication is
// intentional: api/ owns the wire shape (no imports of runtime
// packages), runtime/egress/ owns the compiled matcher and validation
// rules. See docs/15-security.md "Network egress" and roadmap E6.02.
type EgressPolicy struct {
	// AllowedHosts permits specific hostnames or "*.subdomain"
	// wildcards. Matching is case-insensitive.
	AllowedHosts []string `json:"allowed_hosts,omitempty"`

	// AllowedCIDRs permits literal IPs or IPv4/IPv6 CIDR ranges. The
	// private-IP block from docs/02-requirements.md still applies on
	// top.
	AllowedCIDRs []string `json:"allowed_cidrs,omitempty"`

	// DeniedHosts is checked before AllowedHosts so an operator can
	// carve a destination out of a broader wildcard.
	DeniedHosts []string `json:"denied_hosts,omitempty"`

	// DefaultDeny=true means unmatched destinations are rejected with
	// CS_EGRESS_DENIED. DefaultDeny=false means only DeniedHosts are
	// blocked (the private-IP block still applies in both modes).
	DefaultDeny bool `json:"default_deny"`

	// UpdatedAtMS is set by the control plane on every PUT so callers
	// can detect drift. Clients should not set it on input.
	UpdatedAtMS int64 `json:"updated_at_ms,omitempty"`
}
