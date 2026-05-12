// Package egress holds the per-tenant outbound network policy enforced by
// the runtime host APIs (cs.http.fetch today; cs.tcp once added). The
// policy layers on top of the non-negotiable private-IP block from
// docs/02-requirements.md: an entry that resolves to a private range is
// still rejected by the runtime, regardless of what the allowlist says.
//
// See docs/15-security.md "Network egress" for the contract and
// roadmap E6.02 (#24) for the design.
package egress

import (
	"fmt"
	"net"
	"strings"
)

// Policy is the persisted shape stored under the tenant's egress key in
// KVRocks (see internal/kv.TenantEgressPolicyKey). It is intentionally
// small and JSON-friendly so the control-plane CRUD endpoints can round
// trip it without a translation layer.
type Policy struct {
	// AllowedHosts is the list of hostnames a tenant may dial. Entries
	// are matched case-insensitively. A leading "*." wildcard matches
	// any number of subdomain labels (e.g. "*.example.com" matches
	// "api.example.com" and "v1.api.example.com" but not
	// "example.com").
	AllowedHosts []string `json:"allowed_hosts,omitempty"`

	// AllowedCIDRs is the list of literal IP / CIDR destinations a
	// tenant may dial. IPv4 and IPv6 CIDRs both work via stdlib
	// net.ParseCIDR; bare IPs are accepted and interpreted as /32
	// (IPv4) or /128 (IPv6).
	AllowedCIDRs []string `json:"allowed_cidrs,omitempty"`

	// DeniedHosts is consulted before AllowedHosts. It exists so an
	// operator can carve a destination out of a wildcard ("allow
	// *.example.com but deny abuse.example.com") without having to
	// enumerate the surviving leaves. Wildcards are accepted here too.
	DeniedHosts []string `json:"denied_hosts,omitempty"`

	// DefaultDeny flips the unmatched-destination behaviour:
	//   - true  -> only AllowedHosts/AllowedCIDRs may be dialled.
	//   - false -> everything not in DeniedHosts is permitted (still
	//              subject to the private-IP block in the runtime).
	// The control plane defaults new tenants to DefaultDeny=true
	// because that matches the "deny by default" acceptance criterion
	// in roadmap E6.02.
	DefaultDeny bool `json:"default_deny"`
}

// Matcher is the immutable, validated form of a Policy. Build it once at
// activation time and pass it to the runtime. The zero value is a
// permissive matcher that defers to the runtime's private-IP block (used
// when a tenant has not yet uploaded a policy and we want to keep the
// legacy "manifest http.allowHosts only" behaviour — see issue #24
// acceptance criterion 1 for why production calls Compile with
// DefaultDeny=true).
type Matcher struct {
	allowedHosts []hostPattern
	deniedHosts  []hostPattern
	allowedCIDRs []*net.IPNet
	defaultDeny  bool
}

type hostPattern struct {
	raw      string
	wildcard bool   // true when raw begins with "*."
	suffix   string // the part after "*." (lower-cased) when wildcard
	exact    string // the full host (lower-cased) when not wildcard
}

// Compile validates a Policy and returns a Matcher. Compile rejects
// unparseable CIDRs and host strings that are obviously malformed so the
// control plane fails closed at PUT time rather than at invocation time.
func Compile(p Policy) (*Matcher, error) {
	m := &Matcher{defaultDeny: p.DefaultDeny}
	for _, h := range p.AllowedHosts {
		pat, err := parseHostPattern(h)
		if err != nil {
			return nil, fmt.Errorf("allowed_hosts: %w", err)
		}
		m.allowedHosts = append(m.allowedHosts, pat)
	}
	for _, h := range p.DeniedHosts {
		pat, err := parseHostPattern(h)
		if err != nil {
			return nil, fmt.Errorf("denied_hosts: %w", err)
		}
		m.deniedHosts = append(m.deniedHosts, pat)
	}
	for _, c := range p.AllowedCIDRs {
		ipNet, err := parseCIDROrIP(c)
		if err != nil {
			return nil, fmt.Errorf("allowed_cidrs: %w", err)
		}
		m.allowedCIDRs = append(m.allowedCIDRs, ipNet)
	}
	return m, nil
}

// Allowed reports whether the egress policy permits a connection to
// host. host may be a hostname or a literal IPv4/IPv6 address; the
// matcher tries the host string against the host patterns first, then
// (if host parses as an IP) against the CIDR allowlist.
//
// The private-IP block in internal/runtime/runner.go runs AFTER this
// returns true; Matcher.Allowed never short-circuits the private-IP
// check.
func (m *Matcher) Allowed(host string) bool {
	if m == nil {
		// Nil matcher means "no tenant policy installed". Treat as
		// default-allow so callers that have not been wired up yet
		// keep their existing behaviour. The runtime still applies
		// the manifest http.allowHosts list and the private-IP block.
		return true
	}
	host = strings.TrimSpace(strings.ToLower(host))
	if host == "" {
		return false
	}

	// Explicit deny wins regardless of default-deny / default-allow.
	for _, pat := range m.deniedHosts {
		if pat.matches(host) {
			return false
		}
	}

	if matchesAnyHost(host, m.allowedHosts) {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		for _, cidr := range m.allowedCIDRs {
			if cidr.Contains(ip) {
				return true
			}
		}
	}
	return !m.defaultDeny
}

// DenyReason returns a short reason string suitable for embedding in the
// CS_EGRESS_DENIED error message. It is purely informational; the
// runtime is the authoritative gatekeeper.
func (m *Matcher) DenyReason(host string) string {
	if m == nil {
		return "no policy installed"
	}
	host = strings.TrimSpace(strings.ToLower(host))
	for _, pat := range m.deniedHosts {
		if pat.matches(host) {
			return fmt.Sprintf("host %q matched deny rule %q", host, pat.raw)
		}
	}
	if m.defaultDeny {
		return fmt.Sprintf("host %q not in allowlist (default-deny)", host)
	}
	return fmt.Sprintf("host %q rejected", host)
}

// Validate runs Compile and discards the Matcher. It is the cheap
// "would this policy parse?" form used by the control-plane PUT handler.
// It also catches a few semantic mistakes (a host that appears in both
// AllowedHosts and DeniedHosts is almost certainly an editing bug).
func Validate(p Policy) error {
	if _, err := Compile(p); err != nil {
		return err
	}
	seen := map[string]struct{}{}
	for _, h := range p.AllowedHosts {
		seen[strings.ToLower(strings.TrimSpace(h))] = struct{}{}
	}
	for _, h := range p.DeniedHosts {
		key := strings.ToLower(strings.TrimSpace(h))
		if _, ok := seen[key]; ok {
			return fmt.Errorf("host %q appears in both allowed_hosts and denied_hosts", key)
		}
	}
	return nil
}

func parseHostPattern(raw string) (hostPattern, error) {
	clean := strings.ToLower(strings.TrimSpace(raw))
	if clean == "" {
		return hostPattern{}, fmt.Errorf("empty host pattern")
	}
	if strings.HasPrefix(clean, "*.") {
		suffix := clean[2:]
		if suffix == "" || strings.Contains(suffix, "*") {
			return hostPattern{}, fmt.Errorf("invalid wildcard %q", raw)
		}
		return hostPattern{raw: clean, wildcard: true, suffix: suffix}, nil
	}
	if strings.Contains(clean, "*") {
		return hostPattern{}, fmt.Errorf("wildcards are only supported as a leading label: %q", raw)
	}
	return hostPattern{raw: clean, exact: clean}, nil
}

func parseCIDROrIP(raw string) (*net.IPNet, error) {
	clean := strings.TrimSpace(raw)
	if clean == "" {
		return nil, fmt.Errorf("empty cidr entry")
	}
	if _, ipNet, err := net.ParseCIDR(clean); err == nil {
		return ipNet, nil
	}
	// Bare IP: synthesise a /32 or /128 mask so callers can list
	// literal addresses without having to remember CIDR notation.
	ip := net.ParseIP(clean)
	if ip == nil {
		return nil, fmt.Errorf("not a valid IP or CIDR: %q", raw)
	}
	bits := 32
	if ip.To4() == nil {
		bits = 128
	}
	mask := net.CIDRMask(bits, bits)
	return &net.IPNet{IP: ip.Mask(mask), Mask: mask}, nil
}

func (p hostPattern) matches(host string) bool {
	if p.wildcard {
		// "*.example.com" matches "x.example.com" but never "example.com"
		// (we intentionally require at least one subdomain label so the
		// pattern is not a footgun for the apex).
		if !strings.HasSuffix(host, "."+p.suffix) {
			return false
		}
		// Reject the case where the host literally equals the suffix
		// (which strings.HasSuffix with the leading "." already prevents
		// for the apex, but keep the explicit guard for readability).
		return len(host) > len(p.suffix)+1
	}
	return host == p.exact
}

func matchesAnyHost(host string, patterns []hostPattern) bool {
	for _, pat := range patterns {
		if pat.matches(host) {
			return true
		}
	}
	return false
}
