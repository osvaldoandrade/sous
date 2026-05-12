package egress

import (
	"strings"
	"testing"
)

func TestMatcher_ExactHostAndCaseInsensitive(t *testing.T) {
	m, err := Compile(Policy{
		AllowedHosts: []string{"Api.Example.com"},
		DefaultDeny:  true,
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !m.Allowed("api.example.com") {
		t.Fatal("exact host should be allowed (case-insensitive)")
	}
	if !m.Allowed("API.EXAMPLE.COM") {
		t.Fatal("uppercase host should match")
	}
	if m.Allowed("other.example.com") {
		t.Fatal("unrelated host should be denied under default-deny")
	}
}

func TestMatcher_Wildcard(t *testing.T) {
	m, err := Compile(Policy{AllowedHosts: []string{"*.example.com"}, DefaultDeny: true})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !m.Allowed("api.example.com") {
		t.Fatal("wildcard should match subdomain")
	}
	if !m.Allowed("v1.api.example.com") {
		t.Fatal("wildcard should match deeper subdomains")
	}
	if m.Allowed("example.com") {
		t.Fatal("wildcard should not match apex")
	}
	if m.Allowed("evil.com") {
		t.Fatal("wildcard should not match unrelated host")
	}
}

func TestMatcher_DefaultAllowVsDenyList(t *testing.T) {
	m, err := Compile(Policy{
		DeniedHosts: []string{"abuse.example.com", "*.malware.test"},
		DefaultDeny: false,
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !m.Allowed("ok.example.com") {
		t.Fatal("unmatched host should be allowed under default-allow")
	}
	if m.Allowed("abuse.example.com") {
		t.Fatal("explicit deny should win")
	}
	if m.Allowed("a.malware.test") {
		t.Fatal("wildcard deny should win")
	}
}

func TestMatcher_CIDR(t *testing.T) {
	m, err := Compile(Policy{
		AllowedCIDRs: []string{"203.0.113.0/24", "2001:db8::/32", "8.8.8.8"},
		DefaultDeny:  true,
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !m.Allowed("203.0.113.42") {
		t.Fatal("IP inside CIDR should be allowed")
	}
	if m.Allowed("203.0.114.1") {
		t.Fatal("IP outside CIDR should be denied under default-deny")
	}
	if !m.Allowed("2001:db8:cafe::1") {
		t.Fatal("IPv6 inside CIDR should be allowed")
	}
	if !m.Allowed("8.8.8.8") {
		t.Fatal("bare IP entry should match exactly")
	}
	if m.Allowed("8.8.4.4") {
		t.Fatal("non-matching bare IP should be denied")
	}
}

func TestMatcher_DefaultDenyEmptyPolicy(t *testing.T) {
	m, err := Compile(Policy{DefaultDeny: true})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if m.Allowed("anywhere.test") {
		t.Fatal("empty default-deny policy must reject every host")
	}
}

func TestMatcher_DefaultAllowEmptyPolicy(t *testing.T) {
	m, err := Compile(Policy{})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if !m.Allowed("anywhere.test") {
		t.Fatal("empty default-allow policy must accept every host")
	}
}

func TestMatcher_NilMatcherAllows(t *testing.T) {
	var m *Matcher
	if !m.Allowed("anywhere.test") {
		t.Fatal("nil matcher should be permissive (legacy behaviour)")
	}
	if got := m.DenyReason("anywhere.test"); !strings.Contains(got, "no policy") {
		t.Fatalf("nil matcher DenyReason got %q", got)
	}
}

func TestMatcher_DenyReason(t *testing.T) {
	m, err := Compile(Policy{
		AllowedHosts: []string{"good.test"},
		DeniedHosts:  []string{"bad.test"},
		DefaultDeny:  true,
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if r := m.DenyReason("bad.test"); !strings.Contains(r, "deny rule") {
		t.Fatalf("DenyReason for explicit deny: %q", r)
	}
	if r := m.DenyReason("unlisted.test"); !strings.Contains(r, "default-deny") {
		t.Fatalf("DenyReason for unlisted host: %q", r)
	}
}

func TestCompile_RejectsBadEntries(t *testing.T) {
	cases := []struct {
		name string
		p    Policy
		msg  string
	}{
		{"empty host", Policy{AllowedHosts: []string{""}}, "empty"},
		{"wildcard mid-label", Policy{AllowedHosts: []string{"foo.*.test"}}, "wildcard"},
		{"bare star", Policy{AllowedHosts: []string{"*."}}, "wildcard"},
		{"bad CIDR", Policy{AllowedCIDRs: []string{"not-an-ip"}}, "valid IP or CIDR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile(tc.p)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Fatalf("err %q did not contain %q", err, tc.msg)
			}
		})
	}
}

func TestValidate_RejectsOverlap(t *testing.T) {
	err := Validate(Policy{
		AllowedHosts: []string{"api.example.com"},
		DeniedHosts:  []string{"api.example.com"},
	})
	if err == nil || !strings.Contains(err.Error(), "both") {
		t.Fatalf("expected overlap error, got %v", err)
	}
}

func TestValidate_AcceptsValidPolicy(t *testing.T) {
	err := Validate(Policy{
		AllowedHosts: []string{"*.example.com"},
		AllowedCIDRs: []string{"2001:db8::/32"},
		DeniedHosts:  []string{"abuse.example.com"},
		DefaultDeny:  true,
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestMatcher_EmptyHostAlwaysDenied(t *testing.T) {
	m, _ := Compile(Policy{DefaultDeny: false})
	if m.Allowed("") {
		t.Fatal("empty host must never be allowed")
	}
}
