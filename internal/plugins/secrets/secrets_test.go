package secrets_test

import (
	"testing"

	"github.com/osvaldoandrade/sous/internal/plugins/secrets"
)

func TestParseRef(t *testing.T) {
	cases := []struct {
		in       string
		wantName string
		wantPath string
		wantFmt  string
		wantErr  bool
	}{
		{in: "STRIPE_KEY", wantName: "STRIPE_KEY", wantPath: "STRIPE_KEY"},
		{in: "STRIPE_KEY=payments/stripe", wantName: "STRIPE_KEY", wantPath: "payments/stripe"},
		{in: "TIER=payments/multi#json:tier", wantName: "TIER", wantPath: "payments/multi", wantFmt: "json:tier"},
		{in: "TIER=payments/multi#json-field:tier", wantName: "TIER", wantPath: "payments/multi", wantFmt: "json-field:tier"},
		{in: "  KEY  =  path/x  ", wantName: "KEY", wantPath: "path/x"},
		{in: "", wantErr: true},
		{in: "=value", wantErr: true},
	}
	for _, c := range cases {
		got, err := secrets.ParseRef(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("ParseRef(%q): expected error, got %+v", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseRef(%q): %v", c.in, err)
			continue
		}
		if got.Name != c.wantName || got.Path != c.wantPath || got.Format != c.wantFmt {
			t.Errorf("ParseRef(%q)=%+v, want name=%s path=%s fmt=%s", c.in, got, c.wantName, c.wantPath, c.wantFmt)
		}
	}
}

func TestExtractValue(t *testing.T) {
	raw := `{"api_key":"abc","tier":7,"flag":true,"missing":null}`
	if v, err := secrets.ExtractValue(raw, ""); err != nil || v != raw {
		t.Fatalf("raw passthrough: v=%q err=%v", v, err)
	}
	if v, err := secrets.ExtractValue(raw, "raw"); err != nil || v != raw {
		t.Fatalf("raw mode: v=%q err=%v", v, err)
	}
	if v, err := secrets.ExtractValue(raw, "json-field:api_key"); err != nil || v != "abc" {
		t.Fatalf("api_key: v=%q err=%v", v, err)
	}
	if v, err := secrets.ExtractValue(raw, "json-field:tier"); err != nil || v != "7" {
		t.Fatalf("tier: v=%q err=%v", v, err)
	}
	if v, err := secrets.ExtractValue(raw, "json-field:flag"); err != nil || v != "true" {
		t.Fatalf("flag: v=%q err=%v", v, err)
	}
	if _, err := secrets.ExtractValue(raw, "json-field:nope"); err == nil {
		t.Fatalf("missing key should error")
	}
	if _, err := secrets.ExtractValue(`{`, "json-field:x"); err == nil {
		t.Fatalf("bad JSON should error")
	}
	if _, err := secrets.ExtractValue("x", "weird"); err == nil {
		t.Fatalf("unknown format should error")
	}
}
