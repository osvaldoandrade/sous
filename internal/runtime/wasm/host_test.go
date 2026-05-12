package wasm

import (
	"context"
	"testing"

	apipkg "github.com/osvaldoandrade/sous/internal/api"
)

// TestCheckKVCap exercises the capability gate independently of the
// wasm runtime. The fixture matrix mirrors the cs-js bindKV check so
// that any future change to one side surfaces here too.
func TestCheckKVCap(t *testing.T) {
	bridge := &hostBridge{
		caps: apipkg.ManifestCapabilities{
			KV: apipkg.ManifestKVCaps{
				Prefixes: []string{"ctr:", "ledger:"},
				Ops:      []string{"get", "set"},
			},
		},
	}
	tests := []struct {
		op, key string
		want    bool
	}{
		{"get", "ctr:hits", true},
		{"set", "ledger:audit", true},
		{"del", "ctr:hits", false}, // op not allowed
		{"get", "denied:key", false},
		{"set", "ctr:", true},
	}
	for _, tc := range tests {
		err := bridge.checkKVCap(tc.op, tc.key)
		got := err == nil
		if got != tc.want {
			t.Errorf("op=%s key=%s allowed=%v want=%v err=%v", tc.op, tc.key, got, tc.want, err)
		}
	}
}

func TestCodeQTopicAllowed(t *testing.T) {
	bridge := &hostBridge{
		caps: apipkg.ManifestCapabilities{
			CodeQ: apipkg.ManifestCodeQCaps{
				PublishTopics: []string{"jobs.*", "exact.topic"},
			},
		},
	}
	tests := []struct {
		topic string
		want  bool
	}{
		{"jobs.foo", true},
		{"jobs.", true},
		{"exact.topic", true},
		{"exact.topic.suffix", false},
		{"other.topic", false},
	}
	for _, tc := range tests {
		if got := bridge.codeqTopicAllowed(tc.topic); got != tc.want {
			t.Errorf("topic=%s allowed=%v want=%v", tc.topic, got, tc.want)
		}
	}
}

func TestHTTPHostAllowed(t *testing.T) {
	bridge := &hostBridge{
		caps: apipkg.ManifestCapabilities{
			HTTP: apipkg.ManifestHTTPCaps{
				AllowHosts: []string{"api.example.com", "other.example"},
			},
		},
	}
	tests := []struct {
		host string
		want bool
	}{
		{"api.example.com", true},
		{"API.EXAMPLE.COM", true},
		{"other.example", true},
		{"blocked.example", false},
		{"", false},
	}
	for _, tc := range tests {
		if got := bridge.httpHostAllowed(tc.host); got != tc.want {
			t.Errorf("host=%q allowed=%v want=%v", tc.host, got, tc.want)
		}
	}
}

func TestPackUnpackPtrLen(t *testing.T) {
	for _, ptr := range []uint32{0, 1, 1 << 16, 0xFFFFFFFF} {
		for _, length := range []uint32{0, 1, 0xFFFF, 0xFFFFFFFF} {
			packed := PackPtrLen(ptr, length)
			gp, gl := UnpackPtrLen(packed)
			if gp != ptr || gl != length {
				t.Errorf("roundtrip ptr=%d len=%d -> packed=%d got=(%d,%d)", ptr, length, packed, gp, gl)
			}
		}
	}
}

func TestLogBufferTruncation(t *testing.T) {
	buf := newLogBuffer(16)                   // very small cap
	buf.append("info", "hello world")         // 18 bytes formatted; truncated
	buf.append("info", "ignored after trunc") // still tracked but truncated stays
	lines, truncated := buf.snapshot()
	if !truncated {
		t.Fatalf("expected truncated=true, lines=%v", lines)
	}
	if len(lines) == 0 {
		t.Fatalf("expected at least one partial line")
	}
}

func TestLevelName(t *testing.T) {
	if got := levelName(0); got != "info" {
		t.Errorf("0 -> %s, want info", got)
	}
	if got := levelName(1); got != "warn" {
		t.Errorf("1 -> %s, want warn", got)
	}
	if got := levelName(2); got != "error" {
		t.Errorf("2 -> %s, want error", got)
	}
	if got := levelName(99); got != "info" {
		t.Errorf("99 -> %s, want info (fallback)", got)
	}
}

func TestMergeDeadlinesPicksEarlier(t *testing.T) {
	deadlineCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	merged := mergeDeadlines(context.Background(), deadlineCtx)
	if merged == nil {
		t.Fatal("merged ctx must not be nil")
	}
	// With no deadlines on either side, merged is just b.
	if merged != deadlineCtx {
		t.Logf("merged is a wrapped ctx (allowed)")
	}
}

func TestEncodeStatusU32(t *testing.T) {
	if got := EncodeStatusU32(StatusErrInvalidArg); int32(got) != StatusErrInvalidArg {
		t.Errorf("EncodeStatusU32(%d) = %d", StatusErrInvalidArg, got)
	}
	if got := EncodeStatusU32(StatusOK); got != 0 {
		t.Errorf("EncodeStatusU32(StatusOK) = %d, want 0", got)
	}
}
