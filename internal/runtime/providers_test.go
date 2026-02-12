package runtime

import (
	"context"
	"testing"
	"time"
)

func TestNopCodeQPublish(t *testing.T) {
	if err := (NopCodeQ{}).Publish(context.Background(), "topic", map[string]any{"x": 1}); err != nil {
		t.Fatalf("NopCodeQ publish returned error: %v", err)
	}
}

func TestMemoryKVSetGetDelAndTTL(t *testing.T) {
	kv := NewMemoryKV()
	ctx := context.Background()

	if err := kv.Set(ctx, "ctr:1", `{"ok":true}`, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	got, err := kv.Get(ctx, "ctr:1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != `{"ok":true}` {
		t.Fatalf("Get returned %q", got)
	}

	if err := kv.Del(ctx, "ctr:1"); err != nil {
		t.Fatalf("Del failed: %v", err)
	}
	got, err = kv.Get(ctx, "ctr:1")
	if err != nil {
		t.Fatalf("Get after Del failed: %v", err)
	}
	if got != "" {
		t.Fatalf("Get after Del = %q, want empty", got)
	}

	if err := kv.Set(ctx, "ctr:ttl", "v", 1); err != nil {
		t.Fatalf("Set ttl failed: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	got, err = kv.Get(ctx, "ctr:ttl")
	if err != nil {
		t.Fatalf("Get ttl failed: %v", err)
	}
	if got != "" {
		t.Fatalf("expired key = %q, want empty", got)
	}
}

func TestJSONString(t *testing.T) {
	if got := JSONString(nil); got != "null" {
		t.Fatalf("JSONString(nil) = %q", got)
	}
	if got := JSONString(map[string]any{"a": 1}); got != `{"a":1}` {
		t.Fatalf("JSONString(map) = %q", got)
	}
	// Channels are not JSON-serializable.
	if got := JSONString(make(chan int)); got != "null" {
		t.Fatalf("JSONString(non-serializable) = %q", got)
	}
}
