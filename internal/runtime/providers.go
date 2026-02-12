package runtime

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

type KVProvider interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key string, value string, ttlSeconds int) error
	Del(ctx context.Context, key string) error
}

type CodeQProvider interface {
	Publish(ctx context.Context, topic string, payload any) error
}

type NopCodeQ struct{}

func (NopCodeQ) Publish(context.Context, string, any) error { return nil }

type MemoryKV struct {
	mu   sync.RWMutex
	data map[string]memoryItem
}

type memoryItem struct {
	value     string
	expiresAt time.Time
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{data: make(map[string]memoryItem)}
}

func (m *MemoryKV) Get(ctx context.Context, key string) (string, error) {
	_ = ctx
	m.mu.RLock()
	item, ok := m.data[key]
	m.mu.RUnlock()
	if !ok {
		return "", nil
	}
	if !item.expiresAt.IsZero() && time.Now().After(item.expiresAt) {
		m.mu.Lock()
		delete(m.data, key)
		m.mu.Unlock()
		return "", nil
	}
	return item.value, nil
}

func (m *MemoryKV) Set(ctx context.Context, key string, value string, ttlSeconds int) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	it := memoryItem{value: value}
	if ttlSeconds > 0 {
		it.expiresAt = time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	}
	m.data[key] = it
	return nil
}

func (m *MemoryKV) Del(ctx context.Context, key string) error {
	_ = ctx
	m.mu.Lock()
	delete(m.data, key)
	m.mu.Unlock()
	return nil
}

func JSONString(v any) string {
	if v == nil {
		return "null"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "null"
	}
	return string(b)
}
