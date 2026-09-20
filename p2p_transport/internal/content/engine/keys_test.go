package engine

import (
	"context"
	"testing"

	"cipher/internal/content/core"
)

func TestLocalKeyProvider_DeleteWipesMemory(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id := core.ContentID{1, 2, 3, 4}
	key := []byte("secret-key-1234567890123456789012") // 32 bytes

	if err := provider.Put(ctx, id, key); err != nil {
		t.Fatalf("unexpected Put error: %v", err)
	}

	internalSlice := provider.keys[id]
	if len(internalSlice) == 0 {
		t.Fatal("expected non-empty internal slice")
	}

	if err := provider.Delete(ctx, id); err != nil {
		t.Fatalf("unexpected Delete error: %v", err)
	}

	if _, err := provider.Get(ctx, id); err == nil {
		t.Fatal("expected Get after Delete to fail")
	}

	for i, b := range internalSlice {
		if b != 0 {
			t.Errorf("byte at index %d is %d, expected 0", i, b)
		}
	}
}

func TestLocalKeyProvider_PutReplacementWipesMemory(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id := core.ContentID{1, 2, 3, 4}
	key1 := []byte("initial-key-12345678901234567890")
	key2 := []byte("updated-key-12345678901234567890")

	if err := provider.Put(ctx, id, key1); err != nil {
		t.Fatalf("unexpected Put error: %v", err)
	}

	slice1 := provider.keys[id]

	if err := provider.Put(ctx, id, key2); err != nil {
		t.Fatalf("unexpected Put replacement error: %v", err)
	}

	for i, b := range slice1 {
		if b != 0 {
			t.Errorf("original byte at index %d is %d, expected 0", i, b)
		}
	}

	got, err := provider.Get(ctx, id)
	if err != nil {
		t.Fatalf("unexpected Get error: %v", err)
	}
	if string(got) != string(key2) {
		t.Errorf("got key %s, expected %s", got, key2)
	}
}

func TestLocalKeyProvider_CloseWipesMemory(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id1 := core.ContentID{1}
	id2 := core.ContentID{2}
	key1 := []byte("key1-1234567890123456789012345678")
	key2 := []byte("key2-1234567890123456789012345678")

	_ = provider.Put(ctx, id1, key1)
	_ = provider.Put(ctx, id2, key2)

	slice1 := provider.keys[id1]
	slice2 := provider.keys[id2]

	if err := provider.Close(); err != nil {
		t.Fatalf("unexpected Close error: %v", err)
	}

	for i, b := range slice1 {
		if b != 0 {
			t.Errorf("slice1 byte at %d is %d, expected 0", i, b)
		}
	}
	for i, b := range slice2 {
		if b != 0 {
			t.Errorf("slice2 byte at %d is %d, expected 0", i, b)
		}
	}

	if len(provider.keys) != 0 {
		t.Errorf("map len %d, expected 0", len(provider.keys))
	}
}

func TestLocalKeyProvider_ClearWipesMemory(t *testing.T) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id := core.ContentID{5}
	key := []byte("clear-key-1234567890123456789012")

	_ = provider.Put(ctx, id, key)
	slice := provider.keys[id]

	if err := provider.Clear(); err != nil {
		t.Fatalf("unexpected Clear error: %v", err)
	}

	for i, b := range slice {
		if b != 0 {
			t.Errorf("slice byte at %d is %d, expected 0", i, b)
		}
	}

	if len(provider.keys) != 0 {
		t.Errorf("map len %d, expected 0", len(provider.keys))
	}
}

func TestWipe(t *testing.T) {
	// Nil slice
	Wipe(nil)

	// Empty slice
	Wipe([]byte{})

	// Non-empty slice
	buf := []byte{0xFF, 0xAA, 0x55, 0x12}
	Wipe(buf)
	for i, b := range buf {
		if b != 0 {
			t.Errorf("byte at index %d is %d, expected 0", i, b)
		}
	}
}

func BenchmarkLocalKeyProvider_Get(b *testing.B) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id := core.ContentID{1, 2, 3}
	key := make([]byte, 32)
	_ = provider.Put(ctx, id, key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = provider.Get(ctx, id)
	}
}

func BenchmarkLocalKeyProvider_Put(b *testing.B) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id := core.ContentID{1, 2, 3}
	key := make([]byte, 32)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = provider.Put(ctx, id, key)
	}
}

func BenchmarkLocalKeyProvider_Delete(b *testing.B) {
	provider := NewLocalKeyProvider()
	ctx := context.Background()
	id := core.ContentID{1, 2, 3}
	key := make([]byte, 32)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = provider.Put(ctx, id, key)
		_ = provider.Delete(ctx, id)
	}
}

func BenchmarkLocalKeyProvider_Wipe(b *testing.B) {
	key := make([]byte, 32)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Wipe(key)
	}
}
