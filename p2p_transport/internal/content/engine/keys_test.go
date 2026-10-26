package engine

import (
	"bytes"
	"context"
	"os"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
)

func TestZeroize(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5, 255, 128}
	Zeroize(data)
	for i, b := range data {
		if b != 0 {
			t.Fatalf("expected 0 at index %d, got %d", i, b)
		}
	}
}

func TestKeyWrapper_ReleaseAndDelete(t *testing.T) {
	rawKey := []byte("secret-key-123456789012345678901")
	wrapper := core.NewKeyWrapper(rawKey)

	b := wrapper.Bytes()
	if !bytes.Equal(b, rawKey) {
		t.Fatalf("expected handle bytes to match raw key")
	}

	wrapper.Release()

	if wrapper.Bytes() != nil {
		t.Errorf("expected wrapper.Bytes() to be nil after Release()")
	}

	for i, v := range rawKey {
		if v != 0 {
			t.Errorf("expected raw key byte at index %d to be zeroed out, got %d", i, v)
		}
	}

	// Double release check (idempotency)
	wrapper.Release()
}

func TestLocalKeyProvider_ZeroizeOnPutAndDeleteAndRelease(t *testing.T) {
	p := NewLocalKeyProvider()
	ctx := context.Background()

	var id core.ContentID
	id[0] = 42

	key1 := []byte("secret-key-123456789012345678901")
	if err := p.Put(ctx, id, key1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	p.mu.RLock()
	slice1 := p.keys[id]
	p.mu.RUnlock()

	if bytes.Equal(slice1, make([]byte, len(key1))) {
		t.Fatal("expected slice1 to contain secret key data")
	}

	// Overwrite key
	key2 := []byte("new-secret-key-98765432109876543210")
	if err := p.Put(ctx, id, key2); err != nil {
		t.Fatalf("Put overwrite failed: %v", err)
	}

	// Verify slice1 was zeroed out on overwrite
	for i, b := range slice1 {
		if b != 0 {
			t.Fatalf("slice1 byte at index %d was not zeroed on Put overwrite: %d", i, b)
		}
	}

	p.mu.RLock()
	slice2 := p.keys[id]
	p.mu.RUnlock()

	// Delete key
	if err := p.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify slice2 was zeroed out on Delete
	for i, b := range slice2 {
		if b != 0 {
			t.Fatalf("slice2 byte at index %d was not zeroed on Delete: %d", i, b)
		}
	}

	// Verify key is gone
	if _, err := p.Get(ctx, id); err == nil {
		t.Fatal("expected error getting deleted key, got nil")
	}

	// Test Release method
	key3 := []byte("release-test-key-123456789012345")
	if err := p.Put(ctx, id, key3); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	p.mu.RLock()
	slice3 := p.keys[id]
	p.mu.RUnlock()

	if err := p.Release(ctx, id); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	for i, b := range slice3 {
		if b != 0 {
			t.Fatalf("slice3 byte at index %d was not zeroed on Release: %d", i, b)
		}
	}
}

type trackingKeyProvider struct {
	inner          *LocalKeyProvider
	capturedPutKey []byte
	returnedGetKey []byte
}

func (m *trackingKeyProvider) Put(ctx context.Context, id core.ContentID, key []byte) error {
	m.capturedPutKey = key
	return m.inner.Put(ctx, id, key)
}

func (m *trackingKeyProvider) Get(ctx context.Context, id core.ContentID) ([]byte, error) {
	k, err := m.inner.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	m.returnedGetKey = k
	return k, nil
}

func (m *trackingKeyProvider) Delete(ctx context.Context, id core.ContentID) error {
	return m.inner.Delete(ctx, id)
}

func TestContentEngine_ZeroizeTransientKeys(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "content-zeroize-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	localKeys := NewLocalKeyProvider()
	tracker := &trackingKeyProvider{inner: localKeys}

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("failed to init storage: %v", err)
	}
	store := storage.NewFSStore(tmpDir)

	eng := NewContentEngine(config, enc, dig, store, store, tracker, store)

	ctx := context.Background()
	originalData := []byte("hello world transient key test data")
	reader := bytes.NewReader(originalData)

	m, err := eng.Ingest(ctx, reader, manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Verify capturedPutKey was zeroed out after Ingest returned
	if len(tracker.capturedPutKey) == 0 {
		t.Fatal("expected capturedPutKey to be captured during Ingest")
	}
	for i, b := range tracker.capturedPutKey {
		if b != 0 {
			t.Fatalf("transient ingest key byte at index %d was not zeroed: %d", i, b)
		}
	}

	var outBuf bytes.Buffer
	if err := eng.Reassemble(ctx, m, &outBuf); err != nil {
		t.Fatalf("Reassemble failed: %v", err)
	}

	// Verify returnedGetKey was zeroed out after Reassemble returned
	if len(tracker.returnedGetKey) == 0 {
		t.Fatal("expected returnedGetKey to be captured during Reassemble")
	}
	for i, b := range tracker.returnedGetKey {
		if b != 0 {
			t.Fatalf("transient reassemble key byte at index %d was not zeroed: %d", i, b)
		}
	}
}
