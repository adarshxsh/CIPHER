package engine

import (
	"context"
	"crypto/rand"
	"os"
	"testing"

	"cipher/internal/content/core"
)

func TestLocalKeyProvider_Persistence(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "local_key_provider_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	ctx := context.Background()
	p1 := NewLocalKeyProvider(tmpDir)

	var contentID core.ContentID
	if _, err := rand.Read(contentID[:]); err != nil {
		t.Fatalf("Failed to generate content ID: %v", err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("Failed to generate key: %v", err)
	}

	if err := p1.Put(ctx, contentID, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Retrieve from new provider instance (simulating restart)
	p2 := NewLocalKeyProvider(tmpDir)
	gotKey, err := p2.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Get after restart failed: %v", err)
	}
	if string(gotKey) != string(key) {
		t.Fatalf("Key mismatch after restart")
	}

	// Delete from p2
	if err := p2.Delete(ctx, contentID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	p3 := NewLocalKeyProvider(tmpDir)
	if _, err := p3.Get(ctx, contentID); err == nil {
		t.Fatalf("Expected key to be deleted, but Get succeeded")
	}
}

func TestKeyManagerAliasAndFingerprint(t *testing.T) {
	km := NewLocalKeyProvider()
	if km == nil {
		t.Fatalf("Failed to instantiate KeyManager")
	}

	key := []byte("01234567890123456789012345678901")
	fp := Fingerprint(key)
	if fp == "" || fp == string(key) {
		t.Errorf("Fingerprint returned invalid string: %s", fp)
	}

	redacted := RedactedKey(key)
	if redacted != "[REDACTED]" {
		t.Errorf("Expected [REDACTED], got %s", redacted)
	}
}
