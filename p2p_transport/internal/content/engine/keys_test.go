package engine

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"testing"

	"cipher/internal/content/core"
)

func TestLocalKeyProvider_InMemory(t *testing.T) {
	ctx := context.Background()
	kp := NewLocalKeyProvider()

	var id core.ContentID
	rand.Read(id[:])

	key := make([]byte, 32)
	rand.Read(key)

	// Get non-existent
	if _, err := kp.Get(ctx, id); err == nil {
		t.Fatalf("expected error when getting non-existent key")
	}

	// Put
	if err := kp.Put(ctx, id, key); err != nil {
		t.Fatalf("unexpected error on Put: %v", err)
	}

	// Get
	gotKey, err := kp.Get(ctx, id)
	if err != nil {
		t.Fatalf("unexpected error on Get: %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Fatalf("retrieved key mismatch")
	}

	// Delete
	if err := kp.Delete(ctx, id); err != nil {
		t.Fatalf("unexpected error on Delete: %v", err)
	}
	if _, err := kp.Get(ctx, id); err == nil {
		t.Fatalf("expected error after Delete")
	}
}

func TestLocalKeyProvider_EncryptedPersistence(t *testing.T) {
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "keystore_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kp1, err := NewEncryptedKeyProvider(tmpDir, nil)
	if err != nil {
		t.Fatalf("failed to create encrypted key provider: %v", err)
	}

	var id core.ContentID
	rand.Read(id[:])

	key := make([]byte, 32)
	rand.Read(key)

	// Put key
	if err := kp1.Put(ctx, id, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify file exists and is encrypted (does not contain plaintext key)
	keyFile := kp1.keyPath(id)
	data, err := os.ReadFile(keyFile)
	if err != nil {
		t.Fatalf("failed to read persisted key file: %v", err)
	}
	if bytes.Contains(data, key) {
		t.Fatalf("persisted key file contains unencrypted plaintext key!")
	}

	// Create a new key provider instance pointing to the same storeDir (simulating restart)
	kp2 := NewLocalKeyProvider(tmpDir)

	retrievedKey, err := kp2.Get(ctx, id)
	if err != nil {
		t.Fatalf("failed to retrieve key after reload: %v", err)
	}
	if !bytes.Equal(retrievedKey, key) {
		t.Fatalf("reloaded key mismatch")
	}

	// Delete key via kp2
	if err := kp2.Delete(ctx, id); err != nil {
		t.Fatalf("failed to delete key: %v", err)
	}

	// Verify file is deleted
	if _, err := os.Stat(kp1.keyPath(id)); !os.IsNotExist(err) {
		t.Fatalf("expected key file to be deleted")
	}
}

func TestLocalKeyProvider_CorruptKeyFile(t *testing.T) {
	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "keystore_corrupt_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kp, err := NewEncryptedKeyProvider(tmpDir, nil)
	if err != nil {
		t.Fatalf("failed to create provider: %v", err)
	}

	var id core.ContentID
	rand.Read(id[:])

	// Write garbage file
	badFile := kp.keyPath(id)
	os.WriteFile(badFile, []byte("bad_data"), 0600)

	if _, err := kp.Get(ctx, id); err == nil {
		t.Fatalf("expected error when getting corrupt key file")
	}
}
