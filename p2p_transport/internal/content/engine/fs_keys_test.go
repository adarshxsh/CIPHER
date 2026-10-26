package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
)

func TestFSKeyProvider_BasicOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-keys-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kp, err := NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to create FSKeyProvider: %v", err)
	}

	ctx := context.Background()
	var contentID core.ContentID
	rand.Read(contentID[:])

	key := make([]byte, 32)
	rand.Read(key)

	// Put
	if err := kp.Put(ctx, contentID, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get (cache)
	gotKey, err := kp.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(gotKey) != string(key) {
		t.Fatalf("key mismatch: got %x, want %x", gotKey, key)
	}

	// Verify file on disk exists and has 0600 permissions
	keyPath := filepath.Join(tmpDir, "keys", hex.EncodeToString(contentID[:])+".key")
	info, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("key file does not exist on disk: %v", err)
	}
	mode := info.Mode().Perm()
	if mode != 0600 {
		t.Errorf("key file mode is %o, expected 0600", mode)
	}

	// Test process restart recovery by creating new FSKeyProvider
	kp2, err := NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to create second FSKeyProvider: %v", err)
	}

	gotKey2, err := kp2.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Get after restart failed: %v", err)
	}
	if string(gotKey2) != string(key) {
		t.Fatalf("key mismatch after restart: got %x, want %x", gotKey2, key)
	}

	// Delete
	if err := kp2.Delete(ctx, contentID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = kp2.Get(ctx, contentID)
	if err == nil {
		t.Fatal("expected error getting deleted key, got nil")
	}

	if _, err := os.Stat(keyPath); !os.IsNotExist(err) {
		t.Fatalf("expected key file to be deleted, but it exists")
	}
}

func TestFSKeyProvider_HexFormatFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "fs-keys-hex-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	kp, err := NewFSKeyProvider(tmpDir)
	if err != nil {
		t.Fatalf("failed to create FSKeyProvider: %v", err)
	}

	ctx := context.Background()
	var contentID core.ContentID
	rand.Read(contentID[:])

	key := make([]byte, 32)
	rand.Read(key)
	hexKey := hex.EncodeToString(key)

	// Write hex string directly to disk
	keysDir := filepath.Join(tmpDir, "keys")
	os.MkdirAll(keysDir, 0700)
	keyPath := filepath.Join(keysDir, hex.EncodeToString(contentID[:])+".key")
	if err := os.WriteFile(keyPath, []byte(hexKey+"\n"), 0600); err != nil {
		t.Fatalf("failed to write hex key file: %v", err)
	}

	// Get should parse hex string
	gotKey, err := kp.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Get failed for hex file: %v", err)
	}
	if string(gotKey) != string(key) {
		t.Fatalf("key mismatch for hex file: got %x, want %x", gotKey, key)
	}
}
