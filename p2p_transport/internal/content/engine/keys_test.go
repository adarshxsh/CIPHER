package engine

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
)

func TestFileKeyVault_BasicCRUD(t *testing.T) {
	tmpDir := t.TempDir()
	vault, err := NewFileKeyVault(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FileKeyVault: %v", err)
	}

	ctx := context.Background()
	var contentID core.ContentID
	copy(contentID[:], []byte("12345678901234567890123456789012"))
	key := []byte("secret_key_32bytes_long_12345678")

	// 1. Get before Put -> error
	_, err = vault.Get(ctx, contentID)
	if err == nil {
		t.Errorf("Expected error getting non-existent key, got nil")
	}

	// 2. Put key
	if err := vault.Put(ctx, contentID, key); err != nil {
		t.Fatalf("Failed to Put key: %v", err)
	}

	// 3. Get key
	retrievedKey, err := vault.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Failed to Get key: %v", err)
	}
	if !bytes.Equal(key, retrievedKey) {
		t.Errorf("Key mismatch: expected %x, got %x", key, retrievedKey)
	}

	// 4. Delete key
	if err := vault.Delete(ctx, contentID); err != nil {
		t.Fatalf("Failed to Delete key: %v", err)
	}

	// 5. Get after Delete -> error
	_, err = vault.Get(ctx, contentID)
	if err == nil {
		t.Errorf("Expected error after Delete, got nil")
	}
}

func TestFileKeyVault_PermissionsAndAtomicWrite(t *testing.T) {
	tmpDir := t.TempDir()
	vaultPath := filepath.Join(tmpDir, "keys.json")

	vault, err := NewFileKeyVault(vaultPath)
	if err != nil {
		t.Fatalf("Failed to create FileKeyVault: %v", err)
	}

	ctx := context.Background()
	var contentID core.ContentID
	copy(contentID[:], []byte("00000000000000000000000000000001"))
	key := []byte("key_32bytes_padding_123456789012")

	if err := vault.Put(ctx, contentID, key); err != nil {
		t.Fatalf("Failed to Put key: %v", err)
	}

	// Check file permissions on keys.json
	info, err := os.Stat(vaultPath)
	if err != nil {
		t.Fatalf("Failed to stat keys.json: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Errorf("Expected file permissions 0600, got %o", perm)
	}

	// Ensure no leftover temporary files remain in directory
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("Failed to read temp dir: %v", err)
	}

	for _, entry := range entries {
		if entry.Name() != "keys.json" {
			t.Errorf("Unexpected file remaining in vault directory: %s", entry.Name())
		}
	}
}

func TestFileKeyVault_PersistenceAcrossRestarts(t *testing.T) {
	tmpDir := t.TempDir()

	ctx := context.Background()
	var contentID core.ContentID
	copy(contentID[:], []byte("restart_test_content_id_32bytes!"))
	key := []byte("restart_test_secret_key_32bytes!")

	// Daemon run 1: Store key in vault
	vault1, err := NewFileKeyVault(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create initial FileKeyVault: %v", err)
	}
	if err := vault1.Put(ctx, contentID, key); err != nil {
		t.Fatalf("Failed to put key in initial vault: %v", err)
	}

	// Daemon restart simulation: Create a brand new vault instance pointing to same dir
	vault2, err := NewFileKeyVault(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create second FileKeyVault: %v", err)
	}

	retrievedKey, err := vault2.Get(ctx, contentID)
	if err != nil {
		t.Fatalf("Failed to recover key after simulated daemon restart: %v", err)
	}

	if !bytes.Equal(key, retrievedKey) {
		t.Errorf("Recovered key mismatch across restart: expected %x, got %x", key, retrievedKey)
	}
}

func TestContentEngine_DaemonRestartDecryption(t *testing.T) {
	storeDir := t.TempDir()
	if err := storage.NewFSStorage(storeDir); err != nil {
		t.Fatalf("Failed to create store dir: %v", err)
	}

	ctx := context.Background()
	originalData := []byte("Persistent key vault test payload for full daemon restart scenario!")

	// 1. Initialize original engine and ingest data
	config := core.EngineConfig{ChunkSize: 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	vault1, err := NewFileKeyVault(storeDir)
	if err != nil {
		t.Fatalf("Failed to create key vault 1: %v", err)
	}
	store1 := storage.NewFSStore(storeDir)
	eng1 := NewContentEngine(config, enc, dig, store1, store1, vault1, store1)

	m, err := eng1.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data: %v", err)
	}

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("Failed to serialize manifest: %v", err)
	}
	if err := eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes); err != nil {
		t.Fatalf("Failed to store manifest: %v", err)
	}

	// Verify key vault file exists and mode is 0600
	keysFilePath := filepath.Join(storeDir, "keys.json")
	info, err := os.Stat(keysFilePath)
	if err != nil {
		t.Fatalf("Expected keys.json to exist at %s: %v", keysFilePath, err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected keys.json mode 0600, got %o", perm)
	}

	// 2. Simulate Daemon Restart: Create completely new ContentEngine instance
	vault2, err := NewFileKeyVault(storeDir)
	if err != nil {
		t.Fatalf("Failed to create key vault 2: %v", err)
	}
	store2 := storage.NewFSStore(storeDir)
	eng2 := NewContentEngine(config, enc, dig, store2, store2, vault2, store2)

	// Reassemble and decrypt using the second engine instance
	var buf bytes.Buffer
	if err := eng2.Reassemble(ctx, m, &buf); err != nil {
		t.Fatalf("Failed to reassemble/decrypt content after daemon restart: %v", err)
	}

	if !bytes.Equal(originalData, buf.Bytes()) {
		t.Errorf("Decrypted data mismatch after restart!\nExpected: %s\nGot: %s", originalData, buf.Bytes())
	}
}
