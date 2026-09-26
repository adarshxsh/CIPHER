package engine

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
)

func TestFSKeyProvider_PutGetDelete(t *testing.T) {
	dir := t.TempDir()
	kp := NewFSKeyProvider(dir)
	ctx := context.Background()

	var id core.ContentID
	copy(id[:], []byte("01234567890123456789012345678901"))

	key := []byte("secret_key_32_bytes_long_1234567")

	if err := kp.Put(ctx, id, key); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Verify permissions
	keysDir := filepath.Join(dir, "keys")
	dirInfo, err := os.Stat(keysDir)
	if err != nil {
		t.Fatalf("Stat keys directory failed: %v", err)
	}
	if dirInfo.Mode().Perm() != 0700 {
		t.Errorf("Expected keys directory permissions 0700, got %o", dirInfo.Mode().Perm())
	}

	keyPath := filepath.Join(keysDir, hex.EncodeToString(id[:])+".key")
	fileInfo, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Stat key file failed: %v", err)
	}
	if fileInfo.Mode().Perm() != 0600 {
		t.Errorf("Expected key file permissions 0600, got %o", fileInfo.Mode().Perm())
	}

	// Test Get
	gotKey, err := kp.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !bytes.Equal(gotKey, key) {
		t.Fatalf("Get key mismatch: got %x, want %x", gotKey, key)
	}

	// Test recovery across provider restart
	kp2 := NewFSKeyProvider(dir)
	gotKey2, err := kp2.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get after restart failed: %v", err)
	}
	if !bytes.Equal(gotKey2, key) {
		t.Fatalf("Key mismatch after restart: got %x, want %x", gotKey2, key)
	}

	// Test Delete
	if err := kp2.Delete(ctx, id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, err = kp2.Get(ctx, id)
	if err == nil {
		t.Fatalf("Get after Delete should fail, but succeeded")
	}

	if _, err := os.Stat(keyPath); !os.IsNotExist(err) {
		t.Fatalf("Key file should be removed from disk")
	}
}

func TestContentEngine_KeyPersistenceAcrossRestart(t *testing.T) {
	storeDir := t.TempDir()
	ctx := context.Background()

	if err := storage.NewFSStorage(storeDir); err != nil {
		t.Fatalf("NewFSStorage failed: %v", err)
	}
	store := storage.NewFSStore(storeDir)
	keys1 := NewFSKeyProvider(storeDir)
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()

	eng1 := NewContentEngine(config, enc, dig, store, store, keys1, store)

	payload := []byte("Hello, persistent key engine test across restart!")
	r := bytes.NewReader(payload)

	m, err := eng1.Ingest(ctx, r, manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Verify engine 1 reassemble
	var out1 bytes.Buffer
	if err := eng1.Reassemble(ctx, m, &out1); err != nil {
		t.Fatalf("Reassemble 1 failed: %v", err)
	}
	if !bytes.Equal(out1.Bytes(), payload) {
		t.Fatalf("Reassembled 1 payload mismatch")
	}

	// Simulate complete process restart with fresh engine & key provider instance
	keys2 := NewFSKeyProvider(storeDir)
	eng2 := NewContentEngine(config, enc, dig, store, store, keys2, store)

	var out2 bytes.Buffer
	if err := eng2.Reassemble(ctx, m, &out2); err != nil {
		t.Fatalf("Reassemble 2 (restarted) failed: %v", err)
	}
	if !bytes.Equal(out2.Bytes(), payload) {
		t.Fatalf("Reassembled 2 payload mismatch after restart")
	}
}

func TestExportAndLoadKeyFromFile(t *testing.T) {
	dir := t.TempDir()
	exportPath := filepath.Join(dir, "exported", "sub", "content.key")
	key := []byte("01234567890123456789012345678901") // 32 bytes

	if err := ExportKey(exportPath, key); err != nil {
		t.Fatalf("ExportKey failed: %v", err)
	}

	info, err := os.Stat(exportPath)
	if err != nil {
		t.Fatalf("Stat exported key file failed: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Errorf("Expected exported key file permissions 0600, got %o", info.Mode().Perm())
	}

	// Load binary key
	loadedKey, err := LoadKeyFromFile(exportPath)
	if err != nil {
		t.Fatalf("LoadKeyFromFile (binary) failed: %v", err)
	}
	if !bytes.Equal(loadedKey, key) {
		t.Fatalf("Loaded binary key mismatch: got %x, want %x", loadedKey, key)
	}

	// Test hex-encoded key file
	hexPath := filepath.Join(dir, "hex.key")
	hexStr := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef\n"
	if err := os.WriteFile(hexPath, []byte(hexStr), 0600); err != nil {
		t.Fatalf("WriteFile hex key failed: %v", err)
	}

	loadedHexKey, err := LoadKeyFromFile(hexPath)
	if err != nil {
		t.Fatalf("LoadKeyFromFile (hex) failed: %v", err)
	}
	if len(loadedHexKey) != 32 {
		t.Fatalf("Expected 32-byte key from hex, got %d bytes", len(loadedHexKey))
	}
	expectedDecoded, _ := hex.DecodeString("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
	if !bytes.Equal(loadedHexKey, expectedDecoded) {
		t.Fatalf("Loaded hex key mismatch")
	}
}
