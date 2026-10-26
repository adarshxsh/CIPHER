package test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
)

func TestKeyPersistenceAcrossRestart(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cipher-restart-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	storePath := filepath.Join(tmpDir, "store")
	keysDir := filepath.Join(storePath, "keys")

	ctx := context.Background()
	originalData := []byte("Hello, CIPHER persistent key recovery across restarts!")

	// 1. Initialize Engine 1
	if err := storage.NewFSStorage(storePath); err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	kp1, err := engine.NewFSKeyProvider(keysDir)
	if err != nil {
		t.Fatalf("failed to create kp1: %v", err)
	}
	store1 := storage.NewFSStore(storePath)
	eng1 := engine.NewContentEngine(
		core.EngineConfig{ChunkSize: 32 * 1024},
		crypto.NewChaCha20Encryptor(),
		verifier.NewSHA256Digest(),
		store1,
		store1,
		kp1,
		store1,
	)

	m, err := eng1.Ingest(ctx, bytes.NewReader(originalData), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Verify key file exists and has 0600 permissions
	keyFilePath := filepath.Join(keysDir, hex.EncodeToString(m.Descriptor.ID[:]))
	info, err := os.Stat(keyFilePath)
	if err != nil {
		t.Fatalf("Key file was not persisted: %v", err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("Expected key file permissions 0600, got %o", info.Mode().Perm())
	}

	// 2. Simulate Process Restart: Initialize Engine 2 on the same store path
	kp2, err := engine.NewFSKeyProvider(keysDir)
	if err != nil {
		t.Fatalf("failed to create kp2: %v", err)
	}
	store2 := storage.NewFSStore(storePath)
	eng2 := engine.NewContentEngine(
		core.EngineConfig{ChunkSize: 32 * 1024},
		crypto.NewChaCha20Encryptor(),
		verifier.NewSHA256Digest(),
		store2,
		store2,
		kp2,
		store2,
	)

	// Reassemble using eng2 without manual key insertion
	var outBuf bytes.Buffer
	if err := eng2.Reassemble(ctx, m, &outBuf); err != nil {
		t.Fatalf("Reassemble after restart failed: %v", err)
	}

	if !bytes.Equal(outBuf.Bytes(), originalData) {
		t.Fatalf("Reassembled data mismatch after restart!\nGot: %s\nWant: %s", outBuf.String(), string(originalData))
	}
}

func TestPublisherCLI_KeyRedactionAndExport(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cipher-cli-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	inputFile := filepath.Join(tmpDir, "input.txt")
	testData := make([]byte, 1024)
	rand.Read(testData)
	if err := os.WriteFile(inputFile, testData, 0644); err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}

	// Find root dir of p2p_transport package
	rootDir, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("failed to resolve root dir: %v", err)
	}

	// Test 1: Default execution (redacted output)
	store1 := filepath.Join(tmpDir, "store1")
	cmd1 := exec.Command("go", "run", "./cmd/publisher/main.go", "-file", inputFile, "-seed=false", "-store", store1, "-p", "4101", "-ws-port", "0")
	cmd1.Dir = rootDir
	out1, err := cmd1.CombinedOutput()
	if err != nil {
		t.Fatalf("publisher command failed: %v, output: %s", err, string(out1))
	}

	outStr1 := string(out1)
	if !strings.Contains(outStr1, "Decryption Key: [REDACTED]") {
		t.Fatalf("Expected default publisher stdout to contain 'Decryption Key: [REDACTED]', output was:\n%s", outStr1)
	}

	// Test 2: Execution with -show-key
	store2 := filepath.Join(tmpDir, "store2")
	cmd2 := exec.Command("go", "run", "./cmd/publisher/main.go", "-file", inputFile, "-seed=false", "-store", store2, "-p", "4102", "-ws-port", "0", "-show-key")
	cmd2.Dir = rootDir
	out2, err := cmd2.CombinedOutput()
	if err != nil {
		t.Fatalf("publisher with -show-key failed: %v, output: %s", err, string(out2))
	}

	outStr2 := string(out2)
	if strings.Contains(outStr2, "Decryption Key: [REDACTED]") {
		t.Fatalf("Expected -show-key to unredact key, output was:\n%s", outStr2)
	}

	// Test 3: Execution with -key-out
	store3 := filepath.Join(tmpDir, "store3")
	keyOutPath := filepath.Join(tmpDir, "exported.key")
	cmd3 := exec.Command("go", "run", "./cmd/publisher/main.go", "-file", inputFile, "-seed=false", "-store", store3, "-p", "4103", "-ws-port", "0", "-key-out", keyOutPath)
	cmd3.Dir = rootDir
	out3, err := cmd3.CombinedOutput()
	if err != nil {
		t.Fatalf("publisher with -key-out failed: %v, output: %s", err, string(out3))
	}

	outStr3 := string(out3)
	if !strings.Contains(outStr3, "Decryption Key: [REDACTED]") {
		t.Fatalf("Expected -key-out publisher stdout to remain redacted, output was:\n%s", outStr3)
	}

	keyData, err := os.ReadFile(keyOutPath)
	if err != nil {
		t.Fatalf("Exported key file not created: %v", err)
	}

	keyHexStr := strings.TrimSpace(string(keyData))
	if len(keyHexStr) != 64 {
		t.Fatalf("Expected 64-char hex key string in exported key file, got len %d: %s", len(keyHexStr), keyHexStr)
	}

	info3, err := os.Stat(keyOutPath)
	if err != nil {
		t.Fatalf("failed to stat key-out file: %v", err)
	}
	if info3.Mode().Perm() != 0600 {
		t.Fatalf("Expected key-out file permissions 0600, got %o", info3.Mode().Perm())
	}
}
