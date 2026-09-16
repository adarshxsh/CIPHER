package test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
)

func TestKeyRedactionAndKeyfilePersistence(t *testing.T) {
	tempDir := t.TempDir()
	storeDir := filepath.Join(tempDir, "store_pub")
	inputFile := filepath.Join(tempDir, "input.txt")
	outClientFile := filepath.Join(tempDir, "out_client.txt")

	secretContent := []byte("Hello, CIPHER key redaction test payload!")
	if err := os.WriteFile(inputFile, secretContent, 0644); err != nil {
		t.Fatalf("Failed to create input file: %v", err)
	}

	// 1. Run Publisher to ingest file with -seed=false
	pubBin := filepath.Join(tempDir, "publisher")
	buildCmd := exec.Command("go", "build", "-o", pubBin, "../cmd/publisher")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build publisher: %v, output: %s", err, out)
	}

	cmd := exec.Command(pubBin, "-file", inputFile, "-store", storeDir, "-seed=false", "-p", "49100", "-ws-port", "0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("Publisher failed to run: %v\nStderr: %s", err, stderr.String())
	}

	outStr := stdout.String() + "\n" + stderr.String()

	// Verify redacted notice is present
	if !strings.Contains(outStr, "[REDACTED - Saved to") {
		t.Errorf("Expected redacted notice in publisher output, got:\n%s", outStr)
	}

	// Extract ContentID from stdout
	reCID := regexp.MustCompile(`ContentID\s+:\s+([a-fA-F0-9]{64})`)
	matches := reCID.FindStringSubmatch(outStr)
	if len(matches) < 2 {
		t.Fatalf("Failed to extract ContentID from publisher output:\n%s", outStr)
	}
	contentIDHex := matches[1]

	// Verify zero raw keys printed in output
	// Check key file on disk
	keyPath := filepath.Join(storeDir, "keys", contentIDHex+".key")
	stat, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Expected key file at %s: %v", keyPath, err)
	}

	if perm := stat.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected key file permissions 0600, got %o", perm)
	}

	rawKey, err := os.ReadFile(keyPath)
	if err != nil || len(rawKey) != 32 {
		t.Fatalf("Failed to read 32-byte key from keyfile: %v", err)
	}
	rawKeyHex := hex.EncodeToString(rawKey)

	if strings.Contains(outStr, rawKeyHex) {
		t.Errorf("SECURITY RISK: Raw hex key %s was printed in publisher logs!", rawKeyHex)
	}

	// 2. Build Client and verify reassembly using -key-file
	clientBin := filepath.Join(tempDir, "client")
	buildClientCmd := exec.Command("go", "build", "-o", clientBin, "../cmd/client")
	if out, err := buildClientCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build client: %v, output: %s", err, out)
	}

	// Engine reassembly direct test using key file
	var cID core.ContentID
	cBytes, _ := hex.DecodeString(contentIDHex)
	copy(cID[:], cBytes)

	keys := engine.NewLocalKeyProvider(storeDir)
	store := storage.NewFSStore(storeDir)
	config := core.EngineConfig{ChunkSize: 32 * 1024}
	eng := engine.NewContentEngine(config, crypto.NewChaCha20Encryptor(), verifier.NewSHA256Digest(), store, store, keys, store)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mBytes, err := eng.GetManifestBytes(ctx, cID)
	if err != nil {
		t.Fatalf("Failed to get manifest bytes: %v", err)
	}

	m, err := manifest.Deserialize(mBytes)
	if err != nil {
		t.Fatalf("Failed to deserialize manifest: %v", err)
	}

	outF, err := os.Create(outClientFile)
	if err != nil {
		t.Fatalf("Failed to create out file: %v", err)
	}
	defer outF.Close()

	if err := eng.Reassemble(ctx, m, outF); err != nil {
		t.Fatalf("Reassemble failed: %v", err)
	}

	reassembledData, err := os.ReadFile(outClientFile)
	if err != nil {
		t.Fatalf("Failed to read reassembled file: %v", err)
	}

	if string(reassembledData) != string(secretContent) {
		t.Errorf("Reassembled content mismatch: expected %q, got %q", string(secretContent), string(reassembledData))
	}
}

func TestPeerKeyRedactionNoticeAndSampleCommand(t *testing.T) {
	tempDir := t.TempDir()
	storeDir := filepath.Join(tempDir, "peer_store")
	inputFile := filepath.Join(tempDir, "peer_input.txt")

	if err := os.WriteFile(inputFile, []byte("Peer test content"), 0644); err != nil {
		t.Fatalf("Failed to create input file: %v", err)
	}

	peerBin := filepath.Join(tempDir, "peer")
	buildCmd := exec.Command("go", "build", "-o", peerBin, "../cmd/peer")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build peer: %v, output: %s", err, out)
	}

	cmd := exec.Command(peerBin, "-ingest", inputFile, "-store", storeDir, "-p", "49200", "-ws-port", "0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run peer briefly to ingest and print sample commands
	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start peer: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	// Give it time to ingest
	time.Sleep(2 * time.Second)
	cmd.Process.Kill()
	<-done

	outStr := stdout.String() + "\n" + stderr.String()

	// Check for redaction notice
	if !strings.Contains(outStr, "[REDACTED - Saved to") {
		t.Fatalf("Expected peer ingest log to contain redaction notice, got:\n%s", outStr)
	}

	// Check sample command contains -key-file
	if !strings.Contains(outStr, "-key-file") {
		t.Fatalf("Expected sample peer command to contain -key-file, got:\n%s", outStr)
	}

	// Verify key file permissions
	reCID := regexp.MustCompile(`ContentID:\s+([a-fA-F0-9]{64})`)
	matches := reCID.FindStringSubmatch(outStr)
	if len(matches) < 2 {
		t.Fatalf("Failed to parse ContentID from peer output:\n%s", outStr)
	}
	contentIDHex := matches[1]

	keyPath := filepath.Join(storeDir, "keys", fmt.Sprintf("%s.key", contentIDHex))
	stat, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("Keyfile not found at %s: %v", keyPath, err)
	}
	if stat.Mode().Perm() != 0600 {
		t.Errorf("Expected 0600 permissions, got %o", stat.Mode().Perm())
	}
}
