package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"cipher/internal/content/storage"
)

func buildPublisherBinary(t *testing.T) string {
	t.Helper()
	tempBinDir, err := os.MkdirTemp("", "pub_bin_*")
	if err != nil {
		t.Fatalf("Failed to create temp bin dir: %v", err)
	}
	binPath := filepath.Join(tempBinDir, "publisher")
	cmd := exec.Command("go", "build", "-o", binPath, ".")
	cmd.Dir = "."
	if output, err := cmd.CombinedOutput(); err != nil {
		os.RemoveAll(tempBinDir)
		t.Fatalf("Failed to build publisher binary: %v\nOutput: %s", err, string(output))
	}
	t.Cleanup(func() {
		os.RemoveAll(tempBinDir)
	})
	return binPath
}

func TestPublisher_DefaultRedactionAndKeyPersistence(t *testing.T) {
	binPath := buildPublisherBinary(t)

	tempDir, err := os.MkdirTemp("", "pub_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	inputFile := filepath.Join(tempDir, "input.txt")
	if err := os.WriteFile(inputFile, []byte("Hello CIPHER Publisher Key Persistence Test"), 0644); err != nil {
		t.Fatalf("Failed to write input file: %v", err)
	}

	storeDir := filepath.Join(tempDir, "store")

	cmd := exec.Command(binPath, "-file", inputFile, "-store", storeDir, "-seed=false", "-p", "45101", "-ws-port", "0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("Publisher execution failed: %v\nStderr: %s", err, stderr.String())
	}

	outStr := stdout.String()

	// Verify standard output contains redaction string
	if !strings.Contains(outStr, "Decryption Key: [REDACTED - STORED LOCALLY]") {
		t.Errorf("Expected stdout to contain 'Decryption Key: [REDACTED - STORED LOCALLY]', got:\n%s", outStr)
	}

	// Parse ContentID
	var contentIDHex string
	for _, line := range strings.Split(outStr, "\n") {
		if strings.HasPrefix(line, "ContentID") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				contentIDHex = parts[len(parts)-1]
			}
		}
	}

	if contentIDHex == "" {
		t.Fatalf("Failed to parse ContentID from publisher stdout:\n%s", outStr)
	}

	// Verify key file permissions on disk
	keysDir := filepath.Join(storeDir, "keys")
	keysDirInfo, err := os.Stat(keysDir)
	if err != nil {
		t.Fatalf("Failed to stat keys directory: %v", err)
	}
	if keysDirInfo.Mode().Perm() != os.FileMode(0700) {
		t.Errorf("Expected keys dir permissions 0700, got %o", keysDirInfo.Mode().Perm())
	}

	keyFilePath := filepath.Join(keysDir, contentIDHex+".key")
	keyFileInfo, err := os.Stat(keyFilePath)
	if err != nil {
		t.Fatalf("Failed to stat key file at %s: %v", keyFilePath, err)
	}
	if keyFileInfo.Mode().Perm() != os.FileMode(0600) {
		t.Errorf("Expected key file permissions 0600, got %o", keyFileInfo.Mode().Perm())
	}

	// Verify key can be read via FSKeyProvider across process cycles
	kp := storage.NewFSKeyProvider(storeDir)
	cIDBytes, err := hex.DecodeString(contentIDHex)
	if err != nil || len(cIDBytes) != 32 {
		t.Fatalf("Invalid content ID hex: %s", contentIDHex)
	}
	var contentID [32]byte
	copy(contentID[:], cIDBytes)

	storedKey, err := kp.Get(context.Background(), contentID)
	if err != nil {
		t.Fatalf("Failed to retrieve stored key from FSKeyProvider: %v", err)
	}
	if len(storedKey) != 32 {
		t.Fatalf("Expected 32-byte key, got %d bytes", len(storedKey))
	}
}

func TestPublisher_ShowKeyFlag(t *testing.T) {
	binPath := buildPublisherBinary(t)

	tempDir, err := os.MkdirTemp("", "pub_showkey_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	inputFile := filepath.Join(tempDir, "input.txt")
	os.WriteFile(inputFile, []byte("Testing --show-key flag"), 0644)
	storeDir := filepath.Join(tempDir, "store")

	cmd := exec.Command(binPath, "-file", inputFile, "-store", storeDir, "-seed=false", "--show-key", "-p", "45102", "-ws-port", "0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("Publisher execution failed: %v\nStderr: %s", err, stderr.String())
	}

	outStr := stdout.String()

	if strings.Contains(outStr, "[REDACTED - STORED LOCALLY]") {
		t.Errorf("Expected stdout NOT to contain '[REDACTED - STORED LOCALLY]' when --show-key is set, got:\n%s", outStr)
	}

	var keyHex string
	for _, line := range strings.Split(outStr, "\n") {
		if strings.HasPrefix(line, "Decryption Key:") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				keyHex = parts[len(parts)-1]
			}
		}
	}

	if len(keyHex) != 64 {
		t.Fatalf("Expected 64-character hex key in stdout when --show-key is used, got '%s'", keyHex)
	}
}

func TestPublisher_KeyOutFlag(t *testing.T) {
	binPath := buildPublisherBinary(t)

	tempDir, err := os.MkdirTemp("", "pub_keyout_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	inputFile := filepath.Join(tempDir, "input.txt")
	os.WriteFile(inputFile, []byte("Testing --key-out flag"), 0644)
	storeDir := filepath.Join(tempDir, "store")
	keyOutPath := filepath.Join(tempDir, "out_keys", "secret.key")

	cmd := exec.Command(binPath, "-file", inputFile, "-store", storeDir, "-seed=false", "--key-out", keyOutPath, "-p", "45103", "-ws-port", "0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("Publisher execution failed: %v\nStderr: %s", err, stderr.String())
	}

	// Verify key file was created at keyOutPath with mode 0600
	fileInfo, err := os.Stat(keyOutPath)
	if err != nil {
		t.Fatalf("Failed to stat key-out file %s: %v", keyOutPath, err)
	}

	if fileInfo.Mode().Perm() != os.FileMode(0600) {
		t.Errorf("Expected key-out file mode 0600, got %o", fileInfo.Mode().Perm())
	}

	rawKeyBytes, err := os.ReadFile(keyOutPath)
	if err != nil {
		t.Fatalf("Failed to read key-out file: %v", err)
	}

	if len(rawKeyBytes) != 32 {
		t.Fatalf("Expected 32 key bytes in key-out file, got %d", len(rawKeyBytes))
	}
}
