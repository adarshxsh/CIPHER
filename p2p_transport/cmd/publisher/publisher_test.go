package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestPublisherKeyRedactionAndExport(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "publisher_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create test file
	testFilePath := filepath.Join(tempDir, "test.txt")
	if err := os.WriteFile(testFilePath, []byte("hello publisher test content"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Build publisher binary for testing
	binPath := filepath.Join(tempDir, "publisher_bin")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build publisher binary: %v, output: %s", err, out)
	}

	hexKeyRegex := regexp.MustCompile(`Decryption Key: ([a-f0-9]{64})`)

	// 1. Test Default Ingestion (Key Redacted)
	t.Run("DefaultRedaction", func(t *testing.T) {
		cmd := exec.Command(binPath, "-seed=false", "-store", filepath.Join(tempDir, "store1"), "-file", testFilePath)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			t.Fatalf("Publisher command failed: %v, output: %s", err, out.String())
		}

		outputStr := out.String()
		if !strings.Contains(outputStr, "Decryption Key: [REDACTED]") {
			t.Errorf("Expected output to contain 'Decryption Key: [REDACTED]', got:\n%s", outputStr)
		}
		if hexKeyRegex.MatchString(outputStr) {
			t.Errorf("Found unmasked hexadecimal key in default output:\n%s", outputStr)
		}
	})

	// 2. Test --show-key
	t.Run("ShowKeyFlag", func(t *testing.T) {
		cmd := exec.Command(binPath, "-seed=false", "--show-key", "-store", filepath.Join(tempDir, "store2"), "-file", testFilePath)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			t.Fatalf("Publisher command failed: %v, output: %s", err, out.String())
		}

		outputStr := out.String()
		matches := hexKeyRegex.FindStringSubmatch(outputStr)
		if len(matches) < 2 {
			t.Fatalf("Expected output to contain raw hexadecimal decryption key, got:\n%s", outputStr)
		}
		if len(matches[1]) != 64 {
			t.Errorf("Expected 64-character hex key, got len=%d: %s", len(matches[1]), matches[1])
		}
	})

	// 3. Test --key-out
	t.Run("KeyOutFlag", func(t *testing.T) {
		keyOutFile := filepath.Join(tempDir, "exported.key")
		cmd := exec.Command(binPath, "-seed=false", "--key-out", keyOutFile, "-store", filepath.Join(tempDir, "store3"), "-file", testFilePath)
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			t.Fatalf("Publisher command failed: %v, output: %s", err, out.String())
		}

		outputStr := out.String()
		if !strings.Contains(outputStr, "Decryption Key: [REDACTED]") {
			t.Errorf("Expected default output to redact key even with --key-out, got:\n%s", outputStr)
		}

		// Check exported file
		info, err := os.Stat(keyOutFile)
		if err != nil {
			t.Fatalf("Exported key file does not exist: %v", err)
		}
		if info.Mode().Perm() != 0600 {
			t.Errorf("Expected key file permission 0600, got %o", info.Mode().Perm())
		}

		content, err := os.ReadFile(keyOutFile)
		if err != nil {
			t.Fatalf("Failed to read exported key file: %v", err)
		}
		trimmedKey := strings.TrimSpace(string(content))
		if len(trimmedKey) != 64 {
			t.Errorf("Expected 64-char hex key string in file, got len=%d: '%s'", len(trimmedKey), trimmedKey)
		}
	})
}
