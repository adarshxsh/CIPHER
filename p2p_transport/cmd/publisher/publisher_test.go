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

func TestPublisherKeyRedaction(t *testing.T) {
	// Build publisher binary for testing
	tempDir := t.TempDir()
	binaryPath := filepath.Join(tempDir, "publisher")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, ".")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build publisher binary: %v\nOutput: %s", err, string(out))
	}

	// Create test file to publish
	testFile := filepath.Join(tempDir, "input.dat")
	if err := os.WriteFile(testFile, []byte("test content for publisher redaction test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	t.Run("Default Redaction", func(t *testing.T) {
		storeDir := filepath.Join(tempDir, "store_default")
		cmd := exec.Command(binaryPath,
			"-file", testFile,
			"-store", storeDir,
			"-seed=false",
			"-p", "59101",
			"-ws-port", "0",
		)
		var outBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &outBuf

		if err := cmd.Run(); err != nil {
			t.Fatalf("Publisher failed to run: %v\nOutput:\n%s", err, outBuf.String())
		}

		output := outBuf.String()
		if !strings.Contains(output, "Decryption Key: [REDACTED]") {
			t.Errorf("Expected output to contain 'Decryption Key: [REDACTED]', got:\n%s", output)
		}

		// Ensure no 64-character hex key appears after "Decryption Key: "
		re := regexp.MustCompile(`Decryption Key:\s+([a-fA-F0-9]{64})`)
		if matches := re.FindStringSubmatch(output); len(matches) > 0 {
			t.Errorf("Found raw hex key in default output: %s", matches[1])
		}
	})

	t.Run("Opt-In --show-key", func(t *testing.T) {
		storeDir := filepath.Join(tempDir, "store_showkey")
		cmd := exec.Command(binaryPath,
			"-file", testFile,
			"-store", storeDir,
			"-seed=false",
			"-p", "59102",
			"-ws-port", "0",
			"--show-key",
		)
		var outBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &outBuf

		if err := cmd.Run(); err != nil {
			t.Fatalf("Publisher with --show-key failed to run: %v\nOutput:\n%s", err, outBuf.String())
		}

		output := outBuf.String()
		if strings.Contains(output, "Decryption Key: [REDACTED]") {
			t.Errorf("Output should not contain '[REDACTED]' when --show-key is passed, got:\n%s", output)
		}

		re := regexp.MustCompile(`Decryption Key:\s+([a-fA-F0-9]{64})`)
		if matches := re.FindStringSubmatch(output); len(matches) < 2 {
			t.Errorf("Expected 64-character hex decryption key in output, got:\n%s", output)
		}
	})
}
