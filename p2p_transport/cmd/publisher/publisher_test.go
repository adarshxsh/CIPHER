package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublisherKeyRedaction(t *testing.T) {
	tempDir := t.TempDir()
	binPath := filepath.Join(tempDir, "publisher")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	buildCmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build publisher binary: %v, output: %s", err, string(out))
	}

	sampleFilePath := filepath.Join(tempDir, "sample.txt")
	if err := os.WriteFile(sampleFilePath, []byte("test content for publisher key redaction"), 0600); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	storeDir := filepath.Join(tempDir, "store_pub")

	t.Run("Default Redaction", func(t *testing.T) {
		cmd := exec.Command(binPath, "-file", sampleFilePath, "-seed=false", "-store", storeDir)
		outputBytes, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Publisher command failed: %v, output: %s", err, string(outputBytes))
		}
		output := string(outputBytes)

		if !strings.Contains(output, "Decryption Key: [REDACTED]") {
			t.Errorf("Expected 'Decryption Key: [REDACTED]', got output:\n%s", output)
		}

		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, "Decryption Key:") && !strings.Contains(line, "[REDACTED]") {
				t.Errorf("Raw key exposed on line: %s", line)
			}
		}
	})

	t.Run("Opt-In Show Key", func(t *testing.T) {
		cmd := exec.Command(binPath, "-file", sampleFilePath, "-seed=false", "-store", storeDir, "-show-key")
		outputBytes, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Publisher command with -show-key failed: %v, output: %s", err, string(outputBytes))
		}
		output := string(outputBytes)

		if strings.Contains(output, "Decryption Key: [REDACTED]") {
			t.Errorf("Expected raw decryption key, but found [REDACTED] in output:\n%s", output)
		}

		var foundKeyHex bool
		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, "Decryption Key:") {
				parts := strings.Fields(line)
				if len(parts) >= 3 && len(parts[len(parts)-1]) == 64 {
					foundKeyHex = true
				}
			}
		}
		if !foundKeyHex {
			t.Errorf("Expected 64-char hex key output for Decryption Key, got:\n%s", output)
		}
	})

	t.Run("Export Key File", func(t *testing.T) {
		exportedKeyFile := filepath.Join(tempDir, "exported_pub.key")
		cmd := exec.Command(binPath, "-file", sampleFilePath, "-seed=false", "-store", storeDir, "-export-key-file", exportedKeyFile)
		outputBytes, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("Publisher command with -export-key-file failed: %v, output: %s", err, string(outputBytes))
		}

		keyData, err := os.ReadFile(exportedKeyFile)
		if err != nil {
			t.Fatalf("Failed to read exported key file: %v", err)
		}
		keyStr := strings.TrimSpace(string(keyData))
		if len(keyStr) != 64 {
			t.Errorf("Expected 64-character hex key in exported file, got len %d: %s", len(keyStr), keyStr)
		}
	})
}
