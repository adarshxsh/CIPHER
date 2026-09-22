package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func runPeerIngestAndCapture(binPath string, args ...string) (string, error) {
	cmd := exec.Command(binPath, args...)

	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	if err := cmd.Start(); err != nil {
		return "", err
	}

	done := make(chan error, 1)
	go func() {
		for i := 0; i < 50; i++ {
			if strings.Contains(outBuf.String(), "-----------------------------------------------------------") {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if cmd.Process != nil {
			_ = cmd.Process.Signal(os.Interrupt)
		}
		done <- cmd.Wait()
	}()

	err := <-done
	return outBuf.String(), err
}

func TestPeerKeyRedaction(t *testing.T) {
	tempDir := t.TempDir()
	binPath := filepath.Join(tempDir, "peer")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	buildCmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build peer binary: %v, output: %s", err, string(out))
	}

	sampleFilePath := filepath.Join(tempDir, "sample.txt")
	if err := os.WriteFile(sampleFilePath, []byte("test content for peer key redaction"), 0600); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	t.Run("Default Redaction", func(t *testing.T) {
		storeDir := filepath.Join(tempDir, "store_peer1")
		output, _ := runPeerIngestAndCapture(binPath, "-ingest", sampleFilePath, "-p", "49101", "-ws-port", "0", "-store", storeDir)

		if !strings.Contains(output, "Key: [REDACTED]") {
			t.Errorf("Expected 'Key: [REDACTED]', got output:\n%s", output)
		}
		if !strings.Contains(output, `-key "[REDACTED]"`) {
			t.Errorf("Expected '-key \"[REDACTED]\"' in sample command output, got:\n%s", output)
		}
	})

	t.Run("Opt-In Show Key", func(t *testing.T) {
		storeDir := filepath.Join(tempDir, "store_peer2")
		output, _ := runPeerIngestAndCapture(binPath, "-ingest", sampleFilePath, "-p", "49102", "-ws-port", "0", "-store", storeDir, "-show-key")

		if strings.Contains(output, "Key: [REDACTED]") {
			t.Errorf("Expected raw key, got [REDACTED] in output:\n%s", output)
		}

		var foundKeyLine bool
		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, "Key:") && !strings.Contains(line, "ContentID:") {
				parts := strings.Fields(line)
				if len(parts) >= 2 && len(parts[len(parts)-1]) == 64 {
					foundKeyLine = true
				}
			}
		}
		if !foundKeyLine {
			t.Errorf("Expected 64-char raw key in output, got:\n%s", output)
		}
	})

	t.Run("Export Key File", func(t *testing.T) {
		storeDir := filepath.Join(tempDir, "store_peer3")
		exportedKeyFile := filepath.Join(tempDir, "exported_peer.key")
		_, _ = runPeerIngestAndCapture(binPath, "-ingest", sampleFilePath, "-p", "49103", "-ws-port", "0", "-store", storeDir, "-export-key-file", exportedKeyFile)

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
