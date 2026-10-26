package main

import (
	"bufio"
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

func runPeerIngestUntilDone(cmd *exec.Cmd) (string, error) {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	var mu sync.Mutex

	if err := cmd.Start(); err != nil {
		return "", err
	}

	var wg sync.WaitGroup
	wg.Add(2)

	triggerSignal := func() {
		time.Sleep(50 * time.Millisecond)
		_ = cmd.Process.Signal(os.Interrupt)
	}

	readStream := func(r io.Reader) {
		defer wg.Done()
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			line := scanner.Text()
			mu.Lock()
			buf.WriteString(line + "\n")
			mu.Unlock()

			if strings.Contains(line, "To download this file on another peer") || strings.Contains(line, "-----------------------------------------------------------") {
				go triggerSignal()
			}
		}
	}

	go readStream(stdoutPipe)
	go readStream(stderrPipe)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		_ = cmd.Wait()
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}

	mu.Lock()
	res := buf.String()
	mu.Unlock()
	return res, nil
}

func TestPeerKeyRedactionAndExport(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "peer_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	testFilePath := filepath.Join(tempDir, "test.txt")
	if err := os.WriteFile(testFilePath, []byte("hello peer test content"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	binPath := filepath.Join(tempDir, "peer_bin")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build peer binary: %v, output: %s", err, out)
	}

	hexKeyRegex := regexp.MustCompile(`Key: ([a-f0-9]{64})`)

	// 1. Test Default Ingestion (Key Redacted)
	t.Run("DefaultRedaction", func(t *testing.T) {
		cmd := exec.Command(binPath, "-p", "49001", "-ws-port", "0", "-store", filepath.Join(tempDir, "store1"), "-ingest", testFilePath)
		outputStr, err := runPeerIngestUntilDone(cmd)
		if err != nil {
			t.Fatalf("Failed running peer command: %v", err)
		}

		if !strings.Contains(outputStr, "Key: [REDACTED]") {
			t.Errorf("Expected output to contain 'Key: [REDACTED]', got:\n%s", outputStr)
		}
		if !strings.Contains(outputStr, `-key "[REDACTED]"`) {
			t.Errorf("Expected copy-paste block to contain '-key \"[REDACTED]\"', got:\n%s", outputStr)
		}
		if hexKeyRegex.MatchString(outputStr) {
			t.Errorf("Found unmasked hexadecimal key in default output:\n%s", outputStr)
		}
	})

	// 2. Test --show-key
	t.Run("ShowKeyFlag", func(t *testing.T) {
		cmd := exec.Command(binPath, "-p", "49002", "-ws-port", "0", "--show-key", "-store", filepath.Join(tempDir, "store2"), "-ingest", testFilePath)
		outputStr, err := runPeerIngestUntilDone(cmd)
		if err != nil {
			t.Fatalf("Failed running peer command: %v", err)
		}

		matches := hexKeyRegex.FindStringSubmatch(outputStr)
		if len(matches) < 2 {
			t.Fatalf("Expected output to contain raw hexadecimal key, got:\n%s", outputStr)
		}
		if len(matches[1]) != 64 {
			t.Errorf("Expected 64-character hex key, got len=%d: %s", len(matches[1]), matches[1])
		}
		expectedHelperStr := `-key "` + matches[1] + `"`
		if !strings.Contains(outputStr, expectedHelperStr) {
			t.Errorf("Expected copy-paste block to contain '%s', got:\n%s", expectedHelperStr, outputStr)
		}
	})

	// 3. Test --key-out
	t.Run("KeyOutFlag", func(t *testing.T) {
		keyOutFile := filepath.Join(tempDir, "peer_exported.key")
		cmd := exec.Command(binPath, "-p", "49003", "-ws-port", "0", "--key-out", keyOutFile, "-store", filepath.Join(tempDir, "store3"), "-ingest", testFilePath)
		outputStr, err := runPeerIngestUntilDone(cmd)
		if err != nil {
			t.Fatalf("Failed running peer command: %v", err)
		}

		if !strings.Contains(outputStr, "Key: [REDACTED]") {
			t.Errorf("Expected default output to redact key even with --key-out, got:\n%s", outputStr)
		}

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
