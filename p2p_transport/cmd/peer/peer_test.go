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

func TestPeerKeyRedaction(t *testing.T) {
	tempDir := t.TempDir()
	binaryPath := filepath.Join(tempDir, "peer")
	buildCmd := exec.Command("go", "build", "-o", binaryPath, ".")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build peer binary: %v\nOutput: %s", err, string(out))
	}

	testFile := filepath.Join(tempDir, "ingest_input.dat")
	if err := os.WriteFile(testFile, []byte("test content for peer redaction test"), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	t.Run("Default Redaction", func(t *testing.T) {
		storeDir := filepath.Join(tempDir, "peer_store_default")
		// Launch peer -ingest with a short timeout / signal handling if needed, but ingest exits after printing unless fetch/resume is passed. Wait, let's verify if peer exits or waits.
		// Wait! In main.go, at the end of main():
		// ch := make(chan os.Signal, 1)
		// signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		// <-ch
		// Peer waits for SIGINT/SIGTERM unless killed or terminated.
		// So we start the process, wait for the ingest output in stdout/stderr, then interrupt/kill it.
		cmd := exec.Command(binaryPath,
			"-ingest", testFile,
			"-store", storeDir,
			"-p", "59201",
			"-ws-port", "0",
		)
		var outBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &outBuf

		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start peer: %v", err)
		}

		// Wait for output to contain the complete message or timeout
		done := make(chan error, 1)
		go func() {
			for {
				output := outBuf.String()
				if strings.Contains(output, "To download this file on another peer") {
					break
				}
				if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
					break
				}
			}
			done <- nil
		}()

		<-done
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()

		output := outBuf.String()
		if !strings.Contains(output, "Key: [REDACTED]") {
			t.Errorf("Expected output to contain 'Key: [REDACTED]', got:\n%s", output)
		}

		if !strings.Contains(output, `-key "<decryption-key-hex>"`) {
			t.Errorf("Expected output to contain '-key \"<decryption-key-hex>\"', got:\n%s", output)
		}

		// Ensure no 64-character hex key appears after "Key: "
		re := regexp.MustCompile(`Key:\s+([a-fA-F0-9]{64})`)
		if matches := re.FindStringSubmatch(output); len(matches) > 0 {
			t.Errorf("Found raw hex key in default peer output: %s", matches[1])
		}
	})

	t.Run("Opt-In --show-key", func(t *testing.T) {
		storeDir := filepath.Join(tempDir, "peer_store_showkey")
		cmd := exec.Command(binaryPath,
			"-ingest", testFile,
			"-store", storeDir,
			"-p", "59202",
			"-ws-port", "0",
			"--show-key",
		)
		var outBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &outBuf

		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start peer: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			for {
				output := outBuf.String()
				if strings.Contains(output, "To download this file on another peer") {
					break
				}
				if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
					break
				}
			}
			done <- nil
		}()

		<-done
		_ = cmd.Process.Signal(os.Interrupt)
		_ = cmd.Wait()

		output := outBuf.String()
		if strings.Contains(output, "Key: [REDACTED]") {
			t.Errorf("Output should not contain 'Key: [REDACTED]' when --show-key is passed, got:\n%s", output)
		}

		re := regexp.MustCompile(`Key:\s+([a-fA-F0-9]{64})`)
		if matches := re.FindStringSubmatch(output); len(matches) < 2 {
			t.Errorf("Expected 64-character hex key in output, got:\n%s", output)
		}

		reCmd := regexp.MustCompile(`-key "([a-fA-F0-9]{64})"`)
		if matches := reCmd.FindStringSubmatch(output); len(matches) < 2 {
			t.Errorf("Expected 64-character hex key in command template, got:\n%s", output)
		}
	})
}
