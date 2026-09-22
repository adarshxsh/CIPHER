package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestPeerKeyMasking(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "peer-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "sample.dat")
	if err := os.WriteFile(testFile, []byte("hello cipher peer test payload"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	binPath := filepath.Join(tmpDir, "peer_bin")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	buildCmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build peer binary: %v\nOutput: %s", err, string(out))
	}

	t.Run("Default Redacted Ingest Behavior", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, binPath, "-ingest", testFile, "-p", "57001", "-ws-port", "0", "-store", filepath.Join(tmpDir, "store1"))
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out

		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start peer command: %v", err)
		}

		// Wait until logs are produced
		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)
			if strings.Contains(out.String(), "Ingest complete") {
				break
			}
		}

		_ = cmd.Process.Kill()
		_ = cmd.Wait()

		output := out.String()
		if !strings.Contains(output, "Key: [REDACTED]") {
			t.Errorf("expected log to contain 'Key: [REDACTED]', got:\n%s", output)
		}

		if !strings.Contains(output, `-key "[REDACTED]"`) {
			t.Errorf("expected usage snippet to contain '-key \"[REDACTED]\"', got:\n%s", output)
		}

		re := regexp.MustCompile(`Key:\s+([a-fA-F0-9]{64})`)
		if re.MatchString(output) {
			t.Errorf("unmasked raw hex decryption key found in redacted peer log:\n%s", output)
		}
	})

	t.Run("Unmasked --show-key Ingest Behavior", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, binPath, "--show-key", "-ingest", testFile, "-p", "57002", "-ws-port", "0", "-store", filepath.Join(tmpDir, "store2"))
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out

		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start peer command: %v", err)
		}

		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)
			if strings.Contains(out.String(), "Ingest complete") {
				break
			}
		}

		_ = cmd.Process.Kill()
		_ = cmd.Wait()

		output := out.String()
		re := regexp.MustCompile(`Key:\s+([a-fA-F0-9]{64})`)
		if !re.MatchString(output) {
			t.Errorf("expected raw 32-byte hex key in peer output when --show-key is set, got:\n%s", output)
		}
	})
}
