package test

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

var (
	pubBin  string
	peerBin string
)

func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "cli-test-bin-*")
	if err != nil {
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	pubBin = filepath.Join(tmpDir, "publisher")
	peerBin = filepath.Join(tmpDir, "peer")

	cmdPub := exec.Command("go", "build", "-o", pubBin, "../cmd/publisher")
	if err := cmdPub.Run(); err != nil {
		os.Exit(1)
	}

	cmdPeer := exec.Command("go", "build", "-o", peerBin, "../cmd/peer")
	if err := cmdPeer.Run(); err != nil {
		os.Exit(1)
	}

	os.Exit(m.Run())
}

func TestPublisherKeyRedaction(t *testing.T) {
	tmpDir := t.TempDir()
	sampleFile := filepath.Join(tmpDir, "sample.txt")
	if err := os.WriteFile(sampleFile, []byte("hello world publisher test"), 0644); err != nil {
		t.Fatalf("failed to create sample file: %v", err)
	}

	hexKeyRegex := regexp.MustCompile(`Decryption Key:\s+([a-fA-F0-9]{64})`)

	t.Run("Default Redaction", func(t *testing.T) {
		storeDir := filepath.Join(tmpDir, "store_default")
		cmd := exec.Command(pubBin, "-seed=false", "-file", sampleFile, "-store", storeDir, "-p", "49201", "-ws-port", "0")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			t.Fatalf("publisher command failed: %v\nStderr: %s", err, stderr.String())
		}

		out := stdout.String()
		if !strings.Contains(out, "Decryption Key: [REDACTED]") {
			t.Errorf("Expected 'Decryption Key: [REDACTED]' in default output, got:\n%s", out)
		}
		if hexKeyRegex.MatchString(out) {
			t.Errorf("Unmasked 64-char hex key found in default publisher output:\n%s", out)
		}
	})

	t.Run("Show Key Explicit Flag", func(t *testing.T) {
		storeDir := filepath.Join(tmpDir, "store_showkey")
		cmd := exec.Command(pubBin, "-show-key", "-seed=false", "-file", sampleFile, "-store", storeDir, "-p", "49202", "-ws-port", "0")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			t.Fatalf("publisher command failed: %v\nStderr: %s", err, stderr.String())
		}

		out := stdout.String()
		if strings.Contains(out, "Decryption Key: [REDACTED]") {
			t.Errorf("Did not expect '[REDACTED]' when -show-key is supplied, got:\n%s", out)
		}
		if !hexKeyRegex.MatchString(out) {
			t.Errorf("Expected 64-char hex key in output when -show-key is supplied, got:\n%s", out)
		}
	})

	t.Run("Show Key Equals True Flag", func(t *testing.T) {
		storeDir := filepath.Join(tmpDir, "store_showkey_true")
		cmd := exec.Command(pubBin, "-show-key=true", "-seed=false", "-file", sampleFile, "-store", storeDir, "-p", "49203", "-ws-port", "0")
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			t.Fatalf("publisher command failed: %v\nStderr: %s", err, stderr.String())
		}

		out := stdout.String()
		if strings.Contains(out, "Decryption Key: [REDACTED]") {
			t.Errorf("Did not expect '[REDACTED]' when -show-key=true is supplied, got:\n%s", out)
		}
		if !hexKeyRegex.MatchString(out) {
			t.Errorf("Expected 64-char hex key in output when -show-key=true is supplied, got:\n%s", out)
		}
	})
}

func TestPeerKeyRedaction(t *testing.T) {
	tmpDir := t.TempDir()
	sampleFile := filepath.Join(tmpDir, "peer_sample.txt")
	if err := os.WriteFile(sampleFile, []byte("hello world peer test"), 0644); err != nil {
		t.Fatalf("failed to create sample file: %v", err)
	}

	hexKeyRegex := regexp.MustCompile(`Key:\s+([a-fA-F0-9]{64})`)
	hexHelperKeyRegex := regexp.MustCompile(`-key\s+"([a-fA-F0-9]{64})"`)

	runPeerIngest := func(testName string, extraArgs ...string) (string, error) {
		storeDir := filepath.Join(tmpDir, "peer_store_"+testName)
		args := append([]string{"-ingest", sampleFile, "-store", storeDir, "-p", "49301", "-ws-port", "0"}, extraArgs...)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, peerBin, args...)
		var combined bytes.Buffer
		cmd.Stdout = &combined
		cmd.Stderr = &combined

		_ = cmd.Start()

		// Poll for expected output before killing process
		for i := 0; i < 30; i++ {
			time.Sleep(100 * time.Millisecond)
			if strings.Contains(combined.String(), "To download this file on another peer") {
				break
			}
		}

		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		_ = cmd.Wait()

		return combined.String(), nil
	}

	t.Run("Default Redaction", func(t *testing.T) {
		out, err := runPeerIngest("default")
		if err != nil {
			t.Fatalf("runPeerIngest failed: %v", err)
		}

		if !strings.Contains(out, "Key: [REDACTED]") {
			t.Errorf("Expected 'Key: [REDACTED]' in default peer output, got:\n%s", out)
		}
		if !strings.Contains(out, `-key "[REDACTED]"`) {
			t.Errorf("Expected `-key \"[REDACTED]\"` in default peer helper text, got:\n%s", out)
		}
		if hexKeyRegex.MatchString(out) || hexHelperKeyRegex.MatchString(out) {
			t.Errorf("Unmasked 64-char hex key found in default peer output:\n%s", out)
		}
	})

	t.Run("Show Key Explicit Flag", func(t *testing.T) {
		out, err := runPeerIngest("showkey", "-show-key")
		if err != nil {
			t.Fatalf("runPeerIngest failed: %v", err)
		}

		if strings.Contains(out, "Key: [REDACTED]") {
			t.Errorf("Did not expect '[REDACTED]' when -show-key is supplied, got:\n%s", out)
		}
		if !hexKeyRegex.MatchString(out) {
			t.Errorf("Expected 64-char hex key in peer log output when -show-key is supplied, got:\n%s", out)
		}
		if !hexHelperKeyRegex.MatchString(out) {
			t.Errorf("Expected 64-char hex key in peer helper text when -show-key is supplied, got:\n%s", out)
		}
	})
}
