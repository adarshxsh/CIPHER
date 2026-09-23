package test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

func TestCLIRedactionPublisher(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cipher-pub-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Build publisher binary
	pubBin := filepath.Join(tmpDir, "publisher")
	buildCmd := exec.Command("go", "build", "-o", pubBin, "../cmd/publisher")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build publisher binary: %v\nOutput: %s", err, string(out))
	}

	// Create test file
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("hello world publisher test payload"), 0600); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Test 1: Default execution (redacted)
	store1 := filepath.Join(tmpDir, "store1")
	cmdDefault := exec.Command(pubBin, "-file", testFile, "-seed=false", "-store", store1, "-ws-port", "0")
	var outBufDefault bytes.Buffer
	cmdDefault.Stdout = &outBufDefault
	cmdDefault.Stderr = &outBufDefault
	if err := cmdDefault.Run(); err != nil {
		t.Fatalf("Default publisher execution failed: %v\nOutput: %s", err, outBufDefault.String())
	}
	outStrDefault := outBufDefault.String()

	if !strings.Contains(outStrDefault, "Decryption Key: [REDACTED]") {
		t.Errorf("Expected default output to contain 'Decryption Key: [REDACTED]', got:\n%s", outStrDefault)
	}
	if !strings.Contains(outStrDefault, "Key Fingerprint:") {
		t.Errorf("Expected default output to contain 'Key Fingerprint:', got:\n%s", outStrDefault)
	}

	// Verify no 64-character raw hex key is printed after Decryption Key
	rawKeyRegex := regexp.MustCompile(`Decryption Key:\s+([a-f0-9]{64})`)
	if rawKeyRegex.MatchString(outStrDefault) {
		t.Errorf("Default output contains raw 64-char hex key!\nOutput: %s", outStrDefault)
	}

	// Test 2: Opt-in reveal flag (--show-decryption-key)
	store2 := filepath.Join(tmpDir, "store2")
	cmdReveal := exec.Command(pubBin, "-file", testFile, "-seed=false", "-store", store2, "-ws-port", "0", "--show-decryption-key")
	var outBufReveal bytes.Buffer
	cmdReveal.Stdout = &outBufReveal
	cmdReveal.Stderr = &outBufReveal
	if err := cmdReveal.Run(); err != nil {
		t.Fatalf("Revealed publisher execution failed: %v\nOutput: %s", err, outBufReveal.String())
	}
	outStrReveal := outBufReveal.String()

	if strings.Contains(outStrReveal, "Decryption Key: [REDACTED]") {
		t.Errorf("Expected revealed output NOT to contain 'Decryption Key: [REDACTED]', got:\n%s", outStrReveal)
	}
	match := rawKeyRegex.FindStringSubmatch(outStrReveal)
	if len(match) < 2 {
		t.Errorf("Expected revealed output to contain 64-char raw hex key after 'Decryption Key:', got:\n%s", outStrReveal)
	}
}

func TestCLIRedactionPeer(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cipher-peer-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Build peer binary
	peerBin := filepath.Join(tmpDir, "peer")
	buildCmd := exec.Command("go", "build", "-o", peerBin, "../cmd/peer")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("Failed to build peer binary: %v\nOutput: %s", err, string(out))
	}

	// Create test file
	testFile := filepath.Join(tmpDir, "test.dat")
	if err := os.WriteFile(testFile, []byte("hello world peer test payload"), 0600); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	runPeerIngest := func(extraArgs ...string) string {
		store := filepath.Join(tmpDir, "store_"+filepath.Base(t.Name()))
		args := append([]string{"-ingest", testFile, "-store", store, "-p", "0", "-ws-port", "0"}, extraArgs...)
		cmd := exec.Command(peerBin, args...)
		var outBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &outBuf

		if err := cmd.Start(); err != nil {
			t.Fatalf("Failed to start peer: %v", err)
		}

		// Wait for output to contain completion indicator or timeout
		done := make(chan error)
		go func() {
			for i := 0; i < 50; i++ {
				if strings.Contains(outBuf.String(), "To download this file") {
					cmd.Process.Kill()
					done <- nil
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
			cmd.Process.Kill()
			done <- nil
		}()
		<-done
		cmd.Wait()
		return outBuf.String()
	}

	// Test 1: Default execution (redacted)
	outStrDefault := runPeerIngest()

	if !strings.Contains(outStrDefault, "Key: [REDACTED]") {
		t.Errorf("Expected default output to contain 'Key: [REDACTED]', got:\n%s", outStrDefault)
	}
	if !strings.Contains(outStrDefault, "Key Fingerprint:") {
		t.Errorf("Expected default output to contain 'Key Fingerprint:', got:\n%s", outStrDefault)
	}
	if !strings.Contains(outStrDefault, `-key "[REDACTED]"`) {
		t.Errorf("Expected default command hint to contain '-key \"[REDACTED]\"', got:\n%s", outStrDefault)
	}

	// Verify no 64-character raw hex key is printed after Key:
	rawKeyRegex := regexp.MustCompile(`Key:\s+([a-f0-9]{64})`)
	if rawKeyRegex.MatchString(outStrDefault) {
		t.Errorf("Default peer output contains raw 64-char hex key!\nOutput: %s", outStrDefault)
	}

	// Test 2: Opt-in reveal flag (--show-decryption-key)
	outStrReveal := runPeerIngest("--show-decryption-key")

	if strings.Contains(outStrReveal, "Key: [REDACTED]") {
		t.Errorf("Expected revealed output NOT to contain 'Key: [REDACTED]', got:\n%s", outStrReveal)
	}
	match := rawKeyRegex.FindStringSubmatch(outStrReveal)
	if len(match) < 2 {
		t.Errorf("Expected revealed peer output to contain 64-char raw hex key, got:\n%s", outStrReveal)
	}
}
