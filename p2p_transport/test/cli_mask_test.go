package test_test

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

func TestCLIMasking(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cli_mask_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Build publisher binary
	publisherBin := filepath.Join(tmpDir, "publisher")
	cmdBuildPub := exec.Command("go", "build", "-o", publisherBin, "../cmd/publisher")
	cmdBuildPub.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmdBuildPub.CombinedOutput(); err != nil {
		t.Fatalf("failed to build publisher: %v\nOutput: %s", err, string(out))
	}

	// Build peer binary
	peerBin := filepath.Join(tmpDir, "peer")
	cmdBuildPeer := exec.Command("go", "build", "-o", peerBin, "../cmd/peer")
	cmdBuildPeer.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmdBuildPeer.CombinedOutput(); err != nil {
		t.Fatalf("failed to build peer: %v\nOutput: %s", err, string(out))
	}

	// Create a dummy file to ingest
	inputFile := filepath.Join(tmpDir, "input.txt")
	if err := os.WriteFile(inputFile, []byte("hello world cipher test payload"), 0644); err != nil {
		t.Fatalf("failed to write input file: %v", err)
	}

	hexKeyRegex := regexp.MustCompile(`[0-9a-fA-F]{64}`)

	t.Run("PublisherDefaultRedacted", func(t *testing.T) {
		storeDir := filepath.Join(tmpDir, "pub_store_default")
		cmd := exec.Command(publisherBin, "-seed=false", "-file", inputFile, "-store", storeDir, "-p", "49010", "-ws-port", "0")
		var outBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &outBuf

		if err := cmd.Run(); err != nil {
			t.Fatalf("publisher command failed: %v\nOutput: %s", err, outBuf.String())
		}

		output := outBuf.String()
		if !strings.Contains(output, "Decryption Key: [REDACTED]") {
			t.Errorf("expected output to contain 'Decryption Key: [REDACTED]', got:\n%s", output)
		}

		// Extract line for Decryption Key and ensure no hex key is exposed
		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, "Decryption Key:") {
				if hexKeyRegex.MatchString(line) {
					t.Errorf("unmasked key found in default publisher output line: %s", line)
				}
			}
		}
	})

	t.Run("PublisherShowKeyExplicit", func(t *testing.T) {
		storeDir := filepath.Join(tmpDir, "pub_store_showkey")
		cmd := exec.Command(publisherBin, "--show-key", "-seed=false", "-file", inputFile, "-store", storeDir, "-p", "49011", "-ws-port", "0")
		var outBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &outBuf

		if err := cmd.Run(); err != nil {
			t.Fatalf("publisher command with --show-key failed: %v\nOutput: %s", err, outBuf.String())
		}

		output := outBuf.String()
		if strings.Contains(output, "Decryption Key: [REDACTED]") {
			t.Errorf("output should NOT contain '[REDACTED]' when --show-key is set, got:\n%s", output)
		}

		foundRawKey := false
		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, "Decryption Key:") {
				if hexKeyRegex.MatchString(line) {
					foundRawKey = true
				}
			}
		}

		if !foundRawKey {
			t.Errorf("expected raw hex key in output when --show-key is enabled, got:\n%s", output)
		}
	})

	t.Run("PeerDefaultRedacted", func(t *testing.T) {
		storeDir := filepath.Join(tmpDir, "peer_store_default")
		cmd := exec.Command(peerBin, "-ingest", inputFile, "-store", storeDir, "-p", "49020", "-ws-port", "0")
		var outBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &outBuf

		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start peer: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
		}()

		var output string
		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)
			output = outBuf.String()
			if strings.Contains(output, "Ingest complete!") {
				break
			}
		}

		cmd.Process.Signal(os.Interrupt)
		time.Sleep(100 * time.Millisecond)
		_ = cmd.Process.Kill()
		_ = <-done

		output = outBuf.String()
		if !strings.Contains(output, "Key: [REDACTED]") {
			t.Errorf("expected peer output to contain 'Key: [REDACTED]', got:\n%s", output)
		}

		if !strings.Contains(output, `-key "[REDACTED]"`) {
			t.Errorf("expected peer output to contain '-key \"[REDACTED]\"', got:\n%s", output)
		}
	})

	t.Run("PeerShowKeyExplicit", func(t *testing.T) {
		storeDir := filepath.Join(tmpDir, "peer_store_showkey")
		cmd := exec.Command(peerBin, "--show-key", "-ingest", inputFile, "-store", storeDir, "-p", "49021", "-ws-port", "0")
		var outBuf bytes.Buffer
		cmd.Stdout = &outBuf
		cmd.Stderr = &outBuf

		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start peer: %v", err)
		}

		done := make(chan error, 1)
		go func() {
			done <- cmd.Wait()
		}()

		var output string
		for i := 0; i < 50; i++ {
			time.Sleep(100 * time.Millisecond)
			output = outBuf.String()
			if strings.Contains(output, "Ingest complete!") {
				break
			}
		}

		cmd.Process.Signal(os.Interrupt)
		time.Sleep(100 * time.Millisecond)
		_ = cmd.Process.Kill()
		_ = <-done

		output = outBuf.String()
		if strings.Contains(output, "Key: [REDACTED]") {
			t.Errorf("peer output should NOT contain 'Key: [REDACTED]' when --show-key is set, got:\n%s", output)
		}

		foundRawKey := false
		for _, line := range strings.Split(output, "\n") {
			if strings.Contains(line, "Key:") && hexKeyRegex.MatchString(line) {
				foundRawKey = true
			}
		}

		if !foundRawKey {
			t.Errorf("expected raw hex key in peer output when --show-key is set, got:\n%s", output)
		}
	})
}
