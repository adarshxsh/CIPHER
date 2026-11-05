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

func TestPublisherKeyMasking(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "publisher-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	testFile := filepath.Join(tmpDir, "sample.dat")
	if err := os.WriteFile(testFile, []byte("hello cipher publisher test payload"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	binPath := filepath.Join(tmpDir, "publisher_bin")
	buildCmd := exec.Command("go", "build", "-o", binPath, ".")
	buildCmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to build publisher binary: %v\nOutput: %s", err, string(out))
	}

	t.Run("Default Redacted Behavior", func(t *testing.T) {
		cmd := exec.Command(binPath, "-file", testFile, "-seed=false", "-store", filepath.Join(tmpDir, "store1"))
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			t.Fatalf("publisher command failed: %v\nOutput: %s", err, out.String())
		}

		output := out.String()
		if !strings.Contains(output, "Decryption Key: [REDACTED]") {
			t.Errorf("expected output to contain 'Decryption Key: [REDACTED]', got:\n%s", output)
		}

		re := regexp.MustCompile(`Decryption Key:\s+([a-fA-F0-9]{64})`)
		if re.MatchString(output) {
			t.Errorf("unmasked raw hex decryption key found in redacted output:\n%s", output)
		}
	})

	t.Run("Unmasked --show-key Behavior", func(t *testing.T) {
		cmd := exec.Command(binPath, "-file", testFile, "-seed=false", "--show-key", "-store", filepath.Join(tmpDir, "store2"))
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			t.Fatalf("publisher command failed: %v\nOutput: %s", err, out.String())
		}

		output := out.String()
		re := regexp.MustCompile(`Decryption Key:\s+([a-fA-F0-9]{64})`)
		match := re.FindStringSubmatch(output)
		if len(match) < 2 {
			t.Errorf("expected raw 32-byte hex decryption key in output when --show-key is set, got:\n%s", output)
		}
	})
}
