package test

import (
	"bytes"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Helper function to build a binary for testing
func buildBinary(t *testing.T, pkgPath, binName string) string {
	t.Helper()
	tempDir := t.TempDir()
	outBin := filepath.Join(tempDir, binName)
	cmd := exec.Command("go", "build", "-o", outBin, pkgPath)
	cmd.Dir = ".."
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to build %s: %v\nOutput: %s", binName, err, string(out))
	}
	return outBin
}

func TestPublisherKeyRedactionAndExport(t *testing.T) {
	pubBin := buildBinary(t, "./cmd/publisher", "publisher")

	tempDir := t.TempDir()
	sampleFile := filepath.Join(tempDir, "sample.txt")
	if err := os.WriteFile(sampleFile, []byte("Hello Publisher Key Masking Test Data"), 0644); err != nil {
		t.Fatalf("Failed to write sample file: %v", err)
	}

	keyOutFile := filepath.Join(tempDir, "exported.key")
	storeDir := filepath.Join(tempDir, "pub_store")

	cmd := exec.Command(pubBin, "-file", sampleFile, "-seed=false", "-store", storeDir, "-key-out", keyOutFile, "-p", "14005", "-ws-port", "0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		t.Fatalf("Publisher failed: %v\nStderr: %s\nStdout: %s", err, stderr.String(), stdout.String())
	}

	combinedOutput := stdout.String() + "\n" + stderr.String()

	// 1. Verify key file exported with 0600 permissions
	info, err := os.Stat(keyOutFile)
	if err != nil {
		t.Fatalf("Exported keyfile stat error: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected keyfile permission 0600, got %o", perm)
	}
	keyBytes, err := os.ReadFile(keyOutFile)
	if err != nil || len(keyBytes) != 32 {
		t.Fatalf("Expected 32-byte raw key in keyfile, got %d bytes (err: %v)", len(keyBytes), err)
	}

	rawHexKey := hex.EncodeToString(keyBytes)

	// 2. Verify raw hex key string is NOT disclosed in logs/output
	if bytes.Contains([]byte(combinedOutput), []byte(rawHexKey)) {
		t.Fatalf("Found raw secret hex key disclosure in publisher output:\n%s", combinedOutput)
	}

	// 3. Verify masked output format (e.g. "... [masked]")
	if !bytes.Contains(stdout.Bytes(), []byte("[masked]")) {
		t.Errorf("Expected [masked] indicator in publisher stdout, got:\n%s", stdout.String())
	}
}

func TestPublisherKeyOutFailureAbortsWithoutLeak(t *testing.T) {
	pubBin := buildBinary(t, "./cmd/publisher", "publisher")

	tempDir := t.TempDir()
	sampleFile := filepath.Join(tempDir, "sample.txt")
	os.WriteFile(sampleFile, []byte("Fail Test Data"), 0644)

	invalidKeyOut := filepath.Join(tempDir, "nonexistent_dir", "key.bin")
	storeDir := filepath.Join(tempDir, "pub_store")

	cmd := exec.Command(pubBin, "-file", sampleFile, "-seed=false", "-store", storeDir, "-key-out", invalidKeyOut, "-p", "14006", "-ws-port", "0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err == nil {
		t.Fatalf("Publisher should have failed on invalid key-out path")
	}

	combined := stdout.String() + "\n" + stderr.String()
	if bytes.Contains([]byte(combined), []byte("[masked]")) || bytes.Contains([]byte(combined), []byte("Decryption Key:")) {
		t.Errorf("Publisher output on error should not print summary with key info:\n%s", combined)
	}
}

func TestPeerKeyRedactionAndExport(t *testing.T) {
	peerBin := buildBinary(t, "./cmd/peer", "peer")

	tempDir := t.TempDir()
	sampleFile := filepath.Join(tempDir, "peer_sample.txt")
	os.WriteFile(sampleFile, []byte("Hello Peer Ingest Key Masking Test Data"), 0644)

	keyOutFile := filepath.Join(tempDir, "peer_exported.key")
	storeDir := filepath.Join(tempDir, "peer_store")

	cmd := exec.Command(peerBin, "-ingest", sampleFile, "-store", storeDir, "-key-out", keyOutFile, "-p", "14001", "-ws-port", "0")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("Failed to start peer: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	for i := 0; i < 50; i++ {
		if _, err := os.Stat(keyOutFile); err == nil {
			break
		}
		exec.Command("sleep", "0.1").Run()
	}

	cmd.Process.Signal(os.Interrupt)
	<-done

	combinedOutput := stdout.String() + "\n" + stderr.String()

	info, err := os.Stat(keyOutFile)
	if err != nil {
		t.Fatalf("Exported keyfile stat error: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected keyfile permission 0600, got %o", perm)
	}

	keyBytes, err := os.ReadFile(keyOutFile)
	if err != nil || len(keyBytes) != 32 {
		t.Fatalf("Expected 32-byte raw key in keyfile, got %d bytes (err: %v)", len(keyBytes), err)
	}

	rawHexKey := hex.EncodeToString(keyBytes)

	// 1. Verify raw secret key string is NOT in peer output
	if bytes.Contains([]byte(combinedOutput), []byte(rawHexKey)) {
		t.Fatalf("Found raw secret hex key disclosure in peer output:\n%s", combinedOutput)
	}

	// 2. Verify masked log output
	if !bytes.Contains([]byte(combinedOutput), []byte("[masked]")) {
		t.Errorf("Expected [masked] indicator in peer log output, got:\n%s", combinedOutput)
	}

	// 3. Verify sample download command uses keyOutFile path or placeholder instead of raw key
	if !bytes.Contains([]byte(combinedOutput), []byte(keyOutFile)) && !bytes.Contains([]byte(combinedOutput), []byte("<keyfile>")) {
		t.Errorf("Sample command should refer to key file or placeholder, got:\n%s", combinedOutput)
	}
}

func TestContentTestKeyPermissions(t *testing.T) {
	ctBin := buildBinary(t, "./cmd/content-test", "content-test")

	tempDir := t.TempDir()
	sampleFile := filepath.Join(tempDir, "ct_sample.txt")
	os.WriteFile(sampleFile, []byte("Content Test Key Export Data"), 0644)

	keyOutFile := filepath.Join(tempDir, "ct_exported.key")
	manifestFile := filepath.Join(tempDir, "manifest.json")

	cmd := exec.Command(ctBin, "-ingest", sampleFile, "-manifest", manifestFile, "-key-out", keyOutFile)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("content-test failed: %v\nStderr: %s\nStdout: %s", err, stderr.String(), stdout.String())
	}

	info, err := os.Stat(keyOutFile)
	if err != nil {
		t.Fatalf("Exported keyfile stat error: %v", err)
	}
	if perm := info.Mode().Perm(); perm != 0600 {
		t.Errorf("Expected keyfile permission 0600, got %o", perm)
	}

	keyBytes, err := os.ReadFile(keyOutFile)
	if err != nil || len(keyBytes) != 32 {
		t.Fatalf("Expected 32-byte raw key in keyfile, got %d bytes (err: %v)", len(keyBytes), err)
	}

	rawHexKey := hex.EncodeToString(keyBytes)
	combined := stdout.String() + "\n" + stderr.String()
	if bytes.Contains([]byte(combined), []byte(rawHexKey)) {
		t.Fatalf("Found raw secret hex key disclosure in content-test output:\n%s", combined)
	}
}
