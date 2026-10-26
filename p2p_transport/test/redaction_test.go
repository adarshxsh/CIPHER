package test_test

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCLIKewRedactionAndExport(t *testing.T) {
	// Build publisher and client binaries
	tmpDir := t.TempDir()
	pubBin := filepath.Join(tmpDir, "publisher")
	clientBin := filepath.Join(tmpDir, "client")

	cmdBuildPub := exec.Command("go", "build", "-o", pubBin, "../cmd/publisher")
	if out, err := cmdBuildPub.CombinedOutput(); err != nil {
		t.Fatalf("failed to build publisher: %v, output: %s", err, string(out))
	}

	cmdBuildClient := exec.Command("go", "build", "-o", clientBin, "../cmd/client")
	if out, err := cmdBuildClient.CombinedOutput(); err != nil {
		t.Fatalf("failed to build client: %v, output: %s", err, string(out))
	}

	// Create test file
	testFile := filepath.Join(tmpDir, "test.dat")
	testData := []byte("hello world key redaction test payload 1234567890")
	if err := os.WriteFile(testFile, testData, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	pubStore := filepath.Join(tmpDir, "store_pub")
	pubIdentity := filepath.Join(tmpDir, "pub_identity.key")
	keyOut := filepath.Join(tmpDir, "exported.key")

	// Start publisher in background and capture combined stdout/stderr
	cmdPub := exec.Command(pubBin, "-p", "49100", "-ws-port", "0", "-identity", pubIdentity, "-file", testFile, "-store", pubStore, "-key-out", keyOut, "-seed=true")
	
	pubStdout, err := cmdPub.StdoutPipe()
	if err != nil {
		t.Fatalf("failed to get stdout pipe: %v", err)
	}
	pubStderr, err := cmdPub.StderrPipe()
	if err != nil {
		t.Fatalf("failed to get stderr pipe: %v", err)
	}

	pubOutBuf := &bytes.Buffer{}
	combinedReader := io.MultiReader(pubStdout, pubStderr)

	if err := cmdPub.Start(); err != nil {
		t.Fatalf("failed to start publisher: %v", err)
	}
	defer func() {
		if cmdPub.Process != nil {
			cmdPub.Process.Kill()
		}
	}()

	var contentID string
	var pubAddr string

	scanner := bufio.NewScanner(combinedReader)
	for scanner.Scan() {
		line := scanner.Text()
		pubOutBuf.WriteString(line + "\n")

		if strings.HasPrefix(line, "ContentID") {
			parts := strings.Fields(line)
			if len(parts) >= 3 {
				contentID = parts[len(parts)-1]
			}
		}
		if strings.Contains(line, "/ip4/127.0.0.1/tcp/49100/p2p/") {
			parts := strings.Fields(line)
			pubAddr = parts[len(parts)-1]
		}
		if strings.Contains(line, "===================================================") {
			break
		}
	}

	if contentID == "" || pubAddr == "" {
		t.Fatalf("failed to parse ContentID or address from publisher output:\n%s", pubOutBuf.String())
	}

	pubOut := pubOutBuf.String()

	// 1. Verify log output contains [REDACTED]
	if !strings.Contains(pubOut, "Decryption Key: [REDACTED]") {
		t.Errorf("expected 'Decryption Key: [REDACTED]' in publisher output, got:\n%s", pubOut)
	}

	// 2. Check exported key file existence and permissions (0600)
	keyInfo, err := os.Stat(keyOut)
	if err != nil {
		t.Fatalf("exported key file does not exist at %s: %v", keyOut, err)
	}
	perm := keyInfo.Mode().Perm()
	if perm != 0600 {
		t.Errorf("expected exported key file permission 0600, got %o", perm)
	}

	rawKeyBytes, err := os.ReadFile(keyOut)
	if err != nil {
		t.Fatalf("failed to read exported key file: %v", err)
	}
	if len(rawKeyBytes) != 32 {
		t.Fatalf("expected 32 raw bytes in key file, got %d bytes", len(rawKeyBytes))
	}

	rawKeyHex := hex.EncodeToString(rawKeyBytes)
	// 3. Verify raw key hex string does NOT appear anywhere in the log output
	if strings.Contains(pubOut, rawKeyHex) {
		t.Errorf("SECURITY RISK: Raw decryption key hex string %s found in publisher output logs!", rawKeyHex)
	}

	// 4. Run client with -key-file to reassemble
	clientStore := filepath.Join(tmpDir, "store_client")
	clientIdentity := filepath.Join(tmpDir, "client_identity.key")
	outFile := filepath.Join(tmpDir, "reassembled.dat")

	time.Sleep(500 * time.Millisecond)

	cmdClient := exec.Command(clientBin, "-p", "49101", "-ws-port", "0", "-identity", clientIdentity, "-store", clientStore, "-d", pubAddr, "-fetch", contentID, "-key-file", keyOut, "-out", outFile)
	clientOutBytes, err := cmdClient.CombinedOutput()
	if err != nil {
		t.Fatalf("client command failed: %v, output:\n%s", err, string(clientOutBytes))
	}

	reassembledData, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatalf("failed to read reassembled output file: %v", err)
	}

	if !bytes.Equal(testData, reassembledData) {
		t.Fatalf("reassembled data mismatch!\nExpected: %s\nGot:      %s", string(testData), string(reassembledData))
	}
}
