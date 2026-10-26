package transfer_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/protocol"
	"cipher/internal/transfer"
)

func setupMockNetwork(t testing.TB) (host.Host, host.Host) {
	mocknet := mocknet.New()

	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestTransfer_SinglePassAndVerification(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	tempDir := t.TempDir()
	testFileName := "test_file.dat"
	testFilePath := filepath.Join(tempDir, testFileName)

	// Create a test file with 512 KiB random payload
	fileSize := 512 * 1024
	fileData := make([]byte, fileSize)
	if _, err := rand.Read(fileData); err != nil {
		t.Fatalf("Failed to generate random test data: %v", err)
	}
	if err := os.WriteFile(testFilePath, fileData, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	expectedChecksum := sha256.Sum256(fileData)

	// Setup receiver handler on h2
	receiveErrCh := make(chan error, 1)
	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		err := transfer.Receive(s)
		receiveErrCh <- err
	})

	// Open stream from h1 to h2
	stream, err := h1.NewStream(t.Context(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Change working dir so downloads directory goes into tempDir
	origWd, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(origWd)

	// Perform Send
	if err := transfer.Send(stream, testFilePath); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	// Wait for Receive to complete
	if err := <-receiveErrCh; err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	// Verify received file in downloads/
	receivedPath := filepath.Join(tempDir, "downloads", testFileName)
	receivedData, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("Failed to read received file: %v", err)
	}

	if len(receivedData) != len(fileData) {
		t.Errorf("Received data size mismatch: expected %d, got %d", len(fileData), len(receivedData))
	}

	if !bytes.Equal(receivedData, fileData) {
		t.Errorf("Received data content mismatch")
	}

	receivedChecksum := sha256.Sum256(receivedData)
	if receivedChecksum != expectedChecksum {
		t.Errorf("Checksum mismatch: expected %x, got %x", expectedChecksum, receivedChecksum)
	}
}

func TestTransfer_DiskReadVolume(t *testing.T) {
	tempDir := t.TempDir()
	testFilePath := filepath.Join(tempDir, "sample.bin")

	fileSize := 256 * 1024
	fileData := make([]byte, fileSize)
	rand.Read(fileData)
	os.WriteFile(testFilePath, fileData, 0644)

	h1, h2 := setupMockNetwork(t)

	receiveErrCh := make(chan error, 1)
	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		receiveErrCh <- transfer.Receive(s)
	})

	stream, err := h1.NewStream(t.Context(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	origWd, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(origWd)

	if err := transfer.Send(stream, testFilePath); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if err := <-receiveErrCh; err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	// Confirm received file is identical
	recData, err := os.ReadFile(filepath.Join(tempDir, "downloads", "sample.bin"))
	if err != nil {
		t.Fatalf("Failed to read downloaded file: %v", err)
	}
	if !bytes.Equal(recData, fileData) {
		t.Fatalf("Downloaded data does not match original")
	}
}
