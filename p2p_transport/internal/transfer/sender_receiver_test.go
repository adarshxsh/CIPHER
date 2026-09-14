package transfer_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/transfer"
)

func setupMockStreamPair(t *testing.T) (host.Host, host.Host) {
	t.Helper()
	mn := mocknet.New()

	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to create host 1: %v", err)
	}

	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to create host 2: %v", err)
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatalf("failed to link mock hosts: %v", err)
	}

	return h1, h2
}

func TestSendReceive_SinglePassTrailingChecksum(t *testing.T) {
	h1, h2 := setupMockStreamPair(t)
	defer h1.Close()
	defer h2.Close()

	// Prepare temp test directory and file
	tempDir := t.TempDir()
	sourceFilePath := filepath.Join(tempDir, "test_file.bin")
	fileSize := 512 * 1024 // 512 KB
	fileData := make([]byte, fileSize)
	if _, err := rand.Read(fileData); err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}

	if err := os.WriteFile(sourceFilePath, fileData, 0644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	// Change working directory to tempDir so Receive outputs into downloads/ inside tempDir
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get wd: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("failed to chdir to tempDir: %v", err)
	}
	defer func() {
		_ = os.Chdir(origWd)
	}()

	errCh := make(chan error, 1)

	h2.SetStreamHandler("/cipher/filetransfer/1.0.0", func(s network.Stream) {
		errCh <- transfer.Receive(s)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s, err := h1.NewStream(ctx, h2.ID(), "/cipher/filetransfer/1.0.0")
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	sendErrCh := make(chan error, 1)
	go func() {
		sendErrCh <- transfer.Send(s, sourceFilePath)
	}()

	select {
	case err := <-sendErrCh:
		if err != nil {
			t.Fatalf("Send returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Send timed out")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Receive returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Receive timed out")
	}

	// Verify received file in downloads/test_file.bin
	receivedPath := filepath.Join("downloads", "test_file.bin")
	receivedData, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}

	if !bytes.Equal(receivedData, fileData) {
		t.Fatalf("received data content mismatch")
	}
}

func TestReceive_ChecksumMismatch_DeletesOutput(t *testing.T) {
	h1, h2 := setupMockStreamPair(t)
	defer h1.Close()
	defer h2.Close()

	tempDir := t.TempDir()

	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get wd: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("failed to chdir to tempDir: %v", err)
	}
	defer func() {
		_ = os.Chdir(origWd)
	}()

	receiveErrCh := make(chan error, 1)
	h2.SetStreamHandler("/cipher/filetransfer/1.0.0", func(s network.Stream) {
		receiveErrCh <- transfer.Receive(s)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := h1.NewStream(ctx, h2.ID(), "/cipher/filetransfer/1.0.0")
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	// Write valid header with trailing checksum flag (zeroed checksum)
	header := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: "corrupt.txt",
		FileSize: 10,
		Checksum: [32]byte{},
	}
	if err := header.WriteTo(s); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	// Write 10 bytes payload
	payload := []byte("0123456789")
	if _, err := s.Write(payload); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}

	// Write BAD trailing checksum (all 0xFF bytes)
	badChecksum := [32]byte{}
	for i := range badChecksum {
		badChecksum[i] = 0xFF
	}
	if _, err := s.Write(badChecksum[:]); err != nil {
		t.Fatalf("failed to write trailing checksum: %v", err)
	}
	s.Close()

	select {
	case err := <-receiveErrCh:
		if err == nil {
			t.Fatalf("Expected Receive to fail on checksum mismatch, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Receive timed out")
	}

	// Verify corrupted file was removed
	corruptPath := filepath.Join("downloads", "corrupt.txt")
	if _, err := os.Stat(corruptPath); !os.IsNotExist(err) {
		t.Fatalf("Expected corrupt output file to be deleted, but it exists")
	}
}

func TestReceive_LegacyHeaderChecksum(t *testing.T) {
	h1, h2 := setupMockStreamPair(t)
	defer h1.Close()
	defer h2.Close()

	tempDir := t.TempDir()

	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get wd: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("failed to chdir to tempDir: %v", err)
	}
	defer func() {
		_ = os.Chdir(origWd)
	}()

	receiveErrCh := make(chan error, 1)
	h2.SetStreamHandler("/cipher/filetransfer/1.0.0", func(s network.Stream) {
		receiveErrCh <- transfer.Receive(s)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := h1.NewStream(ctx, h2.ID(), "/cipher/filetransfer/1.0.0")
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	payload := []byte("Legacy header payload test data")
	sum := sha256.Sum256(payload)

	// Header contains valid non-zero checksum
	header := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: "legacy.txt",
		FileSize: uint64(len(payload)),
		Checksum: sum,
	}
	if err := header.WriteTo(s); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	// Write payload only, NO trailing checksum frame
	if _, err := s.Write(payload); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}
	s.Close()

	select {
	case err := <-receiveErrCh:
		if err != nil {
			t.Fatalf("Receive with legacy header failed: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Receive timed out")
	}

	receivedPath := filepath.Join("downloads", "legacy.txt")
	data, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("failed to read received legacy file: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Fatalf("legacy received data mismatch")
	}
}

// Pipe mock for testing single pass streaming startup latency & immediate header write
type countingReader struct {
	readCount int
	data      []byte
	offset    int
}

func (cr *countingReader) Read(p []byte) (int, error) {
	cr.readCount++
	if cr.offset >= len(cr.data) {
		return 0, net.ErrClosed
	}
	n := copy(p, cr.data[cr.offset:])
	cr.offset += n
	return n, nil
}
