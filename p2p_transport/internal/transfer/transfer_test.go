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
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/protocol"
	"cipher/internal/transfer"
)

func setupTestNetwork(t *testing.T) (host.Host, host.Host) {
	t.Helper()
	mn := mocknet.New()

	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer 1: %v", err)
	}

	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer 2: %v", err)
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatalf("failed to link peers: %v", err)
	}

	return h1, h2
}

func TestTransfer_HeaderPlaceholderAndTrailer(t *testing.T) {
	h1, h2 := setupTestNetwork(t)

	// Create a test file
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "testdata.bin")
	fileData := make([]byte, 100*1024) // 100 KB
	if _, err := rand.Read(fileData); err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}
	if err := os.WriteFile(filePath, fileData, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	expectedHash := sha256.Sum256(fileData)

	headerReadChan := make(chan transfer.Header, 1)
	payloadReadChan := make(chan []byte, 1)
	trailerReadChan := make(chan [32]byte, 1)

	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		defer s.Close()

		var h transfer.Header
		if err := h.ReadFrom(s); err != nil {
			t.Errorf("failed to read header: %v", err)
			return
		}
		headerReadChan <- h

		payload := make([]byte, h.FileSize)
		if _, err := ioReadFull(s, payload); err != nil {
			t.Errorf("failed to read payload: %v", err)
			return
		}
		payloadReadChan <- payload

		var trailer [32]byte
		if _, err := ioReadFull(s, trailer[:]); err != nil {
			t.Errorf("failed to read trailer: %v", err)
			return
		}
		trailerReadChan <- trailer
	})

	s, err := h1.NewStream(t.Context(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	if err := transfer.Send(s, filePath); err != nil {
		t.Fatalf("transfer.Send failed: %v", err)
	}

	header := <-headerReadChan
	payload := <-payloadReadChan
	trailer := <-trailerReadChan

	// 1. Verify zero-valued checksum placeholder in header
	var zeroChecksum [32]byte
	if !bytes.Equal(header.Checksum[:], zeroChecksum[:]) {
		t.Errorf("expected header checksum to be zero placeholder, got %x", header.Checksum)
	}

	// 2. Verify payload matches original file data
	if !bytes.Equal(payload, fileData) {
		t.Errorf("payload mismatch")
	}

	// 3. Verify 32-byte trailer matches calculated SHA-256
	if !bytes.Equal(trailer[:], expectedHash[:]) {
		t.Errorf("expected trailer %x, got %x", expectedHash, trailer)
	}
}

func TestTransfer_EndToEnd(t *testing.T) {
	h1, h2 := setupTestNetwork(t)

	// Change directory to temporary directory so downloads folder is isolated
	workDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get wd: %v", err)
	}
	if err := os.Chdir(workDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}
	defer func() { _ = os.Chdir(origWd) }()

	filePath := filepath.Join(workDir, "sample.dat")
	fileData := make([]byte, 256*1024) // 256 KB
	if _, err := rand.Read(fileData); err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}
	if err := os.WriteFile(filePath, fileData, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	recvErrChan := make(chan error, 1)
	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		err := transfer.Receive(s)
		recvErrChan <- err
	})

	s, err := h1.NewStream(t.Context(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	if err := transfer.Send(s, filePath); err != nil {
		t.Fatalf("transfer.Send failed: %v", err)
	}

	if err := <-recvErrChan; err != nil {
		t.Fatalf("transfer.Receive failed: %v", err)
	}

	downloadedPath := filepath.Join(workDir, "downloads", "sample.dat")
	downloadedData, err := os.ReadFile(downloadedPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if !bytes.Equal(downloadedData, fileData) {
		t.Fatalf("downloaded file contents do not match source file")
	}
}

func TestTransfer_ChecksumMismatchError(t *testing.T) {
	h1, h2 := setupTestNetwork(t)

	recvErrChan := make(chan error, 1)
	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		err := transfer.Receive(s)
		recvErrChan <- err
	})

	s, err := h1.NewStream(t.Context(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	// Manually write header, payload, and a corrupted trailer
	filename := "corrupt.dat"
	payload := []byte("hello world payload data")
	h := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: [32]byte{},
	}

	if err := h.WriteTo(s); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	if _, err := s.Write(payload); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}

	// Send invalid 32-byte trailer
	corruptTrailer := [32]byte{1, 2, 3, 4}
	if _, err := s.Write(corruptTrailer[:]); err != nil {
		t.Fatalf("failed to write corrupt trailer: %v", err)
	}

	_ = s.Close()

	recvErr := <-recvErrChan
	if recvErr == nil {
		t.Fatalf("expected checksum mismatch error, got nil")
	}
}

func ioReadFull(s network.Stream, buf []byte) (int, error) {
	read := 0
	for read < len(buf) {
		n, err := s.Read(buf[read:])
		read += n
		if err != nil {
			return read, err
		}
	}
	return read, nil
}
