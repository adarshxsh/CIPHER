package transfer

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/protocol"
)

func TestReceive_Success(t *testing.T) {
	defer os.RemoveAll("downloads")

	net := mocknet.New()
	h1, err := net.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer h1: %v", err)
	}
	h2, err := net.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer h2: %v", err)
	}
	if err := net.LinkAll(); err != nil {
		t.Fatalf("failed to link peers: %v", err)
	}

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "recv_test.txt")
	testData := []byte("Receive integration test file content.")
	if err := os.WriteFile(filePath, testData, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	recvErrCh := make(chan error, 1)
	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		recvErrCh <- Receive(s)
	})

	s, err := h1.NewStream(context.Background(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	if err := Send(s, filePath); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if err := <-recvErrCh; err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	downloadedPath := filepath.Join("downloads", "recv_test.txt")
	gotData, err := os.ReadFile(downloadedPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if string(gotData) != string(testData) {
		t.Errorf("content mismatch: expected %q, got %q", testData, gotData)
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	defer os.RemoveAll("downloads")

	net := mocknet.New()
	h1, err := net.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer h1: %v", err)
	}
	h2, err := net.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer h2: %v", err)
	}
	if err := net.LinkAll(); err != nil {
		t.Fatalf("failed to link peers: %v", err)
	}

	recvErrCh := make(chan error, 1)
	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		recvErrCh <- Receive(s)
	})

	s, err := h1.NewStream(context.Background(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	filename := "corrupt.txt"
	payload := []byte("Some payload data for corrupt transfer test.")
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: [32]byte{},
	}

	if err := header.WriteTo(s); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	if _, err := s.Write(payload); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}

	// Write wrong checksum trailer
	badChecksum := [32]byte{1, 2, 3, 4, 5}
	if _, err := s.Write(badChecksum[:]); err != nil {
		t.Fatalf("failed to write bad trailer checksum: %v", err)
	}
	s.Close()

	err = <-recvErrCh
	if err == nil {
		t.Fatalf("expected checksum mismatch error, got nil")
	}
}

func TestReceive_TruncatedTrailer(t *testing.T) {
	defer os.RemoveAll("downloads")

	net := mocknet.New()
	h1, err := net.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer h1: %v", err)
	}
	h2, err := net.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer h2: %v", err)
	}
	if err := net.LinkAll(); err != nil {
		t.Fatalf("failed to link peers: %v", err)
	}

	recvErrCh := make(chan error, 1)
	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		recvErrCh <- Receive(s)
	})

	s, err := h1.NewStream(context.Background(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	filename := "truncated.txt"
	payload := []byte("Truncated trailer payload test.")
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: [32]byte{},
	}

	if err := header.WriteTo(s); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	if _, err := s.Write(payload); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}

	// Write only 10 bytes of trailer instead of 32 bytes, then close stream
	s.Write([]byte("shorttrailer"))
	s.Close()

	err = <-recvErrCh
	if err == nil {
		t.Fatalf("expected error on truncated trailer, got nil")
	}
}

func TestReceive_ZeroByteFile(t *testing.T) {
	defer os.RemoveAll("downloads")

	net := mocknet.New()
	h1, err := net.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer h1: %v", err)
	}
	h2, err := net.GenPeer()
	if err != nil {
		t.Fatalf("failed to generate peer h2: %v", err)
	}
	if err := net.LinkAll(); err != nil {
		t.Fatalf("failed to link peers: %v", err)
	}

	recvErrCh := make(chan error, 1)
	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		recvErrCh <- Receive(s)
	})

	s, err := h1.NewStream(context.Background(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	filename := "empty.txt"
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: 0,
		Checksum: [32]byte{},
	}

	if err := header.WriteTo(s); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	emptyHash := sha256.Sum256([]byte{})
	if _, err := s.Write(emptyHash[:]); err != nil {
		t.Fatalf("failed to write checksum trailer: %v", err)
	}
	s.Close()

	if err := <-recvErrCh; err != nil {
		t.Fatalf("Receive failed on zero-byte file: %v", err)
	}

	downloadedPath := filepath.Join("downloads", "empty.txt")
	gotData, err := os.ReadFile(downloadedPath)
	if err != nil {
		t.Fatalf("failed to read empty downloaded file: %v", err)
	}

	if len(gotData) != 0 {
		t.Errorf("expected 0 bytes, got %d", len(gotData))
	}
}
