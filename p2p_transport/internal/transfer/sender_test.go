package transfer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/protocol"
)

func TestSend_SinglePassAndNonSeekable(t *testing.T) {
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
	fifoPath := filepath.Join(tmpDir, "test_fifo.txt")
	if err := syscall.Mkfifo(fifoPath, 0600); err != nil {
		t.Fatalf("failed to create fifo: %v", err)
	}

	testData := []byte("Hello, single-pass non-seekable streaming test data!")

	errCh := make(chan error, 1)
	var recBuf bytes.Buffer

	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		defer s.Close()
		_, err := io.Copy(&recBuf, s)
		errCh <- err
	})

	go func() {
		f, err := os.OpenFile(fifoPath, os.O_WRONLY, 0600)
		if err != nil {
			return
		}
		defer f.Close()
		f.Write(testData)
	}()

	s, err := h1.NewStream(context.Background(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	// Send using non-seekable FIFO file
	if err := Send(s, fifoPath); err != nil {
		t.Fatalf("Send failed on non-seekable fifo stream: %v", err)
	}

	if err := <-errCh; err != nil && err != io.EOF {
		t.Fatalf("receiver failed to read stream: %v", err)
	}

	var header Header
	recReader := bytes.NewReader(recBuf.Bytes())
	if err := header.ReadFrom(recReader); err != nil {
		t.Fatalf("failed to parse received header: %v", err)
	}

	if header.Filename != "test_fifo.txt" {
		t.Errorf("expected filename 'test_fifo.txt', got %s", header.Filename)
	}

	payload := make([]byte, len(testData))
	if _, err := io.ReadFull(recReader, payload); err != nil {
		t.Fatalf("failed to read payload: %v", err)
	}
	if !bytes.Equal(payload, testData) {
		t.Errorf("payload mismatch: expected %q, got %q", testData, payload)
	}

	var trailerChecksum [32]byte
	if _, err := io.ReadFull(recReader, trailerChecksum[:]); err != nil {
		t.Fatalf("failed to read trailer checksum: %v", err)
	}

	expectedHash := sha256.Sum256(testData)
	if !bytes.Equal(trailerChecksum[:], expectedHash[:]) {
		t.Errorf("trailer checksum mismatch: expected %x, got %x", expectedHash, trailerChecksum)
	}
}

func TestSend_NormalFile(t *testing.T) {
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
	filePath := filepath.Join(tmpDir, "sample.bin")
	testData := []byte("Normal file single-pass transfer test data payload.")
	if err := os.WriteFile(filePath, testData, 0644); err != nil {
		t.Fatalf("failed to create sample file: %v", err)
	}

	errCh := make(chan error, 1)
	var recBuf bytes.Buffer

	h2.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		defer s.Close()
		_, err := io.Copy(&recBuf, s)
		errCh <- err
	})

	s, err := h1.NewStream(context.Background(), h2.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	if err := Send(s, filePath); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	if err := <-errCh; err != nil && err != io.EOF {
		t.Fatalf("receiver failed to read stream: %v", err)
	}

	var header Header
	recReader := bytes.NewReader(recBuf.Bytes())
	if err := header.ReadFrom(recReader); err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	payload := make([]byte, len(testData))
	if _, err := io.ReadFull(recReader, payload); err != nil {
		t.Fatalf("failed to read payload: %v", err)
	}

	var trailerChecksum [32]byte
	if _, err := io.ReadFull(recReader, trailerChecksum[:]); err != nil {
		t.Fatalf("failed to read trailer checksum: %v", err)
	}

	expectedHash := sha256.Sum256(testData)
	if !bytes.Equal(trailerChecksum[:], expectedHash[:]) {
		t.Errorf("checksum mismatch: expected %x, got %x", expectedHash, trailerChecksum)
	}
}
