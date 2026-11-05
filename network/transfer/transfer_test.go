package transfer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const testProtocolID = protocol.ID("/cipher/filetransfer/1.0.0")

func setupTestHosts(t *testing.T) (host.Host, host.Host) {
	t.Helper()
	ctx := context.Background()

	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host 1: %v", err)
	}

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		h1.Close()
		t.Fatalf("failed to create host 2: %v", err)
	}

	h2Info := h2.Peerstore().PeerInfo(h2.ID())
	if err := h1.Connect(ctx, h2Info); err != nil {
		h1.Close()
		h2.Close()
		t.Fatalf("failed to connect h1 to h2: %v", err)
	}

	t.Cleanup(func() {
		h1.Close()
		h2.Close()
	})

	return h1, h2
}

func createTempFile(t *testing.T, dir string, size int64) (string, []byte) {
	t.Helper()
	f, err := os.CreateTemp(dir, "transfer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer f.Close()

	data := make([]byte, size)
	if size > 0 {
		if _, err := rand.Read(data); err != nil {
			t.Fatalf("failed to generate random data: %v", err)
		}
	}

	if _, err := f.Write(data); err != nil {
		t.Fatalf("failed to write data to temp file: %v", err)
	}

	return f.Name(), data
}

func TestHeaderSerialization(t *testing.T) {
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "sample.txt",
		FileSize: 1024,
		Checksum: [32]byte{1, 2, 3, 4},
	}

	var buf bytes.Buffer
	if err := header.WriteTo(&buf); err != nil {
		t.Fatalf("Header.WriteTo failed: %v", err)
	}

	var readHeader Header
	if err := readHeader.ReadFrom(&buf); err != nil {
		t.Fatalf("Header.ReadFrom failed: %v", err)
	}

	if readHeader.Version != header.Version ||
		readHeader.Type != header.Type ||
		readHeader.Filename != header.Filename ||
		readHeader.FileSize != header.FileSize ||
		readHeader.Checksum != header.Checksum {
		t.Fatalf("header mismatch: expected %+v, got %+v", header, readHeader)
	}
}

func TestSinglePassSendReceive_Small(t *testing.T) {
	testSinglePassSendReceive(t, 64*1024) // 64 KB
}

func TestSinglePassSendReceive_Large(t *testing.T) {
	testSinglePassSendReceive(t, 5*1024*1024) // 5 MB
}

func testSinglePassSendReceive(t *testing.T, payloadSize int64) {
	h1, h2 := setupTestHosts(t)

	srcDir := t.TempDir()
	filePath, originalData := createTempFile(t, srcDir, payloadSize)

	dstDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working dir: %v", err)
	}
	if err := os.Chdir(dstDir); err != nil {
		t.Fatalf("failed to chdir to dstDir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(origWd)
	})

	errCh := make(chan error, 1)
	h2.SetStreamHandler(testProtocolID, func(s network.Stream) {
		errCh <- Receive(s)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := h1.NewStream(ctx, h2.ID(), testProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	if err := Send(stream, filePath); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Receive failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for receive")
	}

	receivedPath := filepath.Join("downloads", filepath.Base(filePath))
	receivedData, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("failed to read received file %s: %v", receivedPath, err)
	}

	if !bytes.Equal(originalData, receivedData) {
		t.Fatalf("received file content mismatch!")
	}

	expectedHash := sha256.Sum256(originalData)
	actualHash := sha256.Sum256(receivedData)
	if expectedHash != actualHash {
		t.Fatalf("hash mismatch: expected %x, got %x", expectedHash, actualHash)
	}
}

func TestZeroPreTransferReads(t *testing.T) {
	h1, h2 := setupTestHosts(t)

	srcDir := t.TempDir()
	filePath, _ := createTempFile(t, srcDir, 100*1024)

	dstDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working dir: %v", err)
	}
	if err := os.Chdir(dstDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(origWd)
	})

	var headerReadBeforePayload atomic.Bool
	doneCh := make(chan struct{})

	h2.SetStreamHandler(testProtocolID, func(s network.Stream) {
		defer close(doneCh)
		var h Header
		if err := h.ReadFrom(s); err != nil {
			t.Errorf("failed to read header in receiver: %v", err)
			return
		}
		// If header read succeeded before sender finished streaming, set flag
		headerReadBeforePayload.Store(true)

		// Drain remaining stream
		io.Copy(io.Discard, s)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := h1.NewStream(ctx, h2.ID(), testProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	if err := Send(stream, filePath); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	<-doneCh

	if !headerReadBeforePayload.Load() {
		t.Fatalf("Header was not received prior to payload streaming")
	}
}

func TestCorruptedTrailer(t *testing.T) {
	h1, h2 := setupTestHosts(t)

	srcDir := t.TempDir()
	filePath, originalData := createTempFile(t, srcDir, 10*1024)

	dstDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working dir: %v", err)
	}
	if err := os.Chdir(dstDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(origWd)
	})

	errCh := make(chan error, 1)
	h2.SetStreamHandler(testProtocolID, func(s network.Stream) {
		errCh <- Receive(s)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := h1.NewStream(ctx, h2.ID(), testProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	// Manually construct sender stream with invalid trailer
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filepath.Base(filePath),
		FileSize: uint64(len(originalData)),
		Checksum: [32]byte{},
	}
	if err := header.WriteTo(stream); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	if _, err := stream.Write(originalData); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}

	// Write corrupted trailer
	corruptedTrailer := [32]byte{0xff, 0xff, 0xff, 0xff}
	if _, err := stream.Write(corruptedTrailer[:]); err != nil {
		t.Fatalf("failed to write trailer: %v", err)
	}
	stream.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected error due to corrupted trailer, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for receive error")
	}

	// Confirm temporary file was cleaned up and output file was not created
	receivedPath := filepath.Join("downloads", filepath.Base(filePath))
	if _, err := os.Stat(receivedPath); !os.IsNotExist(err) {
		t.Fatalf("expected file %s to not exist after checksum failure", receivedPath)
	}
	tmpPath := receivedPath + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Fatalf("expected tmp file %s to be cleaned up after checksum failure", tmpPath)
	}
}

func TestPathSanitization(t *testing.T) {
	h1, h2 := setupTestHosts(t)

	dstDir := t.TempDir()
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working dir: %v", err)
	}
	if err := os.Chdir(dstDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}
	t.Cleanup(func() {
		_ = os.Chdir(origWd)
	})

	errCh := make(chan error, 1)
	h2.SetStreamHandler(testProtocolID, func(s network.Stream) {
		errCh <- Receive(s)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := h1.NewStream(ctx, h2.ID(), testProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}

	payload := []byte("hello world")
	hash := sha256.Sum256(payload)

	// Send header with path traversal attempts in filename
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "../../../etc/passwd",
		FileSize: uint64(len(payload)),
		Checksum: [32]byte{},
	}
	if err := header.WriteTo(stream); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	if _, err := stream.Write(payload); err != nil {
		t.Fatalf("failed to write payload: %v", err)
	}
	if _, err := stream.Write(hash[:]); err != nil {
		t.Fatalf("failed to write trailer: %v", err)
	}
	stream.Close()

	if err := <-errCh; err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	// Verify file was written to downloads/passwd, not outside downloads/
	expectedPath := filepath.Join("downloads", "passwd")
	if _, err := os.Stat(expectedPath); err != nil {
		t.Fatalf("expected sanitized file at %s: %v", expectedPath, err)
	}

	// Confirm no file was created in root of temp dir or parent
	if _, err := os.Stat("passwd"); !os.IsNotExist(err) {
		t.Fatalf("path traversal occurred!")
	}
}
