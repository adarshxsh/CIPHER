package transfer_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/protocol"
	"cipher/internal/transfer"
	"cipher/internal/transport"
)

func setupRealNetwork(t testing.TB) (host.Host, host.Host) {
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		h1.Close()
		h2.Close()
	})

	ctx := context.Background()
	h2Info := host.InfoFromHost(h2)
	if err := h1.Connect(ctx, *h2Info); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestTransfer_ReceiverReadDeadline(t *testing.T) {
	origTimeout := transfer.ReadTimeout
	transfer.ReadTimeout = 100 * time.Millisecond
	defer func() { transfer.ReadTimeout = origTimeout }()

	h1, h2 := setupRealNetwork(t)

	errChan := make(chan error, 1)
	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		errChan <- transfer.Receive(s)
	})

	// Open stream from h2 but send no header data
	t2 := transport.NewTransport(h2)
	stream, err := t2.OpenStream(context.Background(), h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	select {
	case err := <-errChan:
		if err == nil {
			t.Fatalf("Expected Receive to return error on read deadline timeout")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Timed out waiting for Receive to time out")
	}
}

func TestTransfer_SenderWriteDeadline(t *testing.T) {
	origTimeout := transfer.WriteTimeout
	transfer.WriteTimeout = -1 * time.Second
	defer func() { transfer.WriteTimeout = origTimeout }()

	h1, h2 := setupRealNetwork(t)

	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		defer s.Close()
	})

	tmpFile := filepath.Join(t.TempDir(), "test.txt")
	if err := os.WriteFile(tmpFile, []byte("hello world"), 0644); err != nil {
		t.Fatal(err)
	}

	t2 := transport.NewTransport(h2)
	stream, err := t2.OpenStream(context.Background(), h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	err = transfer.Send(stream, tmpFile)
	if err == nil {
		t.Fatalf("Expected Send to fail due to expired write deadline")
	}
}

func TestTransfer_ActiveTransferSucceeds(t *testing.T) {
	h1, h2 := setupRealNetwork(t)

	// Set up receiver on h1
	recvDone := make(chan error, 1)
	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		recvDone <- transfer.Receive(s)
	})

	// Create a test file
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "sample.dat")
	data := []byte("The quick brown fox jumps over the lazy dog")
	if err := os.WriteFile(srcFile, data, 0644); err != nil {
		t.Fatal(err)
	}

	// Send file from h2
	t2 := transport.NewTransport(h2)
	stream, err := t2.OpenStream(context.Background(), h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	if err := transfer.Send(stream, srcFile); err != nil {
		t.Fatalf("Send failed: %v", err)
	}

	select {
	case err := <-recvDone:
		if err != nil {
			t.Fatalf("Receive failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Timed out waiting for Receive completion")
	}

	// Verify file was received in downloads directory
	downloadedFile := filepath.Join("downloads", "sample.dat")
	t.Cleanup(func() { os.RemoveAll("downloads") })

	gotData, err := os.ReadFile(downloadedFile)
	if err != nil {
		t.Fatalf("Failed to read downloaded file: %v", err)
	}
	if string(gotData) != string(data) {
		t.Errorf("Content mismatch: got %q, want %q", string(gotData), string(data))
	}
}
