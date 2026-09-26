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
)

func setupRealTCPHosts(t *testing.T) (host.Host, host.Host) {
	t.Helper()
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host 1: %v", err)
	}
	t.Cleanup(func() { h1.Close() })

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host 2: %v", err)
	}
	t.Cleanup(func() { h2.Close() })

	err = h2.Connect(context.Background(), *host.InfoFromHost(h1))
	if err != nil {
		t.Fatalf("Failed to connect hosts: %v", err)
	}

	return h1, h2
}

func TestTransferReceive_TimeoutOnStalledSender(t *testing.T) {
	origReadTimeout := transfer.ReadTimeout
	transfer.ReadTimeout = 50 * time.Millisecond
	defer func() { transfer.ReadTimeout = origReadTimeout }()

	h1, h2 := setupRealTCPHosts(t)

	errChan := make(chan error, 1)
	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		errChan <- transfer.Receive(s)
	})

	s, err := h2.NewStream(context.Background(), h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Sender stalls and sends nothing
	time.Sleep(150 * time.Millisecond)

	select {
	case err := <-errChan:
		if err == nil {
			t.Fatal("Expected Receive to fail with read timeout, got nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for Receive handler to return error")
	}
}

func TestTransferSend_TimeoutOnStalledReceiver(t *testing.T) {
	origWriteTimeout := transfer.WriteTimeout
	transfer.WriteTimeout = 50 * time.Millisecond
	defer func() { transfer.WriteTimeout = origWriteTimeout }()

	h1, h2 := setupRealTCPHosts(t)

	// Receiver accepts stream but stops reading after reading 1 byte, causing writer buffer fill / deadline
	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		defer s.Close()
		var buf [1]byte
		_, _ = s.Read(buf[:])
		time.Sleep(200 * time.Millisecond)
	})

	s, err := h2.NewStream(context.Background(), h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Create a large temporary file to send so stream buffer fills
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_file.dat")
	data := make([]byte, 5*1024*1024) // 5 MB
	if err := os.WriteFile(filePath, data, 0600); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}

	err = transfer.Send(s, filePath)
	if err == nil {
		t.Fatal("Expected Send to fail when receiver stalls, got nil")
	}
}
