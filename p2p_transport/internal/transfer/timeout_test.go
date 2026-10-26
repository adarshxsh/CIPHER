package transfer_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/protocol"
	"cipher/internal/transfer"
)

func setupTransferRealNetwork(t testing.TB) (host.Host, host.Host) {
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
	addrInfo := peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	}
	if err := h2.Connect(context.Background(), addrInfo); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestTransfer_ReceiverTimeoutOnStalledSender(t *testing.T) {
	h1, h2 := setupTransferRealNetwork(t)

	// h1 is Receiver
	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		err := transfer.Receive(s)
		if err == nil {
			t.Errorf("Expected Receive to fail on stalled sender, got nil")
		}
	})

	// h2 starts transfer, writes header, then stalls
	s, err := h2.NewStream(context.Background(), h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	header := &transfer.Header{
		Version:  transfer.ProtocolVersion1,
		Type:     transfer.MsgTypeFileTransfer,
		Filename: "stalled.txt",
		FileSize: 1024 * 1024, // 1MB
	}
	if err := header.WriteTo(s); err != nil {
		t.Fatalf("Failed to write header: %v", err)
	}

	// Stall after writing header (write only 10 bytes then stop)
	_, _ = s.Write([]byte("0123456789"))

	// Give receiver time to hit deadline and exit
	time.Sleep(1 * time.Second)
	s.Close()
}

func TestTransfer_SenderTimeoutOnStalledReceiver(t *testing.T) {
	h1, h2 := setupTransferRealNetwork(t)

	// Create temp 5MB file to send
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "bigfile.bin")
	data := make([]byte, 5*1024*1024)
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		t.Fatalf("Failed to write temp file: %v", err)
	}

	// h1 is Receiver that reads header then stops reading
	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		var header transfer.Header
		_ = header.ReadFrom(s)
		// Stop reading from stream
		time.Sleep(1 * time.Second)
		s.Close()
	})

	// h2 sends file
	s, err := h2.NewStream(context.Background(), h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	err = transfer.Send(s, filePath)
	if err == nil {
		t.Fatal("Expected Send to fail when receiver stops reading")
	}
	if !strings.Contains(err.Error(), "failed to send file data") && !strings.Contains(err.Error(), "i/o timeout") {
		t.Errorf("Unexpected error: %v", err)
	}
}
