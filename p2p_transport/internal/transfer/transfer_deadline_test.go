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
	"github.com/libp2p/go-libp2p/core/peerstore"

	"cipher/internal/protocol"
	"cipher/internal/transfer"
)

func setupTCPHosts(t testing.TB) (host.Host, host.Host) {
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

	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), peerstore.PermanentAddrTTL)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h2.Connect(ctx, h1.Peerstore().PeerInfo(h1.ID())); err != nil {
		t.Fatalf("Failed to connect hosts: %v", err)
	}

	return h1, h2
}

func TestTransfer_ReceiverStalledPeerDeadline(t *testing.T) {
	h1, h2 := setupTCPHosts(t)

	// Server registers stream handler that reads header then stalls
	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		defer s.Close()
		// Send incomplete header bytes then stall
		_, _ = s.Write([]byte{1, 1}) // protocol version 1, type 1, but no filename length/data
		time.Sleep(2 * time.Second)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := h2.NewStream(ctx, h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Set short deadline on stream to test timeout handling
	_ = stream.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

	err = transfer.Receive(stream)
	if err == nil {
		t.Fatal("Expected error receiving from stalled peer, got nil")
	}
}

func TestTransfer_SenderStalledPeerDeadline(t *testing.T) {
	h1, h2 := setupTCPHosts(t)

	// Receiver accepts stream but does not read from socket (stalling sender)
	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		defer s.Close()
		time.Sleep(2 * time.Second)
	})

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test.txt")
	if err := os.WriteFile(filePath, []byte("hello world deadline test"), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := h2.NewStream(ctx, h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Set short write deadline
	_ = stream.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))

	// Attempt send - should complete or handle deadline write
	err = transfer.Send(stream, filePath)
	// Send writes a small header and small payload. If socket buffers accept it, it succeeds, or if write deadline triggers it errors.
	if err != nil {
		// Verify stream reset on error
		var buf [1]byte
		_, readErr := stream.Read(buf[:])
		if readErr == nil {
			t.Errorf("Expected stream to be closed or reset after error")
		}
	}
}

func TestTransfer_ReceiverStalledBodyDeadline(t *testing.T) {
	h1, h2 := setupTCPHosts(t)

	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		defer s.Close()
		// Send header claiming 100MB file, then stop sending
		hdr := &transfer.Header{
			Version:  transfer.ProtocolVersion1,
			Type:     transfer.MsgTypeFileTransfer,
			Filename: "large.bin",
			FileSize: 100 * 1024 * 1024,
			Checksum: [32]byte{},
		}
		if err := hdr.WriteTo(s); err != nil {
			return
		}
		// Send 10 bytes then stall
		_, _ = s.Write(make([]byte, 10))
		time.Sleep(2 * time.Second)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := h2.NewStream(ctx, h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Set short read deadline
	_ = stream.SetReadDeadline(time.Now().Add(100 * time.Millisecond))

	err = transfer.Receive(stream)
	if err == nil {
		t.Fatal("Expected error when receiving file payload from stalled peer")
	}

	// Clean up downloaded file artifact if created in downloads/
	os.RemoveAll("downloads")
}
