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
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/protocol"
	"cipher/internal/transfer"
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
	err = h2.Connect(context.Background(), peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestStalledTransfer_ReceiveTimeout(t *testing.T) {
	h1, h2 := setupRealNetwork(t)

	done := make(chan error, 1)
	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		err := transfer.Receive(s, transfer.WithTransferReadTimeout(100*time.Millisecond))
		done <- err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Peer 2 sends nothing (stalled)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Expected Receive to fail due to read deadline timeout")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Receive did not time out within expected duration")
	}
}

func TestTransfer_SuccessWithDeadlines(t *testing.T) {
	h1, h2 := setupRealNetwork(t)

	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "test_file.txt")
	testData := []byte("Hello CIPHER stream deadlines test!")
	if err := os.WriteFile(srcFile, testData, 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	done := make(chan error, 1)
	h1.SetStreamHandler(protocol.FileTransferProtocolID, func(s network.Stream) {
		err := transfer.Receive(s, transfer.WithTransferReadTimeout(1*time.Second), transfer.WithTransferWriteTimeout(1*time.Second))
		done <- err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	sendErr := transfer.Send(s, srcFile, transfer.WithTransferReadTimeout(1*time.Second), transfer.WithTransferWriteTimeout(1*time.Second))
	if sendErr != nil {
		t.Fatalf("Send failed: %v", sendErr)
	}

	recErr := <-done
	if recErr != nil {
		t.Fatalf("Receive failed: %v", recErr)
	}

	// Verify downloaded file
	downloadedFile := filepath.Join("downloads", filepath.Base(srcFile))
	defer os.RemoveAll("downloads")

	data, err := os.ReadFile(downloadedFile)
	if err != nil {
		t.Fatalf("Failed to read downloaded file: %v", err)
	}
	if string(data) != string(testData) {
		t.Fatalf("Content mismatch: expected %q, got %q", string(testData), string(data))
	}
}
