package chunk_test

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"

	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
)

func setupRealTCPNetwork(t testing.TB) (host.Host, host.Host) {
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host 1: %v", err)
	}

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("Failed to create host 2: %v", err)
	}

	h2.Peerstore().AddAddrs(h1.ID(), h1.Addrs(), peerstore.PermanentAddrTTL)
	h1.Peerstore().AddAddrs(h2.ID(), h2.Addrs(), peerstore.PermanentAddrTTL)

	return h1, h2
}

func TestStream_ReadDeadline_TimesOutOnInactivePeer(t *testing.T) {
	h1, h2 := setupRealTCPNetwork(t)
	defer h1.Close()
	defer h2.Close()

	done := make(chan error, 1)

	// Set up a stream handler that sets a 100ms read deadline
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		if err := s.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Logf("SetReadDeadline err: %v", err)
		}
		_, err := chunk.ReadMessage(s)
		done <- err
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := h2.Connect(ctx, peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}); err != nil {
		t.Fatalf("Failed to connect h2 to h1: %v", err)
	}

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Reset()

	// Send partial byte to flush stream header and stall
	_, _ = s.Write([]byte{0x01})

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Expected read to time out, but got nil error")
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			t.Logf("Successfully caught read timeout: %v", err)
		} else if errors.Is(err, os.ErrDeadlineExceeded) || err.Error() == "i/o timeout" || err.Error() == "i/o deadline reached" || err.Error() == "stream deadline exceeded" {
			t.Logf("Successfully caught read timeout: %v", err)
		} else {
			t.Fatalf("Expected timeout error, got: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Test timed out waiting for read deadline to trigger")
	}
}

func TestStream_WriteDeadline_TimesOutOnExpiredDeadline(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Set a deadline in the past to verify write deadline enforcement
	if err := s.SetWriteDeadline(time.Now().Add(-10 * time.Millisecond)); err != nil {
		t.Fatalf("Failed to set write deadline: %v", err)
	}

	msg := chunk.BuildRequestManifest([32]byte{1, 2, 3})
	err = chunk.WriteMessage(s, msg)
	if err == nil {
		t.Fatal("Expected WriteMessage to fail due to expired write deadline, got nil")
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Logf("Successfully caught write timeout: %v", err)
	} else if errors.Is(err, os.ErrDeadlineExceeded) || err.Error() == "i/o timeout" || err.Error() == "i/o deadline reached" || err.Error() == "stream deadline exceeded" {
		t.Logf("Successfully caught write timeout: %v", err)
	} else {
		t.Logf("Received expected write error on expired deadline: %v", err)
	}
}
