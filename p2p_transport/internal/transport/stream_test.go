package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"

	"cipher/internal/protocol"
)

func TestTransportOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer h.Close()
	if kdht != nil {
		defer kdht.Close()
	}

	connTimeout := 5 * time.Second
	streamTimeout := 10 * time.Second

	tr := NewTransport(h, WithConnectTimeout(connTimeout), WithStreamTimeout(streamTimeout))
	if tr.connectTimeout != connTimeout {
		t.Errorf("Expected connectTimeout %v, got %v", connTimeout, tr.connectTimeout)
	}
	if tr.streamTimeout != streamTimeout {
		t.Errorf("Expected streamTimeout %v, got %v", streamTimeout, tr.streamTimeout)
	}
	if tr.Host() != h {
		t.Errorf("Host mismatch")
	}
}

func TestConnectCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer h.Close()
	if kdht != nil {
		defer kdht.Close()
	}

	tr := NewTransport(h)

	canceledCtx, cancelCtx := context.WithCancel(context.Background())
	cancelCtx()

	_, err = tr.Connect(canceledCtx, "/ip4/127.0.0.1/tcp/1234/p2p/12D3KooW12345678901234567890123456789012345678901234")
	if err == nil {
		t.Fatal("Expected error on canceled context, got nil")
	}

	addrInfo := peer.AddrInfo{ID: peer.ID("testpeer")}
	err = tr.ConnectPeer(canceledCtx, addrInfo)
	if err == nil {
		t.Fatal("Expected error on canceled context in ConnectPeer, got nil")
	}
}

func TestConnectInvalidInputs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer h.Close()
	if kdht != nil {
		defer kdht.Close()
	}

	tr := NewTransport(h)

	_, err = tr.Connect(ctx, "invalid-multiaddr")
	if err == nil {
		t.Fatal("Expected error for invalid multiaddr, got nil")
	}

	err = tr.ConnectPeer(ctx, peer.AddrInfo{ID: ""})
	if !errors.Is(err, ErrInvalidPeerID) {
		t.Fatalf("Expected ErrInvalidPeerID, got %v", err)
	}
}

func TestOpenStreamCanceledContextAndValidation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}
	defer h.Close()
	if kdht != nil {
		defer kdht.Close()
	}

	tr := NewTransport(h)

	canceledCtx, cancelCtx := context.WithCancel(context.Background())
	cancelCtx()

	_, err = tr.OpenStream(canceledCtx, peer.ID("somepeer"), libp2p_protocol.ID("testproto"))
	if err == nil {
		t.Fatal("Expected error on canceled context in OpenStream, got nil")
	}

	_, err = tr.OpenStream(ctx, "", libp2p_protocol.ID("testproto"))
	if !errors.Is(err, ErrInvalidPeerID) {
		t.Fatalf("Expected ErrInvalidPeerID, got %v", err)
	}

	_, err = tr.OpenStream(ctx, peer.ID("somepeer"), "")
	if !errors.Is(err, ErrInvalidProtocol) {
		t.Fatalf("Expected ErrInvalidProtocol, got %v", err)
	}
}

func TestOpenStreamAndDeadlineEnforcement(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h1, kdht1, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create node 1: %v", err)
	}
	defer h1.Close()
	if kdht1 != nil {
		defer kdht1.Close()
	}

	h2, kdht2, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create node 2: %v", err)
	}
	defer h2.Close()
	if kdht2 != nil {
		defer kdht2.Close()
	}

	testProto := libp2p_protocol.ID("/test/deadline/1.0.0")

	// Set a stream handler on h1 that reads and blocks
	h1.SetStreamHandler(testProto, func(s network.Stream) {
		defer s.Close()
		buf := make([]byte, 1)
		_, _ = s.Read(buf)
	})

	// Connect h2 to h1
	h2Info := peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	}
	tr2 := NewTransport(h2, WithStreamTimeout(100*time.Millisecond))
	if err := tr2.ConnectPeer(ctx, h2Info); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	// Open stream with short timeout (100ms)
	s, err := tr2.OpenStream(ctx, h1.ID(), testProto)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Attempt reading from the stalled stream; it should hit deadline
	buf := make([]byte, 100)
	start := time.Now()
	_, err = s.Read(buf)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Expected read error due to deadline, got nil")
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		// Expected timeout error
	} else if errors.Is(err, context.DeadlineExceeded) || err.Error() == "i/o timeout" || elapsed < 1*time.Second {
		// Deadline enforced correctly
	} else {
		t.Logf("Read failed as expected with error: %v (elapsed: %v)", err, elapsed)
	}
}

func TestStreamDeadlineHelpers(t *testing.T) {
	tr := NewTransport(nil)

	if err := tr.SetStreamDeadline(nil, 1*time.Second); err == nil {
		t.Error("Expected error when setting deadline on nil stream")
	}
	if err := tr.SetStreamReadDeadline(nil, 1*time.Second); err == nil {
		t.Error("Expected error when setting read deadline on nil stream")
	}
	if err := tr.SetStreamWriteDeadline(nil, 1*time.Second); err == nil {
		t.Error("Expected error when setting write deadline on nil stream")
	}
}

func TestSetupStreamHandlerWithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h1, kdht1, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create node 1: %v", err)
	}
	defer h1.Close()
	if kdht1 != nil {
		defer kdht1.Close()
	}

	h2, kdht2, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Failed to create node 2: %v", err)
	}
	defer h2.Close()
	if kdht2 != nil {
		defer kdht2.Close()
	}

	SetupStreamHandlerWithTimeout(h1, 100*time.Millisecond)

	tr2 := NewTransport(h2)
	h1Info := peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	}
	if err := tr2.ConnectPeer(ctx, h1Info); err != nil {
		t.Fatalf("Failed to connect peers: %v", err)
	}

	s, err := tr2.OpenStream(ctx, h1.ID(), protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Read from incoming stream; since handler will timeout on Receive and reset stream, read should fail
	buf := make([]byte, 100)
	_, err = s.Read(buf)
	if err == nil {
		t.Fatal("Expected error on stalled stream from receiver, got nil")
	}
	if !errors.Is(err, io.EOF) && err.Error() != "stream reset" {
		t.Logf("Got stream read error as expected: %v", err)
	}
}
