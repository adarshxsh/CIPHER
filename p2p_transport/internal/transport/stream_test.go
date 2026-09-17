package transport

import (
	"context"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/protocol"
)

func createConnectedHosts(t *testing.T) (*Transport, *Transport, peer.ID, peer.ID) {
	ctx := context.Background()

	h1, dht1, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("failed to create host 1: %v", err)
	}
	t.Cleanup(func() {
		_ = dht1.Close()
		_ = h1.Close()
	})

	h2, dht2, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("failed to create host 2: %v", err)
	}
	t.Cleanup(func() {
		_ = dht2.Close()
		_ = h2.Close()
	})

	t1 := NewTransport(h1)
	t2 := NewTransport(h2)

	h1Info := peer.AddrInfo{
		ID:    h1.ID(),
		Addrs: h1.Addrs(),
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	if err := t2.ConnectPeer(ctx, h1Info); err != nil {
		t.Fatalf("failed to connect h2 to h1: %v", err)
	}
	if err := t1.ConnectPeer(ctx, h2Info); err != nil {
		t.Fatalf("failed to connect h1 to h2: %v", err)
	}

	return t1, t2, h1.ID(), h2.ID()
}

func TestManagedStream_OpenStreamAndWrapping(t *testing.T) {
	t1, t2, _, h2ID := createConnectedHosts(t)

	var receivedStream network.Stream
	streamReceived := make(chan struct{})

	t2.host.SetStreamHandler(protocol.FileTransferProtocolID, WrapStreamHandler(func(s network.Stream) {
		receivedStream = s
		close(streamReceived)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s1, err := t1.OpenStream(ctx, h2ID, protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	defer s1.Close()

	ms1, ok := s1.(*ManagedStream)
	if !ok {
		t.Fatalf("expected OpenStream to return *ManagedStream, got %T", s1)
	}

	if ms1.IdleTimeout() != DefaultIdleTimeout {
		t.Errorf("expected default idle timeout %v, got %v", DefaultIdleTimeout, ms1.IdleTimeout())
	}
	if ms1.WriteTimeout() != DefaultWriteTimeout {
		t.Errorf("expected default write timeout %v, got %v", DefaultWriteTimeout, ms1.WriteTimeout())
	}

	select {
	case <-streamReceived:
		ms2, ok := receivedStream.(*ManagedStream)
		if !ok {
			t.Fatalf("expected handler to receive *ManagedStream, got %T", receivedStream)
		}
		if ms2.IdleTimeout() != DefaultIdleTimeout {
			t.Errorf("expected handler stream idle timeout %v, got %v", DefaultIdleTimeout, ms2.IdleTimeout())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for incoming stream")
	}
}

func TestManagedStream_IdleTimeoutEnforcement(t *testing.T) {
	t1, t2, _, h2ID := createConnectedHosts(t)

	// Configure short idle timeout on transport
	t1.SetIdleTimeout(100 * time.Millisecond)

	t2.host.SetStreamHandler(protocol.FileTransferProtocolID, WrapStreamHandler(func(s network.Stream) {
		// Handler deliberately stays silent without reading or writing
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := t1.OpenStream(ctx, h2ID, protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	defer s.Close()

	buf := make([]byte, 32)
	start := time.Now()
	_, err = s.Read(buf)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected read to fail due to idle timeout, but got nil error")
	}

	var netErr net.Error
	isTimeout := errors.As(err, &netErr) && netErr.Timeout()
	isDeadline := errors.Is(err, os.ErrDeadlineExceeded)

	if !isTimeout && !isDeadline {
		t.Fatalf("expected timeout error, got %v", err)
	}

	if elapsed > 1*time.Second {
		t.Fatalf("read took too long to time out: %v", elapsed)
	}
}

func TestManagedStream_ExplicitDeadlineOverride(t *testing.T) {
	t1, t2, _, h2ID := createConnectedHosts(t)

	// Set short default idle timeout
	t1.SetIdleTimeout(50 * time.Millisecond)

	readSignal := make(chan struct{})

	t2.host.SetStreamHandler(protocol.FileTransferProtocolID, WrapStreamHandler(func(s network.Stream) {
		// Wait for signal, then write data after 150ms
		<-readSignal
		time.Sleep(150 * time.Millisecond)
		_, _ = s.Write([]byte("hello"))
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := t1.OpenStream(ctx, h2ID, protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	defer s.Close()

	// Override deadline explicitly to 1 second
	err = s.SetReadDeadline(time.Now().Add(1 * time.Second))
	if err != nil {
		t.Fatalf("SetReadDeadline failed: %v", err)
	}

	close(readSignal)

	buf := make([]byte, 32)
	n, err := s.Read(buf)
	if err != nil {
		t.Fatalf("expected read to succeed with explicit deadline override, got: %v", err)
	}

	if string(buf[:n]) != "hello" {
		t.Fatalf("expected 'hello', got '%s'", string(buf[:n]))
	}
}

func TestManagedStream_ActiveTransferExtendsDeadline(t *testing.T) {
	t1, t2, _, h2ID := createConnectedHosts(t)

	// Set 200ms idle timeout
	t1.SetIdleTimeout(200 * time.Millisecond)

	t2.host.SetStreamHandler(protocol.FileTransferProtocolID, WrapStreamHandler(func(s network.Stream) {
		// Send 3 packets 100ms apart (total duration 300ms > 200ms idle timeout)
		for i := 0; i < 3; i++ {
			time.Sleep(100 * time.Millisecond)
			_, _ = s.Write([]byte("data"))
		}
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := t1.OpenStream(ctx, h2ID, protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	defer s.Close()

	buf := make([]byte, 32)
	for i := 0; i < 3; i++ {
		n, err := s.Read(buf)
		if err != nil {
			t.Fatalf("packet %d: expected read to succeed during active transfer, got %v", i, err)
		}
		if string(buf[:n]) != "data" {
			t.Fatalf("packet %d: expected 'data', got '%s'", i, string(buf[:n]))
		}
	}
}

func TestManagedStream_ConfigurableTimeoutSetters(t *testing.T) {
	ms := NewManagedStream(nil, WithIdleTimeout(10*time.Second), WithWriteTimeout(5*time.Second))

	if ms.IdleTimeout() != 10*time.Second {
		t.Errorf("expected 10s idle timeout, got %v", ms.IdleTimeout())
	}
	if ms.WriteTimeout() != 5*time.Second {
		t.Errorf("expected 5s write timeout, got %v", ms.WriteTimeout())
	}

	ms.SetIdleTimeout(20 * time.Second)
	if ms.IdleTimeout() != 20*time.Second {
		t.Errorf("expected 20s idle timeout, got %v", ms.IdleTimeout())
	}

	ms.SetWriteTimeout(15 * time.Second)
	if ms.WriteTimeout() != 15*time.Second {
		t.Errorf("expected 15s write timeout, got %v", ms.WriteTimeout())
	}
}

func TestManagedStream_SetupStreamHandler(t *testing.T) {
	t1, t2, h1ID, _ := createConnectedHosts(t)

	SetupStreamHandler(t1.host)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s, err := t2.OpenStream(ctx, h1ID, protocol.FileTransferProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer s.Close()

	// Writing 0 bytes or closing stream should not crash handler
	_ = s.Close()
}
