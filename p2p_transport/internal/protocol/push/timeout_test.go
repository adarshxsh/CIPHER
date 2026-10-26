package push_test

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/protocol/push"
	"cipher/internal/transport"
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

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func TestPushHandler_IdleStreamTimeout(t *testing.T) {
	origTimeout := push.ReadTimeout
	push.ReadTimeout = 50 * time.Millisecond
	defer func() { push.ReadTimeout = origTimeout }()

	h1, h2 := setupRealTCPHosts(t)
	eng1 := createTestEngine(t)

	push.NewStreamHandler(h1, eng1, nil, true, push.AuthPolicyOpen, nil)

	ctx := context.Background()
	s, err := h2.NewStream(ctx, h1.ID(), protocol.PushTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// Wait longer than ReadTimeout without sending anything
	time.Sleep(150 * time.Millisecond)

	buf := make([]byte, 10)
	_ = s.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err = s.Read(buf)
	if err == nil {
		t.Fatal("Expected error when reading from push stream closed due to read deadline, got nil")
	}
}

func TestPushClient_TimeoutOnStalledProvider(t *testing.T) {
	origTimeout := push.ReadTimeout
	push.ReadTimeout = 50 * time.Millisecond
	defer func() { push.ReadTimeout = origTimeout }()

	h1, h2 := setupRealTCPHosts(t)

	// Server accepts push stream, reads manifest message, but stalls before replying with ACK
	h1.SetStreamHandler(protocol.PushTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		buf := make([]byte, 1024)
		_, _ = s.Read(buf)
		time.Sleep(200 * time.Millisecond)
	})

	client, err := push.NewClient(context.Background(), transport.NewTransport(h2), h1.ID())
	if err != nil {
		t.Fatalf("Failed to create push client: %v", err)
	}
	defer client.Close()

	var dummyID core.ContentID
	err = client.SendManifest(context.Background(), dummyID, nil, []byte("test"))
	if err == nil {
		t.Fatal("Expected SendManifest to fail due to timeout, got nil")
	}
}
