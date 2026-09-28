package push_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/protocol/push"
	"cipher/internal/transport"
)

func createTestPushEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupPushRealNetwork(t testing.TB) (host.Host, host.Host) {
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

func TestPushProtocol_ServerIdleTimeout(t *testing.T) {
	h1, h2 := setupPushRealNetwork(t)
	eng1 := createTestPushEngine(t)

	handler := push.NewStreamHandler(h1, eng1, nil, true, push.AuthPolicyOpen, nil)
	handler.ReadTimeout = 100 * time.Millisecond

	// Open push stream and stall
	s, err := h2.NewStream(context.Background(), h1.ID(), protocol.PushTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open push stream: %v", err)
	}
	defer s.Close()

	done := make(chan struct{})
	go func() {
		buf := make([]byte, 100)
		_, _ = s.Read(buf)
		close(done)
	}()

	select {
	case <-done:
		// Stream closed by push server handler
	case <-time.After(1 * time.Second):
		t.Fatal("Expected push server to close idle stream on timeout")
	}
}

func TestPushProtocol_ClientTimeoutOnStalledServer(t *testing.T) {
	h1, h2 := setupPushRealNetwork(t)

	// Handler that reads message then stalls without sending ACK
	h1.SetStreamHandler(protocol.PushTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		_, _ = push.ReadPushMessage(s)
		time.Sleep(1 * time.Second)
	})

	client, err := push.NewClient(context.Background(), transport.NewTransport(h2), h1.ID())
	if err != nil {
		t.Fatalf("Failed to create push client: %v", err)
	}
	defer client.Close()

	client.WriteTimeout = 100 * time.Millisecond
	client.ReadTimeout = 100 * time.Millisecond
	client.AckTimeout = 100 * time.Millisecond

	var dummyContentID core.ContentID
	err = client.SendManifest(context.Background(), dummyContentID, nil, []byte("{}"))
	if err == nil {
		t.Fatal("Expected SendManifest to time out")
	}
	if !strings.Contains(err.Error(), "i/o timeout") && !strings.Contains(err.Error(), "failed to read") {
		t.Errorf("Unexpected error: %v", err)
	}
}
