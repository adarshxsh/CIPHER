package push_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/push"
	"cipher/internal/transport"
)

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func createManifest(contentID core.ContentID) ([]byte, *manifest.Manifest) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   contentID,
			Type: manifest.TypeFile,
			Size: 100,
		},
		ChunkIDs: []core.ChunkID{},
	}
	data, _ := m.Serialize()
	return data, m
}

func setupMockNetwork(t testing.TB, numPeers int) ([]host.Host, mocknet.Mocknet) {
	mn := mocknet.New()
	hosts := make([]host.Host, numPeers)
	for i := 0; i < numPeers; i++ {
		h, err := mn.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return hosts, mn
}

func TestGlobalSessionLimits(t *testing.T) {
	hosts, _ := setupMockNetwork(t, 4)
	providerHost := hosts[0]
	eng := createTestEngine(t)

	// Set max global sessions to 2
	handler := push.NewStreamHandler(
		providerHost,
		eng,
		nil,
		true,
		push.AuthPolicyOpen,
		nil,
		push.WithMaxGlobalSessions(2),
		push.WithReaperInterval(0),
	)
	defer handler.Close()

	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		clientHost := hosts[i]
		clientTrans := transport.NewTransport(clientHost)

		client, err := push.NewClient(ctx, clientTrans, providerHost.ID())
		if err != nil {
			t.Fatalf("Failed to create push client for peer %d: %v", i, err)
		}

		var contentID core.ContentID
		_, _ = rand.Read(contentID[:])
		manifestData, _ := createManifest(contentID)

		err = client.SendManifest(ctx, contentID, nil, manifestData)
		client.Close()

		if i <= 2 {
			if err != nil {
				t.Fatalf("Expected session %d to succeed, got %v", i, err)
			}
		} else {
			if err == nil {
				t.Fatalf("Expected session %d to fail due to global quota limit, but succeeded", i)
			}
		}
	}

	if count := handler.ActiveSessionsCount(); count != 2 {
		t.Errorf("Expected 2 active sessions, got %d", count)
	}
}

func TestPerPeerSessionQuotas(t *testing.T) {
	hosts, _ := setupMockNetwork(t, 3)
	providerHost := hosts[0]
	clientHostA := hosts[1]
	clientHostB := hosts[2]
	eng := createTestEngine(t)

	// Set max peer sessions to 2
	handler := push.NewStreamHandler(
		providerHost,
		eng,
		nil,
		true,
		push.AuthPolicyOpen,
		nil,
		push.WithMaxPeerSessions(2),
		push.WithReaperInterval(0),
	)
	defer handler.Close()

	ctx := context.Background()

	transA := transport.NewTransport(clientHostA)

	// Peer A opens 3 sessions; 1st & 2nd should succeed, 3rd should fail
	for i := 1; i <= 3; i++ {
		client, err := push.NewClient(ctx, transA, providerHost.ID())
		if err != nil {
			t.Fatalf("Failed to create push client: %v", err)
		}

		var contentID core.ContentID
		_, _ = rand.Read(contentID[:])
		manifestData, _ := createManifest(contentID)

		err = client.SendManifest(ctx, contentID, nil, manifestData)
		client.Close()

		if i <= 2 {
			if err != nil {
				t.Fatalf("Peer A session %d expected to succeed, got %v", i, err)
			}
		} else {
			if err == nil {
				t.Fatalf("Peer A session %d expected to fail due to per-peer quota, but succeeded", i)
			}
		}
	}

	if count := handler.PeerSessionsCount(clientHostA.ID()); count != 2 {
		t.Errorf("Expected Peer A quota count 2, got %d", count)
	}

	// Peer B opens 1 session; should succeed
	transB := transport.NewTransport(clientHostB)

	clientB, err := push.NewClient(ctx, transB, providerHost.ID())
	if err != nil {
		t.Fatal(err)
	}
	var contentIDB core.ContentID
	_, _ = rand.Read(contentIDB[:])
	manifestDataB, _ := createManifest(contentIDB)

	err = clientB.SendManifest(ctx, contentIDB, nil, manifestDataB)
	clientB.Close()

	if err != nil {
		t.Fatalf("Peer B session expected to succeed, got %v", err)
	}

	if count := handler.PeerSessionsCount(clientHostB.ID()); count != 1 {
		t.Errorf("Expected Peer B quota count 1, got %d", count)
	}
	if count := handler.ActiveSessionsCount(); count != 3 {
		t.Errorf("Expected total active sessions 3, got %d", count)
	}
}

func TestPeerIDTracking(t *testing.T) {
	hosts, _ := setupMockNetwork(t, 2)
	providerHost := hosts[0]
	clientHost := hosts[1]
	eng := createTestEngine(t)

	handler := push.NewStreamHandler(
		providerHost,
		eng,
		nil,
		true,
		push.AuthPolicyOpen,
		nil,
		push.WithReaperInterval(0),
	)
	defer handler.Close()

	ctx := context.Background()
	trans := transport.NewTransport(clientHost)

	client, err := push.NewClient(ctx, trans, providerHost.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var contentID core.ContentID
	_, _ = rand.Read(contentID[:])
	manifestData, _ := createManifest(contentID)

	if err := client.SendManifest(ctx, contentID, nil, manifestData); err != nil {
		t.Fatalf("SendManifest failed: %v", err)
	}

	if count := handler.PeerSessionsCount(clientHost.ID()); count != 1 {
		t.Errorf("Expected PeerSessionsCount 1, got %d", count)
	}
}

func TestTTLReaperEviction(t *testing.T) {
	hosts, _ := setupMockNetwork(t, 2)
	providerHost := hosts[0]
	clientHost := hosts[1]
	eng := createTestEngine(t)

	handler := push.NewStreamHandler(
		providerHost,
		eng,
		nil,
		true,
		push.AuthPolicyOpen,
		nil,
		push.WithSessionTTL(100*time.Millisecond),
		push.WithReaperInterval(30*time.Millisecond),
	)
	defer handler.Close()

	ctx := context.Background()
	trans := transport.NewTransport(clientHost)

	client, err := push.NewClient(ctx, trans, providerHost.ID())
	if err != nil {
		t.Fatal(err)
	}

	var contentID core.ContentID
	_, _ = rand.Read(contentID[:])
	manifestData, _ := createManifest(contentID)

	if err := client.SendManifest(ctx, contentID, nil, manifestData); err != nil {
		t.Fatalf("SendManifest failed: %v", err)
	}
	client.Close()

	if count := handler.ActiveSessionsCount(); count != 1 {
		t.Fatalf("Expected 1 active session before reaper, got %d", count)
	}

	// Wait for TTL reaper sweep to evict the session
	time.Sleep(300 * time.Millisecond)

	if count := handler.ActiveSessionsCount(); count != 0 {
		t.Errorf("Expected 0 active sessions after reaper eviction, got %d", count)
	}

	if count := handler.PeerSessionsCount(clientHost.ID()); count != 0 {
		t.Errorf("Expected 0 peer session count after reaper eviction, got %d", count)
	}
}

func TestSessionBatchCompleteCleanup(t *testing.T) {
	hosts, _ := setupMockNetwork(t, 2)
	providerHost := hosts[0]
	clientHost := hosts[1]
	eng := createTestEngine(t)

	handler := push.NewStreamHandler(
		providerHost,
		eng,
		nil,
		true,
		push.AuthPolicyOpen,
		nil,
		push.WithReaperInterval(0),
	)
	defer handler.Close()

	ctx := context.Background()
	trans := transport.NewTransport(clientHost)

	client, err := push.NewClient(ctx, trans, providerHost.ID())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var contentID core.ContentID
	_, _ = rand.Read(contentID[:])
	manifestData, _ := createManifest(contentID)

	// Send manifest with no assigned chunks
	if err := client.SendManifest(ctx, contentID, []core.ChunkID{}, manifestData); err != nil {
		t.Fatalf("SendManifest failed: %v", err)
	}

	if count := handler.PeerSessionsCount(clientHost.ID()); count != 1 {
		t.Fatalf("Expected 1 session before batch complete, got %d", count)
	}

	// Send Batch Complete
	if err := client.SendBatchComplete(ctx, contentID); err != nil {
		t.Fatalf("SendBatchComplete failed: %v", err)
	}

	if count := handler.ActiveSessionsCount(); count != 0 {
		t.Errorf("Expected 0 active sessions after BatchComplete, got %d", count)
	}

	if count := handler.PeerSessionsCount(clientHost.ID()); count != 0 {
		t.Errorf("Expected 0 peer session count after BatchComplete, got %d", count)
	}
}

func TestConcurrentSafety(t *testing.T) {
	hosts, _ := setupMockNetwork(t, 5)
	providerHost := hosts[0]
	eng := createTestEngine(t)

	handler := push.NewStreamHandler(
		providerHost,
		eng,
		nil,
		true,
		push.AuthPolicyOpen,
		nil,
		push.WithMaxGlobalSessions(10),
		push.WithMaxPeerSessions(3),
		push.WithSessionTTL(50*time.Millisecond),
		push.WithReaperInterval(10*time.Millisecond),
	)
	defer handler.Close()

	ctx := context.Background()

	done := make(chan struct{})
	for i := 1; i <= 4; i++ {
		clientHost := hosts[i]
		go func(h host.Host) {
			trans := transport.NewTransport(h)
			for j := 0; j < 10; j++ {
				client, err := push.NewClient(ctx, trans, providerHost.ID())
				if err != nil {
					continue
				}
				var contentID core.ContentID
				_, _ = rand.Read(contentID[:])
				manifestData, _ := createManifest(contentID)

				_ = client.SendManifest(ctx, contentID, nil, manifestData)
				_ = handler.ActiveSessionsCount()
				_ = handler.PeerSessionsCount(h.ID())
				time.Sleep(5 * time.Millisecond)
				client.Close()
			}
			done <- struct{}{}
		}(clientHost)
	}

	for i := 1; i <= 4; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for concurrent test goroutines")
		}
	}
}
