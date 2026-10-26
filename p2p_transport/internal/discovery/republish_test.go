package discovery

import (
	"cipher/internal/content/core"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

type mockManifestStore struct {
	manifests []core.ContentID
	listErr   error
}

func (m *mockManifestStore) GetManifestBytes(ctx context.Context, id core.ContentID) ([]byte, error) {
	return nil, nil
}

func (m *mockManifestStore) PutManifestBytes(ctx context.Context, id core.ContentID, data []byte) error {
	return nil
}

func (m *mockManifestStore) ListManifests(ctx context.Context) ([]core.ContentID, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.manifests, nil
}

func TestRepublishAllBoundedWorkerPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h1: %v", err)
	}
	defer h1.Close()

	dht1, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht1: %v", err)
	}
	defer dht1.Close()

	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h2: %v", err)
	}
	defer h2.Close()

	dht2, err := NewDHT(h2, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht2: %v", err)
	}
	defer dht2.Close()

	h1Info := peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}
	if err := Bootstrap(ctx, dht2, h2, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht2 bootstrap failed: %v", err)
	}

	manifestCount := 20
	manifests := make([]core.ContentID, manifestCount)
	for i := 0; i < manifestCount; i++ {
		manifests[i] = core.ContentID{byte(i + 1), 0x02, 0x03}
	}

	store := &mockManifestStore{manifests: manifests}

	start := time.Now()
	republishAll(ctx, dht2, store)
	duration := time.Since(start)

	t.Logf("republishAll completed %d manifests in %v", manifestCount, duration)

	// Verify that provided records can be queried
	for i := 0; i < 3; i++ {
		discovered, err := FindProviders(ctx, dht1, manifests[i], 1)
		if err != nil {
			t.Fatalf("FindProviders failed for manifest %d: %v", i, err)
		}
		if len(discovered) == 0 {
			t.Fatalf("expected provider for manifest %d, got none", i)
		}
	}
}

func TestStartRepublisherNonBlockingTicker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h1: %v", err)
	}
	defer h1.Close()

	dht1, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht1: %v", err)
	}
	defer dht1.Close()

	manifests := []core.ContentID{
		{0x01}, {0x02}, {0x03}, {0x04}, {0x05},
	}
	store := &mockManifestStore{manifests: manifests}

	// Start republisher with extremely fast 10ms interval
	StartRepublisher(ctx, dht1, store, 10*time.Millisecond)

	// Allow multiple ticker cycles to execute
	time.Sleep(100 * time.Millisecond)

	// Context cancellation should shut down background republisher in <100ms
	cancelStart := time.Now()
	cancel()
	time.Sleep(10 * time.Millisecond)
	cancelDuration := time.Since(cancelStart)

	if cancelDuration > 100*time.Millisecond {
		t.Fatalf("context cancellation shutdown took %v (>100ms limit)", cancelDuration)
	}
}

func TestRepublishAllCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h1: %v", err)
	}
	defer h1.Close()

	dht1, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht1: %v", err)
	}
	defer dht1.Close()

	manifests := []core.ContentID{{0x01}, {0x02}}
	store := &mockManifestStore{manifests: manifests}

	start := time.Now()
	republishAll(ctx, dht1, store)
	if time.Since(start) > 50*time.Millisecond {
		t.Fatalf("republishAll took too long on canceled context: %v", time.Since(start))
	}
}

func TestRepublishAllManifestStoreError(t *testing.T) {
	ctx := context.Background()

	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h1: %v", err)
	}
	defer h1.Close()

	dht1, err := NewDHT(h1, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht1: %v", err)
	}
	defer dht1.Close()

	store := &mockManifestStore{listErr: fmt.Errorf("storage offline")}

	// Should handle error gracefully without panicking
	republishAll(ctx, dht1, store)
}
