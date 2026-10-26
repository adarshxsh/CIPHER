package discovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"cipher/internal/content/core"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

type mockManifestStore struct {
	manifests []core.ContentID
}

func (m *mockManifestStore) GetManifestBytes(ctx context.Context, id core.ContentID) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockManifestStore) PutManifestBytes(ctx context.Context, id core.ContentID, data []byte) error {
	return fmt.Errorf("not implemented")
}

func (m *mockManifestStore) ListManifests(ctx context.Context) ([]core.ContentID, error) {
	return m.manifests, nil
}

func TestRepublishAllParallel50Manifests(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Create Bootstrapper DHT Node
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

	// 2. Create Storage Provider DHT Node
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

	// Bootstrap h2 with h1
	if err := Bootstrap(ctx, dht2, h2, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht2 bootstrap failed: %v", err)
	}

	// 3. Create mock store with 50 manifests
	manifestCount := 50
	mockStore := &mockManifestStore{
		manifests: make([]core.ContentID, manifestCount),
	}
	for i := 0; i < manifestCount; i++ {
		var id core.ContentID
		id[0] = byte(i + 1)
		id[1] = byte((i + 1) >> 8)
		id[31] = 0xAA
		mockStore.manifests[i] = id
	}

	// 4. Run republishAll and time it
	startTime := time.Now()
	republishAll(ctx, dht2, mockStore)
	elapsed := time.Since(startTime)

	t.Logf("republishAll for %d manifests completed in %v", manifestCount, elapsed)

	if elapsed > 10*time.Second {
		t.Fatalf("republishAll took too long: %v", elapsed)
	}

	// 5. Create Query Node h3
	h3, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h3: %v", err)
	}
	defer h3.Close()

	dht3, err := NewDHT(h3, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht3: %v", err)
	}
	defer dht3.Close()

	if err := Bootstrap(ctx, dht3, h3, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht3 bootstrap failed: %v", err)
	}

	// Give DHT records a moment to settle
	time.Sleep(300 * time.Millisecond)

	// 6. Verify DHT announcements succeed across all workers without dropouts
	for i, id := range mockStore.manifests {
		discovered, err := FindProviders(ctx, dht3, id, 5)
		if err != nil {
			t.Fatalf("FindProviders failed for manifest %d (%x): %v", i, id, err)
		}

		found := false
		for _, p := range discovered {
			if p.ID == h2.ID() {
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("expected to discover provider %s for manifest %d (%x), got %v", h2.ID(), i, id, discovered)
		}
	}
}
