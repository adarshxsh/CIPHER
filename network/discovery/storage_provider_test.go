package discovery

import (
	"cipher/internal/content/core"
	"context"
	"sync/atomic"
	"testing"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestStorageProviderDHTRegistration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
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

	// 3. Register h2 as Storage Provider
	if err := RegisterStorageProvider(ctx, dht2); err != nil {
		t.Fatalf("RegisterStorageProvider failed: %v", err)
	}

	// 4. Create Publisher DHT Node
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

	// Bootstrap h3 with h1
	if err := Bootstrap(ctx, dht3, h3, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht3 bootstrap failed: %v", err)
	}

	// Give DHT record a moment to settle
	time.Sleep(300 * time.Millisecond)

	// 5. Query DHT for Storage Providers from h3
	discovered, err := FindStorageProviders(ctx, dht3, 5)
	if err != nil {
		t.Fatalf("FindStorageProviders failed: %v", err)
	}

	found := false
	for _, p := range discovered {
		if p.ID == h2.ID() {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("expected to discover storage provider %s, got %v", h2.ID(), discovered)
	}
}

type mockManifestStore struct {
	manifests []core.ContentID
	listCount atomic.Int32
	listDelay time.Duration
	listErr   error
}

func (m *mockManifestStore) GetManifestBytes(ctx context.Context, id core.ContentID) ([]byte, error) {
	return nil, nil
}

func (m *mockManifestStore) PutManifestBytes(ctx context.Context, id core.ContentID, data []byte) error {
	return nil
}

func (m *mockManifestStore) ListManifests(ctx context.Context) ([]core.ContentID, error) {
	m.listCount.Add(1)
	if m.listDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.listDelay):
		}
	}
	if m.listErr != nil {
		return nil, m.listErr
	}
	return m.manifests, nil
}

func TestRepublisherWorkerPoolAndExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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

	manifests := make([]core.ContentID, 20)
	for i := 0; i < 20; i++ {
		manifests[i] = core.ContentID{byte(i + 1), 0x02, 0x03}
	}

	store := &mockManifestStore{
		manifests: manifests,
	}

	republishAll(ctx, dht2, store)

	if store.listCount.Load() != 1 {
		t.Fatalf("expected listCount 1, got %d", store.listCount.Load())
	}
}

func TestStartRepublisherExecutionGuard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

	manifests := make([]core.ContentID, 5)
	for i := 0; i < 5; i++ {
		manifests[i] = core.ContentID{byte(i + 10)}
	}

	store := &mockManifestStore{
		manifests: manifests,
		listDelay: 200 * time.Millisecond,
	}

	StartRepublisher(ctx, dht1, store, 20*time.Millisecond)

	time.Sleep(350 * time.Millisecond)

	runs := store.listCount.Load()
	if runs > 2 {
		t.Fatalf("execution guard failed: expected at most 2 runs due to active lock, got %d", runs)
	}
}

func TestRepublisherItemTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
		{0x01, 0x02, 0x03},
		{0x04, 0x05, 0x06},
	}
	store := &mockManifestStore{
		manifests: manifests,
	}

	republishAllBounded(ctx, dht1, store, 2, 1*time.Nanosecond)
}

func TestRepublisherErrorHandling(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

	storeErr := &mockManifestStore{
		listErr: context.DeadlineExceeded,
	}

	republishAll(ctx, dht1, storeErr)
}
