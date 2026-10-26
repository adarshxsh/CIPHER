package discovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestFindProviders_BoundaryValidation(t *testing.T) {
	ctx := context.Background()
	var dummyID core.ContentID

	// 1. Nil context
	_, err := FindProviders(nil, nil, dummyID, 5)
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("expected ErrNilContext, got %v", err)
	}

	// 2. Canceled context
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, err = FindProviders(canceledCtx, nil, dummyID, 5)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// 3. Nil DHT
	_, err = FindProviders(ctx, nil, dummyID, 5)
	if !errors.Is(err, ErrNilDHT) {
		t.Fatalf("expected ErrNilDHT, got %v", err)
	}

	// 4. Invalid limit (zero or negative)
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create host: %v", err)
	}
	defer h.Close()

	dhtNode, err := NewDHT(h, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht: %v", err)
	}
	defer dhtNode.Close()

	_, err = FindProviders(ctx, dhtNode, dummyID, 0)
	if !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("expected ErrInvalidLimit for limit=0, got %v", err)
	}

	_, err = FindProviders(ctx, dhtNode, dummyID, -5)
	if !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("expected ErrInvalidLimit for limit=-5, got %v", err)
	}
}

func TestProvide_BoundaryValidation(t *testing.T) {
	ctx := context.Background()
	var dummyID core.ContentID

	// 1. Nil context
	err := Provide(nil, nil, dummyID)
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("expected ErrNilContext, got %v", err)
	}

	// 2. Canceled context
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	err = Provide(canceledCtx, nil, dummyID)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// 3. Nil DHT
	err = Provide(ctx, nil, dummyID)
	if !errors.Is(err, ErrNilDHT) {
		t.Fatalf("expected ErrNilDHT, got %v", err)
	}
}

func TestFindProviders_LimitAndCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// 1. Create Bootstrap Node
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

	h1Info := peer.AddrInfo{ID: h1.ID(), Addrs: h1.Addrs()}

	// 2. Create Provider Node 1
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

	if err := Bootstrap(ctx, dht2, h2, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht2 bootstrap failed: %v", err)
	}

	// 3. Create Provider Node 2
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

	// Content ID to provide
	var contentID core.ContentID
	for i := range contentID {
		contentID[i] = byte(i + 1)
	}

	// Announce content from h2 and h3
	if err := Provide(ctx, dht2, contentID); err != nil {
		t.Fatalf("dht2 Provide failed: %v", err)
	}
	if err := Provide(ctx, dht3, contentID); err != nil {
		t.Fatalf("dht3 Provide failed: %v", err)
	}

	// 4. Create Query Node
	h4, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatalf("failed to create h4: %v", err)
	}
	defer h4.Close()

	dht4, err := NewDHT(h4, dht.ModeServer)
	if err != nil {
		t.Fatalf("failed to create dht4: %v", err)
	}
	defer dht4.Close()

	if err := Bootstrap(ctx, dht4, h4, []peer.AddrInfo{h1Info}); err != nil {
		t.Fatalf("dht4 bootstrap failed: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Test limit = 1: Should stop immediately after receiving 1 provider
	providers, err := FindProviders(ctx, dht4, contentID, 1)
	if err != nil {
		t.Fatalf("FindProviders with limit 1 failed: %v", err)
	}
	if len(providers) != 1 {
		t.Fatalf("expected 1 provider for limit=1, got %d", len(providers))
	}

	// Test limit = 5: Should return all available providers (2)
	providers, err = FindProviders(ctx, dht4, contentID, 5)
	if err != nil {
		t.Fatalf("FindProviders with limit 5 failed: %v", err)
	}
	if len(providers) != 2 {
		t.Fatalf("expected 2 providers for limit=5, got %d", len(providers))
	}
}
