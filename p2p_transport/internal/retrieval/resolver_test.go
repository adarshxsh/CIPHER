package retrieval

import (
	"cipher/internal/content/core"
	"cipher/internal/discovery"
	"cipher/internal/identity"
	"cipher/internal/transport"
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestResolveManifest_DropsUntrustedProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	priv1, _ := identity.GenerateEphemeral()
	h1, dht1, err := transport.NewNode(ctx, 0, 0, priv1, "", false)
	if err != nil {
		t.Fatalf("failed to create node 1: %v", err)
	}
	defer h1.Close()
	defer dht1.Close()

	priv2, _ := identity.GenerateEphemeral()
	h2, dht2, err := transport.NewNode(ctx, 0, 0, priv2, "", false)
	if err != nil {
		t.Fatalf("failed to create node 2: %v", err)
	}
	defer h2.Close()
	defer dht2.Close()

	// Bootstrap dht1 with h2 and dht2 with h1
	if err := discovery.Bootstrap(ctx, dht1, h1, []peer.AddrInfo{{ID: h2.ID(), Addrs: h2.Addrs()}}); err != nil {
		t.Fatalf("bootstrap dht1 failed: %v", err)
	}
	if err := discovery.Bootstrap(ctx, dht2, h2, []peer.AddrInfo{{ID: h1.ID(), Addrs: h1.Addrs()}}); err != nil {
		t.Fatalf("bootstrap dht2 failed: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("testcontentid1234567890123456789"))

	// h1 calls dht1.Provide directly WITHOUT storing a signed provider record (simulating rogue provider)
	cid, err := discovery.ContentIDToCID(contentID)
	if err != nil {
		t.Fatalf("failed to convert content ID to CID: %v", err)
	}
	if err := dht1.Provide(ctx, cid, true); err != nil {
		t.Fatalf("failed to provide cid: %v", err)
	}

	// Attempt to resolve manifest from untrusted provider h1.ID()
	trans2 := transport.NewTransport(h2)
	_, err = ResolveManifest(ctx, contentID, dht2, trans2, nil, []peer.ID{h1.ID()})
	if err == nil {
		t.Fatal("expected ResolveManifest to drop untrusted provider without valid signed record, but got success")
	}
}

func TestResolveManifest_AcceptsVerifiedSignedProvider(t *testing.T) {
	ctx, cancel := context.Background(), func() {}
	ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	priv1, _ := identity.GenerateEphemeral()
	h1, dht1, err := transport.NewNode(ctx, 0, 0, priv1, "", false)
	if err != nil {
		t.Fatalf("failed to create node 1: %v", err)
	}
	defer h1.Close()
	defer dht1.Close()

	priv2, _ := identity.GenerateEphemeral()
	h2, dht2, err := transport.NewNode(ctx, 0, 0, priv2, "", false)
	if err != nil {
		t.Fatalf("failed to create node 2: %v", err)
	}
	defer h2.Close()
	defer dht2.Close()

	// Bootstrap dht1 with h2 and dht2 with h1
	if err := discovery.Bootstrap(ctx, dht1, h1, []peer.AddrInfo{{ID: h2.ID(), Addrs: h2.Addrs()}}); err != nil {
		t.Fatalf("bootstrap dht1 failed: %v", err)
	}
	if err := discovery.Bootstrap(ctx, dht2, h2, []peer.AddrInfo{{ID: h1.ID(), Addrs: h1.Addrs()}}); err != nil {
		t.Fatalf("bootstrap dht2 failed: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("testcontentid1234567890123456789"))

	// h1 announces with valid signature
	if err := discovery.Provide(ctx, dht1, priv1, contentID); err != nil {
		t.Fatalf("Provide failed: %v", err)
	}

	// Verify that discovery.VerifyProviderForContent passes for h1
	if err := discovery.VerifyProviderForContent(ctx, dht2, contentID, h1.ID()); err != nil {
		t.Fatalf("expected VerifyProviderForContent to succeed for h1, got: %v", err)
	}
}
