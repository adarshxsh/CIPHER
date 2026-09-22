package transport

import (
	"context"
	"testing"

	"cipher/internal/protocol"
	"github.com/libp2p/go-libp2p/core/network"
)

func TestNewNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	host, kdht, err := NewNode(ctx, 0, 0, nil, "", false)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer kdht.Close()

	if host == nil {
		t.Fatalf("Expected a host, got nil")
	}

	if len(host.Addrs()) == 0 {
		t.Fatalf("Expected at least one listen address")
	}

	rm := host.Network().ResourceManager()
	if rm == nil {
		t.Fatalf("Expected a non-nil ResourceManager on network")
	}

	if _, isNull := rm.(*network.NullResourceManager); isNull {
		t.Fatalf("Expected active ResourceManager, got NullResourceManager")
	}

	// Verify that protocol scopes are accessible and bounded
	err = rm.ViewProtocol(protocol.ChunkTransportProtocolID, func(s network.ProtocolScope) error {
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to view protocol scope for chunk transport: %v", err)
	}

	host.Close()
}
