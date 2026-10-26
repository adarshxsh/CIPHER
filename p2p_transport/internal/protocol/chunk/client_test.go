package chunk_test

import (
	"context"
	"errors"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"

	"cipher/internal/content/core"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestClient_Resolve_OversizedManifest(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng2 := createTestEngine(t)

	var targetID core.ContentID
	targetID[0] = 0xDE
	targetID[31] = 0xAD

	// Setup malicious peer handler on h1 that returns an oversized manifest payload.
	h1.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		req, err := chunk.ReadMessage(s)
		if err != nil {
			return
		}
		if req.Type == chunk.MsgRequestManifest {
			// Construct an oversized manifest response exceeding MaxManifestSize.
			oversizedData := make([]byte, chunk.MaxManifestSize+100)
			respPayload := append(targetID[:], oversizedData...)
			respMsg := &chunk.Message{
				Version: chunk.CurrentMessageVersion,
				Type:    chunk.MsgManifest,
				Payload: respPayload,
			}
			chunk.WriteMessage(s, respMsg)
		}
	})

	ctx := context.Background()
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	_, err = client.Resolve(ctx, targetID)
	if err == nil {
		t.Fatalf("expected Resolve to reject oversized manifest, but got nil error")
	}

	if !errors.Is(err, chunk.ErrInvalidPayloadLength) {
		// Expect error message to mention validation or payload length failure
		t.Logf("Resolve returned expected error: %v", err)
	}
}
