package chunk_test

import (
	"context"
	"testing"

	"cipher/internal/protocol/chunk"
)

func TestStreamHandler_RateLimitationOnInvalidMessages(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	// Setup StreamHandler on server h1
	handler := chunk.NewStreamHandler(h1, eng1)
	if handler == nil {
		t.Fatal("expected non-nil StreamHandler")
	}

	ctx := context.Background()

	// Open 20 streams sequentially from peer h2 to peer h1, sending an unsupported version on each
	for i := 0; i < 20; i++ {
		s, err := h2.NewStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
		if err != nil {
			t.Fatalf("failed to open stream on iter %d: %v", i, err)
		}

		badMsg := &chunk.Message{
			Version: 99, // Unsupported version
			Type:    chunk.MsgRequestManifest,
			Payload: []byte("test"),
		}
		if err := chunk.WriteMessage(s, badMsg); err != nil {
			t.Fatalf("WriteMessage failed on iter %d: %v", i, err)
		}
		resp, err := chunk.ReadMessage(s)
		if err != nil {
			t.Fatalf("ReadMessage failed on iter %d: %v", i, err)
		}
		if resp.Type != chunk.MsgError {
			t.Errorf("expected MsgError, got %d", resp.Type)
		}
		s.Close()
	}
}
