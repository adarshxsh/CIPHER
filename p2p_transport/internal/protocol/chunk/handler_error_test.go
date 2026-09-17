package chunk_test

import (
	"context"
	"strings"
	"testing"

	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestStreamHandler_ErrorFloodingAndSanitization(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	tr2 := transport.NewTransport(h2)

	s, err := tr2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// 1. Send malicious error message with newlines, ANSI control codes, and non-printable bytes
	maliciousPayload := "Line1\nLine2\r\n\x1b[31mRed\x1b[0m\x00\x07"
	errMsg := chunk.BuildError(chunk.ErrBadRequest, maliciousPayload)
	if err := chunk.WriteMessage(s, errMsg); err != nil {
		t.Fatalf("Failed to write malicious error message: %v", err)
	}

	// 2. Flood stream with 50 high-frequency error frames
	longPayload := strings.Repeat("FloodMessage-", 30) // > 256 bytes
	floodMsg := chunk.BuildError(chunk.ErrInternal, longPayload)

	for i := 0; i < 50; i++ {
		if err := chunk.WriteMessage(s, floodMsg); err != nil {
			t.Fatalf("Failed to write flood error message %d: %v", i, err)
		}
	}
}
