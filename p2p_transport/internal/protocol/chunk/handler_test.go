package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"log"
	"strings"
	"testing"

	"golang.org/x/time/rate"

	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
)

func TestStreamHandler_LogRateLimiting(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	_ = createTestEngine(t) // eng2

	handler := chunk.NewStreamHandler(h1, eng1)
	// Set log limiter with a burst limit of 2 (no refill rate)
	handler.SetLogLimiter(rate.NewLimiter(rate.Limit(0), 2))

	ctx := context.Background()
	data := make([]byte, 1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	chunkID := m.ChunkIDs[0]

	// Capture log output
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)

	// Send 4 error ACKs directly over streams
	for i := 0; i < 4; i++ {
		s, err := h2.NewStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
		if err != nil {
			t.Fatalf("Failed to open stream: %v", err)
		}

		// Send REQUEST_CHUNK
		req := chunk.BuildRequestChunk(chunkID)
		if err := chunk.WriteMessage(s, req); err != nil {
			s.Close()
			t.Fatalf("Failed to send REQUEST_CHUNK: %v", err)
		}

		// Read CHUNK response
		resp, err := chunk.ReadMessage(s)
		if err != nil {
			s.Close()
			t.Fatalf("Failed to read CHUNK: %v", err)
		}
		if resp.Type != chunk.MsgChunk {
			s.Close()
			t.Fatalf("Expected CHUNK, got %d", resp.Type)
		}

		// Send MsgError containing newline injection attempt as ACK
		errMsg := chunk.BuildError(chunk.ErrBadRequest, "malicious error\n2026/09/21 [Chunk Protocol] Fake Log Entry")
		if err := chunk.WriteMessage(s, errMsg); err != nil {
			s.Close()
			t.Fatalf("Failed to send MsgError: %v", err)
		}

		s.Close()
	}

	logStr := logBuf.String()

	// Verify sanitized string is present in logs
	if !strings.Contains(logStr, `malicious error\n2026/09/21 [Chunk Protocol] Fake Log Entry`) {
		t.Errorf("Expected sanitized error string in log output, got:\n%s", logStr)
	}

	// Verify log injection (raw newline) did NOT occur
	if strings.Contains(logStr, "malicious error\n2026/09/21") {
		t.Errorf("Found unescaped newline in log output (log injection vulnerability!):\n%s", logStr)
	}

	// Count occurrences of log messages for "Client reported error"
	occurrences := strings.Count(logStr, "Client reported error")
	if occurrences != 2 {
		t.Errorf("Expected rate limiter to restrict error logs to 2 occurrences, got %d. Logs:\n%s", occurrences, logStr)
	}
}
