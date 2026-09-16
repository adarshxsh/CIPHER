package chunk_test

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"

	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
)

func TestPeerErrorRateLimiter_ThrottlingAndTTL(t *testing.T) {
	peerID := peer.ID("test-peer-1")

	// Create rate limiter: 5 msgs/sec, burst 3, 500ms TTL
	limiter := chunk.NewPeerErrorRateLimiter(rate.Limit(5), 3, 500*time.Millisecond)

	// First 3 logs should be allowed
	for i := 0; i < 3; i++ {
		allowed, suppressed := limiter.Allow(peerID)
		if !allowed {
			t.Errorf("expected attempt %d to be allowed", i)
		}
		if suppressed != 0 {
			t.Errorf("expected 0 suppressed logs, got %d", suppressed)
		}
	}

	// 4th and 5th attempts should be rate-limited
	for i := 0; i < 2; i++ {
		allowed, _ := limiter.Allow(peerID)
		if allowed {
			t.Errorf("expected attempt after burst limit to be suppressed")
		}
	}

	// Sleep 250ms (within TTL of 500ms), rate limiter tokens replenish (5/sec = 1 per 200ms)
	time.Sleep(250 * time.Millisecond)

	// Next allowed attempt should report suppressed count of 2
	allowed, suppressed := limiter.Allow(peerID)
	if !allowed {
		t.Fatalf("expected allowed after wait")
	}
	if suppressed != 2 {
		t.Errorf("expected 2 suppressed logs, got %d", suppressed)
	}

	// Test TTL cleanup: sleep > 500ms TTL so peerID entry expires
	time.Sleep(600 * time.Millisecond)
	otherPeer := peer.ID("test-peer-2")
	limiter.Allow(otherPeer) // Triggers TTL cleanup loop

	// Next attempt for peerID after TTL cleanup starts fresh with 0 suppressed
	allowed, suppressed = limiter.Allow(peerID)
	if !allowed {
		t.Fatalf("expected allowed after TTL expiration")
	}
	if suppressed != 0 {
		t.Errorf("expected 0 suppressed logs after TTL cleanup, got %d", suppressed)
	}
}

func TestStreamHandler_LogSanitizationIntegration(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	// Create handler on server
	chunk.NewStreamHandler(h1, eng1)

	// Capture log output
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	ctx := context.Background()

	// Peer 2 opens a stream directly to h1
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("failed to open stream: %v", err)
	}
	defer s.Close()

	// Send an invalid message type with injected newlines and ANSI sequences
	maliciousPayload := []byte("\x1b[31mINJECTED HEADER\x1b[0m\n[INFO] Fake log entry\r\nSecond Line")
	msg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    0xFE,
		Payload: maliciousPayload,
	}
	if err := chunk.WriteMessage(s, msg); err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	// Read response
	_, _ = chunk.ReadMessage(s)

	logOutput := logBuf.String()

	// Verify log output does not contain raw newline linebreaks injected by malicious message
	if strings.Contains(logOutput, "\n[INFO] Fake log entry") {
		t.Errorf("log injection detected! Output contains raw unescaped line break")
	}
	if strings.Contains(logOutput, "\x1b[31m") {
		t.Errorf("ANSI control sequence leak detected! Output contains unescaped ESC byte")
	}
}

func TestStreamHandler_RateLimiting_NoStreamBlock(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	// Create stream handler with low burst limit (1 log allowed)
	chunk.NewStreamHandler(h1, eng1, chunk.WithErrorRateLimit(rate.Limit(1), 1))

	ctx := context.Background()

	// Send multiple invalid requests rapid-fire over separate streams
	for i := 0; i < 10; i++ {
		s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
		if err != nil {
			t.Fatalf("failed to open stream %d: %v", i, err)
		}

		// Send unknown message type
		msg := &chunk.Message{
			Version: chunk.CurrentMessageVersion,
			Type:    0xFF,
			Payload: []byte("test"),
		}
		if err := chunk.WriteMessage(s, msg); err != nil {
			s.Close()
			t.Fatalf("failed to send message %d: %v", i, err)
		}

		resp, err := chunk.ReadMessage(s)
		if err != nil {
			s.Close()
			t.Fatalf("failed to read response %d: %v", i, err)
		}

		if resp.Type != chunk.MsgError {
			t.Errorf("iteration %d: expected MsgError response, got %v", i, resp.Type)
		}
		s.Close()
	}
}
