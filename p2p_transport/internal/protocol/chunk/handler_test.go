package chunk_test

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"

	"cipher/internal/protocol/chunk"
)

func TestStreamHandler_PerPeerRateLimiting(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	// Create StreamHandler with rate limit of 2 msg/sec, burst 2
	handler := chunk.NewStreamHandlerWithRateLimit(h1, eng1, rate.Limit(2), 2)

	peerA := h2.ID()
	var peerB peer.ID = "12D3KooWOtherPeer123456789"

	// Peer A: 1st allowed
	if !handler.AllowErrorLog(peerA) {
		t.Errorf("Expected 1st call for Peer A to be allowed")
	}
	// Peer A: 2nd allowed
	if !handler.AllowErrorLog(peerA) {
		t.Errorf("Expected 2nd call for Peer A to be allowed")
	}
	// Peer A: 3rd throttled (burst exhausted)
	if handler.AllowErrorLog(peerA) {
		t.Errorf("Expected 3rd call for Peer A to be throttled")
	}

	// Peer B: independent limit, should be allowed!
	if !handler.AllowErrorLog(peerB) {
		t.Errorf("Expected 1st call for Peer B to be allowed independently")
	}
	if !handler.AllowErrorLog(peerB) {
		t.Errorf("Expected 2nd call for Peer B to be allowed independently")
	}
	if handler.AllowErrorLog(peerB) {
		t.Errorf("Expected 3rd call for Peer B to be throttled")
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (sb *safeBuffer) Write(p []byte) (n int, err error) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.Write(p)
}

func (sb *safeBuffer) String() string {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	return sb.buf.String()
}

func TestStreamHandler_LogSanitizationIntegration(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()

	// Capture log output safely
	var logBuf safeBuffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	// Send top-level MsgError containing newline, carriage return, and ANSI escape code
	errFrame := chunk.BuildError(chunk.ErrBadRequest, "Attack:\n[FORGED LOG] Admin login\r\x1b[31mRed\x1b[0m")

	// Open raw stream to h1 to send error message directly
	s, err := h2.NewStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	if err := chunk.WriteMessage(s, errFrame); err != nil {
		t.Fatalf("Failed to write error message: %v", err)
	}

	// Small pause for stream handler to process
	time.Sleep(50 * time.Millisecond)

	logOutput := logBuf.String()
	if strings.Contains(logOutput, "[Chunk Protocol]") {
		// Ensure forged log prefix is escaped properly and single-line
		if strings.Contains(logOutput, "[FORGED LOG]") && !strings.Contains(logOutput, "\\n[FORGED LOG]") {
			t.Errorf("Found unescaped newline forgery in log output: %q", logOutput)
		}
	}
}
