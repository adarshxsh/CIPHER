package chunk_test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"golang.org/x/time/rate"

	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *syncBuffer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf.Reset()
}

func setupThreePeerMockNetwork(t testing.TB) (host.Host, host.Host, host.Host) {
	mn := mocknet.New()

	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h3, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2, h3
}

func TestStreamHandler_LogSanitization(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	_ = chunk.NewStreamHandler(h1, eng1)
	_ = chunk.NewStreamHandler(h2, eng2)

	logBuf := &syncBuffer{}
	log.SetOutput(logBuf)
	defer log.SetOutput(log.Writer())

	ctx := context.Background()
	tp2 := transport.NewTransport(h2)

	// Open stream to Peer 1
	s, err := tp2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}

	// Send an error message with log injection payload (\n, \r, control codes)
	maliciousText := "Error line 1\n[SYSTEM] Admin access granted\r\n\x1b[31mCRITICAL\x1b[0m"
	errMsg := chunk.BuildError(chunk.ErrBadRequest, maliciousText)

	if err := chunk.WriteMessage(s, errMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	s.Close()
	time.Sleep(50 * time.Millisecond) // brief wait for background goroutine stream handling

	logOutput := logBuf.String()

	// Verify newline injection was neutralized
	if strings.Contains(logOutput, "\n[SYSTEM] Admin access granted") {
		t.Errorf("Unescaped newline found in log output!\nLog content:\n%s", logOutput)
	}

	// Verify escaped sequence is present
	expectedSubstring := "Error line 1\\n[SYSTEM] Admin access granted\\r\\n\\x1b[31mCRITICAL\\x1b[0m"
	if !strings.Contains(logOutput, expectedSubstring) {
		t.Errorf("Expected sanitized error text in log output, got:\n%s", logOutput)
	}
}

func TestStreamHandler_PerPeerRateLimitingAndContinuity(t *testing.T) {
	h1, h2, h3 := setupThreePeerMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)
	eng3 := createTestEngine(t)

	handler1 := chunk.NewStreamHandler(h1, eng1)
	// Set low rate limit for test: 1 log msg/sec, burst 2
	handler1.SetLogRateLimit(rate.Limit(1.0), 2)

	_ = chunk.NewStreamHandler(h2, eng2)
	_ = chunk.NewStreamHandler(h3, eng3)

	logBuf := &syncBuffer{}
	log.SetOutput(logBuf)
	defer log.SetOutput(log.Writer())

	ctx := context.Background()

	// 1. Peer A (h2) sends 10 rapid error messages
	tp2 := transport.NewTransport(h2)
	for i := 0; i < 10; i++ {
		s, err := tp2.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
		if err != nil {
			t.Fatalf("Peer A OpenStream failed: %v", err)
		}
		errMsg := chunk.BuildError(chunk.ErrBadRequest, fmt.Sprintf("Burst error %d from Peer A", i))
		_ = chunk.WriteMessage(s, errMsg)
		s.Close()
	}

	time.Sleep(50 * time.Millisecond)

	logOutputA := logBuf.String()
	// Count occurrences of "Burst error" in log output
	countA := strings.Count(logOutputA, "Burst error")
	if countA > 2 {
		t.Errorf("Peer A log count should be throttled to burst limit 2, got %d. Log output:\n%s", countA, logOutputA)
	}

	// Reset log buffer
	logBuf.Reset()

	// 2. Peer B (h3) sends 1 error message - verify it is NOT suppressed by Peer A's burst
	tp3 := transport.NewTransport(h3)
	s3, err := tp3.OpenStream(ctx, h1.ID(), "/cipher/chunk/1.0.0")
	if err != nil {
		t.Fatalf("Peer B OpenStream failed: %v", err)
	}
	errMsgB := chunk.BuildError(chunk.ErrBadRequest, "Error from Peer B")
	_ = chunk.WriteMessage(s3, errMsgB)
	s3.Close()

	time.Sleep(50 * time.Millisecond)

	logOutputB := logBuf.String()
	if !strings.Contains(logOutputB, "Error from Peer B") {
		t.Errorf("Peer B error log was suppressed by Peer A rate limit! Log output:\n%s", logOutputB)
	}

	// 3. Verify message delivery & stream state continuity for Peer A even when rate limited
	m, err := eng1.Ingest(ctx, bytes.NewReader([]byte("test manifest content")), "file")
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client2, err := chunk.NewClient(ctx, tp2, h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed for Peer A: %v", err)
	}
	defer client2.Close()

	resolvedData, err := client2.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Peer A Resolve failed after rate limit reached: %v", err)
	}
	if len(resolvedData) == 0 {
		t.Errorf("Expected valid manifest data for Peer A")
	}
}
