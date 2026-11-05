package chunk

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"golang.org/x/time/rate"
)

func TestStreamHandler_LimiterLifecycleAndGarbageCollection(t *testing.T) {
	h := NewStreamHandler(nil, nil)
	peerA := peer.ID("test-peer-A")
	peerB := peer.ID("test-peer-B")

	// 1. Get limiter for peerA (stream 1)
	limiterA1 := h.getLimiter(peerA)
	if limiterA1 == nil {
		t.Fatal("expected non-nil limiter for peerA")
	}

	h.mu.Lock()
	if len(h.limiters) != 1 {
		t.Errorf("expected 1 limiter in map, got %d", len(h.limiters))
	}
	if h.limiters[peerA].refCount != 1 {
		t.Errorf("expected refCount 1 for peerA, got %d", h.limiters[peerA].refCount)
	}
	h.mu.Unlock()

	// 2. Get limiter for peerA (stream 2)
	limiterA2 := h.getLimiter(peerA)
	if limiterA1 != limiterA2 {
		t.Error("expected same limiter instance for same peer")
	}

	h.mu.Lock()
	if h.limiters[peerA].refCount != 2 {
		t.Errorf("expected refCount 2 for peerA, got %d", h.limiters[peerA].refCount)
	}
	h.mu.Unlock()

	// 3. Get limiter for peerB
	h.getLimiter(peerB)
	h.mu.Lock()
	if len(h.limiters) != 2 {
		t.Errorf("expected 2 limiters in map, got %d", len(h.limiters))
	}
	h.mu.Unlock()

	// 4. Release stream 1 of peerA
	h.putLimiter(peerA)
	h.mu.Lock()
	if h.limiters[peerA].refCount != 1 {
		t.Errorf("expected refCount 1 for peerA after put, got %d", h.limiters[peerA].refCount)
	}
	h.mu.Unlock()

	// 5. Release stream 2 of peerA
	h.putLimiter(peerA)
	h.mu.Lock()
	if _, exists := h.limiters[peerA]; exists {
		t.Error("expected peerA to be garbage collected from limiters map")
	}
	if len(h.limiters) != 1 {
		t.Errorf("expected 1 remaining limiter in map, got %d", len(h.limiters))
	}
	h.mu.Unlock()

	// 6. Release peerB
	h.putLimiter(peerB)
	h.mu.Lock()
	if len(h.limiters) != 0 {
		t.Errorf("expected map to be empty, got %d items", len(h.limiters))
	}
	h.mu.Unlock()
}

func TestStreamHandler_ThrottlesErrorLogsTo5PerSecond(t *testing.T) {
	var buf bytes.Buffer
	origWriter := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(origWriter)

	h := NewStreamHandler(nil, nil)
	peerA := peer.ID("test-peer-A")
	limiter := h.getLimiter(peerA)
	defer h.putLimiter(peerA)

	// Fire 20 rapid error log attempts
	for i := 0; i < 20; i++ {
		h.logError(limiter, "[Chunk Protocol] Error test message %d", i)
	}

	logOutput := buf.String()
	lines := strings.Split(strings.TrimSpace(logOutput), "\n")
	if len(lines) != 5 {
		t.Fatalf("expected 5 log lines in output, got %d lines:\n%s", len(lines), logOutput)
	}
}

func TestStreamHandler_ConfigurableRateLimit(t *testing.T) {
	h := NewStreamHandler(nil, nil)
	h.SetRateLimit(rate.Limit(10), 10)

	peerA := peer.ID("test-peer-configurable")
	limiter := h.getLimiter(peerA)
	defer h.putLimiter(peerA)

	allowedCount := 0
	for i := 0; i < 20; i++ {
		if limiter.Allow() {
			allowedCount++
		}
	}

	if allowedCount != 10 {
		t.Fatalf("expected 10 allowed with configurable limit (burst 10), got %d", allowedCount)
	}
}
