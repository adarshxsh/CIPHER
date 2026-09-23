package push

import (
	"crypto/rand"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestPushSessionTTLSweeper(t *testing.T) {
	handler := NewStreamHandler(nil, nil, nil, true, AuthPolicyOpen, nil)
	defer handler.Close()

	var cidExpired, cidActive core.ContentID
	_, _ = rand.Read(cidExpired[:])
	_, _ = rand.Read(cidActive[:])

	now := time.Now()

	handler.sessionsMu.Lock()
	handler.sessions[cidExpired] = &PendingSession{
		ContentID:       cidExpired,
		StartedAt:       now.Add(-15 * time.Minute),
		UpdatedAt:       now.Add(-11 * time.Minute), // Inactive for > 10 min
		ExpectedChunks:  make(map[core.ChunkID]struct{}),
		CommittedChunks: make(map[core.ChunkID]bool),
	}
	handler.sessions[cidActive] = &PendingSession{
		ContentID:       cidActive,
		StartedAt:       now.Add(-5 * time.Minute),
		UpdatedAt:       now.Add(-2 * time.Minute), // Inactive for < 10 min
		ExpectedChunks:  make(map[core.ChunkID]struct{}),
		CommittedChunks: make(map[core.ChunkID]bool),
	}
	handler.sessionsMu.Unlock()

	purged := handler.SweepExpiredSessions()
	if purged != 1 {
		t.Errorf("expected 1 session purged, got %d", purged)
	}

	handler.sessionsMu.RLock()
	_, expiredExists := handler.sessions[cidExpired]
	_, activeExists := handler.sessions[cidActive]
	handler.sessionsMu.RUnlock()

	if expiredExists {
		t.Errorf("expected expired session %x to be purged, but still exists", cidExpired)
	}
	if !activeExists {
		t.Errorf("expected active session %x to remain, but was purged", cidActive)
	}
}

func TestPushSessionBackgroundSweeperTicker(t *testing.T) {
	handler := NewStreamHandler(
		nil, nil, nil, true, AuthPolicyOpen, nil,
		WithSessionTTL(50*time.Millisecond),
		WithSweepInterval(10*time.Millisecond),
	)

	var cid core.ContentID
	_, _ = rand.Read(cid[:])

	handler.sessionsMu.Lock()
	handler.sessions[cid] = &PendingSession{
		ContentID:       cid,
		StartedAt:       time.Now().Add(-100 * time.Millisecond),
		UpdatedAt:       time.Now().Add(-100 * time.Millisecond),
		ExpectedChunks:  make(map[core.ChunkID]struct{}),
		CommittedChunks: make(map[core.ChunkID]bool),
	}
	handler.sessionsMu.Unlock()

	// Wait for background sweeper ticker to run and clean up
	deadline := time.Now().Add(500 * time.Millisecond)
	cleaned := false
	for time.Now().Before(deadline) {
		handler.sessionsMu.RLock()
		_, exists := handler.sessions[cid]
		handler.sessionsMu.RUnlock()

		if !exists {
			cleaned = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !cleaned {
		t.Errorf("expected background sweeper ticker to purge expired session %x", cid)
	}

	if err := handler.Close(); err != nil {
		t.Errorf("handler.Close() returned error: %v", err)
	}
}

func TestPushSessionActiveMultiChunk(t *testing.T) {
	handler := NewStreamHandler(
		nil, nil, nil, true, AuthPolicyOpen, nil,
		WithSessionTTL(10*time.Minute),
	)
	defer handler.Close()

	var cid core.ContentID
	var chunk1, chunk2 core.ChunkID
	_, _ = rand.Read(cid[:])
	_, _ = rand.Read(chunk1[:])
	_, _ = rand.Read(chunk2[:])

	// Session created 8 minutes ago
	now := time.Now()
	handler.sessionsMu.Lock()
	session := &PendingSession{
		ContentID:       cid,
		StartedAt:       now.Add(-8 * time.Minute),
		UpdatedAt:       now.Add(-8 * time.Minute),
		ExpectedChunks:  map[core.ChunkID]struct{}{chunk1: {}, chunk2: {}},
		CommittedChunks: make(map[core.ChunkID]bool),
	}
	handler.sessions[cid] = session
	handler.sessionsMu.Unlock()

	// 1st sweep: session updated 8 minutes ago, TTL 10m -> should remain
	purged := handler.SweepExpiredSessions()
	if purged != 0 {
		t.Fatalf("expected 0 sessions purged, got %d", purged)
	}

	// Session receives a chunk update -> UpdatedAt refreshed to now
	handler.sessionsMu.Lock()
	session.CommittedChunks[chunk1] = true
	session.UpdatedAt = time.Now()
	handler.sessionsMu.Unlock()

	// Fast forward time conceptually or check that with UpdatedAt=now, session survives another sweep
	purged = handler.SweepExpiredSessions()
	if purged != 0 {
		t.Fatalf("expected 0 sessions purged after chunk update, got %d", purged)
	}

	handler.sessionsMu.RLock()
	_, exists := handler.sessions[cid]
	handler.sessionsMu.RUnlock()

	if !exists {
		t.Errorf("active multi-chunk session should remain intact in sessions map")
	}
}
