package push

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/transport"
)

func setupMockNetwork(t *testing.T) (host.Host, host.Host) {
	t.Helper()
	m := mocknet.New()

	h1, err := m.GenPeer()
	if err != nil {
		t.Fatalf("failed to create peer 1: %v", err)
	}

	h2, err := m.GenPeer()
	if err != nil {
		t.Fatalf("failed to create peer 2: %v", err)
	}

	if err := m.LinkAll(); err != nil {
		t.Fatalf("failed to link peers: %v", err)
	}

	return h1, h2
}

func createTestManifest(t *testing.T) (core.ContentID, *manifest.Manifest, []byte) {
	t.Helper()
	var chunkID core.ChunkID
	_, _ = rand.Read(chunkID[:])

	var contentID core.ContentID
	_, _ = rand.Read(contentID[:])

	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			ID:   contentID,
			Size: 1024,
			Type: manifest.TypeFile,
		},
		ChunkIDs: []core.ChunkID{chunkID},
	}

	manifestBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("failed to serialize manifest: %v", err)
	}

	return contentID, m, manifestBytes
}

func TestPushHandler_CapacityBounds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, h2 := setupMockNetwork(t)

	// Create StreamHandler on h1 with MaxSessions = 2
	handler := NewStreamHandler(
		h1, nil, nil, true, AuthPolicyOpen, nil,
		WithMaxSessions(2),
	)

	// Fill capacity to 2 sessions
	var cid1, cid2 core.ContentID
	_, _ = rand.Read(cid1[:])
	_, _ = rand.Read(cid2[:])

	handler.sessionsMu.Lock()
	handler.sessions[cid1] = &PendingSession{ContentID: cid1, UpdatedAt: time.Now()}
	handler.sessions[cid2] = &PendingSession{ContentID: cid2, UpdatedAt: time.Now()}
	handler.sessionsMu.Unlock()

	if handler.SessionCount() != 2 {
		t.Fatalf("expected 2 active sessions, got %d", handler.SessionCount())
	}

	// Try sending a 3rd PUSH_MANIFEST from h2 to h1
	t2 := transport.NewTransport(h2)
	client, err := NewClient(ctx, t2, h1.ID())
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	cid3, m3, manifestBytes3 := createTestManifest(t)
	err = client.SendManifest(ctx, cid3, m3.ChunkIDs, manifestBytes3)
	if err == nil {
		t.Fatalf("expected SendManifest to fail due to capacity rejection, got nil")
	}

	expectedSubstring := "maximum session capacity reached"
	if !strings.Contains(err.Error(), expectedSubstring) {
		t.Errorf("expected error containing %q, got %q", expectedSubstring, err.Error())
	}

	if handler.SessionCount() != 2 {
		t.Errorf("session count should remain 2 after capacity rejection, got %d", handler.SessionCount())
	}
}

func TestPushHandler_ReacceptsExistingSessionAtCapacity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h1, h2 := setupMockNetwork(t)

	handler := NewStreamHandler(
		h1, nil, nil, true, AuthPolicyOpen, nil,
		WithMaxSessions(2),
	)

	cid1, m1, manifestBytes1 := createTestManifest(t)
	var cid2 core.ContentID
	_, _ = rand.Read(cid2[:])

	handler.sessionsMu.Lock()
	handler.sessions[cid1] = &PendingSession{ContentID: cid1, UpdatedAt: time.Now()}
	handler.sessions[cid2] = &PendingSession{ContentID: cid2, UpdatedAt: time.Now()}
	handler.sessionsMu.Unlock()

	// Re-sending manifest for cid1 (which is already in sessions) should succeed
	t2 := transport.NewTransport(h2)
	client, err := NewClient(ctx, t2, h1.ID())
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	err = client.SendManifest(ctx, cid1, m1.ChunkIDs, manifestBytes1)
	if err != nil {
		t.Fatalf("expected re-sent manifest for existing session to succeed, got: %v", err)
	}
}

func TestPushHandler_TTLSweeper_PurgesStaleSessions(t *testing.T) {
	ttl := 10 * time.Minute
	handler := NewStreamHandler(
		nil, nil, nil, true, AuthPolicyOpen, nil,
		WithSessionTTL(ttl),
	)

	now := time.Now()

	var cidStale, cidActive1, cidActive2 core.ContentID
	_, _ = rand.Read(cidStale[:])
	_, _ = rand.Read(cidActive1[:])
	_, _ = rand.Read(cidActive2[:])

	handler.sessionsMu.Lock()
	// Stale session (inactive 15 minutes)
	handler.sessions[cidStale] = &PendingSession{
		ContentID: cidStale,
		UpdatedAt: now.Add(-15 * time.Minute),
	}

	// Active session 1 (inactive 5 minutes)
	handler.sessions[cidActive1] = &PendingSession{
		ContentID: cidActive1,
		UpdatedAt: now.Add(-5 * time.Minute),
	}

	// Active session 2 (updated just now)
	handler.sessions[cidActive2] = &PendingSession{
		ContentID: cidActive2,
		UpdatedAt: now,
	}
	handler.sessionsMu.Unlock()

	if handler.SessionCount() != 3 {
		t.Fatalf("expected 3 sessions before sweep, got %d", handler.SessionCount())
	}

	purgedCount := handler.SweepStaleSessionsBefore(now)
	if purgedCount != 1 {
		t.Errorf("expected 1 session purged, got %d", purgedCount)
	}

	if handler.SessionCount() != 2 {
		t.Errorf("expected 2 active sessions remaining, got %d", handler.SessionCount())
	}

	if _, exists := handler.GetSession(cidStale); exists {
		t.Errorf("stale session %x should have been purged", cidStale)
	}

	if _, exists := handler.GetSession(cidActive1); !exists {
		t.Errorf("active session 1 %x should still exist", cidActive1)
	}

	if _, exists := handler.GetSession(cidActive2); !exists {
		t.Errorf("active session 2 %x should still exist", cidActive2)
	}
}

func TestPushHandler_TTLSweeper_BackgroundWorker(t *testing.T) {
	handler := NewStreamHandler(
		nil, nil, nil, true, AuthPolicyOpen, nil,
		WithSessionTTL(10*time.Millisecond),
		WithSweepInterval(10*time.Millisecond),
	)

	var cid core.ContentID
	_, _ = rand.Read(cid[:])

	handler.sessionsMu.Lock()
	handler.sessions[cid] = &PendingSession{
		ContentID: cid,
		UpdatedAt: time.Now().Add(-50 * time.Millisecond),
	}
	handler.sessionsMu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler.StartSweeper(ctx)

	// Wait briefly for sweeper loop to trigger
	time.Sleep(50 * time.Millisecond)

	if handler.SessionCount() != 0 {
		t.Errorf("expected background sweeper to purge stale session, count=%d", handler.SessionCount())
	}

	// Cancel context and verify clean shutdown
	cancel()
	time.Sleep(20 * time.Millisecond)
}
