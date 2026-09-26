package push

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
)

func createDummyManifest(contentID core.ContentID, chunkIDs []core.ChunkID) (*manifest.Manifest, []byte) {
	m := &manifest.Manifest{
		Descriptor: manifest.ContentDescriptor{
			ID:   contentID,
			Size: 1024,
		},
		ChunkIDs: chunkIDs,
	}
	bytes, _ := m.Serialize()
	return m, bytes
}

func generatePeerID(i int) peer.ID {
	return peer.ID(fmt.Sprintf("test-peer-%d", i))
}

func TestPerPeerQuotaExceeded(t *testing.T) {
	config := SessionConfig{
		MaxGlobalSessions: 100,
		MaxPeerSessions:   2,
		SessionTTL:        5 * time.Minute,
		PruneInterval:     0, // Disable background ticker for unit test
	}
	sm := NewSessionManager(config)
	defer sm.Close()

	peerA := generatePeerID(1)
	peerB := generatePeerID(2)

	var id1, id2, id3, id4 core.ContentID
	id1[0] = 1
	id2[0] = 2
	id3[0] = 3
	id4[0] = 4

	m1, b1 := createDummyManifest(id1, nil)
	m2, b2 := createDummyManifest(id2, nil)
	m3, b3 := createDummyManifest(id3, nil)
	m4, b4 := createDummyManifest(id4, nil)

	// Peer A creates session 1 and session 2 (reaches quota = 2)
	_, err := sm.CreateSession(peerA, id1, m1, b1, nil)
	if err != nil {
		t.Fatalf("Unexpected error creating session 1: %v", err)
	}

	_, err = sm.CreateSession(peerA, id2, m2, b2, nil)
	if err != nil {
		t.Fatalf("Unexpected error creating session 2: %v", err)
	}

	// Peer A attempts 3rd session -> should be rejected with ErrPeerQuotaExceeded
	_, err = sm.CreateSession(peerA, id3, m3, b3, nil)
	if err != ErrPeerQuotaExceeded {
		t.Fatalf("Expected ErrPeerQuotaExceeded for Peer A 3rd session, got %v", err)
	}

	// Peer B creates a session -> should succeed because Peer B quota is unaffected
	_, err = sm.CreateSession(peerB, id4, m4, b4, nil)
	if err != nil {
		t.Fatalf("Unexpected error creating session for Peer B: %v", err)
	}

	if sm.PeerSessionCount(peerA) != 2 {
		t.Fatalf("Expected Peer A session count 2, got %d", sm.PeerSessionCount(peerA))
	}
	if sm.PeerSessionCount(peerB) != 1 {
		t.Fatalf("Expected Peer B session count 1, got %d", sm.PeerSessionCount(peerB))
	}
}

func TestGlobalCapacityCap(t *testing.T) {
	config := SessionConfig{
		MaxGlobalSessions: 3,
		MaxPeerSessions:   10,
		SessionTTL:        5 * time.Minute,
		PruneInterval:     0,
	}
	sm := NewSessionManager(config)
	defer sm.Close()

	peerA := generatePeerID(1)
	peerB := generatePeerID(2)

	for i := byte(1); i <= 3; i++ {
		var id core.ContentID
		id[0] = i
		m, b := createDummyManifest(id, nil)
		p := peerA
		if i == 3 {
			p = peerB
		}
		_, err := sm.CreateSession(p, id, m, b, nil)
		if err != nil {
			t.Fatalf("Failed to create session %d: %v", i, err)
		}
	}

	if sm.ActiveSessions() != 3 {
		t.Fatalf("Expected 3 active sessions, got %d", sm.ActiveSessions())
	}

	// Attempting 4th session across any peer should fail with ErrGlobalCapacityExceeded
	var id4 core.ContentID
	id4[0] = 4
	m4, b4 := createDummyManifest(id4, nil)
	_, err := sm.CreateSession(peerB, id4, m4, b4, nil)
	if err != ErrGlobalCapacityExceeded {
		t.Fatalf("Expected ErrGlobalCapacityExceeded, got %v", err)
	}
}

func TestStaleSessionTTLPruning(t *testing.T) {
	config := SessionConfig{
		MaxGlobalSessions: 100,
		MaxPeerSessions:   10,
		SessionTTL:        50 * time.Millisecond,
		PruneInterval:     0,
	}
	sm := NewSessionManager(config)
	defer sm.Close()

	peerA := generatePeerID(1)

	var id1, id2 core.ContentID
	id1[0] = 1
	id2[0] = 2

	m1, b1 := createDummyManifest(id1, nil)
	m2, b2 := createDummyManifest(id2, nil)

	_, err := sm.CreateSession(peerA, id1, m1, b1, nil)
	if err != nil {
		t.Fatalf("Failed to create session 1: %v", err)
	}

	_, err = sm.CreateSession(peerA, id2, m2, b2, nil)
	if err != nil {
		t.Fatalf("Failed to create session 2: %v", err)
	}

	// Session 2 receives an update immediately before sleeping
	time.Sleep(30 * time.Millisecond)
	var chunkID core.ChunkID
	chunkID[0] = 99
	sm.RecordChunkCommit(id2, chunkID)

	// Sleep another 30 ms (total time for session 1: 60ms > 50ms TTL; for session 2: 30ms < 50ms TTL)
	time.Sleep(30 * time.Millisecond)

	pruned := sm.PruneStaleSessions()
	if pruned != 1 {
		t.Fatalf("Expected 1 session pruned, got %d", pruned)
	}

	if _, exists := sm.GetSession(id1); exists {
		t.Fatalf("Session 1 should have been pruned")
	}

	if _, exists := sm.GetSession(id2); !exists {
		t.Fatalf("Session 2 should still exist")
	}

	if sm.PeerSessionCount(peerA) != 1 {
		t.Fatalf("Expected peer A session count 1, got %d", sm.PeerSessionCount(peerA))
	}
}

func TestValidPushCommit(t *testing.T) {
	config := SessionConfig{
		MaxGlobalSessions: 10,
		MaxPeerSessions:   5,
		SessionTTL:        5 * time.Minute,
		PruneInterval:     0,
	}
	sm := NewSessionManager(config)
	defer sm.Close()

	peerA := generatePeerID(1)

	var contentID core.ContentID
	contentID[0] = 42

	var chunkID1, chunkID2 core.ChunkID
	chunkID1[0] = 101
	chunkID2[0] = 102

	assigned := []core.ChunkID{chunkID1, chunkID2}
	m, b := createDummyManifest(contentID, assigned)

	session, err := sm.CreateSession(peerA, contentID, m, b, assigned)
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	if session.PeerID != peerA {
		t.Fatalf("Expected session peer ID %s, got %s", peerA, session.PeerID)
	}

	sm.RecordChunkCommit(contentID, chunkID1)
	sm.RecordChunkCommit(contentID, chunkID2)

	s, ok := sm.GetSession(contentID)
	if !ok {
		t.Fatalf("Session should exist")
	}

	if len(s.CommittedChunks) != 2 {
		t.Fatalf("Expected 2 committed chunks, got %d", len(s.CommittedChunks))
	}

	// Remove session on completion
	removed := sm.RemoveSession(contentID)
	if !removed {
		t.Fatalf("Expected session removal to return true")
	}

	if sm.ActiveSessions() != 0 {
		t.Fatalf("Expected 0 active sessions, got %d", sm.ActiveSessions())
	}
	if sm.PeerSessionCount(peerA) != 0 {
		t.Fatalf("Expected peer A session count 0, got %d", sm.PeerSessionCount(peerA))
	}
}

func TestPruneDoesNotDeleteCASData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "cipher_test_cas_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	if err := storage.NewFSStorage(tmpDir); err != nil {
		t.Fatalf("Failed to init storage: %v", err)
	}

	engineConfig := core.EngineConfig{ChunkSize: 32 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(tmpDir)
	eng := engine.NewContentEngine(engineConfig, enc, dig, store, store, keys, store)

	ctx := context.Background()

	// Put a chunk in CAS
	dummyData := []byte("hello world ciphertext")
	hash := dig.Sum(dummyData)
	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			ID:         core.ChunkID(hash),
			CipherSize: uint32(len(dummyData)),
		},
		Data: dummyData,
	}

	if err := eng.PutChunk(ctx, chunk); err != nil {
		t.Fatalf("Failed to put chunk in CAS: %v", err)
	}

	// Create a session that references this chunk, then prune it
	sm := NewSessionManager(SessionConfig{
		MaxGlobalSessions: 10,
		MaxPeerSessions:   5,
		SessionTTL:        1 * time.Millisecond,
		PruneInterval:     0,
	})
	defer sm.Close()

	peerA := generatePeerID(1)
	var contentID core.ContentID
	contentID[0] = 77

	m, b := createDummyManifest(contentID, []core.ChunkID{chunk.Header.ID})
	_, err = sm.CreateSession(peerA, contentID, m, b, []core.ChunkID{chunk.Header.ID})
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}

	sm.RecordChunkCommit(contentID, chunk.Header.ID)

	time.Sleep(10 * time.Millisecond)
	sm.PruneStaleSessions()

	if sm.ActiveSessions() != 0 {
		t.Fatalf("Expected session to be pruned")
	}

	// Verify chunk still exists in CAS
	has, err := eng.HasChunk(ctx, chunk.Header.ID)
	if err != nil || !has {
		t.Fatalf("Chunk in CAS should NOT be deleted when pending session is pruned")
	}
}
