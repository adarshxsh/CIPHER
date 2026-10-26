package manager

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func getTestPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to get peer ID from key: %v", err)
	}
	return id
}

func TestFileSessionManager_SaveOpenDelete(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	testPeerID := getTestPeerID(t)
	var contentID core.ContentID
	contentID[0] = 0xAB

	sess := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  testPeerID,
		TotalChunks: 10,
		Completed:   []bool{true, false, true},
		Status:      StatusInProgress,
	}

	if err := sm.Save(sess); err != nil {
		t.Fatalf("failed to save session: %v", err)
	}

	loaded, err := sm.Open(contentID)
	if err != nil {
		t.Fatalf("failed to open session: %v", err)
	}
	if loaded == nil {
		t.Fatalf("expected session, got nil")
	}
	if loaded.ContentID != contentID {
		t.Errorf("expected ContentID %x, got %x", contentID, loaded.ContentID)
	}
	if loaded.CompletedCount() != 2 {
		t.Errorf("expected completed count 2, got %d", loaded.CompletedCount())
	}

	if err := sm.Delete(contentID); err != nil {
		t.Fatalf("failed to delete session: %v", err)
	}

	loadedAfterDelete, err := sm.Open(contentID)
	if err != nil {
		t.Fatalf("unexpected error opening deleted session: %v", err)
	}
	if loadedAfterDelete != nil {
		t.Errorf("expected nil after delete, got %v", loadedAfterDelete)
	}
}

func TestFileSessionManager_List_MaxFileSize(t *testing.T) {
	tempDir := t.TempDir()
	// Set file size limit to 1000 bytes
	sm, err := NewFileSessionManager(tempDir, WithMaxFileSize(1000))
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	testPeerID := getTestPeerID(t)

	// Small valid session file (~380 bytes)
	var id1 core.ContentID
	id1[0] = 0x01
	sess1 := &TransferSession{
		ContentID:   id1,
		TargetPeer:  testPeerID,
		TotalChunks: 1,
		Status:      StatusInProgress,
	}
	if err := sm.Save(sess1); err != nil {
		t.Fatalf("failed to save sess1: %v", err)
	}

	// Large file created manually > 1000 bytes (e.g. 5000 bytes)
	largePath := filepath.Join(tempDir, "0200000000000000000000000000000000000000000000000000000000000000.json")
	largeData := bytes.Repeat([]byte("a"), 5000)
	if err := os.WriteFile(largePath, largeData, 0644); err != nil {
		t.Fatalf("failed to write large file: %v", err)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}
	if sessions[0].ContentID != id1 {
		t.Errorf("expected content ID %x, got %x", id1, sessions[0].ContentID)
	}
}

func TestFileSessionManager_List_MaxEntries(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir, WithMaxEntries(3))
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	testPeerID := getTestPeerID(t)

	// Create 5 session files with distinct modTimes
	baseTime := time.Now().Add(-10 * time.Minute)
	var createdIDs []core.ContentID
	for i := 1; i <= 5; i++ {
		var id core.ContentID
		id[0] = byte(i)
		sess := &TransferSession{
			ContentID:   id,
			TargetPeer:  testPeerID,
			TotalChunks: i,
			Status:      StatusInProgress,
		}
		if err := sm.Save(sess); err != nil {
			t.Fatalf("failed to save session %d: %v", i, err)
		}
		path := sm.getPath(id)
		modTime := baseTime.Add(time.Duration(i) * time.Minute)
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatalf("failed to chtimes session %d: %v", i, err)
		}
		createdIDs = append(createdIDs, id)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	// MaxEntries is 3, so we expect the 3 most recent sessions (i=5, i=4, i=3)
	if len(sessions) != 3 {
		t.Fatalf("expected 3 sessions, got %d", len(sessions))
	}

	expectedIDs := []core.ContentID{createdIDs[4], createdIDs[3], createdIDs[2]}
	for i, s := range sessions {
		if s.ContentID != expectedIDs[i] {
			t.Errorf("at index %d: expected ContentID %x, got %x", i, expectedIDs[i], s.ContentID)
		}
	}
}

func TestFileSessionManager_List_SortingOrder(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	testPeerID := getTestPeerID(t)

	now := time.Now()
	times := []time.Time{
		now.Add(-30 * time.Minute), // oldest
		now.Add(-10 * time.Minute), // newest
		now.Add(-20 * time.Minute), // middle
	}

	var ids []core.ContentID
	for i, modTime := range times {
		var id core.ContentID
		id[0] = byte(i + 1)
		sess := &TransferSession{
			ContentID:   id,
			TargetPeer:  testPeerID,
			TotalChunks: i + 1,
			Status:      StatusInProgress,
		}
		if err := sm.Save(sess); err != nil {
			t.Fatalf("failed to save session: %v", err)
		}
		path := sm.getPath(id)
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatalf("failed to chtimes session: %v", err)
		}
		ids = append(ids, id)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(sessions) != 3 {
		t.Fatalf("expected 3 sessions, got %d", len(sessions))
	}

	// Expected order: index 1 (newest: -10m), index 2 (middle: -20m), index 0 (oldest: -30m)
	expectedOrder := []core.ContentID{ids[1], ids[2], ids[0]}
	for i, s := range sessions {
		if s.ContentID != expectedOrder[i] {
			t.Errorf("at index %d: expected ContentID %x, got %x", i, expectedOrder[i], s.ContentID)
		}
	}
}

func TestFileSessionManager_List_CorruptFileHandling(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	testPeerID := getTestPeerID(t)

	var id1 core.ContentID
	id1[0] = 0x01
	sess1 := &TransferSession{
		ContentID:   id1,
		TargetPeer:  testPeerID,
		TotalChunks: 5,
		Status:      StatusInProgress,
	}
	if err := sm.Save(sess1); err != nil {
		t.Fatalf("failed to save session 1: %v", err)
	}

	// Corrupt file
	corruptPath := filepath.Join(tempDir, "corrupt.json")
	if err := os.WriteFile(corruptPath, []byte("invalid json {{{"), 0644); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}
	if sessions[0].ContentID != id1 {
		t.Errorf("expected ContentID %x, got %x", id1, sessions[0].ContentID)
	}
}

func TestFileSessionManager_Options(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir, WithMaxFileSize(2048), WithMaxEntries(50))
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	if sm.maxFileSize != 2048 {
		t.Errorf("expected maxFileSize 2048, got %d", sm.maxFileSize)
	}
	if sm.maxEntries != 50 {
		t.Errorf("expected maxEntries 50, got %d", sm.maxEntries)
	}
}
