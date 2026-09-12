package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func createDummyID(b byte) core.ContentID {
	var id core.ContentID
	for i := range id {
		id[i] = b
	}
	return id
}

func createDummyPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
	if err != nil {
		t.Fatalf("failed to generate key pair: %v", err)
	}
	pID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to get peer ID from private key: %v", err)
	}
	return pID
}

func TestFileSessionManager_BasicOps(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	cID := createDummyID(0x01)
	pID := createDummyPeerID(t)

	session := &TransferSession{
		ContentID:   cID,
		TargetPeer:  pID,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now(),
		Status:      StatusInProgress,
	}

	// Save session
	if err := sm.Save(session); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Open session
	loaded, err := sm.Open(cID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if loaded == nil {
		t.Fatalf("Open returned nil session")
	}
	if loaded.ContentID != cID {
		t.Errorf("expected ContentID %v, got %v", cID, loaded.ContentID)
	}
	if loaded.CompletedCount() != 2 {
		t.Errorf("expected CompletedCount 2, got %d", loaded.CompletedCount())
	}

	// List sessions
	list, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 session in List, got %d", len(list))
	}

	// Delete session
	if err := sm.Delete(cID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	loadedAfterDelete, err := sm.Open(cID)
	if err != nil {
		t.Fatalf("Open after delete error: %v", err)
	}
	if loadedAfterDelete != nil {
		t.Fatalf("expected nil after delete, got %v", loadedAfterDelete)
	}
}

func TestFileSessionManager_FileSizeLimit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_limit_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}
	// Set a small limit for testing
	sm.MaxFileSize = 50

	cID := createDummyID(0x02)
	path := filepath.Join(tempDir, fmt.Sprintf("%x.json", cID))

	// Write oversized content (>50 bytes)
	oversizedContent := []byte(`{"content_id":"0202020202020202020202020202020202020202020202020202020202020202","status":"IN_PROGRESS","total_chunks":1000}`)
	if err := os.WriteFile(path, oversizedContent, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	// Open should fail due to size limit
	_, err = sm.Open(cID)
	if err == nil {
		t.Fatalf("expected error on Open for oversized file, got nil")
	}

	// List should skip the oversized file
	list, err := sm.List()
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("expected 0 sessions in List due to size limit, got %d", len(list))
	}
}

func TestFileSessionManager_MaxSessionsLimit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_max_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}
	sm.MaxSessions = 3
	pID := createDummyPeerID(t)

	// Create 5 valid session files
	for i := byte(1); i <= 5; i++ {
		cID := createDummyID(i)
		s := &TransferSession{
			ContentID:   cID,
			TargetPeer:  pID,
			TotalChunks: 1,
			Status:      StatusInProgress,
		}
		if err := sm.Save(s); err != nil {
			t.Fatalf("failed to save session %d: %v", i, err)
		}
	}

	list, err := sm.List()
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if len(list) != 3 {
		t.Fatalf("expected List to return capped 3 sessions, got %d", len(list))
	}
}

func TestFileSessionManager_CorruptedJSON(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_corrupt_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("failed to create session manager: %v", err)
	}

	cID := createDummyID(0x03)
	path := filepath.Join(tempDir, fmt.Sprintf("%x.json", cID))

	// Write invalid JSON content
	if err := os.WriteFile(path, []byte("{corrupt json"), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	// Open should fail unmarshaling
	_, err = sm.Open(cID)
	if err == nil {
		t.Fatalf("expected unmarshal error on Open, got nil")
	}

	// List should skip corrupted JSON
	list, err := sm.List()
	if err != nil {
		t.Fatalf("List error: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("expected 0 sessions in List for corrupted JSON, got %d", len(list))
	}
}
