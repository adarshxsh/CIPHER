package manager

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

var testPeerID, _ = peer.Decode("12D3KooWSDcv1aP93p74jJ9v4Y1pS9kL3yQ21111111111111111")

func TestFileSessionManager_BasicOperations(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("01234567890123456789012345678901"))

	sess := &TransferSession{
		ContentID:   contentID,
		TargetPeer:  testPeerID,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now().Add(-10 * time.Minute),
		Status:      StatusInProgress,
	}

	// Save session
	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Open session
	loaded, err := sm.Open(contentID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if loaded == nil {
		t.Fatalf("Expected loaded session, got nil")
	}
	if loaded.CompletedCount() != 2 {
		t.Errorf("Expected CompletedCount 2, got %d", loaded.CompletedCount())
	}

	// List sessions
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("Expected 1 session in List, got %d", len(sessions))
	}

	// Delete session
	if err := sm.Delete(contentID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	deleted, err := sm.Open(contentID)
	if err != nil {
		t.Fatalf("Open after delete failed: %v", err)
	}
	if deleted != nil {
		t.Errorf("Expected nil session after delete, got %v", deleted)
	}
}

func TestFileSessionManager_OversizedFileSkipped(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	// Valid session
	var validID core.ContentID
	copy(validID[:], []byte("valid_session_id_000000000000000"))
	validSess := &TransferSession{
		ContentID:   validID,
		TargetPeer:  testPeerID,
		TotalChunks: 1,
		Status:      StatusCompleted,
	}
	if err := sm.Save(validSess); err != nil {
		t.Fatalf("Failed to save valid session: %v", err)
	}

	// Oversized session file (> 1 MB)
	var oversizedID core.ContentID
	copy(oversizedID[:], []byte("oversized_session_id_00000000000"))
	oversizedPath := filepath.Join(tempDir, fmt.Sprintf("%x.json", oversizedID))
	bigData := make([]byte, 1024*1024+100) // ~1.0001 MB
	for i := range bigData {
		bigData[i] = 'a'
	}
	if err := os.WriteFile(oversizedPath, bigData, 0644); err != nil {
		t.Fatalf("Failed to write oversized session file: %v", err)
	}

	// Open should fail for oversized file
	_, err = sm.Open(oversizedID)
	if err == nil {
		t.Errorf("Expected Open to return an error for oversized session file, got nil")
	}

	// List should skip the oversized file and return only valid session
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("Expected 1 session (oversized skipped), got %d", len(sessions))
	}
	if sessions[0].ContentID != validID {
		t.Errorf("Expected returned session ID %x, got %x", validID, sessions[0].ContentID)
	}
}

func TestFileSessionManager_NewestFirstOrdering(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	now := time.Now()
	for i := 0; i < 5; i++ {
		var id core.ContentID
		idStr := fmt.Sprintf("session_id_number_%02d_000000000000", i)
		copy(id[:], []byte(idStr))

		sess := &TransferSession{
			ContentID:   id,
			TargetPeer:  testPeerID,
			TotalChunks: i + 1,
			Status:      StatusInProgress,
		}
		if err := sm.Save(sess); err != nil {
			t.Fatalf("Failed to save session %d: %v", i, err)
		}

		// Set modTime explicitly so index 0 is oldest, index 4 is newest
		path := filepath.Join(tempDir, fmt.Sprintf("%x.json", id))
		modTime := now.Add(time.Duration(i) * time.Hour)
		_ = os.Chtimes(path, modTime, modTime)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 5 {
		t.Fatalf("Expected 5 sessions, got %d", len(sessions))
	}

	// First element should be index 4 (newest, TotalChunks=5)
	// Last element should be index 0 (oldest, TotalChunks=1)
	for i := 0; i < len(sessions); i++ {
		expectedTotalChunks := 5 - i
		if sessions[i].TotalChunks != expectedTotalChunks {
			t.Errorf("At index %d: expected TotalChunks %d, got %d", i, expectedTotalChunks, sessions[i].TotalChunks)
		}
	}
}

func TestFileSessionManager_QuantityLimit100(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	now := time.Now()
	totalCreated := 150
	for i := 0; i < totalCreated; i++ {
		var id core.ContentID
		idHex := fmt.Sprintf("%064x", i+1)
		b, _ := hex.DecodeString(idHex)
		copy(id[:], b)

		sess := &TransferSession{
			ContentID:   id,
			TargetPeer:  testPeerID,
			TotalChunks: i + 1,
			Status:      StatusInProgress,
		}
		if err := sm.Save(sess); err != nil {
			t.Fatalf("Save failed for index %d: %v", i, err)
		}

		// Give higher index newer modTime
		path := filepath.Join(tempDir, fmt.Sprintf("%x.json", id))
		modTime := now.Add(time.Duration(i) * time.Minute)
		_ = os.Chtimes(path, modTime, modTime)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(sessions) != 100 {
		t.Fatalf("Expected List() to return capped 100 sessions, got %d", len(sessions))
	}

	// Verify the returned 100 entries are the 100 newest (indexes 149 down to 50, so TotalChunks 150 down to 51)
	if sessions[0].TotalChunks != 150 {
		t.Errorf("Expected newest session TotalChunks 150, got %d", sessions[0].TotalChunks)
	}
	if sessions[99].TotalChunks != 51 {
		t.Errorf("Expected 100th newest session TotalChunks 51, got %d", sessions[99].TotalChunks)
	}
}

func TestFileSessionManager_CorruptAndEmptyFilesSkipped(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	// Valid session
	var validID core.ContentID
	copy(validID[:], []byte("valid_session_id_000000000000000"))
	validSess := &TransferSession{
		ContentID:   validID,
		TargetPeer:  testPeerID,
		TotalChunks: 10,
		Status:      StatusCompleted,
	}
	if err := sm.Save(validSess); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Empty file
	emptyPath := filepath.Join(tempDir, "0000000000000000000000000000000000000000000000000000000000000001.json")
	_ = os.WriteFile(emptyPath, []byte(""), 0644)

	// Corrupt JSON file
	corruptPath := filepath.Join(tempDir, "0000000000000000000000000000000000000000000000000000000000000002.json")
	_ = os.WriteFile(corruptPath, []byte("{invalid json payload..."), 0644)

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("Expected 1 valid session, got %d", len(sessions))
	}
	if sessions[0].ContentID != validID {
		t.Errorf("Expected session ID %x, got %x", validID, sessions[0].ContentID)
	}
}

func TestFileSessionManager_MemoryBounded(t *testing.T) {
	tempDir := t.TempDir()
	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	// Create 1,000 session files
	for i := 0; i < 1000; i++ {
		var id core.ContentID
		idHex := fmt.Sprintf("%064x", i+1)
		b, _ := hex.DecodeString(idHex)
		copy(id[:], b)

		sess := &TransferSession{
			ContentID:   id,
			TargetPeer:  testPeerID,
			TotalChunks: i + 1,
			Status:      StatusInProgress,
		}
		_ = sm.Save(sess)
	}

	runtime.GC()
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 100 {
		t.Fatalf("Expected 100 sessions, got %d", len(sessions))
	}

	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	// HeapAlloc delta should be well under 10MB (typically < 1MB)
	allocDelta := int64(mAfter.HeapAlloc) - int64(mBefore.HeapAlloc)
	t.Logf("List() heap allocation delta with 1,000 session files: %d bytes (%.2f KB)", allocDelta, float64(allocDelta)/1024.0)
	if allocDelta > 10*1024*1024 {
		t.Errorf("List() exceeded 10MB memory threshold: allocated %d bytes", allocDelta)
	}
}
