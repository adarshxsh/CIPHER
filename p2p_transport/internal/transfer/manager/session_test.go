package manager

import (
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

func randomContentID(t *testing.T) core.ContentID {
	t.Helper()
	var id core.ContentID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatalf("failed to generate content id: %v", err)
	}
	return id
}

func randomPeerID(t *testing.T) peer.ID {
	t.Helper()
	// Example valid libp2p peer ID string
	pid, err := peer.Decode("12D3KooWDpjBSpHd137ZLAeSgG3qCMeMsgAou1GKx233f95D4A9e")
	if err != nil {
		t.Fatalf("failed to decode peer id: %v", err)
	}
	return pid
}

func TestFileSessionManager_BasicLifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	cID := randomContentID(t)
	pID := randomPeerID(t)

	sess := &TransferSession{
		ContentID:   cID,
		TargetPeer:  pID,
		Completed:   []bool{true, false, true, true, false},
		TotalChunks: 5,
		StartedAt:   time.Now().Add(-1 * time.Hour),
		Status:      StatusInProgress,
	}

	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	loaded, err := sm.Open(cID)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if loaded == nil {
		t.Fatalf("expected session, got nil")
	}

	if loaded.ContentID != cID {
		t.Errorf("expected ContentID %x, got %x", cID, loaded.ContentID)
	}
	if loaded.CompletedCount() != 3 {
		t.Errorf("expected CompletedCount 3, got %d", loaded.CompletedCount())
	}
	if len(loaded.Completed) != 5 {
		t.Errorf("expected Completed len 5, got %d", len(loaded.Completed))
	}

	if err := sm.Delete(cID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	deleted, err := sm.Open(cID)
	if err != nil {
		t.Fatalf("Open after delete failed: %v", err)
	}
	if deleted != nil {
		t.Errorf("expected nil after delete, got %v", deleted)
	}
}

func TestFileSessionManager_LightweightUnmarshaling(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	cID := randomContentID(t)
	pID := randomPeerID(t)

	total := 1000
	completed := make([]bool, total)
	expectedCount := 0
	for i := 0; i < total; i++ {
		if i%3 == 0 {
			completed[i] = true
			expectedCount++
		}
	}

	sess := &TransferSession{
		ContentID:   cID,
		TargetPeer:  pID,
		Completed:   completed,
		TotalChunks: total,
		StartedAt:   time.Now().Add(-10 * time.Minute),
		Status:      StatusInProgress,
	}

	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	list, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 session, got %d", len(list))
	}

	s := list[0]
	if len(s.Completed) != 0 {
		t.Errorf("expected s.Completed to be empty/nil during listing, got len %d", len(s.Completed))
	}
	if s.CompletedCount() != expectedCount {
		t.Errorf("expected CompletedCount %d, got %d", expectedCount, s.CompletedCount())
	}
	if s.TotalChunks != total {
		t.Errorf("expected TotalChunks %d, got %d", total, s.TotalChunks)
	}
	if s.Status != StatusInProgress {
		t.Errorf("expected Status %s, got %s", StatusInProgress, s.Status)
	}
}

func TestFileSessionManager_FileSizeLimitEnforcement(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Set max file size limit of 600 bytes
	sm, err := NewFileSessionManager(tempDir, WithMaxFileSize(600))
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	cIDSmall := randomContentID(t)
	cIDLarge := randomContentID(t)
	pID := randomPeerID(t)

	smallSess := &TransferSession{
		ContentID:   cIDSmall,
		TargetPeer:  pID,
		Completed:   []bool{true},
		TotalChunks: 1,
		Status:      StatusCompleted,
	}
	if err := sm.Save(smallSess); err != nil {
		t.Fatalf("Save small session failed: %v", err)
	}

	// Create an oversized payload file in session directory
	largePath := filepath.Join(tempDir, fmt.Sprintf("%x.json", cIDLarge))
	oversizedData := make([]byte, 2000)
	for i := range oversizedData {
		oversizedData[i] = 'a'
	}
	if err := os.WriteFile(largePath, oversizedData, 0600); err != nil {
		t.Fatalf("failed to write oversized file: %v", err)
	}

	list, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected 1 session in list (oversized skipped), got %d", len(list))
	}
	if list[0].ContentID != cIDSmall {
		t.Errorf("expected listed session to be small session %x, got %x", cIDSmall, list[0].ContentID)
	}

	// Direct Open on oversized file should return error
	_, err = sm.Open(cIDLarge)
	if err == nil {
		t.Errorf("expected error when opening oversized file, got nil")
	}
}

func TestFileSessionManager_PaginatedListing(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	pID := randomPeerID(t)
	totalSessions := 25
	cIDs := make([]core.ContentID, totalSessions)

	for i := 0; i < totalSessions; i++ {
		cIDs[i] = randomContentID(t)
		sess := &TransferSession{
			ContentID:   cIDs[i],
			TargetPeer:  pID,
			Completed:   []bool{true, false, true},
			TotalChunks: 3,
			StartedAt:   time.Now().Add(time.Duration(-i) * time.Minute),
			Status:      StatusInProgress,
		}
		if err := sm.Save(sess); err != nil {
			t.Fatalf("Save session %d failed: %v", i, err)
		}
		// Ensure distinct modification times
		time.Sleep(2 * time.Millisecond)
	}

	// Page 1: offset 0, limit 10
	p1, err := sm.ListPaged(0, 10)
	if err != nil {
		t.Fatalf("ListPaged page 1 failed: %v", err)
	}
	if len(p1) != 10 {
		t.Fatalf("expected 10 sessions on page 1, got %d", len(p1))
	}

	// Page 2: offset 10, limit 10
	p2, err := sm.ListPaged(10, 10)
	if err != nil {
		t.Fatalf("ListPaged page 2 failed: %v", err)
	}
	if len(p2) != 10 {
		t.Fatalf("expected 10 sessions on page 2, got %d", len(p2))
	}

	// Page 3: offset 20, limit 10
	p3, err := sm.ListPaged(20, 10)
	if err != nil {
		t.Fatalf("ListPaged page 3 failed: %v", err)
	}
	if len(p3) != 5 {
		t.Fatalf("expected 5 sessions on page 3, got %d", len(p3))
	}

	// Page 4: offset 30, limit 10
	p4, err := sm.ListPaged(30, 10)
	if err != nil {
		t.Fatalf("ListPaged page 4 failed: %v", err)
	}
	if len(p4) != 0 {
		t.Fatalf("expected 0 sessions on page 4, got %d", len(p4))
	}

	// Verify no duplicates between pages
	seen := make(map[core.ContentID]bool)
	allPages := append(append(p1, p2...), p3...)
	if len(allPages) != totalSessions {
		t.Fatalf("expected total %d sessions across pages, got %d", totalSessions, len(allPages))
	}

	for _, s := range allPages {
		if seen[s.ContentID] {
			t.Errorf("duplicate session ID %x found across pages", s.ContentID)
		}
		seen[s.ContentID] = true
	}
}

func TestFileSessionManager_BoundedStreamingBatching(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	pID := randomPeerID(t)
	totalFiles := 150

	for i := 0; i < totalFiles; i++ {
		cID := randomContentID(t)
		sess := &TransferSession{
			ContentID:   cID,
			TargetPeer:  pID,
			Completed:   []bool{true},
			TotalChunks: 1,
			Status:      StatusCompleted,
		}
		if err := sm.Save(sess); err != nil {
			t.Fatalf("Save session %d failed: %v", i, err)
		}
	}

	// List with small limit
	sessions, err := sm.ListPaged(0, 20)
	if err != nil {
		t.Fatalf("ListPaged failed: %v", err)
	}
	if len(sessions) != 20 {
		t.Fatalf("expected 20 sessions, got %d", len(sessions))
	}

	// List default max
	all, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(all) != totalFiles {
		t.Fatalf("expected %d sessions, got %d", totalFiles, len(all))
	}
}
