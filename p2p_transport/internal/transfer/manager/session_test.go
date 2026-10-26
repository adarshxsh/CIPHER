package manager

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

func createTestSession(sm *FileSessionManager, id core.ContentID, totalChunks int, status SessionStatus) (*TransferSession, error) {
	// Use a valid peer ID for libp2p peer.ID unmarshaling
	pid, _ := peer.Decode("12D3KooWSoLjuK3P8q813oK2S2y1xG3kK2S2y1xG3kK2S2y1xG3k")
	sess := &TransferSession{
		ContentID:   id,
		TargetPeer:  pid,
		Completed:   make([]bool, totalChunks),
		TotalChunks: totalChunks,
		StartedAt:   time.Now(),
		Status:      status,
	}
	if err := sm.Save(sess); err != nil {
		return nil, err
	}
	return sess, nil
}

func TestListSessions_PaginationAndSorting(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm, err := NewFileSessionManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	// Create 10 sessions with staggered modification times
	var ids []core.ContentID
	for i := 0; i < 10; i++ {
		var id core.ContentID
		rand.Read(id[:])
		ids = append(ids, id)

		sess, err := createTestSession(sm, id, i+1, StatusInProgress)
		if err != nil {
			t.Fatalf("Failed to create session %d: %v", i, err)
		}

		// Ensure distinct ModTime for date sorting testing
		modTime := time.Now().Add(time.Duration(i) * time.Second)
		path := sm.getPath(sess.ContentID)
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatalf("Failed to chtimes for session %d: %v", i, err)
		}
	}

	// The newest session should be index 9, down to index 0.
	// Test page 1: offset=0, limit=3
	page1, err := sm.ListSessions(0, 3)
	if err != nil {
		t.Fatalf("ListSessions failed: %v", err)
	}
	if len(page1) != 3 {
		t.Fatalf("Expected 3 sessions on page 1, got %d", len(page1))
	}
	if page1[0].ContentID != ids[9] {
		t.Errorf("Expected first session to be most recent (ids[9]), got %x vs %x", page1[0].ContentID, ids[9])
	}
	if page1[2].ContentID != ids[7] {
		t.Errorf("Expected third session to be ids[7], got %x vs %x", page1[2].ContentID, ids[7])
	}

	// Test page 2: offset=3, limit=3
	page2, err := sm.ListSessions(3, 3)
	if err != nil {
		t.Fatalf("ListSessions failed: %v", err)
	}
	if len(page2) != 3 {
		t.Fatalf("Expected 3 sessions on page 2, got %d", len(page2))
	}
	if page2[0].ContentID != ids[6] {
		t.Errorf("Expected first session on page 2 to be ids[6], got %x vs %x", page2[0].ContentID, ids[6])
	}

	// Test last page beyond count: offset=9, limit=5 -> should return 1 session
	pageLast, err := sm.ListSessions(9, 5)
	if err != nil {
		t.Fatalf("ListSessions failed: %v", err)
	}
	if len(pageLast) != 1 {
		t.Fatalf("Expected 1 session on last page, got %d", len(pageLast))
	}
	if pageLast[0].ContentID != ids[0] {
		t.Errorf("Expected session on last page to be oldest (ids[0]), got %x vs %x", pageLast[0].ContentID, ids[0])
	}

	// Test offset beyond total: offset=20, limit=5 -> should return empty
	pageEmpty, err := sm.ListSessions(20, 5)
	if err != nil {
		t.Fatalf("ListSessions failed: %v", err)
	}
	if len(pageEmpty) != 0 {
		t.Fatalf("Expected 0 sessions, got %d", len(pageEmpty))
	}

	// Test limit <= 0 (unlimited): offset=0, limit=0 -> should return all 10 sessions
	allSessions, err := sm.ListSessions(0, 0)
	if err != nil {
		t.Fatalf("ListSessions failed: %v", err)
	}
	if len(allSessions) != 10 {
		t.Fatalf("Expected 10 sessions for unlimited query, got %d", len(allSessions))
	}

	// Test default List() -> should return top 50 (here all 10)
	defaultList, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(defaultList) != 10 {
		t.Fatalf("Expected 10 sessions for List(), got %d", len(defaultList))
	}
}

func TestListSessions_StopOnLimit(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_stop_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm, err := NewFileSessionManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	// Create 100 sessions
	for i := 0; i < 100; i++ {
		var id core.ContentID
		rand.Read(id[:])
		if _, err := createTestSession(sm, id, 10, StatusInProgress); err != nil {
			t.Fatalf("Failed to create session: %v", err)
		}
	}

	// Query with limit=5
	sessions, err := sm.ListSessions(0, 5)
	if err != nil {
		t.Fatalf("ListSessions failed: %v", err)
	}
	if len(sessions) != 5 {
		t.Fatalf("Expected 5 sessions, got %d", len(sessions))
	}
}

func TestListSessions_LargeScaleMemoryAndPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large scale performance test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "session_large_scale_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm, err := NewFileSessionManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	// Pre-create 10,000 session files
	totalFiles := 10000
	t.Logf("Generating %d session files...", totalFiles)
	
	// Fast write raw session files
	pid, _ := peer.Decode("12D3KooWSoLjuK3P8q813oK2S2y1xG3kK2S2y1xG3kK2S2y1xG3k")
	sampleSess := &TransferSession{
		ContentID:   core.ContentID{},
		TargetPeer:  pid,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		StartedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Status:      StatusInProgress,
	}
	sessData, err := json.Marshal(sampleSess)
	if err != nil {
		t.Fatalf("Failed to marshal sample session: %v", err)
	}

	for i := 0; i < totalFiles; i++ {
		fileName := fmt.Sprintf("%08d%056d.json", i, 0)
		filePath := filepath.Join(tmpDir, fileName)
		if err := os.WriteFile(filePath, sessData, 0644); err != nil {
			t.Fatalf("Failed to write file %d: %v", i, err)
		}
	}

	// Force GC and capture baseline memory
	runtime.GC()
	var mBefore runtime.MemStats
	runtime.ReadMemStats(&mBefore)

	start := time.Now()
	sessions, err := sm.ListSessions(0, 50)
	duration := time.Since(start)

	var mAfter runtime.MemStats
	runtime.ReadMemStats(&mAfter)

	if err != nil {
		t.Fatalf("ListSessions failed: %v", err)
	}
	if len(sessions) != 50 {
		t.Fatalf("Expected 50 sessions, got %d", len(sessions))
	}

	// Calculate memory delta during listing
	memAllocatedMB := float64(mAfter.TotalAlloc-mBefore.TotalAlloc) / (1024 * 1024)

	t.Logf("ListSessions for %d files took: %v", totalFiles, duration)
	t.Logf("Memory allocated during query: %.2f MB", memAllocatedMB)

	// Verify acceptance criteria:
	// 1. Latency < 100ms
	if duration > 100*time.Millisecond {
		t.Errorf("Latency requirement failed: took %v, expected < 100ms", duration)
	}

	// 2. RAM profile < 10MB
	if memAllocatedMB > 10.0 {
		t.Errorf("Memory requirement failed: allocated %.2f MB, expected < 10 MB", memAllocatedMB)
	}
}
