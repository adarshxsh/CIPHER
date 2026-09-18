package manager

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

func createDummySession(idByte byte, status SessionStatus) *TransferSession {
	var cid core.ContentID
	cid[0] = idByte
	pid, _ := peer.Decode("12D3KooWKn69mpt1AatrtuDNE7sP235eAnM98ks88t4w17mPmyC4")
	return &TransferSession{
		ContentID:   cid,
		TargetPeer:  pid,
		Completed:   []bool{true, false},
		TotalChunks: 2,
		StartedAt:   time.Now(),
		Status:      status,
	}
}

func TestSessionManager_PaginationAndSorting(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm, err := NewFileSessionManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create session manager: %v", err)
	}

	// Create 10 sessions with explicit time ordering
	numSessions := 10
	sessions := make([]*TransferSession, numSessions)
	baseTime := time.Now().Add(-10 * time.Minute)

	for i := 0; i < numSessions; i++ {
		s := createDummySession(byte(i+1), StatusInProgress)
		if err := sm.Save(s); err != nil {
			t.Fatalf("Failed to save session %d: %v", i, err)
		}
		// Artificially update file modification time and cache to guarantee distinct timestamps
		sTime := baseTime.Add(time.Duration(i) * time.Minute)
		filePath := sm.getPath(s.ContentID)
		if err := os.Chtimes(filePath, sTime, sTime); err != nil {
			t.Fatalf("Failed to chtimes for %s: %v", filePath, err)
		}
		sm.mu.Lock()
		sm.modTimeCache[filepath.Base(filePath)] = sTime
		sm.mu.Unlock()

		sessions[i] = s
	}

	// sessions[9] is newest, sessions[0] is oldest.

	// Test 1: Page 1 (offset=0, limit=3)
	p1, err := sm.ListSessionsPaginated(0, 3)
	if err != nil {
		t.Fatalf("ListSessionsPaginated failed: %v", err)
	}
	if len(p1) != 3 {
		t.Fatalf("Expected 3 sessions on page 1, got %d", len(p1))
	}
	// Expected newest: session 9, then 8, then 7
	if p1[0].ContentID[0] != byte(10) || p1[1].ContentID[0] != byte(9) || p1[2].ContentID[0] != byte(8) {
		t.Errorf("Unexpected session IDs on page 1: [%d, %d, %d]", p1[0].ContentID[0], p1[1].ContentID[0], p1[2].ContentID[0])
	}

	// Test 2: Page 2 (offset=3, limit=3)
	p2, err := sm.ListSessionsPaginated(3, 3)
	if err != nil {
		t.Fatalf("ListSessionsPaginated failed: %v", err)
	}
	if len(p2) != 3 {
		t.Fatalf("Expected 3 sessions on page 2, got %d", len(p2))
	}
	if p2[0].ContentID[0] != byte(7) || p2[1].ContentID[0] != byte(6) || p2[2].ContentID[0] != byte(5) {
		t.Errorf("Unexpected session IDs on page 2: [%d, %d, %d]", p2[0].ContentID[0], p2[1].ContentID[0], p2[2].ContentID[0])
	}

	// Test 3: Offset beyond total count (offset=20, limit=5)
	pEmpty, err := sm.ListSessionsPaginated(20, 5)
	if err != nil {
		t.Fatalf("ListSessionsPaginated failed: %v", err)
	}
	if len(pEmpty) != 0 {
		t.Errorf("Expected 0 sessions for offset beyond total, got %d", len(pEmpty))
	}

	// Test 4: ListSessions alias
	pAlias, err := sm.ListSessions(0, 2)
	if err != nil {
		t.Fatalf("ListSessions failed: %v", err)
	}
	if len(pAlias) != 2 || pAlias[0].ContentID[0] != byte(10) {
		t.Errorf("ListSessions alias mismatch")
	}
}

func TestSessionManager_DefaultAndMaxLimits(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_limits_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm, err := NewFileSessionManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create session manager: %v", err)
	}

	// Create 60 sessions
	for i := 0; i < 60; i++ {
		s := createDummySession(byte(i+1), StatusInProgress)
		if err := sm.Save(s); err != nil {
			t.Fatalf("Failed to save session %d: %v", i, err)
		}
	}

	// Test default limit when limit <= 0
	sessions, err := sm.ListSessionsPaginated(0, 0)
	if err != nil {
		t.Fatalf("ListSessionsPaginated failed: %v", err)
	}
	if len(sessions) != DefaultPageLimit {
		t.Errorf("Expected default page limit of %d, got %d", DefaultPageLimit, len(sessions))
	}

	// Test List() uses default limit
	listSessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(listSessions) != DefaultPageLimit {
		t.Errorf("Expected List() to return %d sessions, got %d", DefaultPageLimit, len(listSessions))
	}

	// Test max cap limit
	sessionsAll, err := sm.ListSessionsPaginated(0, 2000)
	if err != nil {
		t.Fatalf("ListSessionsPaginated failed: %v", err)
	}
	if len(sessionsAll) != 60 { // total available is 60, which is < MaxPageLimit (1000)
		t.Errorf("Expected 60 sessions, got %d", len(sessionsAll))
	}
}

func TestSessionManager_ModTimeCachingAndPerformance(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_cache_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm, err := NewFileSessionManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create session manager: %v", err)
	}

	// Create 100 dummy session files directly on disk or via Save
	for i := 0; i < 100; i++ {
		s := createDummySession(byte(i+1), StatusCompleted)
		var cid core.ContentID
		cid[0] = byte((i >> 8) & 0xff)
		cid[1] = byte(i & 0xff)
		s.ContentID = cid
		if err := sm.Save(s); err != nil {
			t.Fatalf("Failed to save session: %v", err)
		}
	}

	// Verify modTimeCache is populated after Save
	sm.mu.RLock()
	cacheSize := len(sm.modTimeCache)
	sm.mu.RUnlock()
	if cacheSize != 100 {
		t.Errorf("Expected modTimeCache size of 100, got %d", cacheSize)
	}

	// Execute ListSessionsPaginated(0, 10) and measure time
	start := time.Now()
	sessions, err := sm.ListSessionsPaginated(0, 10)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("ListSessionsPaginated failed: %v", err)
	}
	if len(sessions) != 10 {
		t.Errorf("Expected 10 sessions, got %d", len(sessions))
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("Listing sessions took too long: %v", elapsed)
	}

	// Verify Delete cleans cache
	firstID := sessions[0].ContentID
	if err := sm.Delete(firstID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	sm.mu.RLock()
	cacheSizeAfterDelete := len(sm.modTimeCache)
	sm.mu.RUnlock()
	if cacheSizeAfterDelete != 99 {
		t.Errorf("Expected modTimeCache size 99 after deletion, got %d", cacheSizeAfterDelete)
	}
}

func TestSessionManager_UnmarshalsOnlyRequestedPageRange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "session_page_unmarshal_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	sm, err := NewFileSessionManager(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create session manager: %v", err)
	}

	// Create 20 valid session files
	for i := 0; i < 20; i++ {
		s := createDummySession(byte(i+1), StatusInProgress)
		if err := sm.Save(s); err != nil {
			t.Fatalf("Save failed: %v", err)
		}
	}

	// Add a corrupt JSON file that has a newer modtime than others
	corruptFile := filepath.Join(tmpDir, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff.json")
	if err := os.WriteFile(corruptFile, []byte("corrupt json data"), 0644); err != nil {
		t.Fatalf("Failed to create corrupt file: %v", err)
	}
	futureTime := time.Now().Add(1 * time.Hour)
	if err := os.Chtimes(corruptFile, futureTime, futureTime); err != nil {
		t.Fatalf("Failed to chtimes corrupt file: %v", err)
	}

	// Query page 1 with limit 5 (corrupt file will be in top 5 due to newest modtime)
	sessions, err := sm.ListSessionsPaginated(0, 5)
	if err != nil {
		t.Fatalf("ListSessionsPaginated error: %v", err)
	}
	// The corrupt file should be skipped during unmarshaling without erroring out
	if len(sessions) != 4 {
		t.Errorf("Expected 4 valid sessions from top 5 files, got %d", len(sessions))
	}
}
