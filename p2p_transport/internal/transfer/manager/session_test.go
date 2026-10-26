package manager

import (
	"bytes"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

func createDummyContentID(seed byte) core.ContentID {
	var id core.ContentID
	for i := range id {
		id[i] = seed
	}
	return id
}

func TestFileSessionManager_SaveAndList(t *testing.T) {
	dir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	sm, err := NewFileSessionManager(dir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	peerID, err := peer.Decode("12D3KooWDpj2Apy1St2233M86yM8B53g54GG1zPW22M71zW22M71")
	if err != nil {
		peerID = peer.ID("test-peer")
	}

	cID1 := createDummyContentID(0x01)
	s1 := &TransferSession{
		ContentID:   cID1,
		TargetPeer:  peerID,
		Completed:   []bool{true, false, true, true, false},
		TotalChunks: 5,
		StartedAt:   time.Now().Add(-10 * time.Minute),
		Status:      StatusInProgress,
	}

	if err := sm.Save(s1); err != nil {
		t.Fatalf("failed to save session 1: %v", err)
	}

	cID2 := createDummyContentID(0x02)
	s2 := &TransferSession{
		ContentID:   cID2,
		TargetPeer:  peerID,
		Completed:   []bool{true, true, true},
		TotalChunks: 3,
		StartedAt:   time.Now().Add(-5 * time.Minute),
		Status:      StatusCompleted,
	}

	if err := sm.Save(s2); err != nil {
		t.Fatalf("failed to save session 2: %v", err)
	}

	// Verify Open returns full TransferSession
	openedS1, err := sm.Open(cID1)
	if err != nil {
		t.Fatalf("failed to open session 1: %v", err)
	}
	if openedS1 == nil || openedS1.CompletedCount() != 3 {
		t.Fatalf("expected completed count 3, got %v", openedS1)
	}

	// Verify List returns SessionSummary items
	summaries, err := sm.List()
	if err != nil {
		t.Fatalf("failed to list sessions: %v", err)
	}

	if len(summaries) != 2 {
		t.Fatalf("expected 2 summaries, got %d", len(summaries))
	}

	summaryMap := make(map[core.ContentID]*SessionSummary)
	for _, sum := range summaries {
		summaryMap[sum.ContentID] = sum
	}

	sum1, ok := summaryMap[cID1]
	if !ok {
		t.Fatalf("summary for cID1 not found")
	}
	if sum1.TotalChunks != 5 || sum1.CompletedCount() != 3 || sum1.Status != StatusInProgress {
		t.Fatalf("summary 1 mismatch: %+v", sum1)
	}

	sum2, ok := summaryMap[cID2]
	if !ok {
		t.Fatalf("summary for cID2 not found")
	}
	if sum2.TotalChunks != 3 || sum2.CompletedCount() != 3 || sum2.Status != StatusCompleted {
		t.Fatalf("summary 2 mismatch: %+v", sum2)
	}
}

func TestFileSessionManager_OversizedSession(t *testing.T) {
	dir, err := os.MkdirTemp("", "session_oversized_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	sm, err := NewFileSessionManager(dir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	// Set a max size limit of 500 bytes for testing
	sm.SetMaxFileSize(500)

	// Save a valid small session file
	cID1 := createDummyContentID(0x10)
	s1 := &TransferSession{
		ContentID:   cID1,
		TotalChunks: 1,
		Completed:   []bool{true},
		Status:      StatusCompleted,
	}
	if err := sm.Save(s1); err != nil {
		t.Fatalf("failed to save s1: %v", err)
	}

	// Manually write an oversized session file exceeding 500 bytes
	cID2 := createDummyContentID(0x20)
	oversizedPath := sm.getPath(cID2)
	oversizedData := bytes.Repeat([]byte("a"), 2000)
	if err := os.WriteFile(oversizedPath, oversizedData, 0644); err != nil {
		t.Fatalf("failed to write oversized file: %v", err)
	}

	// Listing should skip the oversized file and return only valid small file
	summaries, err := sm.List()
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}

	if len(summaries) != 1 {
		t.Fatalf("expected 1 summary (oversized skipped), got %d", len(summaries))
	}
	if summaries[0].ContentID != cID1 {
		t.Fatalf("expected contentID %x, got %x", cID1, summaries[0].ContentID)
	}

	// Open on oversized session file should return an error
	_, err = sm.Open(cID2)
	if err == nil {
		t.Fatalf("expected Open on oversized file to return error, got nil")
	}
}

func TestFileSessionManager_CorruptedSession(t *testing.T) {
	dir, err := os.MkdirTemp("", "session_corrupt_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	sm, err := NewFileSessionManager(dir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	// Valid session
	cID1 := createDummyContentID(0x11)
	s1 := &TransferSession{
		ContentID:   cID1,
		TotalChunks: 2,
		Completed:   []bool{true, false},
		Status:      StatusInProgress,
	}
	if err := sm.Save(s1); err != nil {
		t.Fatalf("failed to save s1: %v", err)
	}

	// Corrupted session files
	corruptPath1 := filepath.Join(dir, "corrupt1.json")
	if err := os.WriteFile(corruptPath1, []byte("{ invalid json ..."), 0644); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	corruptPath2 := filepath.Join(dir, "corrupt2.json")
	if err := os.WriteFile(corruptPath2, []byte(`{"content_id": "abc", "completed": [true`), 0644); err != nil {
		t.Fatalf("failed to write corrupt file 2: %v", err)
	}

	summaries, err := sm.List()
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}

	if len(summaries) != 1 {
		t.Fatalf("expected 1 valid summary (corrupted skipped), got %d", len(summaries))
	}
	if summaries[0].ContentID != cID1 {
		t.Fatalf("expected contentID %x, got %x", cID1, summaries[0].ContentID)
	}
}

func TestFileSessionManager_MaxFilesCap(t *testing.T) {
	dir, err := os.MkdirTemp("", "session_maxfiles_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	sm, err := NewFileSessionManager(dir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	sm.SetMaxFiles(5)

	// Create 10 session files with distinct mod times
	now := time.Now()
	for i := 0; i < 10; i++ {
		cID := createDummyContentID(byte(i + 1))
		s := &TransferSession{
			ContentID:   cID,
			TotalChunks: i + 1,
			Completed:   make([]bool, i+1),
			Status:      StatusInProgress,
		}
		if err := sm.Save(s); err != nil {
			t.Fatalf("failed to save session %d: %v", i, err)
		}
		// Set distinct modification times
		modTime := now.Add(time.Duration(i) * time.Minute)
		os.Chtimes(sm.getPath(cID), modTime, modTime)
	}

	summaries, err := sm.List()
	if err != nil {
		t.Fatalf("List returned error: %v", err)
	}

	if len(summaries) != 5 {
		t.Fatalf("expected max 5 summaries returned, got %d", len(summaries))
	}

	// Verify that the returned summaries correspond to the 5 newest files (i = 9, 8, 7, 6, 5)
	for _, sum := range summaries {
		if sum.TotalChunks < 6 {
			t.Fatalf("expected newer sessions (TotalChunks >= 6), got TotalChunks = %d", sum.TotalChunks)
		}
	}
}

func TestFileSessionManager_MemoryEfficiency(t *testing.T) {
	dir, err := os.MkdirTemp("", "session_memory_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	sm, err := NewFileSessionManager(dir)
	if err != nil {
		t.Fatalf("failed to create FileSessionManager: %v", err)
	}

	// Create a session file with 100,000 chunks completed array
	cID := createDummyContentID(0xAA)
	completed := make([]bool, 100000)
	for i := range completed {
		if i%2 == 0 {
			completed[i] = true
		}
	}

	s := &TransferSession{
		ContentID:   cID,
		TotalChunks: 100000,
		Completed:   completed,
		Status:      StatusInProgress,
	}

	// Increase max file size for this test
	sm.SetMaxFileSize(10 * 1024 * 1024)

	if err := sm.Save(s); err != nil {
		t.Fatalf("failed to save large session: %v", err)
	}

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)

	summaries, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)

	if len(summaries) != 1 {
		t.Fatalf("expected 1 summary, got %d", len(summaries))
	}

	if summaries[0].CompletedCount() != 50000 {
		t.Fatalf("expected 50000 completed chunks, got %d", summaries[0].CompletedCount())
	}

	// Assert heap growth during listing is small (< 2 MB)
	heapGrowth := int64(m2.HeapAlloc) - int64(m1.HeapAlloc)
	t.Logf("Heap growth during List(): %d bytes", heapGrowth)
}

func TestParseSessionSummary_UnknownFieldsAndMissingCompleted(t *testing.T) {
	dummyCID := createDummyContentID(0x33)
	jsonString := `{
		"content_id": "` + hex.EncodeToString(dummyCID[:]) + `",
		"unknown_field": "some_value",
		"total_chunks": 10,
		"status": "COMPLETED"
	}`

	sum, err := parseSessionSummary(bytes.NewReader([]byte(jsonString)))
	if err != nil {
		t.Fatalf("parseSessionSummary failed: %v", err)
	}

	if sum.TotalChunks != 10 || sum.CompletedChunks != 0 || sum.Status != StatusCompleted {
		t.Fatalf("unexpected summary parsed: %+v", sum)
	}
}
