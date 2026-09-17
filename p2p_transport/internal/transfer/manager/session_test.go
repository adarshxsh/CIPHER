package manager

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func TestDecodeSessionSummary(t *testing.T) {
	_, pub, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("Failed to generate key pair: %v", err)
	}
	peerID, err := peer.IDFromPublicKey(pub)
	if err != nil {
		t.Fatalf("Failed to create peer ID: %v", err)
	}

	var contentID core.ContentID
	copy(contentID[:], []byte("12345678901234567890123456789012"))

	now := time.Now().Truncate(time.Second)

	sess := TransferSession{
		ContentID:   contentID,
		TargetPeer:  peerID,
		TotalChunks: 10,
		StartedAt:   now,
		UpdatedAt:   now,
		Status:      StatusInProgress,
		Completed:   []bool{true, false, true, true, false, true, false, false, true, false},
	}

	dir := t.TempDir()
	sm, err := NewFileSessionManager(dir)
	if err != nil {
		t.Fatalf("Failed to create session manager: %v", err)
	}

	if err := sm.Save(&sess); err != nil {
		t.Fatalf("Failed to save session: %v", err)
	}

	path := sm.getPath(contentID)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Failed to open saved session file: %v", err)
	}
	defer f.Close()

	summary, err := DecodeSessionSummary(f)
	if err != nil {
		t.Fatalf("DecodeSessionSummary failed: %v", err)
	}

	if summary.ContentID != contentID {
		t.Errorf("ContentID mismatch: expected %x, got %x", contentID, summary.ContentID)
	}
	if summary.TargetPeer != peerID {
		t.Errorf("TargetPeer mismatch: expected %s, got %s", peerID, summary.TargetPeer)
	}
	if summary.TotalChunks != 10 {
		t.Errorf("TotalChunks mismatch: expected 10, got %d", summary.TotalChunks)
	}
	if summary.CompletedChunks != 5 {
		t.Errorf("CompletedChunks mismatch: expected 5, got %d", summary.CompletedChunks)
	}
	if summary.CompletedCount() != 5 {
		t.Errorf("CompletedCount mismatch: expected 5, got %d", summary.CompletedCount())
	}
	if summary.Status != StatusInProgress {
		t.Errorf("Status mismatch: expected %s, got %s", StatusInProgress, summary.Status)
	}
}

func TestFileSessionManagerPagination(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewFileSessionManager(dir)
	if err != nil {
		t.Fatalf("Failed to create session manager: %v", err)
	}

	// Create 15 session files
	for i := 0; i < 15; i++ {
		var contentID core.ContentID
		contentID[0] = byte(i + 1)
		sess := TransferSession{
			ContentID:   contentID,
			TargetPeer:  peer.ID(fmt.Sprintf("peer-%d", i)),
			TotalChunks: 20,
			StartedAt:   time.Now(),
			UpdatedAt:   time.Now(),
			Status:      StatusInProgress,
			Completed:   make([]bool, 20),
		}
		if err := sm.Save(&sess); err != nil {
			t.Fatalf("Failed to save session %d: %v", i, err)
		}
	}

	// Page 1: limit 5, offset 0
	p1, err := sm.List(0, 5)
	if err != nil {
		t.Fatalf("List page 1 failed: %v", err)
	}
	if len(p1) != 5 {
		t.Errorf("Expected 5 items in page 1, got %d", len(p1))
	}

	// Page 2: limit 5, offset 5
	p2, err := sm.List(5, 5)
	if err != nil {
		t.Fatalf("List page 2 failed: %v", err)
	}
	if len(p2) != 5 {
		t.Errorf("Expected 5 items in page 2, got %d", len(p2))
	}

	// Page 3: limit 10, offset 10
	p3, err := sm.List(10, 10)
	if err != nil {
		t.Fatalf("List page 3 failed: %v", err)
	}
	if len(p3) != 5 {
		t.Errorf("Expected 5 remaining items in page 3, got %d", len(p3))
	}

	// Out of bounds offset
	p4, err := sm.List(20, 5)
	if err != nil {
		t.Fatalf("List page 4 failed: %v", err)
	}
	if len(p4) != 0 {
		t.Errorf("Expected 0 items out of bounds, got %d", len(p4))
	}
}

func TestLargeSessionListingMemoryBounds(t *testing.T) {
	dir := t.TempDir()

	// Generate 1,000 session files with 100,000 chunks each
	// Construct a sample raw JSON file content for fast creation
	var buf bytes.Buffer
	buf.WriteString(`{
  "content_id": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32],
  "target_peer": "12D3KooWDpjMChWrL3229T342y52S21j23000000000000000000",
  "total_chunks": 100000,
  "started_at": "2026-09-16T00:00:00Z",
  "updated_at": "2026-09-16T00:00:00Z",
  "status": "IN_PROGRESS",
  "completed": [`)

	for i := 0; i < 100000; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		if i%2 == 0 {
			buf.WriteString("true")
		} else {
			buf.WriteString("false")
		}
	}
	buf.WriteString(`]
}`)

	jsonBytes := buf.Bytes()

	for i := 0; i < 1000; i++ {
		filename := filepath.Join(dir, fmt.Sprintf("session_%04d.json", i))
		if err := os.WriteFile(filename, jsonBytes, 0644); err != nil {
			t.Fatalf("Failed to write test file %d: %v", i, err)
		}
	}

	sm, err := NewFileSessionManager(dir)
	if err != nil {
		t.Fatalf("Failed to create session manager: %v", err)
	}

	// Force GC before measuring baseline memory
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Retrieve paginated lists over all 1,000 session files
	totalRetrieved := 0
	for offset := 0; offset < 1000; offset += 100 {
		summaries, err := sm.List(offset, 100)
		if err != nil {
			t.Fatalf("sm.List(%d, 100) failed: %v", offset, err)
		}
		totalRetrieved += len(summaries)
		if len(summaries) > 0 {
			if summaries[0].TotalChunks != 100000 {
				t.Fatalf("Expected 100000 total_chunks, got %d", summaries[0].TotalChunks)
			}
		}
	}

	if totalRetrieved != 1000 {
		t.Fatalf("Expected 1000 total retrieved summaries, got %d", totalRetrieved)
	}

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Memory allocated during listing operation
	allocBytes := memAfter.TotalAlloc - memBefore.TotalAlloc
	maxAllowed := uint64(10 * 1024 * 1024) // 10 MB

	t.Logf("TotalAlloc during listing of 1,000 files with 100k chunks: %d bytes (%.2f MB)",
		allocBytes, float64(allocBytes)/(1024*1024))

	if allocBytes > maxAllowed {
		t.Fatalf("Memory allocation exceeded 10 MB limit: allocated %d bytes", allocBytes)
	}
}
