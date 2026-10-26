package manager

import (
	"encoding/json"
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

func TestDecodeSessionHeader(t *testing.T) {
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

	header, err := DecodeSessionHeader(f)
	if err != nil {
		t.Fatalf("DecodeSessionHeader failed: %v", err)
	}

	if header.ContentID != contentID {
		t.Errorf("ContentID mismatch: expected %x, got %x", contentID, header.ContentID)
	}
	if header.TargetPeer != peerID {
		t.Errorf("TargetPeer mismatch: expected %s, got %s", peerID, header.TargetPeer)
	}
	if header.TotalChunks != 10 {
		t.Errorf("TotalChunks mismatch: expected 10, got %d", header.TotalChunks)
	}
	if header.CompletedChunks != 5 {
		t.Errorf("CompletedChunks mismatch: expected 5, got %d", header.CompletedChunks)
	}
	if header.CompletedCount() != 5 {
		t.Errorf("CompletedCount mismatch: expected 5, got %d", header.CompletedCount())
	}
	if header.Status != StatusInProgress {
		t.Errorf("Status mismatch: expected %s, got %s", StatusInProgress, header.Status)
	}
}

func TestFileSessionManagerPaginationAndSorting(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewFileSessionManager(dir)
	if err != nil {
		t.Fatalf("Failed to create session manager: %v", err)
	}

	numSessions := 10
	baseTime := time.Now().Add(-10 * time.Minute)

	pid, _ := peer.Decode("12D3KooWKn69mpt1AatrtuDNE7sP235eAnM98ks88t4w17mPmyC4")

	for i := 0; i < numSessions; i++ {
		var cid core.ContentID
		cid[0] = byte(i + 1)
		s := &TransferSession{
			ContentID:   cid,
			TargetPeer:  pid,
			TotalChunks: 2,
			StartedAt:   baseTime.Add(time.Duration(i) * time.Minute),
			Status:      StatusInProgress,
			Completed:   []bool{true, false},
		}
		if err := sm.Save(s); err != nil {
			t.Fatalf("Failed to save session %d: %v", i, err)
		}

		sTime := baseTime.Add(time.Duration(i) * time.Minute)
		filePath := sm.getPath(cid)
		if err := os.Chtimes(filePath, sTime, sTime); err != nil {
			t.Fatalf("Failed to chtimes for %s: %v", filePath, err)
		}
		sm.mu.Lock()
		sm.modTimeCache[filepath.Base(filePath)] = sTime
		sm.mu.Unlock()
	}

	// Page 1: limit 3, offset 0 (Newest sessions first: cid[0]=10, 9, 8)
	p1, err := sm.List(0, 3)
	if err != nil {
		t.Fatalf("List page 1 failed: %v", err)
	}
	if len(p1) != 3 {
		t.Fatalf("Expected 3 items in page 1, got %d", len(p1))
	}
	if p1[0].ContentID[0] != byte(10) || p1[1].ContentID[0] != byte(9) || p1[2].ContentID[0] != byte(8) {
		t.Errorf("Unexpected session order on page 1: [%d, %d, %d]", p1[0].ContentID[0], p1[1].ContentID[0], p1[2].ContentID[0])
	}

	// Page 2: limit 3, offset 3
	p2, err := sm.List(3, 3)
	if err != nil {
		t.Fatalf("List page 2 failed: %v", err)
	}
	if len(p2) != 3 {
		t.Fatalf("Expected 3 items in page 2, got %d", len(p2))
	}

	// Out of bounds offset
	pEmpty, err := sm.List(20, 5)
	if err != nil {
		t.Fatalf("List page empty failed: %v", err)
	}
	if len(pEmpty) != 0 {
		t.Errorf("Expected 0 items out of bounds, got %d", len(pEmpty))
	}
}

func TestDefaultAndMaxLimits(t *testing.T) {
	dir := t.TempDir()
	sm, err := NewFileSessionManager(dir)
	if err != nil {
		t.Fatalf("Failed to create session manager: %v", err)
	}

	pid, _ := peer.Decode("12D3KooWKn69mpt1AatrtuDNE7sP235eAnM98ks88t4w17mPmyC4")

	for i := 0; i < 60; i++ {
		var cid core.ContentID
		cid[0] = byte((i >> 8) & 0xff)
		cid[1] = byte(i & 0xff)
		s := &TransferSession{
			ContentID:   cid,
			TargetPeer:  pid,
			TotalChunks: 2,
			StartedAt:   time.Now(),
			Status:      StatusInProgress,
			Completed:   []bool{true, false},
		}
		if err := sm.Save(s); err != nil {
			t.Fatalf("Failed to save session %d: %v", i, err)
		}
	}

	// Test default limit when limit <= 0
	sessions, err := sm.List(0, 0)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != DefaultPageLimit {
		t.Errorf("Expected default page limit of %d, got %d", DefaultPageLimit, len(sessions))
	}

	// Test requesting max limit cap
	allSessions, err := sm.List(0, 2000)
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(allSessions) != 60 {
		t.Errorf("Expected 60 sessions, got %d", len(allSessions))
	}
}

func TestLargeSessionListingMemoryBounds(t *testing.T) {
	dir := t.TempDir()

	pid, _ := peer.Decode("12D3KooWKn69mpt1AatrtuDNE7sP235eAnM98ks88t4w17mPmyC4")
	var cid core.ContentID
	cid[0] = 1

	sess := TransferSession{
		ContentID:   cid,
		TargetPeer:  pid,
		TotalChunks: 50000,
		StartedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Status:      StatusInProgress,
		Completed:   make([]bool, 50000),
	}
	for i := 0; i < 50000; i += 2 {
		sess.Completed[i] = true
	}

	jsonBytes, err := json.Marshal(&sess)
	if err != nil {
		t.Fatalf("Failed to marshal test session: %v", err)
	}

	for i := 0; i < 100; i++ {
		filename := filepath.Join(dir, fmt.Sprintf("session_%04d.json", i))
		if err := os.WriteFile(filename, jsonBytes, 0644); err != nil {
			t.Fatalf("Failed to write test file %d: %v", i, err)
		}
	}

	sm, err := NewFileSessionManager(dir)
	if err != nil {
		t.Fatalf("Failed to create session manager: %v", err)
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	totalRetrieved := 0
	for offset := 0; offset < 100; offset += 20 {
		headers, err := sm.List(offset, 20)
		if err != nil {
			t.Fatalf("sm.List(%d, 20) failed: %v", offset, err)
		}
		totalRetrieved += len(headers)
		if len(headers) > 0 {
			if headers[0].TotalChunks != 50000 {
				t.Fatalf("Expected 50000 total_chunks, got %d", headers[0].TotalChunks)
			}
			if headers[0].CompletedChunks != 25000 {
				t.Fatalf("Expected 25000 completed_chunks, got %d", headers[0].CompletedChunks)
			}
		}
	}

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	if totalRetrieved != 100 {
		t.Fatalf("Expected 100 total retrieved headers, got %d", totalRetrieved)
	}

	allocBytes := memAfter.TotalAlloc - memBefore.TotalAlloc
	t.Logf("TotalAlloc during listing: %d bytes (%.2f MB)", allocBytes, float64(allocBytes)/(1024*1024))
}
