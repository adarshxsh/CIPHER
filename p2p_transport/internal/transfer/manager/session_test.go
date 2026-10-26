package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func testPeerID(t *testing.T) peer.ID {
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 256)
	if err != nil {
		t.Fatalf("Failed to generate test key pair: %v", err)
	}
	id, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("Failed to generate peer ID: %v", err)
	}
	return id
}

func TestFileSessionManager_List_FileSizeCap(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	targetPeer := testPeerID(t)

	// 1. Valid small session file
	var id1 core.ContentID
	id1[0] = 0x01
	validSession := &TransferSession{
		ContentID:   id1,
		TargetPeer:  targetPeer,
		TotalChunks: 10,
		Status:      StatusInProgress,
		StartedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}
	if err := sm.Save(validSession); err != nil {
		t.Fatalf("Failed to save valid session: %v", err)
	}

	// 2. Oversized session file (> 512KB)
	var id2 core.ContentID
	id2[0] = 0x02
	oversizedPath := filepath.Join(tempDir, fmt.Sprintf("%x.json", id2))

	// Generate JSON payload larger than 512KB
	largePadding := strings.Repeat("a", int(MaxSessionFileSize)+1024)
	oversizedContent := fmt.Sprintf(`{"content_id":"%x","target_peer":"%s","padding":"%s"}`, id2, targetPeer.String(), largePadding)
	if err := os.WriteFile(oversizedPath, []byte(oversizedContent), 0644); err != nil {
		t.Fatalf("Failed to write oversized file: %v", err)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	if len(sessions) != 1 {
		t.Fatalf("Expected 1 session returned from List(), got %d", len(sessions))
	}
	if sessions[0].ContentID != id1 {
		t.Errorf("Expected session content ID %x, got %x", id1, sessions[0].ContentID)
	}
}

func TestFileSessionManager_List_MaxCountLimit(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	// Set maxListed to 5 for fast testing
	sm.maxListed = 5

	targetPeer := testPeerID(t)

	// Write 10 session files
	for i := 0; i < 10; i++ {
		var id core.ContentID
		id[0] = byte(i + 1)
		session := &TransferSession{
			ContentID:   id,
			TargetPeer:  targetPeer,
			TotalChunks: 5,
			Status:      StatusInProgress,
		}
		if err := sm.Save(session); err != nil {
			t.Fatalf("Failed to save session %d: %v", i, err)
		}
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	if len(sessions) != 5 {
		t.Fatalf("Expected List() to return 5 sessions due to maxListed cap, got %d", len(sessions))
	}
}

func TestFileSessionManager_List_GracefulMalformedHandling(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	targetPeer := testPeerID(t)

	// 1. Valid session
	var validID core.ContentID
	validID[0] = 0xAA
	validSession := &TransferSession{
		ContentID:   validID,
		TargetPeer:  targetPeer,
		TotalChunks: 3,
		Status:      StatusCompleted,
	}
	if err := sm.Save(validSession); err != nil {
		t.Fatalf("Failed to save valid session: %v", err)
	}

	// 2. Malformed JSON file with .json extension
	malformedPath := filepath.Join(tempDir, "malformed.json")
	if err := os.WriteFile(malformedPath, []byte("NOT_VALID_JSON{123"), 0644); err != nil {
		t.Fatalf("Failed to write malformed file: %v", err)
	}

	// 3. Non-JSON text file
	textPath := filepath.Join(tempDir, "notes.txt")
	if err := os.WriteFile(textPath, []byte("some notes"), 0644); err != nil {
		t.Fatalf("Failed to write text file: %v", err)
	}

	// 4. Subdirectory ending in .json
	subDirPath := filepath.Join(tempDir, "subdir.json")
	if err := os.MkdirAll(subDirPath, 0755); err != nil {
		t.Fatalf("Failed to write subdir: %v", err)
	}

	// 5. Temporary .tmp file
	tmpPath := filepath.Join(tempDir, "temp.json.tmp")
	if err := os.WriteFile(tmpPath, []byte(`{"status":"IN_PROGRESS"}`), 0644); err != nil {
		t.Fatalf("Failed to write tmp file: %v", err)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List() returned error: %v", err)
	}

	if len(sessions) != 1 {
		t.Fatalf("Expected 1 valid session, got %d", len(sessions))
	}
	if sessions[0].ContentID != validID {
		t.Errorf("Expected content ID %x, got %x", validID, sessions[0].ContentID)
	}
}

func TestFileSessionManager_Open_OversizedFile(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	var id core.ContentID
	id[0] = 0x05
	path := sm.getPath(id)

	largePadding := strings.Repeat("x", int(MaxSessionFileSize)+100)
	if err := os.WriteFile(path, []byte(largePadding), 0644); err != nil {
		t.Fatalf("Failed to write oversized file: %v", err)
	}

	session, err := sm.Open(id)
	if err == nil {
		t.Fatalf("Expected Open() to fail for oversized file, got session: %v", session)
	}
	if !strings.Contains(err.Error(), "exceeds maximum size limit") {
		t.Errorf("Expected size limit error, got: %v", err)
	}
}

func TestFileSessionManager_Lifecycle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("Failed to create FileSessionManager: %v", err)
	}

	targetPeer := testPeerID(t)

	var id core.ContentID
	id[0] = 0x10
	s1 := &TransferSession{
		ContentID:   id,
		TargetPeer:  targetPeer,
		Completed:   []bool{true, false, true},
		TotalChunks: 3,
		Status:      StatusInProgress,
	}

	// 1. Save
	if err := sm.Save(s1); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// 2. Open
	s2, err := sm.Open(id)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if s2 == nil {
		t.Fatalf("Open returned nil session")
	}
	if s2.CompletedCount() != 2 {
		t.Errorf("Expected CompletedCount 2, got %d", s2.CompletedCount())
	}

	// 3. List
	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("Expected 1 session in List, got %d", len(sessions))
	}

	// 4. Delete
	if err := sm.Delete(id); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 5. Verify Open and List after delete
	s3, err := sm.Open(id)
	if err != nil {
		t.Fatalf("Open after delete failed: %v", err)
	}
	if s3 != nil {
		t.Errorf("Expected nil session after delete, got %v", s3)
	}

	sessionsPostDelete, err := sm.List()
	if err != nil {
		t.Fatalf("List after delete failed: %v", err)
	}
	if len(sessionsPostDelete) != 0 {
		t.Errorf("Expected 0 sessions after delete, got %d", len(sessionsPostDelete))
	}
}
