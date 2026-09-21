package manager

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

func validPeerID(t *testing.T) peer.ID {
	t.Helper()
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("failed to get peer ID: %v", err)
	}
	return pid
}

func TestFileSessionManager_DefaultLimits(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	if sm.MaxSessionFiles != DefaultMaxSessionFiles {
		t.Errorf("expected MaxSessionFiles %d, got %d", DefaultMaxSessionFiles, sm.MaxSessionFiles)
	}
	if sm.MaxSessionFileSize != DefaultMaxSessionFileSize {
		t.Errorf("expected MaxSessionFileSize %d, got %d", DefaultMaxSessionFileSize, sm.MaxSessionFileSize)
	}
}

func TestFileSessionManager_Options(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir, WithMaxSessionFiles(250), WithMaxSessionFileSize(2*1024*1024))
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	if sm.MaxSessionFiles != 250 {
		t.Errorf("expected MaxSessionFiles 250, got %d", sm.MaxSessionFiles)
	}
	if sm.MaxSessionFileSize != 2*1024*1024 {
		t.Errorf("expected MaxSessionFileSize %d, got %d", 2*1024*1024, sm.MaxSessionFileSize)
	}
}

func TestFileSessionManager_MaxSessionFiles(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	pid := validPeerID(t)

	// Create 150 session files
	for i := 0; i < 150; i++ {
		var cid core.ContentID
		cid[0] = byte(i >> 8)
		cid[1] = byte(i)
		session := &TransferSession{
			ContentID:   cid,
			TargetPeer:  pid,
			TotalChunks: 10,
			Status:      StatusInProgress,
		}
		if err := sm.Save(session); err != nil {
			t.Fatalf("failed to save session %d: %v", i, err)
		}
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(sessions) != DefaultMaxSessionFiles {
		t.Errorf("expected %d sessions, got %d", DefaultMaxSessionFiles, len(sessions))
	}

	// Update MaxSessionFiles to 30 and re-test
	sm.MaxSessionFiles = 30
	sessions, err = sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(sessions) != 30 {
		t.Errorf("expected 30 sessions, got %d", len(sessions))
	}
}

func TestFileSessionManager_MaxSessionFileSize_Open(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir, WithMaxSessionFileSize(1000))
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	pid := validPeerID(t)

	// 1. Normal session under 1000 bytes
	var normalCID core.ContentID
	normalCID[0] = 0x01
	normalSess := &TransferSession{
		ContentID:   normalCID,
		TargetPeer:  pid,
		TotalChunks: 5,
		Status:      StatusCompleted,
	}
	if err := sm.Save(normalSess); err != nil {
		t.Fatalf("failed to save normal session: %v", err)
	}

	opened, err := sm.Open(normalCID)
	if err != nil {
		t.Fatalf("Open failed on normal session: %v", err)
	}
	if opened == nil || opened.Status != StatusCompleted {
		t.Errorf("unexpected open result: %+v", opened)
	}

	// 2. Oversized session exceeding 1000 bytes
	var oversizedCID core.ContentID
	oversizedCID[0] = 0x02
	oversizedPath := filepath.Join(tempDir, fmt.Sprintf("%x.json", oversizedCID))

	// Write dummy data > 1000 bytes
	bigData := make([]byte, 1500)
	for i := range bigData {
		bigData[i] = 'a'
	}
	if err := os.WriteFile(oversizedPath, bigData, 0644); err != nil {
		t.Fatalf("failed to write oversized file: %v", err)
	}

	_, err = sm.Open(oversizedCID)
	if err == nil {
		t.Fatalf("expected error when opening oversized session file, got nil")
	}
	if !errors.Is(err, ErrSessionFileTooLarge) {
		t.Errorf("expected ErrSessionFileTooLarge, got %v", err)
	}
}

func TestFileSessionManager_MaxSessionFileSize_List(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir, WithMaxSessionFileSize(500))
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	pid := validPeerID(t)

	// Save 3 normal sessions (<500 bytes)
	for i := 0; i < 3; i++ {
		var cid core.ContentID
		cid[0] = byte(i + 1)
		s := &TransferSession{
			ContentID:   cid,
			TargetPeer:  pid,
			TotalChunks: 2,
			Status:      StatusInProgress,
		}
		if err := sm.Save(s); err != nil {
			t.Fatalf("failed to save session: %v", err)
		}
	}

	// Write 1 oversized file (>500 bytes)
	oversizedPath := filepath.Join(tempDir, "oversized.json")
	if err := os.WriteFile(oversizedPath, make([]byte, 1000), 0644); err != nil {
		t.Fatalf("failed to write oversized file: %v", err)
	}

	// Write 1 malformed JSON file
	malformedPath := filepath.Join(tempDir, "malformed.json")
	if err := os.WriteFile(malformedPath, []byte("NOT_VALID_JSON{{{"), 0644); err != nil {
		t.Fatalf("failed to write malformed file: %v", err)
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(sessions) != 3 {
		t.Errorf("expected 3 valid sessions, got %d", len(sessions))
	}
}

func TestFileSessionManager_Performance_1000Files(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_perf_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	sm, err := NewFileSessionManager(tempDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	pid := validPeerID(t)

	// Pre-create 1,000 session files
	sessionTemplate := TransferSession{
		TargetPeer:  pid,
		TotalChunks: 100,
		Status:      StatusInProgress,
	}
	sessionBytes, err := json.Marshal(sessionTemplate)
	if err != nil {
		t.Fatalf("failed to marshal template: %v", err)
	}

	for i := 0; i < 1000; i++ {
		filePath := filepath.Join(tempDir, fmt.Sprintf("%08x.json", i))
		if err := os.WriteFile(filePath, sessionBytes, 0644); err != nil {
			t.Fatalf("failed to write session file %d: %v", i, err)
		}
	}

	start := time.Now()
	sessions, err := sm.List()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("List failed: %v", err)
	}

	if len(sessions) != DefaultMaxSessionFiles {
		t.Errorf("expected %d sessions, got %d", DefaultMaxSessionFiles, len(sessions))
	}

	if elapsed > 50*time.Millisecond {
		t.Errorf("List operation took %v, expected < 50ms", elapsed)
	} else {
		t.Logf("List operation on 1,000 files completed in %v", elapsed)
	}
}

func TestFileSessionManager_ZeroValueStruct(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "session_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	pid := validPeerID(t)

	// Direct zero-value struct instantiation
	sm := &FileSessionManager{dir: tempDir}

	var cid core.ContentID
	cid[0] = 0x99
	s := &TransferSession{
		ContentID:   cid,
		TargetPeer:  pid,
		TotalChunks: 1,
		Status:      StatusInProgress,
	}
	if err := sm.Save(s); err != nil {
		t.Fatalf("Save failed on zero value struct: %v", err)
	}

	opened, err := sm.Open(cid)
	if err != nil {
		t.Fatalf("Open failed on zero value struct: %v", err)
	}
	if opened == nil {
		t.Fatalf("Open returned nil on zero value struct")
	}

	sessions, err := sm.List()
	if err != nil {
		t.Fatalf("List failed on zero value struct: %v", err)
	}
	if len(sessions) != 1 {
		t.Errorf("expected 1 session, got %d", len(sessions))
	}
}
