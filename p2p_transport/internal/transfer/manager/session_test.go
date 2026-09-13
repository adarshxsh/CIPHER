package manager

import (
	"os"
	"path/filepath"
	"testing"

	"cipher/internal/content/core"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestNewFileSessionManager_Permissions(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "sessions")

	sm, err := NewFileSessionManager(tmpDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}
	_ = sm

	info, err := os.Stat(tmpDir)
	if err != nil {
		t.Fatalf("Stat on session dir failed: %v", err)
	}

	perm := info.Mode().Perm()
	if perm&0777 != 0700 {
		t.Errorf("Expected dir permissions 0700, got %o", perm&0777)
	}
}

func TestNewFileSessionManager_Remediation(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "insecure_sessions")

	// Pre-create directory with insecure permissions (0755)
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	if err := os.Chmod(tmpDir, 0755); err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	info, err := os.Stat(tmpDir)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Mode().Perm()&0777 != 0755 {
		t.Fatalf("Setup check failed: expected 0755, got %o", info.Mode().Perm()&0777)
	}

	// NewFileSessionManager should remediate directory permissions to 0700
	_, err = NewFileSessionManager(tmpDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	info, err = os.Stat(tmpDir)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	perm := info.Mode().Perm()
	if perm&0777 != 0700 {
		t.Errorf("Expected remediated dir permissions 0700, got %o", perm&0777)
	}
}

func TestFileSessionManager_Save_PermissionsAndRemediation(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "sessions")
	sm, err := NewFileSessionManager(tmpDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	var cid core.ContentID
	cid[0] = 0xab
	cid[1] = 0xcd

	sess := &TransferSession{
		ContentID:   cid,
		TargetPeer:  peer.ID("test-peer"),
		Completed:   []bool{true, false},
		TotalChunks: 2,
		Status:      StatusInProgress,
	}

	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	filePath := sm.getPath(cid)
	info, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat on session file failed: %v", err)
	}

	perm := info.Mode().Perm()
	if perm&0777 != 0600 {
		t.Errorf("Expected state file permissions 0600, got %o", perm&0777)
	}

	// Test remediation of existing file with 0644 permissions
	if err := os.Chmod(filePath, 0644); err != nil {
		t.Fatalf("Chmod failed: %v", err)
	}

	info, err = os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if info.Mode().Perm()&0777 != 0644 {
		t.Fatalf("Setup check failed: expected 0644, got %o", info.Mode().Perm()&0777)
	}

	// Save should remediate existing file permissions to 0600
	sess.Status = StatusCompleted
	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save failed on update: %v", err)
	}

	info, err = os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	perm = info.Mode().Perm()
	if perm&0777 != 0600 {
		t.Errorf("Expected remediated file permissions 0600, got %o", perm&0777)
	}
}

func TestFileSessionManager_NonOwnerAccessRejection(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "sessions")
	sm, err := NewFileSessionManager(tmpDir)
	if err != nil {
		t.Fatalf("NewFileSessionManager failed: %v", err)
	}

	var cid core.ContentID
	cid[0] = 0x12

	sess := &TransferSession{
		ContentID:   cid,
		TargetPeer:  peer.ID("test-peer"),
		Completed:   []bool{true},
		TotalChunks: 1,
		Status:      StatusInProgress,
	}

	if err := sm.Save(sess); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	dirInfo, err := os.Stat(tmpDir)
	if err != nil {
		t.Fatalf("Stat dir failed: %v", err)
	}

	// Verify dir group and other permissions are 0
	if dirInfo.Mode().Perm()&0077 != 0 {
		t.Errorf("Directory has non-owner access bits set: %o", dirInfo.Mode().Perm()&0077)
	}

	filePath := sm.getPath(cid)
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		t.Fatalf("Stat file failed: %v", err)
	}

	// Verify file group and other permissions are 0
	if fileInfo.Mode().Perm()&0077 != 0 {
		t.Errorf("File has non-owner access bits set: %o", fileInfo.Mode().Perm()&0077)
	}
}
