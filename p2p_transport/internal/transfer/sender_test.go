package transfer

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSend_NilStream(t *testing.T) {
	err := Send(nil, "somefile.txt")
	if err == nil {
		t.Fatalf("expected error when stream is nil")
	}
}

func TestSend_FileNotFound(t *testing.T) {
	ts := &testStream{}
	err := Send(ts, "non_existent_file_path_12345.txt")
	if err == nil {
		t.Fatalf("expected error when file does not exist")
	}
	if !ts.resetCalled {
		t.Fatalf("expected stream.Reset() to be called on file open error")
	}
}

func TestSend_Success(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "send_test.txt")
	payload := []byte("hello world send test payload")
	if err := os.WriteFile(filePath, payload, 0600); err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}

	var writtenBuf bytes.Buffer
	ts := &testStream{
		writer: &writtenBuf,
	}

	err := Send(ts, filePath)
	if err != nil {
		t.Fatalf("unexpected error during Send: %v", err)
	}
	if ts.resetCalled {
		t.Fatalf("stream.Reset() should not be called on successful send")
	}
	if !ts.closedCalled {
		t.Fatalf("stream.Close() should be called on successful send")
	}

	if writtenBuf.Len() == 0 {
		t.Fatalf("expected header and payload data to be written to stream")
	}
}
