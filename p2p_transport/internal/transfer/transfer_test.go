package transfer

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
)

type mockStream struct {
	c           net.Conn
	resetCalled bool
	closed      bool
}

func (m *mockStream) Read(p []byte) (n int, err error) {
	return m.c.Read(p)
}

func (m *mockStream) Write(p []byte) (n int, err error) {
	return m.c.Write(p)
}

func (m *mockStream) Close() error {
	m.closed = true
	return m.c.Close()
}

func (m *mockStream) Reset() error {
	m.resetCalled = true
	return m.c.Close()
}

func (m *mockStream) ResetWithError(code network.StreamErrorCode) error {
	m.resetCalled = true
	return m.c.Close()
}

func (m *mockStream) CloseRead() error {
	return nil
}

func (m *mockStream) CloseWrite() error {
	return nil
}

func (m *mockStream) SetDeadline(t time.Time) error {
	return m.c.SetDeadline(t)
}

func (m *mockStream) SetReadDeadline(t time.Time) error {
	return m.c.SetReadDeadline(t)
}

func (m *mockStream) SetWriteDeadline(t time.Time) error {
	return m.c.SetWriteDeadline(t)
}

func (m *mockStream) ID() string {
	return "mock-stream-id"
}

func (m *mockStream) Protocol() libp2p_protocol.ID {
	return "/cipher/file/1.0.0"
}

func (m *mockStream) SetProtocol(libp2p_protocol.ID) error {
	return nil
}

func (m *mockStream) Stat() network.Stats {
	return network.Stats{}
}

func (m *mockStream) Scope() network.StreamScope {
	return &network.NullScope{}
}

func (m *mockStream) Conn() network.Conn {
	return nil
}

func newMockPipe() (*mockStream, *mockStream) {
	c1, c2 := net.Pipe()
	return &mockStream{c: c1}, &mockStream{c: c2}
}

func TestHeaderSerialization(t *testing.T) {
	h := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test_file.txt",
		FileSize: 1024,
	}
	var checksum [32]byte
	copy(checksum[:], []byte("01234567890123456789012345678901"))
	h.Checksum = checksum

	buf := &bytes.Buffer{}
	if err := h.WriteTo(buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	if err := h.WriteChecksum(buf); err != nil {
		t.Fatalf("WriteChecksum failed: %v", err)
	}

	readH := &Header{}
	if err := readH.ReadFrom(buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if err := readH.ReadChecksum(buf); err != nil {
		t.Fatalf("ReadChecksum failed: %v", err)
	}

	if readH.Version != h.Version || readH.Type != h.Type || readH.Filename != h.Filename || readH.FileSize != h.FileSize {
		t.Fatalf("Header metadata mismatch: got %+v, want %+v", readH, h)
	}
	if readH.Checksum != h.Checksum {
		t.Fatalf("Checksum mismatch: got %x, want %x", readH.Checksum, h.Checksum)
	}
}

func TestFilenameLengthValidation(t *testing.T) {
	longName := strings.Repeat("a", MaxFilenameSize+1)
	h := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: longName,
		FileSize: 100,
	}

	buf := &bytes.Buffer{}
	err := h.WriteTo(buf)
	if !errors.Is(err, ErrFilenameLengthInvalid) {
		t.Fatalf("expected ErrFilenameLengthInvalid on WriteTo, got %v", err)
	}

	s1, _ := newMockPipe()
	defer s1.Close()

	// Test Send parameter validation with long filename (should fail before opening file)
	longFilePath := filepath.Join(t.TempDir(), longName)

	err = Send(s1, longFilePath)
	if !errors.Is(err, ErrFilenameLengthInvalid) {
		t.Fatalf("expected ErrFilenameLengthInvalid on Send, got %v", err)
	}
	if !s1.resetCalled {
		t.Fatalf("expected s1.Reset() to be called on invalid filename length")
	}
}

func TestNilStreamValidation(t *testing.T) {
	if err := Send(nil, "somepath"); !errors.Is(err, ErrNilStream) {
		t.Fatalf("expected ErrNilStream on Send(nil, ...), got %v", err)
	}
	if err := Receive(nil); !errors.Is(err, ErrNilStream) {
		t.Fatalf("expected ErrNilStream on Receive(nil), got %v", err)
	}
}

func TestEmptyFilePathValidation(t *testing.T) {
	s1, _ := newMockPipe()
	defer s1.Close()

	err := Send(s1, "")
	if !errors.Is(err, ErrEmptyFilePath) {
		t.Fatalf("expected ErrEmptyFilePath on Send(s1, \"\"), got %v", err)
	}
	if !s1.resetCalled {
		t.Fatalf("expected stream reset on empty file path")
	}
}

func TestEndToEndTransfer(t *testing.T) {
	// Create temporary source file
	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "sample.dat")
	data := make([]byte, 500*1024) // 500 KB
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}
	if err := os.WriteFile(srcPath, data, 0644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	// Change working dir for receiver downloads folder during test
	workDir := t.TempDir()
	origWd, _ := os.Getwd()
	if err := os.Chdir(workDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}
	defer os.Chdir(origWd)

	s1, s2 := newMockPipe()

	errCh := make(chan error, 2)

	go func() {
		errCh <- Send(s1, srcPath)
	}()

	go func() {
		errCh <- Receive(s2)
	}()

	for i := 0; i < 2; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("transfer failed with error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("test timed out waiting for transfer completion")
		}
	}

	// Verify downloaded file content and checksum
	recvPath := filepath.Join(workDir, "downloads", "sample.dat")
	recvData, err := os.ReadFile(recvPath)
	if err != nil {
		t.Fatalf("failed to read downloaded file: %v", err)
	}

	if !bytes.Equal(recvData, data) {
		t.Fatalf("downloaded content mismatch")
	}

	srcSum := sha256.Sum256(data)
	recvSum := sha256.Sum256(recvData)
	if srcSum != recvSum {
		t.Fatalf("checksum mismatch between source and received file")
	}
}

func TestSinglePassVerification(t *testing.T) {
	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "singlepass.dat")
	data := []byte("hello single pass streaming data test")
	if err := os.WriteFile(srcPath, data, 0644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	workDir := t.TempDir()
	origWd, _ := os.Getwd()
	if err := os.Chdir(workDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}
	defer os.Chdir(origWd)

	s1, s2 := newMockPipe()

	errCh := make(chan error, 2)
	go func() {
		errCh <- Send(s1, srcPath)
	}()
	go func() {
		errCh <- Receive(s2)
	}()

	for i := 0; i < 2; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("transfer failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for transfer")
		}
	}

	recvPath := filepath.Join(workDir, "downloads", "singlepass.dat")
	recvData, err := os.ReadFile(recvPath)
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}
	if !bytes.Equal(recvData, data) {
		t.Fatalf("received data mismatch: got %s, want %s", string(recvData), string(data))
	}
}

func TestChecksumMismatchHandling(t *testing.T) {
	workDir := t.TempDir()
	origWd, _ := os.Getwd()
	if err := os.Chdir(workDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}
	defer os.Chdir(origWd)

	s1, s2 := newMockPipe()

	// Sender manually sends corrupt checksum
	go func() {
		defer s1.Close()
		payload := []byte("hello world")
		h := &Header{
			Version:  ProtocolVersion1,
			Type:     MsgTypeFileTransfer,
			Filename: "corrupt.txt",
			FileSize: uint64(len(payload)),
		}
		if err := h.WriteTo(s1); err != nil {
			return
		}
		s1.Write(payload)
		// Write incorrect checksum
		var badChecksum [32]byte
		badChecksum[0] = 0xFF
		s1.Write(badChecksum[:])
	}()

	err := Receive(s2)
	if !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
	if !s2.resetCalled {
		t.Fatalf("expected s2.Reset() on checksum mismatch")
	}

	// Verify partial file was cleaned up from downloads directory
	corruptPath := filepath.Join(workDir, "downloads", "corrupt.txt")
	if _, err := os.Stat(corruptPath); !os.IsNotExist(err) {
		t.Fatalf("expected corrupt partial download file to be removed, but it exists")
	}
}
