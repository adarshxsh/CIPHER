package transfer

import (
	"bytes"
	"crypto/sha256"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type mockStream struct {
	rawConn     net.Conn
	mu          sync.Mutex
	resetCalled bool
	closeCalled bool
}

func newMockStream(conn net.Conn) *mockStream {
	return &mockStream{rawConn: conn}
}

func (m *mockStream) Read(p []byte) (int, error) {
	if m.rawConn != nil {
		return m.rawConn.Read(p)
	}
	return 0, io.EOF
}

func (m *mockStream) Write(p []byte) (int, error) {
	if m.rawConn != nil {
		return m.rawConn.Write(p)
	}
	return len(p), nil
}

func (m *mockStream) Reset() error {
	m.mu.Lock()
	m.resetCalled = true
	m.mu.Unlock()
	if m.rawConn != nil {
		return m.rawConn.Close()
	}
	return nil
}

func (m *mockStream) ResetWithError(code network.StreamErrorCode) error {
	return m.Reset()
}

func (m *mockStream) Close() error {
	m.mu.Lock()
	m.closeCalled = true
	m.mu.Unlock()
	if m.rawConn != nil {
		return m.rawConn.Close()
	}
	return nil
}

func (m *mockStream) CloseRead() error              { return nil }
func (m *mockStream) CloseWrite() error             { return nil }
func (m *mockStream) ID() string                    { return "mock-stream-id" }
func (m *mockStream) Protocol() protocol.ID         { return "/cipher/filetransfer/1.0.0" }
func (m *mockStream) SetProtocol(protocol.ID) error { return nil }
func (m *mockStream) Stat() network.Stats           { return network.Stats{} }
func (m *mockStream) Conn() network.Conn            { return nil }
func (m *mockStream) Scope() network.StreamScope    { return &network.NullScope{} }
func (m *mockStream) SetDeadline(t time.Time) error {
	if m.rawConn != nil {
		return m.rawConn.SetDeadline(t)
	}
	return nil
}
func (m *mockStream) SetReadDeadline(t time.Time) error {
	if m.rawConn != nil {
		return m.rawConn.SetReadDeadline(t)
	}
	return nil
}
func (m *mockStream) SetWriteDeadline(t time.Time) error {
	if m.rawConn != nil {
		return m.rawConn.SetWriteDeadline(t)
	}
	return nil
}

func TestReceive_Success(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("downloads") })

	clientConn, serverConn := net.Pipe()
	s := newMockStream(serverConn)

	filename := "test_success.txt"
	content := []byte("Hello, world! Valid file content.")
	checksum := sha256.Sum256(content)

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(content)),
		Checksum: checksum,
	}

	go func() {
		defer clientConn.Close()
		_ = header.WriteTo(clientConn)
		_, _ = clientConn.Write(content)
	}()

	err := Receive(s)
	if err != nil {
		t.Fatalf("Expected Receive to succeed, got: %v", err)
	}

	finalPath := filepath.Join("downloads", filename)
	tmpPath := filepath.Join("downloads", "."+filename+".tmp")

	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("Temporary staging file %s should not exist after success", tmpPath)
	}

	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("Failed to read promoted file %s: %v", finalPath, err)
	}

	if !bytes.Equal(data, content) {
		t.Errorf("Content mismatch. Expected %q, got %q", content, data)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.resetCalled {
		t.Errorf("Expected Reset() NOT to be called on success")
	}
	if !s.closeCalled {
		t.Errorf("Expected Close() to be called on success")
	}
}

func TestReceive_NetworkStreamError(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("downloads") })

	clientConn, serverConn := net.Pipe()
	s := newMockStream(serverConn)

	filename := "test_network_err.txt"
	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: 1000,
		Checksum: [32]byte{1, 2, 3},
	}

	go func() {
		_ = header.WriteTo(clientConn)
		_, _ = clientConn.Write([]byte("Short payload"))
		_ = clientConn.Close() // abrupt close before sending full 1000 bytes
	}()

	err := Receive(s)
	if err == nil {
		t.Fatalf("Expected Receive to fail due to network stream error")
	}

	finalPath := filepath.Join("downloads", filename)
	tmpPath := filepath.Join("downloads", "."+filename+".tmp")

	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Errorf("Final file %s should not exist on failure", finalPath)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("Temporary file %s should be cleaned up on failure", tmpPath)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.resetCalled {
		t.Errorf("Expected Reset() to be called on error")
	}
}

func TestReceive_SizeMismatch(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("downloads") })

	clientConn, serverConn := net.Pipe()
	s := newMockStream(serverConn)

	filename := "test_size_mismatch.txt"
	content := []byte("12345")
	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: 10, // Expects 10, but only 5 sent
		Checksum: sha256.Sum256(content),
	}

	go func() {
		defer clientConn.Close()
		_ = header.WriteTo(clientConn)
		_, _ = clientConn.Write(content)
	}()

	err := Receive(s)
	if err == nil {
		t.Fatalf("Expected Receive to fail due to size mismatch")
	}

	finalPath := filepath.Join("downloads", filename)
	tmpPath := filepath.Join("downloads", "."+filename+".tmp")

	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Errorf("Final file %s should not exist on size mismatch", finalPath)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("Temporary file %s should be cleaned up on failure", tmpPath)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.resetCalled {
		t.Errorf("Expected Reset() to be called on error")
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("downloads") })

	clientConn, serverConn := net.Pipe()
	s := newMockStream(serverConn)

	filename := "test_checksum_mismatch.txt"
	content := []byte("Corrupted content payload")
	invalidChecksum := [32]byte{0xDE, 0xAD, 0xBE, 0xEF}

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(content)),
		Checksum: invalidChecksum,
	}

	go func() {
		defer clientConn.Close()
		_ = header.WriteTo(clientConn)
		_, _ = clientConn.Write(content)
	}()

	err := Receive(s)
	if err == nil {
		t.Fatalf("Expected Receive to fail due to checksum mismatch")
	}

	finalPath := filepath.Join("downloads", filename)
	tmpPath := filepath.Join("downloads", "."+filename+".tmp")

	if _, err := os.Stat(finalPath); !os.IsNotExist(err) {
		t.Errorf("Final file %s should not exist on checksum mismatch", finalPath)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Errorf("Temporary file %s should be cleaned up on checksum mismatch", tmpPath)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.resetCalled {
		t.Errorf("Expected Reset() to be called on checksum mismatch")
	}
}

func TestReceive_HeaderReadError(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("downloads") })

	clientConn, serverConn := net.Pipe()
	s := newMockStream(serverConn)

	go func() {
		_, _ = clientConn.Write([]byte{ProtocolVersion1})
		_ = clientConn.Close()
	}()

	err := Receive(s)
	if err == nil {
		t.Fatalf("Expected Receive to fail due to broken header read")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.resetCalled {
		t.Errorf("Expected Reset() to be called on header read error")
	}
}
