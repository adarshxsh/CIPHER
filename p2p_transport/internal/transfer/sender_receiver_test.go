package transfer

import (
	"bytes"
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

type mockConn struct {
	network.Conn
}

func (m *mockConn) RemotePeer() peer.ID {
	return peer.ID("test-peer-id")
}

func (m *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	maddr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return maddr
}

type mockStream struct {
	netConn net.Conn
	c       network.Conn
}

func (m *mockStream) Read(p []byte) (int, error) {
	return m.netConn.Read(p)
}

func (m *mockStream) Write(p []byte) (int, error) {
	return m.netConn.Write(p)
}

func (m *mockStream) Close() error {
	return m.netConn.Close()
}

func (m *mockStream) CloseWrite() error {
	return nil
}

func (m *mockStream) CloseRead() error {
	return nil
}

func (m *mockStream) Reset() error {
	return m.netConn.Close()
}

func (m *mockStream) ResetWithError(errCode network.StreamErrorCode) error {
	return m.netConn.Close()
}

func (m *mockStream) SetDeadline(t time.Time) error {
	return m.netConn.SetDeadline(t)
}

func (m *mockStream) SetReadDeadline(t time.Time) error {
	return m.netConn.SetReadDeadline(t)
}

func (m *mockStream) SetWriteDeadline(t time.Time) error {
	return m.netConn.SetWriteDeadline(t)
}

func (m *mockStream) ID() string {
	return "test-stream-id"
}

func (m *mockStream) Protocol() libp2p_protocol.ID {
	return libp2p_protocol.ID("/cipher/filetransfer/1.0.0")
}

func (m *mockStream) SetProtocol(id libp2p_protocol.ID) error {
	return nil
}

func (m *mockStream) Stat() network.Stats {
	return network.Stats{}
}

func (m *mockStream) Conn() network.Conn {
	if m.c != nil {
		return m.c
	}
	return &mockConn{}
}

func (m *mockStream) Scope() network.StreamScope {
	return nil
}

func TestSinglePassSendReceive_Success(t *testing.T) {
	tmpDir := t.TempDir()

	// Create test file
	testFileName := "test_data.bin"
	testFilePath := filepath.Join(tmpDir, testFileName)
	testContent := bytes.Repeat([]byte("Single pass file transfer test payload! "), 1000)
	if err := os.WriteFile(testFilePath, testContent, 0644); err != nil {
		t.Fatalf("failed to write temp test file: %v", err)
	}

	// Change working directory for receiver downloads
	origWd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get wd: %v", err)
	}
	defer os.Chdir(origWd)
	if err := os.Chdir(tmpDir); err != nil {
		t.Fatalf("failed to chdir: %v", err)
	}

	c1, c2 := net.Pipe()
	sSender := &mockStream{netConn: c1}
	sReceiver := &mockStream{netConn: c2}

	var wg sync.WaitGroup
	var sendErr, recvErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		sendErr = Send(sSender, testFilePath)
	}()

	go func() {
		defer wg.Done()
		recvErr = Receive(sReceiver)
	}()

	wg.Wait()

	if sendErr != nil {
		t.Fatalf("Send failed: %v", sendErr)
	}
	if recvErr != nil {
		t.Fatalf("Receive failed: %v", recvErr)
	}

	receivedPath := filepath.Join("downloads", testFileName)
	receivedBytes, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}

	if !bytes.Equal(testContent, receivedBytes) {
		t.Fatalf("received content does not match sent content")
	}
}

func TestSinglePassSendReceive_CorruptedPayload(t *testing.T) {
	tmpDir := t.TempDir()

	testFileName := "corrupt_data.bin"
	testFilePath := filepath.Join(tmpDir, testFileName)
	testContent := []byte("Hello, this is content that will be corrupted during transfer!")
	if err := os.WriteFile(testFilePath, testContent, 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	origWd, _ := os.Getwd()
	defer os.Chdir(origWd)
	_ = os.Chdir(tmpDir)

	c1, c2 := net.Pipe()

	corruptWriter := &corruptingConn{Conn: c1, corruptIndex: 35}

	sSender := &mockStream{netConn: corruptWriter}
	sReceiver := &mockStream{netConn: c2}

	var wg sync.WaitGroup
	var recvErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		_ = Send(sSender, testFilePath)
	}()

	go func() {
		defer wg.Done()
		recvErr = Receive(sReceiver)
	}()

	wg.Wait()

	if recvErr == nil {
		t.Fatalf("expected Receive to fail due to checksum mismatch, but got nil")
	}
}

type corruptingConn struct {
	net.Conn
	writtenTotal int64
	corruptIndex int64
}

func (cc *corruptingConn) Write(p []byte) (int, error) {
	pCopy := make([]byte, len(p))
	copy(pCopy, p)

	for i := range pCopy {
		if cc.writtenTotal+int64(i) == cc.corruptIndex {
			pCopy[i] ^= 0xFF // Flip bits
		}
	}
	n, err := cc.Conn.Write(pCopy)
	cc.writtenTotal += int64(n)
	return n, err
}

func TestBackwardCompatibility_ProtocolVersion1(t *testing.T) {
	tmpDir := t.TempDir()

	origWd, _ := os.Getwd()
	defer os.Chdir(origWd)
	_ = os.Chdir(tmpDir)

	c1, c2 := net.Pipe()
	sSender := &mockStream{netConn: c1}
	sReceiver := &mockStream{netConn: c2}

	filename := "v1_test.txt"
	payload := []byte("ProtocolVersion1 legacy stream payload")
	hasher := sha256.New()
	hasher.Write(payload)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: checksum,
	}

	var wg sync.WaitGroup
	var recvErr error

	wg.Add(2)
	go func() {
		defer wg.Done()
		defer sSender.Close()
		if err := header.WriteTo(sSender); err != nil {
			return
		}
		_, _ = sSender.Write(payload)
	}()

	go func() {
		defer wg.Done()
		recvErr = Receive(sReceiver)
	}()

	wg.Wait()

	if recvErr != nil {
		t.Fatalf("Receive failed for ProtocolVersion1: %v", recvErr)
	}

	receivedBytes, err := os.ReadFile(filepath.Join("downloads", filename))
	if err != nil {
		t.Fatalf("failed to read received v1 file: %v", err)
	}
	if !bytes.Equal(payload, receivedBytes) {
		t.Fatalf("payload mismatch for ProtocolVersion1")
	}
}

func TestHeaderProtocolVersion2WireFormat(t *testing.T) {
	var buf bytes.Buffer
	header := &Header{
		Version:  ProtocolVersion2,
		Type:     MsgTypeFileTransfer,
		Filename: "test.txt",
		FileSize: 100,
	}

	if err := header.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// Read back
	var readHeader Header
	if err := readHeader.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if readHeader.Version != ProtocolVersion2 {
		t.Errorf("expected version %d, got %d", ProtocolVersion2, readHeader.Version)
	}
	if readHeader.Filename != "test.txt" {
		t.Errorf("expected filename 'test.txt', got %s", readHeader.Filename)
	}
	if readHeader.FileSize != 100 {
		t.Errorf("expected size 100, got %d", readHeader.FileSize)
	}
	if buf.Len() != 0 {
		t.Errorf("expected 0 bytes remaining in buffer, got %d", buf.Len())
	}
}
