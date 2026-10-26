package transfer

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
)

// mockStream wraps net.Conn and embeds network.Stream interface.
type mockStream struct {
	conn net.Conn
	network.Stream
}

func (m *mockStream) Read(p []byte) (int, error) {
	return m.conn.Read(p)
}

func (m *mockStream) Write(p []byte) (int, error) {
	return m.conn.Write(p)
}

func (m *mockStream) Close() error {
	return m.conn.Close()
}

func (m *mockStream) Conn() network.Conn {
	return nil
}

func TestHeaderWriteRead(t *testing.T) {
	origHeader := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "test_file.txt",
		FileSize: 1024,
	}

	buf := new(bytes.Buffer)
	if err := origHeader.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	readHeader := &Header{}
	if err := readHeader.ReadFrom(buf); err != nil {
		t.Fatalf("failed to read header: %v", err)
	}

	if readHeader.Version != origHeader.Version {
		t.Errorf("version mismatch: expected %d, got %d", origHeader.Version, readHeader.Version)
	}
	if readHeader.Type != origHeader.Type {
		t.Errorf("type mismatch: expected %d, got %d", origHeader.Type, readHeader.Type)
	}
	if readHeader.Filename != origHeader.Filename {
		t.Errorf("filename mismatch: expected %s, got %s", origHeader.Filename, readHeader.Filename)
	}
	if readHeader.FileSize != origHeader.FileSize {
		t.Errorf("filesize mismatch: expected %d, got %d", origHeader.FileSize, readHeader.FileSize)
	}
}

func TestSinglePassSendAndReceive(t *testing.T) {
	tempDir := t.TempDir()
	srcPath := filepath.Join(tempDir, "sample.bin")

	// Generate 1MB of random test data
	testData := make([]byte, 1024*1024)
	if _, err := rand.Read(testData); err != nil {
		t.Fatalf("failed to generate random test data: %v", err)
	}
	if err := os.WriteFile(srcPath, testData, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	expectedHash := sha256.Sum256(testData)

	serverConn, clientConn := net.Pipe()
	sStream := &mockStream{conn: serverConn}
	rStream := &mockStream{conn: clientConn}

	defer os.RemoveAll("downloads")

	errCh := make(chan error, 2)

	go func() {
		errCh <- Send(sStream, srcPath)
	}()

	go func() {
		errCh <- Receive(rStream)
	}()

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("transfer failed with error: %v", err)
		}
	}

	receivedPath := filepath.Join("downloads", "sample.bin")
	receivedData, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}

	if !bytes.Equal(testData, receivedData) {
		t.Fatal("received content does not match sent content")
	}

	receivedHash := sha256.Sum256(receivedData)
	if !bytes.Equal(expectedHash[:], receivedHash[:]) {
		t.Fatal("received hash does not match expected hash")
	}
}

func TestReceiveTrailingChecksumMismatch(t *testing.T) {
	tempDir := t.TempDir()
	srcPath := filepath.Join(tempDir, "corrupt.txt")
	testData := []byte("Hello, Cipher P2P Transfer!")
	if err := os.WriteFile(srcPath, testData, 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	serverConn, clientConn := net.Pipe()
	sStream := &mockStream{conn: serverConn}
	rStream := &mockStream{conn: clientConn}

	defer os.RemoveAll("downloads")

	// Send header, data, but bad trailing checksum
	go func() {
		defer sStream.Close()

		header := &Header{
			Version:  ProtocolVersion1,
			Type:     MsgTypeFileTransfer,
			Filename: "corrupt.txt",
			FileSize: uint64(len(testData)),
		}
		_ = header.WriteTo(sStream)
		_, _ = sStream.Write(testData)

		// Send invalid 32-byte trailing checksum
		badChecksum := make([]byte, 32)
		_, _ = sStream.Write(badChecksum)
	}()

	err := Receive(rStream)
	if err == nil {
		t.Fatal("expected checksum mismatch error, but got nil")
	}
}
