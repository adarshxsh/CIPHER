package transfer

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	libp2p_protocol "github.com/libp2p/go-libp2p/core/protocol"
)

type pipeStream struct {
	c net.Conn
}

func (p *pipeStream) Read(b []byte) (int, error)                       { return p.c.Read(b) }
func (p *pipeStream) Write(b []byte) (int, error)                      { return p.c.Write(b) }
func (p *pipeStream) Close() error                                     { return p.c.Close() }
func (p *pipeStream) CloseWrite() error                                { return p.c.Close() }
func (p *pipeStream) CloseRead() error                                 { return p.c.Close() }
func (p *pipeStream) Reset() error                                     { return p.c.Close() }
func (p *pipeStream) ResetWithError(network.StreamErrorCode) error    { return p.c.Close() }
func (p *pipeStream) SetDeadline(t time.Time) error                    { return p.c.SetDeadline(t) }
func (p *pipeStream) SetReadDeadline(t time.Time) error               { return p.c.SetReadDeadline(t) }
func (p *pipeStream) SetWriteDeadline(t time.Time) error              { return p.c.SetWriteDeadline(t) }
func (p *pipeStream) ID() string                                       { return "1" }
func (p *pipeStream) Conn() network.Conn                               { return nil }
func (p *pipeStream) Protocol() libp2p_protocol.ID                     { return "file-transfer/1.0" }
func (p *pipeStream) SetProtocol(libp2p_protocol.ID) error             { return nil }
func (p *pipeStream) Stat() network.Stats                              { return network.Stats{} }
func (p *pipeStream) Scope() network.StreamScope                       { return nil }

func TestHeaderReadWrite(t *testing.T) {
	header := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "testfile.txt",
		FileSize: 1024,
	}

	buf := new(bytes.Buffer)
	if err := header.WriteTo(buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	var readHeader Header
	if err := readHeader.ReadFrom(buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if readHeader.Version != header.Version ||
		readHeader.Type != header.Type ||
		readHeader.Filename != header.Filename ||
		readHeader.FileSize != header.FileSize {
		t.Fatalf("Header mismatch: expected %+v, got %+v", header, readHeader)
	}
}

func TestInStreamTransfer(t *testing.T) {
	// Create a temporary directory for test file
	tmpDir, err := os.MkdirTemp("", "transfer_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	srcPath := filepath.Join(tmpDir, "sample.dat")
	dataSize := 256 * 1024 // 256 KB
	data := make([]byte, dataSize)
	if _, err := rand.Read(data); err != nil {
		t.Fatalf("failed to generate random data: %v", err)
	}
	if err := os.WriteFile(srcPath, data, 0644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	expectedHasher := sha256.New()
	expectedHasher.Write(data)
	expectedChecksum := expectedHasher.Sum(nil)

	clientConn, serverConn := net.Pipe()
	senderStream := &pipeStream{c: clientConn}
	receiverStream := &pipeStream{c: serverConn}

	// Ensure downloads directory is cleaned up after test
	defer os.RemoveAll("downloads")

	errChan := make(chan error, 2)

	go func() {
		errChan <- Send(senderStream, srcPath)
	}()

	go func() {
		errChan <- Receive(receiverStream)
	}()

	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			t.Fatalf("Transfer failed with error: %v", err)
		}
	}

	dstPath := filepath.Join("downloads", "sample.dat")
	receivedData, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}

	if !bytes.Equal(data, receivedData) {
		t.Fatalf("received content does not match transmitted content")
	}

	receivedHasher := sha256.New()
	receivedHasher.Write(receivedData)
	receivedChecksum := receivedHasher.Sum(nil)

	if !bytes.Equal(expectedChecksum, receivedChecksum) {
		t.Fatalf("checksum mismatch on received file")
	}
}

func TestChecksumMismatch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "transfer_corrupt_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	srcPath := filepath.Join(tmpDir, "corrupt.dat")
	data := []byte("Hello, world! Data integrity test.")
	if err := os.WriteFile(srcPath, data, 0644); err != nil {
		t.Fatalf("failed to write source file: %v", err)
	}

	clientConn, serverConn := net.Pipe()

	corruptingConn := &corruptingWriterConn{Conn: clientConn}

	senderStream := &pipeStream{c: corruptingConn}
	receiverStream := &pipeStream{c: serverConn}

	defer os.RemoveAll("downloads")

	errChan := make(chan error, 2)

	go func() {
		errChan <- Send(senderStream, srcPath)
	}()

	go func() {
		errChan <- Receive(receiverStream)
	}()

	sendErr := <-errChan
	recvErr := <-errChan

	if sendErr != nil {
		t.Fatalf("Sender failed unexpectedly: %v", sendErr)
	}

	if recvErr == nil {
		t.Fatalf("Expected Receiver to fail with checksum mismatch, but got nil")
	}
}

type corruptingWriterConn struct {
	net.Conn
}

func (c *corruptingWriterConn) Write(b []byte) (n int, err error) {
	// Corrupt trailing checksum byte if writing 32-byte footer
	if len(b) == 32 {
		corrupted := make([]byte, len(b))
		copy(corrupted, b)
		corrupted[len(corrupted)-1] ^= 0xFF
		return c.Conn.Write(corrupted)
	}
	return c.Conn.Write(b)
}
