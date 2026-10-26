package transfer

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

type mockStream struct {
	c net.Conn
}

func (m *mockStream) Read(p []byte) (n int, err error)                        { return m.c.Read(p) }
func (m *mockStream) Write(p []byte) (n int, err error)                       { return m.c.Write(p) }
func (m *mockStream) Close() error                                           { return m.c.Close() }
func (m *mockStream) CloseWrite() error                                      { return nil }
func (m *mockStream) CloseRead() error                                       { return nil }
func (m *mockStream) Reset() error                                           { return m.Close() }
func (m *mockStream) ResetWithError(errCode network.StreamErrorCode) error   { return m.Close() }
func (m *mockStream) SetDeadline(t time.Time) error                          { return m.c.SetDeadline(t) }
func (m *mockStream) SetReadDeadline(t time.Time) error                      { return m.c.SetReadDeadline(t) }
func (m *mockStream) SetWriteDeadline(t time.Time) error                     { return m.c.SetWriteDeadline(t) }
func (m *mockStream) ID() string                                             { return "mock-stream" }
func (m *mockStream) Protocol() protocol.ID                                  { return "/cipher/filetransfer/1.0.0" }
func (m *mockStream) SetProtocol(p protocol.ID) error                        { return nil }
func (m *mockStream) Stat() network.Stats                                    { return network.Stats{} }
func (m *mockStream) Conn() network.Conn                                      { return nil }
func (m *mockStream) Scope() network.StreamScope                             { return nil }

func TestHeader_ReadWrite(t *testing.T) {
	origHeader := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: "testfile.txt",
		FileSize: 1024,
	}

	buf := new(bytes.Buffer)
	if err := origHeader.WriteTo(buf); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	readHeader := &Header{}
	if err := readHeader.ReadFrom(buf); err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if readHeader.Version != origHeader.Version ||
		readHeader.Type != origHeader.Type ||
		readHeader.Filename != origHeader.Filename ||
		readHeader.FileSize != origHeader.FileSize {
		t.Fatalf("Header mismatch: got %+v, want %+v", readHeader, origHeader)
	}
}

func TestSendAndReceive_Success(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "source.bin")
	content := []byte("Hello, CIPHER single-pass transfer streaming!")
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	sConn, rConn := net.Pipe()
	sStream := &mockStream{c: sConn}
	rStream := &mockStream{c: rConn}

	done := make(chan error, 2)

	go func() {
		done <- Receive(rStream)
	}()

	go func() {
		done <- Send(sStream, srcFile)
	}()

	err1 := <-done
	err2 := <-done

	if err1 != nil {
		t.Errorf("Error in transfer pair: %v", err1)
	}
	if err2 != nil {
		t.Errorf("Error in transfer pair: %v", err2)
	}

	receivedPath := filepath.Join("downloads", "source.bin")
	defer os.Remove(receivedPath)

	recvContent, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}

	if !bytes.Equal(recvContent, content) {
		t.Fatalf("received content mismatch: got %q, want %q", recvContent, content)
	}
}

func TestSendAndReceive_CorruptedPayload(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "corrupt_payload.bin")
	content := bytes.Repeat([]byte("A"), 1024)
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	sConn, rConn := net.Pipe()
	sStream := &mockStream{c: sConn}

	corruptedRConn := &corruptingConn{
		Conn:          rConn,
		bytesRead:     0,
		corruptOffset: 50,
	}
	rStream := &mockStream{c: corruptedRConn}

	done := make(chan error, 2)

	go func() {
		done <- Receive(rStream)
	}()

	go func() {
		done <- Send(sStream, srcFile)
	}()

	err1 := <-done
	err2 := <-done

	os.Remove(filepath.Join("downloads", "corrupt_payload.bin"))

	var recvErr error
	if err1 != nil {
		recvErr = err1
	} else if err2 != nil {
		recvErr = err2
	}

	if recvErr == nil {
		t.Fatalf("expected Receive to fail due to corrupted payload, but it succeeded")
	} else {
		t.Logf("Receive correctly failed on corrupted payload: %v", recvErr)
	}
}

func TestSendAndReceive_CorruptedTrailer(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "corrupt_trailer.bin")
	content := []byte("Small payload for trailer corruption test")
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	sConn, rConn := net.Pipe()
	sStream := &mockStream{c: sConn}

	headerLen := 1 + 1 + 2 + len("corrupt_trailer.bin") + 8
	payloadLen := len(content)
	trailerCorruptOffset := headerLen + payloadLen + 5

	corruptedRConn := &corruptingConn{
		Conn:          rConn,
		bytesRead:     0,
		corruptOffset: trailerCorruptOffset,
	}
	rStream := &mockStream{c: corruptedRConn}

	done := make(chan error, 2)

	go func() {
		done <- Receive(rStream)
	}()

	go func() {
		done <- Send(sStream, srcFile)
	}()

	err1 := <-done
	err2 := <-done

	os.Remove(filepath.Join("downloads", "corrupt_trailer.bin"))

	var recvErr error
	if err1 != nil {
		recvErr = err1
	} else if err2 != nil {
		recvErr = err2
	}

	if recvErr == nil {
		t.Fatalf("expected Receive to fail due to corrupted trailing checksum, but it succeeded")
	} else {
		t.Logf("Receive correctly failed on corrupted trailing checksum: %v", recvErr)
	}
}

type corruptingConn struct {
	net.Conn
	bytesRead     int
	corruptOffset int
}

func (c *corruptingConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	for i := 0; i < n; i++ {
		if c.bytesRead+i == c.corruptOffset {
			b[i] ^= 0xFF
		}
	}
	c.bytesRead += n
	return n, err
}

func TestSend_SinglePass(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "single_pass.bin")
	content := bytes.Repeat([]byte("SinglePassStreamingData1234567890"), 30000) // ~1MB
	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	sConn, rConn := net.Pipe()
	sStream := &mockStream{c: sConn}
	rStream := &mockStream{c: rConn}

	done := make(chan error, 2)

	go func() {
		done <- Receive(rStream)
	}()

	go func() {
		done <- Send(sStream, srcFile)
	}()

	err1 := <-done
	err2 := <-done

	receivedPath := filepath.Join("downloads", "single_pass.bin")
	defer os.Remove(receivedPath)

	if err1 != nil || err2 != nil {
		t.Fatalf("Single pass send failed: receiver err=%v, sender err=%v", err1, err2)
	}

	recvContent, err := os.ReadFile(receivedPath)
	if err != nil {
		t.Fatalf("failed to read received file: %v", err)
	}

	if !bytes.Equal(recvContent, content) {
		t.Fatalf("received content mismatch")
	}
}
