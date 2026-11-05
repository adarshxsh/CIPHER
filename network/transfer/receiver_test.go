package transfer

import (
	"bytes"
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type mockConn struct {
	network.Conn
}

func (m *mockConn) RemotePeer() peer.ID {
	return peer.ID("mock-peer-id")
}

func (m *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	ma, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return ma
}

type mockStream struct {
	network.Stream
	reader    io.Reader
	wasReset  bool
	wasClosed bool
	conn      *mockConn
}

func (m *mockStream) Read(p []byte) (n int, err error) {
	if m.reader == nil {
		return 0, io.EOF
	}
	return m.reader.Read(p)
}

func (m *mockStream) Close() error {
	m.wasClosed = true
	return nil
}

func (m *mockStream) Reset() error {
	m.wasReset = true
	return nil
}

func (m *mockStream) Conn() network.Conn {
	if m.conn == nil {
		m.conn = &mockConn{}
	}
	return m.conn
}

func TestReceive_Success(t *testing.T) {
	filename := "test_success.txt"
	content := []byte("hello world successful transfer")
	outPath := filepath.Join("downloads", filename)
	defer os.Remove(outPath)

	hasher := sha256.New()
	hasher.Write(content)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(content)),
		Checksum: checksum,
	}

	buf := new(bytes.Buffer)
	if err := hdr.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(content)

	ms := &mockStream{reader: buf}
	err := Receive(ms)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if !ms.wasClosed {
		t.Errorf("expected stream to be closed on success")
	}
	if ms.wasReset {
		t.Errorf("expected stream not to be reset on success")
	}

	gotData, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("failed to read output file %s: %v", outPath, err)
	}
	if !bytes.Equal(gotData, content) {
		t.Errorf("content mismatch: expected %q, got %q", string(content), string(gotData))
	}
}

func TestReceive_HeaderReadError(t *testing.T) {
	ms := &mockStream{reader: bytes.NewReader([]byte{1, 2})} // incomplete header
	err := Receive(ms)
	if err == nil {
		t.Fatalf("expected error on header read, got nil")
	}
	if !ms.wasReset {
		t.Errorf("expected stream to be reset on header error")
	}
}

func TestReceive_InvalidProtocolVersion(t *testing.T) {
	hdr := &Header{
		Version:  99,
		Type:     MsgTypeFileTransfer,
		Filename: "test_bad_version.txt",
		FileSize: 10,
	}
	buf := new(bytes.Buffer)
	if err := hdr.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}

	ms := &mockStream{reader: buf}
	err := Receive(ms)
	if err == nil {
		t.Fatalf("expected error on invalid version, got nil")
	}
	if !ms.wasReset {
		t.Errorf("expected stream to be reset on invalid version")
	}
}

func TestReceive_MidStreamTruncatedData(t *testing.T) {
	filename := "test_truncated.txt"
	outPath := filepath.Join("downloads", filename)
	defer os.Remove(outPath)

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: 100, // Claim 100 bytes
	}

	buf := new(bytes.Buffer)
	if err := hdr.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write([]byte("short data")) // Only 10 bytes provided

	ms := &mockStream{reader: buf}
	err := Receive(ms)
	if err == nil {
		t.Fatalf("expected error on truncated data, got nil")
	}
	if !ms.wasReset {
		t.Errorf("expected stream to be reset on truncated data")
	}

	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("expected partial file %s to be removed, but it still exists", outPath)
	}
}

func TestReceive_ChecksumMismatch(t *testing.T) {
	filename := "test_checksum_mismatch.txt"
	outPath := filepath.Join("downloads", filename)
	defer os.Remove(outPath)

	content := []byte("corrupted data content")
	var wrongChecksum [32]byte
	wrongChecksum[0] = 0xff

	hdr := &Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(content)),
		Checksum: wrongChecksum,
	}

	buf := new(bytes.Buffer)
	if err := hdr.WriteTo(buf); err != nil {
		t.Fatalf("failed to write header: %v", err)
	}
	buf.Write(content)

	ms := &mockStream{reader: buf}
	err := Receive(ms)
	if err == nil {
		t.Fatalf("expected error on checksum mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("expected checksum mismatch error message, got: %v", err)
	}
	if !ms.wasReset {
		t.Errorf("expected stream to be reset on checksum mismatch")
	}

	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Errorf("expected corrupted file %s to be removed, but it still exists", outPath)
	}
}
