package transfer

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type mockConn struct {
	network.Conn
}

func (m *mockConn) RemotePeer() peer.ID {
	return peer.ID("test-peer-id")
}

func (m *mockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	ma, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/1234")
	return ma
}

type mockStream struct {
	network.Stream
	reader      io.Reader
	resetCalled bool
	closeCalled bool
}

func (m *mockStream) Read(p []byte) (int, error) {
	if m.reader == nil {
		return 0, io.EOF
	}
	return m.reader.Read(p)
}

func (m *mockStream) Reset() error {
	m.resetCalled = true
	return nil
}

func (m *mockStream) Close() error {
	m.closeCalled = true
	return nil
}

func (m *mockStream) Conn() network.Conn {
	return &mockConn{}
}

func buildStreamBuffer(filename string, payload []byte, corruptChecksum bool) *bytes.Buffer {
	hasher := sha256.New()
	hasher.Write(payload)
	var checksum [32]byte
	copy(checksum[:], hasher.Sum(nil))

	if corruptChecksum {
		checksum[0] ^= 0xFF
	}

	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: uint64(len(payload)),
		Checksum: checksum,
	}

	buf := new(bytes.Buffer)
	_ = header.WriteTo(buf)
	buf.Write(payload)
	return buf
}

func TestReceiveSuccess(t *testing.T) {
	filename := "test_success.txt"
	payload := []byte("Hello, CIPHER P2P Network!")
	buf := buildStreamBuffer(filename, payload, false)

	stream := &mockStream{reader: buf}
	downloadsDir := "downloads"
	outPath := filepath.Join(downloadsDir, filename)
	defer os.Remove(outPath)

	err := Receive(stream)
	if err != nil {
		t.Fatalf("expected Receive to succeed, got error: %v", err)
	}

	if !stream.closeCalled {
		t.Errorf("expected Close to be called on success")
	}
	if stream.resetCalled {
		t.Errorf("expected Reset NOT to be called on success")
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("expected output file to exist, got: %v", err)
	}
	if !bytes.Equal(data, payload) {
		t.Errorf("expected file content %q, got %q", payload, data)
	}
}

func TestReceiveHeaderNonEOFError(t *testing.T) {
	customErr := errors.New("network connection broken during header")
	stream := &mockStream{reader: errorReader{err: customErr}}

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on broken header read")
	}

	if !stream.resetCalled {
		t.Errorf("expected Reset to be called on non-EOF header error")
	}
}

func TestReceiveHeaderEOFError(t *testing.T) {
	stream := &mockStream{reader: errorReader{err: io.EOF}}

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on EOF header read")
	}

	if stream.resetCalled {
		t.Errorf("expected Reset NOT to be called on EOF header error")
	}
	if !stream.closeCalled {
		t.Errorf("expected Close to be called on EOF header error")
	}
}

func TestReceiveDataCopyError(t *testing.T) {
	filename := "test_copy_err.txt"
	payload := []byte("Some partial content")
	buf := buildStreamBuffer(filename, payload, false)

	// Combine header bytes with error reader
	headerLen := buf.Len() - len(payload)
	headerBytes := buf.Bytes()[:headerLen]

	r := io.MultiReader(bytes.NewReader(headerBytes), errorReader{err: errors.New("connection reset during data")})
	stream := &mockStream{reader: r}

	outPath := filepath.Join("downloads", filename)
	_ = os.Remove(outPath)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error during data copy")
	}

	if !stream.resetCalled {
		t.Errorf("expected Reset to be called on data copy error")
	}

	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("expected partial file %s to be unlinked, but it exists", outPath)
	}
}

func TestReceiveSizeMismatch(t *testing.T) {
	filename := "test_size_mismatch.txt"
	payload := []byte("Short")
	// Header says size is 100, but payload is 5 bytes
	header := Header{
		Version:  ProtocolVersion1,
		Type:     MsgTypeFileTransfer,
		Filename: filename,
		FileSize: 100,
		Checksum: [32]byte{},
	}
	buf := new(bytes.Buffer)
	_ = header.WriteTo(buf)
	buf.Write(payload)

	stream := &mockStream{reader: buf}
	outPath := filepath.Join("downloads", filename)
	_ = os.Remove(outPath)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on size mismatch")
	}

	if !stream.resetCalled {
		t.Errorf("expected Reset to be called on size mismatch")
	}

	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("expected output file %s to be unlinked, but it exists", outPath)
	}
}

func TestReceiveChecksumMismatch(t *testing.T) {
	filename := "test_checksum_mismatch.txt"
	payload := []byte("Data payload")
	buf := buildStreamBuffer(filename, payload, true) // corrupt checksum

	stream := &mockStream{reader: buf}
	outPath := filepath.Join("downloads", filename)
	_ = os.Remove(outPath)

	err := Receive(stream)
	if err == nil {
		t.Fatalf("expected error on checksum mismatch")
	}

	if !stream.resetCalled {
		t.Errorf("expected Reset to be called on checksum mismatch")
	}

	if _, statErr := os.Stat(outPath); !os.IsNotExist(statErr) {
		t.Errorf("expected corrupt file %s to be unlinked, but it exists", outPath)
	}
}

type errorReader struct {
	err error
}

func (e errorReader) Read(p []byte) (int, error) {
	return 0, e.err
}
