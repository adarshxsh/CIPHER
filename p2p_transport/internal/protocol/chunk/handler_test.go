package chunk_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"

	"cipher/internal/protocol/chunk"
)

type mockChunkConn struct {
	network.Conn
}

func (m *mockChunkConn) RemotePeer() peer.ID {
	return peer.ID("mockPeer")
}

type mockChunkStream struct {
	network.Stream
	readBuf *bytes.Buffer
	readErr error
	closed  bool
	reset   bool
}

func newMockChunkStream(data []byte, readErr error) *mockChunkStream {
	return &mockChunkStream{
		readBuf: bytes.NewBuffer(data),
		readErr: readErr,
	}
}

func (m *mockChunkStream) Read(p []byte) (n int, err error) {
	if m.readBuf.Len() > 0 {
		return m.readBuf.Read(p)
	}
	if m.readErr != nil {
		return 0, m.readErr
	}
	return 0, io.EOF
}

func (m *mockChunkStream) Write(p []byte) (n int, err error) {
	return len(p), nil
}

func (m *mockChunkStream) Close() error {
	m.closed = true
	return nil
}

func (m *mockChunkStream) Reset() error {
	m.reset = true
	return nil
}

func (m *mockChunkStream) Conn() network.Conn {
	return &mockChunkConn{}
}

func TestHandler_UnhandledReadErrorStreamReset(t *testing.T) {
	eng := createTestEngine(t)
	// Create mock host
	h1, _ := setupMockNetwork(t)
	handler := chunk.NewStreamHandler(h1, eng)

	// Stream with corrupted data that causes unhandled read error
	s := newMockChunkStream([]byte{0xFF, 0xFF, 0xFF}, errors.New("read error: unexpected connection failure"))

	// Call stream handler directly
	handler.HandleStream(s)

	if !s.reset {
		t.Errorf("Expected stream to be Reset() on unhandled read error, but it was not reset")
	}
	if s.closed {
		t.Errorf("Expected stream NOT to be Close()'d on unhandled read error")
	}
}

func TestHandler_CleanEOFClose(t *testing.T) {
	eng := createTestEngine(t)
	h1, _ := setupMockNetwork(t)
	handler := chunk.NewStreamHandler(h1, eng)

	s := newMockChunkStream([]byte{}, io.EOF)

	handler.HandleStream(s)

	if !s.closed {
		t.Errorf("Expected stream to be Close()'d on clean EOF")
	}
	if s.reset {
		t.Errorf("Expected stream NOT to be Reset() on clean EOF")
	}
}
