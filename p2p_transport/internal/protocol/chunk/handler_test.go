package chunk_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func createHandlerTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func TestHandler_InvalidErrorAckPayload(t *testing.T) {
	mocknet := mocknet.New()
	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}

	eng1 := createHandlerTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := []byte("hello world data chunk test")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	tp2 := transport.NewTransport(h2)
	s, err := tp2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer s.Close()

	// Send RequestChunk
	reqMsg := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(s, reqMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	// Read Chunk response
	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected CHUNK message, got %d", resp.Type)
	}

	// Send invalid MsgError ACK (invalid error code 0xFF)
	invalidErrorMsg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    chunk.MsgError,
		Payload: []byte{0xFF, 'b', 'a', 'd'},
	}
	if err := chunk.WriteMessage(s, invalidErrorMsg); err != nil {
		t.Fatalf("WriteMessage for invalid error failed: %v", err)
	}

	// Read next message; expected stream closed by handler
	buf := make([]byte, 100)
	_, err = s.Read(buf)
	if err == nil {
		t.Fatalf("Expected stream to be closed after invalid error payload, but read succeeded")
	}
}

func TestHandler_MultilineLogInjectionAck(t *testing.T) {
	mocknet := mocknet.New()
	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}

	eng1 := createHandlerTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := []byte("testing multiline log injection ack payload")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	tp2 := transport.NewTransport(h2)
	s, err := tp2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer s.Close()

	// Send RequestChunk
	reqMsg := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(s, reqMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	// Read Chunk response
	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected CHUNK message, got %d", resp.Type)
	}

	// Send MsgError ACK with multiline payload
	multilinePayload := append([]byte{byte(chunk.ErrBadRequest)}, []byte("error\n[LOG INJECTION] line 2\r\nline 3")...)
	multilineErrorMsg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    chunk.MsgError,
		Payload: multilinePayload,
	}
	if err := chunk.WriteMessage(s, multilineErrorMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}
	s.CloseWrite()
}

func TestHandler_ValidErrorAck(t *testing.T) {
	mocknet := mocknet.New()
	h1, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}

	eng1 := createHandlerTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := []byte("testing valid error ack payload")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	tp2 := transport.NewTransport(h2)
	s, err := tp2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer s.Close()

	// Send RequestChunk
	reqMsg := chunk.BuildRequestChunk(m.ChunkIDs[0])
	if err := chunk.WriteMessage(s, reqMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	// Read Chunk response
	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected CHUNK message, got %d", resp.Type)
	}

	// Send valid MsgError ACK
	validMsg := chunk.BuildError(chunk.ErrInternal, "unable to write chunk to disk")
	if err := chunk.WriteMessage(s, validMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}
	s.CloseWrite()
}
