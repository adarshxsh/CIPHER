package chunk_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
)

func setupTestNodes(t testing.TB) (host.Host, host.Host, *engine.ContentEngine, *engine.ContentEngine) {
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

	createEng := func() *engine.ContentEngine {
		config := core.EngineConfig{ChunkSize: 256 * 1024}
		enc := crypto.NewChaCha20Encryptor()
		dig := verifier.NewSHA256Digest()
		keys := engine.NewLocalKeyProvider()
		store := storage.NewFSStore(t.TempDir())
		return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
	}

	eng1 := createEng()
	eng2 := createEng()

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	return h1, h2, eng1, eng2
}

func TestStreamHandler_EnforcesValidateMessage(t *testing.T) {
	h1, h2, _, _ := setupTestNodes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// 1. Send message with unsupported protocol version (e.g. version 2)
	invalidVersionMsg := &chunk.Message{
		Version: chunk.CurrentMessageVersion + 1,
		Type:    chunk.MsgRequestManifest,
		Payload: make([]byte, chunk.ContentIDSize),
	}

	if err := chunk.WriteMessage(stream, invalidVersionMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	resp, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("Failed to read error response: %v", err)
	}

	if resp.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError response for invalid version, got type %d", resp.Type)
	}

	code, msgStr, err := chunk.ParseError(resp.Payload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}

	if code != chunk.ErrUnsupportedMessage {
		t.Errorf("Expected code ErrUnsupportedMessage (%d), got %d", chunk.ErrUnsupportedMessage, code)
	}
	if !strings.Contains(msgStr, "unsupported") {
		t.Errorf("Expected error message to mention unsupported, got %q", msgStr)
	}
}

func TestStreamHandler_CapsPerStreamMessageCount(t *testing.T) {
	h1, h2, eng1, _ := setupTestNodes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Ingest a dummy manifest in eng1 so valid requests succeed
	manifestData := []byte("manifest content")
	var contentID core.ContentID
	contentID[0] = 0x01
	eng1.PutManifestBytes(ctx, contentID, manifestData)

	stream, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	reqMsg := chunk.BuildRequestManifest(contentID)

	// Send 4 valid messages (MaxMessagesPerStream = 4)
	for i := 1; i <= chunk.MaxMessagesPerStream; i++ {
		if err := chunk.WriteMessage(stream, reqMsg); err != nil {
			t.Fatalf("Message %d WriteMessage failed: %v", i, err)
		}
		resp, err := chunk.ReadMessage(stream)
		if err != nil {
			t.Fatalf("Message %d ReadMessage response failed: %v", i, err)
		}
		if resp.Type != chunk.MsgManifest {
			t.Fatalf("Message %d expected MsgManifest, got type %d", i, resp.Type)
		}
	}

	// Message 5: exceeds MaxMessagesPerStream (4)
	if err := chunk.WriteMessage(stream, reqMsg); err != nil {
		t.Fatalf("Message 5 WriteMessage failed: %v", err)
	}

	resp, err := chunk.ReadMessage(stream)
	if err != nil {
		// Handler may close stream or send error
		return
	}

	if resp.Type != chunk.MsgError {
		t.Fatalf("Expected MsgError for exceeding rate limit, got type %d", resp.Type)
	}

	code, msgStr, _ := chunk.ParseError(resp.Payload)
	if code != chunk.ErrBadRequest {
		t.Errorf("Expected ErrBadRequest (%d), got %d", chunk.ErrBadRequest, code)
	}
	if !strings.Contains(msgStr, "rate limit") {
		t.Errorf("Expected error message to mention rate limit, got %q", msgStr)
	}

	// Verify subsequent read on stream fails (stream closed by handler)
	buf := make([]byte, 10)
	_, err = stream.Read(buf)
	if err == nil {
		t.Errorf("Expected stream read to fail after rate limit exceeded")
	}
}

func TestStreamHandler_LogInjectionAndAdversarialInputs(t *testing.T) {
	h1, h2, _, _ := setupTestNodes(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer stream.Close()

	// Send an error message with embedded newlines and ANSI control codes
	adversarialMsg := chunk.BuildError(chunk.ErrBadRequest, "Error line 1\n[FAKE LOG HEADER] Line 2\r\x1b[31mRED TEXT\x00")

	if err := chunk.WriteMessage(stream, adversarialMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	// Give handler a moment to process and log
	time.Sleep(50 * time.Millisecond)

	// Verify ParseError produces a clean, single-line sanitized string
	_, sanitizedMsg, err := chunk.ParseError(adversarialMsg.Payload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}

	if strings.Contains(sanitizedMsg, "\n") || strings.Contains(sanitizedMsg, "\r") {
		t.Errorf("Sanitized message contains raw line breaks: %q", sanitizedMsg)
	}
}
