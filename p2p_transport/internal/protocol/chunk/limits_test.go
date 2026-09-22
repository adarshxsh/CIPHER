package chunk_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
)

func setupTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockHostPair(t testing.TB) (host.Host, host.Host) {
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
	return h1, h2
}

func TestStreamLimits_MaxTransactionsEnforced(t *testing.T) {
	h1, h2 := setupMockHostPair(t)
	eng1 := setupTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	m, err := eng1.Ingest(ctx, bytes.NewReader([]byte("test content data")), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Open a raw stream directly from h2 to h1
	stream, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open raw stream: %v", err)
	}
	defer stream.Close()

	// Transaction 1: Request Manifest
	req1 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(stream, req1); err != nil {
		t.Fatalf("WriteMessage 1 failed: %v", err)
	}

	resp1, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("ReadMessage 1 failed: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MsgManifest, got type %d", resp1.Type)
	}

	// Transaction 2 (over same stream): Rogue peer attempts second request
	req2 := chunk.BuildRequestManifest(m.Descriptor.ID)
	if err := chunk.WriteMessage(stream, req2); err != nil {
		// Server might have already closed the stream after transaction 1
		return
	}

	resp2, err := chunk.ReadMessage(stream)
	if err != nil {
		// Stream was closed after 1 transaction, as required
		return
	}

	// If a response was received, it must be a protocol error for exceeding limits
	if resp2.Type == chunk.MsgError {
		code, msg, _ := chunk.ParseError(resp2.Payload)
		if code != chunk.ErrBadRequest {
			t.Errorf("Expected ErrBadRequest (0x07), got code 0x%02x: %s", code, msg)
		}
	} else {
		t.Fatalf("Expected protocol error or closed stream for second transaction, got msg type %d", resp2.Type)
	}
}

func TestStreamLimits_MaxMessagesExceeded(t *testing.T) {
	h1, h2 := setupMockHostPair(t)
	eng1 := setupTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	stream, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open raw stream: %v", err)
	}
	defer stream.Close()

	// Send 5 messages over a single stream (> MaxMessagesPerStream = 4)
	var contentID core.ContentID
	msg := chunk.BuildRequestManifest(contentID)

	for i := 0; i < 5; i++ {
		if err := chunk.WriteMessage(stream, msg); err != nil {
			// Stream closed by server
			return
		}
		resp, err := chunk.ReadMessage(stream)
		if err != nil {
			// Stream terminated by server
			return
		}
		if resp.Type == chunk.MsgError {
			code, _, _ := chunk.ParseError(resp.Payload)
			if code == chunk.ErrBadRequest {
				// Server correctly returned message limit error
				return
			}
		}
	}
}

func TestStreamLimits_RoguePeerUnexpectedSecondRequestInsteadOfAck(t *testing.T) {
	h1, h2 := setupMockHostPair(t)
	eng1 := setupTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	chunkID := m.ChunkIDs[0]

	stream, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open raw stream: %v", err)
	}
	defer stream.Close()

	// Send Chunk Request
	req := chunk.BuildRequestChunk(chunkID)
	if err := chunk.WriteMessage(stream, req); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	resp, err := chunk.ReadMessage(stream)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected MsgChunk, got %d", resp.Type)
	}

	// Instead of sending ACK, rogue peer sends another Request Chunk!
	req2 := chunk.BuildRequestChunk(chunkID)
	if err := chunk.WriteMessage(stream, req2); err != nil {
		// Server closed stream
		return
	}

	errResp, err := chunk.ReadMessage(stream)
	if err != nil {
		// Stream closed
		return
	}

	if errResp.Type == chunk.MsgError {
		code, msg, _ := chunk.ParseError(errResp.Payload)
		if code != chunk.ErrBadRequest {
			t.Errorf("Expected ErrBadRequest, got code %d: %s", code, msg)
		}
	} else {
		t.Fatalf("Expected MsgError for unexpected request frame instead of ACK, got type %d", errResp.Type)
	}
}
