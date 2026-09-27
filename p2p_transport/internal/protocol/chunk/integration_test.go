package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

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
	"cipher/internal/transport"
)

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir()) // isolated per engine
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockNetwork(t testing.TB) (host.Host, host.Host) {
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

func TestChunkProtocol_Integration(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	// Setup Handler on Peer 1 (Server)
	chunk.NewStreamHandler(h1, eng1)
	
	// Ensure Peer 2 has the handler too (symmetric protocol requirement)
	chunk.NewStreamHandler(h2, eng2)

	// Peer 1 ingests 1MB file
	ctx := context.Background()
	dataSize := 1024 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)
	
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Peer 2 wants to fetch it
	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// 1. Resolve Manifest
	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Failed to resolve manifest: %v", err)
	}

	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Failed to deserialize manifest: %v", err)
	}

	// 2. Download
	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	// NOTE: We must give eng2 the decryption key to reassemble locally, as key transfer is out of scope.
	// Since keys aren't exposed, let's just verify download succeeds!
	
	if len(m2.ChunkIDs) != 4 { // 1MB / 256KB = 4 chunks
		t.Errorf("Expected 4 chunks, got %d", len(m2.ChunkIDs))
	}

	for _, chunkID := range m2.ChunkIDs {
		// verify eng2 has it
		if _, err := eng2.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("eng2 missing chunk %x", chunkID)
		}
	}
}

func TestChunkProtocol_InvalidPeer(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	client, err := chunk.NewClient(context.Background(), transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	var badID core.ContentID
	_, err = client.Resolve(context.Background(), badID)
	if err == nil {
		t.Fatalf("Expected error for invalid ContentID")
	}
	if err.Error() != "remote error (code 1): manifest not found" {
		t.Errorf("Unexpected error msg: %v", err)
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func TestStreamHandler_SanitizesIncomingErrorLogs(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	var logBuf safeBuffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	ctx := context.Background()
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("NewStream failed: %v", err)
	}

	errMsg := chunk.BuildError(chunk.ErrBadRequest, "Bad Request\nLine 2\r\nInject Fake Log Line\x00Control")
	if err := chunk.WriteMessage(s, errMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	s.Close()

	output := logBuf.String()
	if !strings.Contains(output, `Bad Request\\nLine 2\\r\\nInject Fake Log Line\\x00Control`) {
		t.Errorf("Expected sanitized error message in log output, got: %q", output)
	}
}

func TestStreamHandler_SanitizesChunkAckErrorLog(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	var logBuf safeBuffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	ctx := context.Background()
	var chunkID core.ChunkID
	chunkID[0] = 0xAA
	eng1.PutChunk(ctx, &core.Chunk{
		Header: core.ChunkHeader{
			Version:    chunk.CurrentMessageVersion,
			ID:         chunkID,
			PlainSize:  15,
			CipherSize: 15,
		},
		Data: []byte("test_chunk_data"),
	})

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("NewStream failed: %v", err)
	}
	defer s.Close()

	req := chunk.BuildRequestChunk(chunkID)
	if err := chunk.WriteMessage(s, req); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	resp, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if resp.Type != chunk.MsgChunk {
		t.Fatalf("Expected CHUNK, got %v", resp.Type)
	}

	errMsg := chunk.BuildError(chunk.ErrIntegrityMismatch, "Corrupted!\nMultiline\r\nInject\x00")
	if err := chunk.WriteMessage(s, errMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	output := logBuf.String()
	if !strings.Contains(output, `Corrupted!\\nMultiline\\r\\nInject\\x00`) {
		t.Errorf("Expected sanitized error in ACK log output, got: %q", output)
	}
}
