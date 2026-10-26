package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/net/mock"
	"golang.org/x/time/rate"

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

func TestHandler_ErrorLogRateLimiting(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	// Create rate limiter with burst=3, rate=1 per minute
	limiter := chunk.NewPeerRateLimiter(rate.Limit(1.0/60.0), 3, 100, 10*time.Minute)
	chunk.NewStreamHandlerWithLimiter(h1, eng1, limiter)

	// Capture log output
	origWriter := log.Writer()
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(origWriter)

	ctx := context.Background()
	tr2 := transport.NewTransport(h2)
	stream, err := tr2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer stream.Close()

	// Send 8 error messages rapidly
	for i := 0; i < 8; i++ {
		errMsg := chunk.BuildError(chunk.ErrBadRequest, fmt.Sprintf("burst error %d", i))
		if err := chunk.WriteMessage(stream, errMsg); err != nil {
			t.Fatalf("WriteMessage failed at %d: %v", i, err)
		}
	}

	// Give a short moment for stream handler goroutine to process
	time.Sleep(50 * time.Millisecond)

	logOutput := logBuf.String()
	// Count occurrences of "burst error" in log output
	count := strings.Count(logOutput, "burst error")
	if count != 3 {
		t.Errorf("expected exactly 3 logged error messages due to rate limit, got %d. Log output:\n%s", count, logOutput)
	}
}

func TestHandler_LogSanitization(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)

	origWriter := log.Writer()
	var logBuf bytes.Buffer
	log.SetOutput(&logBuf)
	defer log.SetOutput(origWriter)

	ctx := context.Background()
	tr2 := transport.NewTransport(h2)
	stream, err := tr2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	defer stream.Close()

	// Malicious error payload with newlines and ANSI escape codes
	maliciousText := "Error!\n2026-09-16 [FORGED] Fake log line\r\n\x1b[31mRED ALERT\x1b[0m"
	errMsg := chunk.BuildError(chunk.ErrBadRequest, maliciousText)
	if err := chunk.WriteMessage(stream, errMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	logOutput := logBuf.String()
	// The logged message should be a single line containing sanitized text without control chars or ANSI codes
	if strings.Contains(logOutput, "\x1b[31m") {
		t.Errorf("log output contains ANSI escape sequence: %q", logOutput)
	}
	// Verify that forged log line is sanitized into a single line
	expectedSubstring := "Error!2026-09-16 [FORGED] Fake log lineRED ALERT"
	if !strings.Contains(logOutput, expectedSubstring) {
		t.Errorf("expected sanitized string %q in log output, got:\n%s", expectedSubstring, logOutput)
	}
}
