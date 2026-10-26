package chunk

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/transport"
)

// MockInterceptor allows unit and integration tests to intercept chunk payloads.
type MockInterceptor struct {
	InterceptFunc func(ctx context.Context, chunk *core.Chunk) (*core.Chunk, error)
}

func (m *MockInterceptor) InterceptChunk(ctx context.Context, chunk *core.Chunk) (*core.Chunk, error) {
	if m.InterceptFunc != nil {
		return m.InterceptFunc(ctx, chunk)
	}
	return chunk, nil
}

// CorruptChunkInterceptor corrupts chunk payloads for testing protocol resilience.
type CorruptChunkInterceptor struct {
	Probability float64
}

func NewCorruptChunkInterceptor(prob float64) *CorruptChunkInterceptor {
	return &CorruptChunkInterceptor{Probability: prob}
}

func (c *CorruptChunkInterceptor) InterceptChunk(ctx context.Context, chunk *core.Chunk) (*core.Chunk, error) {
	if chunk == nil || len(chunk.Data) == 0 {
		return chunk, nil
	}
	if c.Probability > 0 {
		corrupted := *chunk
		corrupted.Data = append([]byte(nil), chunk.Data...)
		corrupted.Data[0] ^= 0xFF
		return &corrupted, nil
	}
	return chunk, nil
}

// SimulateCorruptedChunk returns a ChunkInterceptor that corrupts chunks for testing.
func SimulateCorruptedChunk(prob float64) ChunkInterceptor {
	return NewCorruptChunkInterceptor(prob)
}

func TestChunkProtocol_InterceptorCorruption(t *testing.T) {
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

	createEngine := func() *engine.ContentEngine {
		config := core.EngineConfig{ChunkSize: 256 * 1024}
		enc := crypto.NewChaCha20Encryptor()
		dig := verifier.NewSHA256Digest()
		keys := engine.NewLocalKeyProvider()
		store := storage.NewFSStore(t.TempDir())
		return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
	}

	eng1 := createEngine()
	eng2 := createEngine()

	// Register stream handler on h1 with SimulateCorruptedChunk interceptor
	handler := NewStreamHandler(h1, eng1, WithInterceptor(SimulateCorruptedChunk(1.0)))
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}

	// Ingest sample data on h1
	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	// Create client on h2
	client, err := NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Failed to resolve manifest: %v", err)
	}
	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Failed to deserialize manifest: %v", err)
	}

	// Downloading corrupted chunk must fail
	err = client.Download(ctx, m2.ChunkIDs)
	if err == nil {
		t.Fatal("Expected download to fail due to corrupted chunk, but it succeeded")
	}
}

func TestChunkProtocol_MockInterceptor(t *testing.T) {
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

	createEngine := func() *engine.ContentEngine {
		config := core.EngineConfig{ChunkSize: 256 * 1024}
		enc := crypto.NewChaCha20Encryptor()
		dig := verifier.NewSHA256Digest()
		keys := engine.NewLocalKeyProvider()
		store := storage.NewFSStore(t.TempDir())
		return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
	}

	eng1 := createEngine()
	eng2 := createEngine()

	interceptCalled := false
	mockInt := &MockInterceptor{
		InterceptFunc: func(ctx context.Context, chunk *core.Chunk) (*core.Chunk, error) {
			interceptCalled = true
			return chunk, nil
		},
	}

	NewStreamHandler(h1, eng1, WithInterceptor(mockInt))

	ctx := context.Background()
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	resolvedData, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Failed to resolve manifest: %v", err)
	}
	m2, err := manifest.Deserialize(resolvedData)
	if err != nil {
		t.Fatalf("Failed to deserialize manifest: %v", err)
	}

	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}

	if !interceptCalled {
		t.Error("Expected MockInterceptor to be invoked during chunk download")
	}
}
