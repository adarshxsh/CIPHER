package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/reputation"
	"cipher/internal/transport"
)

func TestChunkClient_FetchChunk_RecordsFailureOnHashMismatch(t *testing.T) {
	mn := mocknet.New()

	hServer, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	hClient, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}

	engServer := createTestEngine(t)
	engClient := createTestEngine(t)

	chunk.NewStreamHandler(hServer, engServer)
	chunk.NewStreamHandler(hClient, engClient)

	ctx := context.Background()

	// Server ingests file
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := engServer.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Corrupt chunk in engServer
	corruptChunk := &core.Chunk{
		Header: core.ChunkHeader{ID: m.ChunkIDs[0], PlainSize: 100, CipherSize: 100},
		Data:   []byte("BAD_CORRUPTED_CHUNK_DATA"),
	}
	_ = engServer.PutChunk(ctx, corruptChunk)

	repMgr := reputation.NewPeerReputationManager()

	client, err := chunk.NewClient(ctx, transport.NewTransport(hClient), hServer.ID(), engClient)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	client.SetReputationManager(repMgr)

	// Fetch chunk - should fail hash verification
	_, err = client.FetchChunk(ctx, m.ChunkIDs[0])
	if err == nil {
		t.Fatalf("Expected FetchChunk to fail due to hash mismatch")
	}

	// Check reputation score for hServer.ID()
	score := repMgr.GetScore(hServer.ID())
	if score != -10.0 {
		t.Fatalf("Expected server peer score to be -10.0 after hash mismatch, got %f", score)
	}
}
