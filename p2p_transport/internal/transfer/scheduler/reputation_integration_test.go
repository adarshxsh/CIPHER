package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"

	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/reputation"
	"cipher/internal/transfer/scheduler"
	"cipher/internal/transport"
)

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 256 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func TestScheduler_BansCorruptPeerAndCompletesDownload(t *testing.T) {
	mn := mocknet.New()

	hGood, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	hBad, err := mn.GenPeer()
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

	engGood := createTestEngine(t)
	engBad := createTestEngine(t)
	engClient := createTestEngine(t)

	// Setup StreamHandler on Good peer and Bad peer
	chunk.NewStreamHandler(hGood, engGood)
	chunk.NewStreamHandler(hBad, engBad)
	chunk.NewStreamHandler(hClient, engClient)

	ctx := context.Background()

	// Good peer ingests a file with 4 chunks
	data := make([]byte, 1024*1024)
	rand.Read(data)

	m, err := engGood.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Bad peer stores corrupt chunks with hash mismatch
	for _, cid := range m.ChunkIDs {
		corruptChunk := &core.Chunk{
			Header: core.ChunkHeader{ID: cid, PlainSize: 100, CipherSize: 100},
			Data:   []byte("CORRUPT_PAYLOAD_GARBAGE_DATA"),
		}
		_ = engBad.PutChunk(ctx, corruptChunk)
	}

	// Create Scheduler with ReputationManager
	repMgr := reputation.NewPeerReputationManager()
	sched := scheduler.NewScheduler(transport.NewTransport(hClient), engClient, 10)
	sched.Reputation = repMgr

	// Define tasks and sources
	var tasks []scheduler.ChunkTask
	for i, cid := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: cid,
		})
	}

	sources := []scheduler.Source{
		{PeerID: hBad.ID()},
		{PeerID: hGood.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Expected scheduler run to succeed by isolating bad peer, got error: %v", err)
	}

	// Bad peer should be penalized and banned
	if !repMgr.IsBanned(hBad.ID()) {
		t.Fatalf("Expected bad peer %s to be banned after serving corrupt chunks", hBad.ID())
	}

	// Good peer should NOT be banned
	if repMgr.IsBanned(hGood.ID()) {
		t.Fatalf("Good peer %s should not be banned", hGood.ID())
	}
}
