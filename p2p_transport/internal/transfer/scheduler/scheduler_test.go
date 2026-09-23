package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transfer/scheduler"
	"cipher/internal/transport"
)

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 64 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func createCorruptHandler(h host.Host) {
	h.SetStreamHandler("/cipher/chunk/1.0.0", func(s network.Stream) {
		defer s.Close()
		for {
			msg, err := chunk.ReadMessage(s)
			if err != nil {
				return
			}
			if msg.Type == chunk.MsgRequestChunk {
				chunkID, err := chunk.ParseRequestChunk(msg.Payload)
				if err != nil {
					return
				}
				corruptChunk := &core.Chunk{
					Header: core.ChunkHeader{
						ID:        chunkID,
						Index:     0,
						PlainSize: 100,
					},
					Data: []byte("THIS IS INTENTIONALLY CORRUPTED DATA FROM BAD PROVIDER"),
				}
				resp, _ := chunk.BuildChunk(corruptChunk)
				if err := chunk.WriteMessage(s, resp); err != nil {
					return
				}
			}
		}
	})
}

func TestScheduler_FaultInjection_50PercentCorruptSwarm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mocknet := mocknet.New()

	clientHost, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	healthyHost1, _ := mocknet.GenPeer()
	healthyHost2, _ := mocknet.GenPeer()
	corruptHost1, _ := mocknet.GenPeer()
	corruptHost2, _ := mocknet.GenPeer()

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}

	clientEng := createTestEngine(t)
	healthyEng1 := createTestEngine(t)
	healthyEng2 := createTestEngine(t)

	// Ingest 256KB file (4 chunks)
	data := make([]byte, 256*1024)
	rand.Read(data)

	m, err := healthyEng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	for _, id := range m.ChunkIDs {
		chk, err := healthyEng1.GetChunk(ctx, id)
		if err != nil {
			t.Fatalf("Failed to get chunk: %v", err)
		}
		if err := healthyEng2.PutChunk(ctx, chk); err != nil {
			t.Fatalf("Failed to put chunk: %v", err)
		}
	}

	chunk.NewStreamHandler(healthyHost1, healthyEng1)
	chunk.NewStreamHandler(healthyHost2, healthyEng2)

	createCorruptHandler(corruptHost1)
	createCorruptHandler(corruptHost2)

	clientTrans := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(clientTrans, clientEng, 10)

	var tasks []scheduler.ChunkTask
	for i, id := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: id,
		})
	}

	sources := []scheduler.Source{
		{PeerID: corruptHost1.ID()},
		{PeerID: healthyHost1.ID()},
		{PeerID: corruptHost2.ID()},
		{PeerID: healthyHost2.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(tasks))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed unexpectedly: %v", err)
	}

	for _, id := range m.ChunkIDs {
		has, err := clientEng.HasChunk(ctx, id)
		if err != nil || !has {
			t.Fatalf("Client engine missing chunk %x", id)
		}
	}

	if !sched.Reputation.IsExcluded(corruptHost1.ID().String()) {
		t.Errorf("Expected corruptHost1 to be excluded, score: %f", sched.Reputation.GetScore(corruptHost1.ID().String()))
	}
	if !sched.Reputation.IsExcluded(corruptHost2.ID().String()) {
		t.Errorf("Expected corruptHost2 to be excluded, score: %f", sched.Reputation.GetScore(corruptHost2.ID().String()))
	}

	if sched.Reputation.IsExcluded(healthyHost1.ID().String()) {
		t.Errorf("Expected healthyHost1 NOT to be excluded, score: %f", sched.Reputation.GetScore(healthyHost1.ID().String()))
	}
	if sched.Reputation.IsExcluded(healthyHost2.ID().String()) {
		t.Errorf("Expected healthyHost2 NOT to be excluded, score: %f", sched.Reputation.GetScore(healthyHost2.ID().String()))
	}
}
