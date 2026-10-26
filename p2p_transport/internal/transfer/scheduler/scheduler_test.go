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
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transfer/reputation"
	"cipher/internal/transfer/scheduler"
	"cipher/internal/transport"
)

func createTestEngine(t testing.TB) *engine.ContentEngine {
	config := core.EngineConfig{ChunkSize: 64 * 1024} // 64KB chunks
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

// setupCorruptStreamHandler sets up a stream handler on a host that responds to CHUNK requests with corrupted data.
func setupCorruptStreamHandler(h host.Host) {
	h.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
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
				// Return corrupted data with invalid hash
				corruptData := make([]byte, 1024)
				rand.Read(corruptData)
				c := &core.Chunk{
					Header: core.ChunkHeader{
						Version:    chunk.CurrentMessageVersion,
						ID:         chunkID,
						PlainSize:  uint32(len(corruptData)),
						CipherSize: uint32(len(corruptData)),
					},
					Data: corruptData,
				}
				resp, err := chunk.BuildChunk(c)
				if err != nil {
					return
				}
				if err := chunk.WriteMessage(s, resp); err != nil {
					return
				}
			}
		}
	})
}

func TestScheduler_SwarmDownloadWithCorruptPeers(t *testing.T) {
	mocknet := mocknet.New()

	// Create 5 hosts: 1 client host + 2 good peers + 2 corrupt peers (50% corrupt peers)
	clientHost, err := mocknet.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	good1, _ := mocknet.GenPeer()
	good2, _ := mocknet.GenPeer()
	bad1, _ := mocknet.GenPeer()
	bad2, _ := mocknet.GenPeer()

	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}

	clientEng := createTestEngine(t)
	goodEng1 := createTestEngine(t)
	goodEng2 := createTestEngine(t)

	// Attach normal handlers to good peers
	chunk.NewStreamHandler(good1, goodEng1)
	chunk.NewStreamHandler(good2, goodEng2)

	// Attach corrupt handlers to bad peers
	setupCorruptStreamHandler(bad1)
	setupCorruptStreamHandler(bad2)

	// Prepare data on good peers
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dataSize := 512 * 1024 // 8 chunks (8 * 64KB)
	data := make([]byte, dataSize)
	rand.Read(data)

	m1, err := goodEng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest good1 failed: %v", err)
	}
	for _, chunkID := range m1.ChunkIDs {
		chunkData, err := goodEng1.GetChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("Failed to get chunk from good1: %v", err)
		}
		if err := goodEng2.PutChunk(ctx, chunkData); err != nil {
			t.Fatalf("Failed to put chunk into good2: %v", err)
		}
	}

	// Setup tasks and sources
	var tasks []scheduler.ChunkTask
	for i, cID := range m1.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:    i,
			ChunkID:  cID,
			Attempts: 0,
		})
	}

	sources := []scheduler.Source{
		{PeerID: bad1.ID()},
		{PeerID: good1.ID()},
		{PeerID: bad2.ID()},
		{PeerID: good2.ID()},
	}

	// Create ReputationManager with short backoffs for test
	repCfg := reputation.ReputationConfig{
		BaseBackoff:         20 * time.Millisecond,
		MaxBackoff:          100 * time.Millisecond,
		BackoffMultiplier:   2.0,
		QuarantineThreshold: 1,
		PenaltyIntegrity:    10,
		PenaltyTransient:    1,
	}
	rep := reputation.NewReputationManager(repCfg)

	clientTrans := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(clientTrans, clientEng, 5, rep)

	completions := make(chan scheduler.WorkerResult, len(tasks))
	errCh := make(chan error, 1)

	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
		close(completions)
	}()

	receivedChunks := 0
	for range completions {
		receivedChunks++
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Scheduler.Run failed unexpectedly: %v", err)
	}

	if receivedChunks != len(tasks) {
		t.Fatalf("Expected %d completed chunks, got %d", len(tasks), receivedChunks)
	}

	// Verify client engine received all chunks
	for _, chunkID := range m1.ChunkIDs {
		if _, err := clientEng.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("Client missing chunk %x", chunkID)
		}
	}

	// Verify that bad peers were penalized and quarantined
	if !rep.IsQuarantined(bad1.ID()) {
		t.Errorf("Expected bad peer 1 (%s) to be quarantined", bad1.ID())
	}
	if !rep.IsQuarantined(bad2.ID()) {
		t.Errorf("Expected bad peer 2 (%s) to be quarantined", bad2.ID())
	}
	if rep.GetScore(bad1.ID()) == 0 {
		t.Errorf("Expected positive penalty score for bad peer 1, got %d", rep.GetScore(bad1.ID()))
	}
	if rep.GetScore(bad2.ID()) == 0 {
		t.Errorf("Expected positive penalty score for bad peer 2, got %d", rep.GetScore(bad2.ID()))
	}

	// Good peers should not be quarantined
	if rep.IsQuarantined(good1.ID()) || rep.IsQuarantined(good2.ID()) {
		t.Errorf("Good peers should not be quarantined")
	}
}
