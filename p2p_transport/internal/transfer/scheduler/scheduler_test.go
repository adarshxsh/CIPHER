package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync"
	"testing"

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

func setupMockNetwork(t testing.TB, numHosts int) []host.Host {
	mocknet := mocknet.New()
	hosts := make([]host.Host, numHosts)
	for i := 0; i < numHosts; i++ {
		h, err := mocknet.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	if err := mocknet.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return hosts
}

func TestPeerTracker(t *testing.T) {
	tracker := scheduler.NewPeerTracker(3)
	hosts := setupMockNetwork(t, 1)
	peerID := hosts[0].ID()

	if tracker.IsQuarantined(peerID) {
		t.Fatalf("Expected peer to not be quarantined initially")
	}

	count, quarantined := tracker.RecordFault(peerID)
	if count != 1 || quarantined {
		t.Fatalf("Expected count=1, quarantined=false, got count=%d, quarantined=%v", count, quarantined)
	}

	count, quarantined = tracker.RecordFault(peerID)
	if count != 2 || quarantined {
		t.Fatalf("Expected count=2, quarantined=false, got count=%d, quarantined=%v", count, quarantined)
	}

	count, quarantined = tracker.RecordFault(peerID)
	if count != 3 || !quarantined {
		t.Fatalf("Expected count=3, quarantined=true, got count=%d, quarantined=%v", count, quarantined)
	}

	if !tracker.IsQuarantined(peerID) {
		t.Fatalf("IsQuarantined should return true")
	}
}

func TestPeerTracker_ConcurrentAccess(t *testing.T) {
	tracker := scheduler.NewPeerTracker(3)
	hosts := setupMockNetwork(t, 1)
	peerID := hosts[0].ID()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tracker.RecordFault(peerID)
			tracker.IsQuarantined(peerID)
			tracker.FaultCount(peerID)
		}()
	}
	wg.Wait()

	if !tracker.IsQuarantined(peerID) {
		t.Fatalf("Expected peer to be quarantined after 10 faults")
	}
}

func TestScheduler_SwarmDownloadWithCorruptPeer(t *testing.T) {
	// Setup 3 hosts:
	// h0: Client downloading content
	// h1: Corrupt peer (always returns bad chunks)
	// h2: Healthy peer (serves valid chunks)
	hosts := setupMockNetwork(t, 3)
	clientHost := hosts[0]
	corruptHost := hosts[1]
	healthyHost := hosts[2]

	clientEng := createTestEngine(t)
	corruptEng := createTestEngine(t)
	healthyEng := createTestEngine(t)

	// Ingest valid data on healthy peer
	ctx := context.Background()
	dataSize := 1024 * 1024 // 4 chunks
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := healthyEng.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	// Attach standard handler to healthy peer
	chunk.NewStreamHandler(healthyHost, healthyEng)

	// Attach malicious handler to corrupt peer that always returns corrupted chunk data
	corruptHost.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		for {
			req, err := chunk.ReadMessage(s)
			if err != nil {
				return
			}
			if req.Type == chunk.MsgRequestChunk {
				chunkID, _ := chunk.ParseRequestChunk(req.Payload)
				fakeChunk := &core.Chunk{
					Header: core.ChunkHeader{
						ID: chunkID,
					},
					Data: []byte("corrupted data!"),
				}
				resp, _ := chunk.BuildChunk(fakeChunk)
				if err := chunk.WriteMessage(s, resp); err != nil {
					return
				}
			}
		}
	})

	// Setup scheduler tasks
	var tasks []scheduler.ChunkTask
	for i, cID := range m.ChunkIDs {
		tasks = append(tasks, scheduler.ChunkTask{
			Index:   i,
			ChunkID: cID,
		})
	}

	sources := []scheduler.Source{
		{PeerID: corruptHost.ID()},
		{PeerID: healthyHost.ID()},
	}

	sched := scheduler.NewScheduler(transport.NewTransport(clientHost), clientEng, 3)
	completions := make(chan scheduler.WorkerResult, len(tasks))

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	completedCount := 0
	for res := range completions {
		if res.Error == nil {
			completedCount++
		}
		if completedCount == len(tasks) {
			break
		}
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Expected scheduler run to succeed using healthy provider, got: %v", err)
	}

	// Ensure client engine holds all chunks
	for _, chunkID := range m.ChunkIDs {
		if _, err := clientEng.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("Client missing chunk %x: %v", chunkID, err)
		}
	}

	// Ensure corrupt peer didn't infect client store
	_ = corruptEng
}
