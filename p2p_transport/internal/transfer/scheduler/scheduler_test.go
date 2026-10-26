package scheduler_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

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
	config := core.EngineConfig{ChunkSize: 64 * 1024} // 64KB chunks
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(t.TempDir())
	return engine.NewContentEngine(config, enc, dig, store, store, keys, store)
}

func setupMockSwarm(t testing.TB, numPeers int) ([]host.Host, mocknet.Mocknet) {
	mn := mocknet.New()
	hosts := make([]host.Host, numPeers)
	for i := 0; i < numPeers; i++ {
		h, err := mn.GenPeer()
		if err != nil {
			t.Fatal(err)
		}
		hosts[i] = h
	}
	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return hosts, mn
}

func TestScheduler_MaliciousPeerTerminationAndReassignment(t *testing.T) {
	hosts, _ := setupMockSwarm(t, 3)
	clientHost := hosts[0]
	badHost := hosts[1]
	goodHost := hosts[2]

	engClient := createTestEngine(t)
	engGood := createTestEngine(t)

	ctx := context.Background()

	// Ingest test data into goodHost
	dataSize := 256 * 1024 // 4 chunks of 64KB
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := engGood.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data: %v", err)
	}

	// Good peer has normal stream handler
	chunk.NewStreamHandler(goodHost, engGood)

	// Bad peer returns corrupted data on every chunk request
	var badPeerAttempts int32
	badHost.SetStreamHandler(protocol.ChunkTransportProtocolID, func(s network.Stream) {
		defer s.Close()
		atomic.AddInt32(&badPeerAttempts, 1)

		req, err := chunk.ReadMessage(s)
		if err != nil {
			return
		}
		if req.Type == chunk.MsgRequestChunk {
			var chunkID core.ChunkID
			copy(chunkID[:], req.Payload)
			badChunk := &core.Chunk{
				Header: core.ChunkHeader{
					Version:    1,
					Index:      0,
					ID:         chunkID,
					CipherSize: 16,
				},
				Data: []byte("corrupted data!"),
			}
			msg, _ := chunk.BuildChunk(badChunk)
			_ = chunk.WriteMessage(s, msg)
		}
	})

	clientTransport := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(clientTransport, engClient, 10)
	sched.InitialBackoff = 1 * time.Millisecond // fast test execution
	sched.MaxIntegrityFailures = 3

	tasks := make([]scheduler.ChunkTask, len(m.ChunkIDs))
	for i, id := range m.ChunkIDs {
		tasks[i] = scheduler.ChunkTask{
			Index:   i,
			ChunkID: id,
		}
	}

	sources := []scheduler.Source{
		{PeerID: badHost.ID()},
		{PeerID: goodHost.ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(m.ChunkIDs))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed: %v", err)
	}

	// Verify all chunks completed
	if len(completions) != len(m.ChunkIDs) {
		t.Errorf("Expected %d completions, got %d", len(m.ChunkIDs), len(completions))
	}

	// Verify bad peer was capped at 3 attempt cycles (MaxIntegrityFailures)
	attempts := atomic.LoadInt32(&badPeerAttempts)
	if attempts > 3 {
		t.Errorf("Malicious peer consumed %d chunk attempt cycles, expected <= 3", attempts)
	}

	// Verify client engine has all chunks
	for _, chunkID := range m.ChunkIDs {
		if _, err := engClient.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("Client missing chunk %x: %v", chunkID, err)
		}
	}
}

func TestPeerTracker_ExponentialBackoffAndMetrics(t *testing.T) {
	tracker := scheduler.NewPeerTracker(3, 5, 10*time.Millisecond, 100*time.Millisecond)
	p := peer.ID("peer-test-1")

	if tracker.IsPenalized(p) {
		t.Fatal("New peer should not be penalized")
	}

	// 1st integrity failure
	tracker.RecordFailure(p, chunk.ErrIntegrityMismatch)
	m := tracker.GetMetrics(p)
	if m.IntegrityFailures != 1 || m.Failures != 1 {
		t.Errorf("Expected 1 integrity failure, got metrics: %+v", m)
	}
	if m.Backoff != 10*time.Millisecond {
		t.Errorf("Expected backoff 10ms, got %v", m.Backoff)
	}

	// 2nd integrity failure
	tracker.RecordFailure(p, chunk.ErrIntegrityMismatch)
	m = tracker.GetMetrics(p)
	if m.IntegrityFailures != 2 {
		t.Errorf("Expected 2 integrity failures, got metrics: %+v", m)
	}
	if m.Backoff != 20*time.Millisecond {
		t.Errorf("Expected backoff 20ms, got %v", m.Backoff)
	}
	if tracker.IsPenalized(p) {
		t.Fatal("Peer should not be penalized after 2 failures (threshold 3)")
	}

	// 3rd integrity failure -> penalized!
	tracker.RecordFailure(p, chunk.ErrIntegrityMismatch)
	if !tracker.IsPenalized(p) {
		t.Fatal("Peer should be penalized after 3 integrity failures")
	}
}

func TestScheduler_SwarmWithMultipleMaliciousPeers(t *testing.T) {
	// Swarm of 1 client, 2 malicious peers (30-50% of candidate peers), 3 healthy peers
	hosts, _ := setupMockSwarm(t, 6)
	clientHost := hosts[0]
	badHost1 := hosts[1]
	badHost2 := hosts[2]
	goodHosts := hosts[3:]

	engClient := createTestEngine(t)
	engGood := createTestEngine(t)

	ctx := context.Background()

	// Ingest test data into goodHost
	dataSize := 512 * 1024 // 8 chunks of 64KB
	data := make([]byte, dataSize)
	rand.Read(data)

	m, err := engGood.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest data: %v", err)
	}

	for _, gh := range goodHosts {
		chunk.NewStreamHandler(gh, engGood)
	}

	badHandler := func(s network.Stream) {
		defer s.Close()
		req, err := chunk.ReadMessage(s)
		if err != nil {
			return
		}
		if req.Type == chunk.MsgRequestChunk {
			var chunkID core.ChunkID
			copy(chunkID[:], req.Payload)
			badChunk := &core.Chunk{
				Header: core.ChunkHeader{
					Version:    1,
					Index:      0,
					ID:         chunkID,
					CipherSize: 16,
				},
				Data: []byte("invalid payload!"),
			}
			msg, _ := chunk.BuildChunk(badChunk)
			_ = chunk.WriteMessage(s, msg)
		}
	}

	badHost1.SetStreamHandler(protocol.ChunkTransportProtocolID, badHandler)
	badHost2.SetStreamHandler(protocol.ChunkTransportProtocolID, badHandler)

	clientTransport := transport.NewTransport(clientHost)
	sched := scheduler.NewScheduler(clientTransport, engClient, 10)
	sched.InitialBackoff = 1 * time.Millisecond
	sched.MaxIntegrityFailures = 3

	tasks := make([]scheduler.ChunkTask, len(m.ChunkIDs))
	for i, id := range m.ChunkIDs {
		tasks[i] = scheduler.ChunkTask{
			Index:   i,
			ChunkID: id,
		}
	}

	sources := []scheduler.Source{
		{PeerID: badHost1.ID()},
		{PeerID: badHost2.ID()},
		{PeerID: goodHosts[0].ID()},
		{PeerID: goodHosts[1].ID()},
		{PeerID: goodHosts[2].ID()},
	}

	completions := make(chan scheduler.WorkerResult, len(m.ChunkIDs))

	err = sched.Run(ctx, tasks, sources, completions)
	if err != nil {
		t.Fatalf("Scheduler.Run failed in swarm test: %v", err)
	}

	if len(completions) != len(m.ChunkIDs) {
		t.Errorf("Expected %d completions, got %d", len(m.ChunkIDs), len(completions))
	}

	for _, chunkID := range m.ChunkIDs {
		if _, err := engClient.GetChunk(ctx, chunkID); err != nil {
			t.Errorf("Client engine missing chunk %x: %v", chunkID, err)
		}
	}
}
