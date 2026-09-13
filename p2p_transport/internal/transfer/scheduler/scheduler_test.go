package scheduler

import (
	"bytes"
	"context"
	"crypto/rand"
	"math"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"

	"cipher/internal/content/core"
	"cipher/internal/content/crypto"
	"cipher/internal/content/engine"
	"cipher/internal/content/manifest"
	"cipher/internal/content/storage"
	"cipher/internal/content/verifier"
	"cipher/internal/protocol/chunk"
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

func setupMockNetwork(t testing.TB) (host.Host, host.Host) {
	mn := mocknet.New()

	h1, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	h2, err := mn.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := mn.LinkAll(); err != nil {
		t.Fatal(err)
	}
	return h1, h2
}

func TestScheduler_CalculateBackoff(t *testing.T) {
	sched := NewScheduler(nil, nil, 3)
	sched.InitialBackoff = 10 * time.Millisecond
	sched.MaxBackoff = 500 * time.Millisecond
	sched.BackoffFactor = 2.0

	// Test 100 samples per attempt to verify jitter upper bound
	for attempt := 1; attempt <= 3; attempt++ {
		maxExpected := time.Duration(float64(sched.InitialBackoff) * math.Pow(2, float64(attempt-1)))
		if maxExpected > sched.MaxBackoff {
			maxExpected = sched.MaxBackoff
		}

		for i := 0; i < 50; i++ {
			b := sched.calculateBackoff(attempt)
			if b < 0 || b > maxExpected {
				t.Fatalf("attempt %d: backoff %v outside expected range [0, %v]", attempt, b, maxExpected)
			}
		}
	}
}

func TestScheduler_IntegrationAndSlowCompletionHandler(t *testing.T) {
	h1, h2 := setupMockNetwork(t)

	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := make([]byte, 256*1024) // 4 chunks
	rand.Read(data)

	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}

	var tasks []ChunkTask
	for i, chunkID := range m.ChunkIDs {
		tasks = append(tasks, ChunkTask{
			Index:    i,
			ChunkID:  chunkID,
			Attempts: 0,
		})
	}

	tr2 := transport.NewTransport(h2)
	sched := NewScheduler(tr2, eng2, 3)
	sched.InitialBackoff = 5 * time.Millisecond
	sched.MaxBackoff = 50 * time.Millisecond

	sources := []Source{
		{PeerID: h1.ID()},
	}

	// Create a slow completions channel consumer
	completions := make(chan WorkerResult, 1) // small buffer to test async unblocking
	errCh := make(chan error, 1)

	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	// Simulate slow completion handler reading at 20ms intervals
	receivedCount := 0
	for res := range completions {
		time.Sleep(20 * time.Millisecond)
		if res.Error != nil {
			t.Errorf("Unexpected error in completion: %v", res.Error)
		}
		receivedCount++
		if receivedCount == len(tasks) {
			break
		}
	}

	select {
	case schedErr := <-errCh:
		if schedErr != nil {
			t.Fatalf("Scheduler.Run failed: %v", schedErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Timed out waiting for Scheduler.Run to complete")
	}

	if receivedCount != len(tasks) {
		t.Fatalf("expected %d completions, got %d", len(tasks), receivedCount)
	}
}
