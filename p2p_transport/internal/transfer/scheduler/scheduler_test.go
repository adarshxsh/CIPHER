package scheduler

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

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

func TestCalculateBackoff(t *testing.T) {
	sched := NewScheduler(nil, nil, 3)
	sched.DisableJitter = true

	// Attempt 1: 100ms
	b1 := sched.CalculateBackoff(1)
	if b1 != 100*time.Millisecond {
		t.Errorf("Expected 100ms for attempt 1, got %v", b1)
	}

	// Attempt 2: 200ms
	b2 := sched.CalculateBackoff(2)
	if b2 != 200*time.Millisecond {
		t.Errorf("Expected 200ms for attempt 2, got %v", b2)
	}

	// Attempt 3: 400ms
	b3 := sched.CalculateBackoff(3)
	if b3 != 400*time.Millisecond {
		t.Errorf("Expected 400ms for attempt 3, got %v", b3)
	}

	// Attempt 10: capped at MaxBackoff (5s)
	b10 := sched.CalculateBackoff(10)
	if b10 != 5*time.Second {
		t.Errorf("Expected 5s max backoff for attempt 10, got %v", b10)
	}

	// Test with jitter enabled
	sched.DisableJitter = false
	bj := sched.CalculateBackoff(1)
	if bj < 100*time.Millisecond || bj > 125*time.Millisecond {
		t.Errorf("Expected backoff with jitter in [100ms, 125ms], got %v", bj)
	}
}

func TestChunkQueueCapacityAndQueueFull(t *testing.T) {
	q := NewChunkQueueWithCapacity(nil, 3)

	if q.Cap() != 3 {
		t.Fatalf("Expected capacity 3, got %d", q.Cap())
	}

	// Push 3 tasks
	for i := 0; i < 3; i++ {
		err := q.Push(ChunkTask{Index: i})
		if err != nil {
			t.Fatalf("Failed to push task %d: %v", i, err)
		}
	}

	if q.Len() != 3 {
		t.Fatalf("Expected queue length 3, got %d", q.Len())
	}

	// Push 4th task should return ErrQueueFull
	err := q.Push(ChunkTask{Index: 3})
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Expected ErrQueueFull, got %v", err)
	}

	// Pop one task
	ctx := context.Background()
	task, ok := q.PopForPeer(ctx, "peer1")
	if !ok || task.Index != 0 {
		t.Fatalf("Expected task 0, got %v, ok=%v", task, ok)
	}

	// Now pushing should succeed again
	err = q.Push(ChunkTask{Index: 3})
	if err != nil {
		t.Fatalf("Expected successful push after pop, got %v", err)
	}
}

func TestChunkQueueDelayedPriorityQueue(t *testing.T) {
	q := NewChunkQueueWithCapacity(nil, 10)

	now := time.Now()
	taskFuture2 := ChunkTask{Index: 2, ReadyAt: now.Add(200 * time.Millisecond)}
	taskFuture1 := ChunkTask{Index: 1, ReadyAt: now.Add(50 * time.Millisecond)}
	taskImmediate := ChunkTask{Index: 0}

	_ = q.Push(taskFuture2)
	_ = q.Push(taskFuture1)
	_ = q.Push(taskImmediate)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// 1st pop should return immediate task right away
	t0 := time.Now()
	task, ok := q.PopForPeer(ctx, "")
	if !ok || task.Index != 0 {
		t.Fatalf("Expected immediate task 0, got %v, ok=%v", task, ok)
	}
	if elapsed := time.Since(t0); elapsed > 20*time.Millisecond {
		t.Errorf("Immediate task pop took too long: %v", elapsed)
	}

	// 2nd pop should wait ~50ms and return task 1
	t1 := time.Now()
	task, ok = q.PopForPeer(ctx, "")
	if !ok || task.Index != 1 {
		t.Fatalf("Expected task 1, got %v, ok=%v", task, ok)
	}
	if elapsed := time.Since(t1); elapsed < 35*time.Millisecond {
		t.Errorf("Delayed task 1 returned too fast: %v", elapsed)
	}

	// 3rd pop should wait until 200ms mark and return task 2
	t2 := time.Now()
	task, ok = q.PopForPeer(ctx, "")
	if !ok || task.Index != 2 {
		t.Fatalf("Expected task 2, got %v, ok=%v", task, ok)
	}
	if elapsed := time.Since(t2); elapsed < 100*time.Millisecond {
		t.Errorf("Delayed task 2 returned too fast: %v", elapsed)
	}
}

func TestChunkQueuePopForPeerMissedPeers(t *testing.T) {
	q := NewChunkQueueWithCapacity(nil, 10)

	taskMissed := ChunkTask{
		Index:       0,
		MissedPeers: map[string]bool{"peerA": true},
	}
	taskAvailable := ChunkTask{
		Index:       1,
		MissedPeers: map[string]bool{},
	}

	_ = q.Push(taskMissed)
	_ = q.Push(taskAvailable)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// peerA should skip taskMissed (index 0) and get taskAvailable (index 1)
	task, ok := q.PopForPeer(ctx, "peerA")
	if !ok || task.Index != 1 {
		t.Fatalf("Expected peerA to get task 1, got index %d (ok=%v)", task.Index, ok)
	}

	// peerB should get remaining taskMissed (index 0)
	task, ok = q.PopForPeer(ctx, "peerB")
	if !ok || task.Index != 0 {
		t.Fatalf("Expected peerB to get task 0, got index %d (ok=%v)", task.Index, ok)
	}
}

func TestChunkQueueContextCancellation(t *testing.T) {
	q := NewChunkQueueWithCapacity(nil, 10)

	// Queue is empty
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	t0 := time.Now()
	_, ok := q.PopForPeer(ctx, "peer1")
	if ok {
		t.Fatalf("Expected PopForPeer to return false on context cancellation")
	}
	if elapsed := time.Since(t0); elapsed < 40*time.Millisecond {
		t.Fatalf("PopForPeer returned too early: %v", elapsed)
	}
}

func TestChunkQueueCloseUnblocksWaiters(t *testing.T) {
	q := NewChunkQueueWithCapacity(nil, 10)

	done := make(chan bool)
	go func() {
		ctx := context.Background()
		_, ok := q.PopForPeer(ctx, "peer1")
		done <- ok
	}()

	time.Sleep(20 * time.Millisecond)
	q.Close()

	select {
	case ok := <-done:
		if ok {
			t.Fatalf("Expected PopForPeer to return false after q.Close()")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("q.Close() failed to unblock waiting PopForPeer")
	}
}

func TestScheduler_Run_NetworkRecovery(t *testing.T) {
	// Setup mock network
	net := mocknet.New()
	h1, err := net.GenPeer()
	if err != nil {
		t.Fatal(err)
	}
	h2, err := net.GenPeer()
	if err != nil {
		t.Fatal(err)
	}

	if err := net.LinkAll(); err != nil {
		t.Fatal(err)
	}

	engStore := t.TempDir()
	engConfig := core.EngineConfig{ChunkSize: 64 * 1024}
	enc := crypto.NewChaCha20Encryptor()
	dig := verifier.NewSHA256Digest()
	keys := engine.NewLocalKeyProvider()
	store := storage.NewFSStore(engStore)

	eng1 := engine.NewContentEngine(engConfig, enc, dig, store, store, keys, store)
	eng2 := engine.NewContentEngine(engConfig, enc, dig, store, store, keys, store)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	// Ingest test chunk into eng1 (provider)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data := []byte("hello network recovery test data")
	mfst, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Failed to ingest chunk: %v", err)
	}

	// Set corruption probability = 1.0 to simulate packet corruption / network error on 1st attempt
	chunk.TestCorruptProb = 1.0
	defer func() { chunk.TestCorruptProb = 0.0 }()

	tClient := transport.NewTransport(h2)
	sched := NewScheduler(tClient, eng2, 5)
	sched.BaseBackoff = 30 * time.Millisecond
	sched.DisableJitter = true

	tasks := []ChunkTask{
		{Index: 0, ChunkID: mfst.ChunkIDs[0]},
	}
	sources := []Source{
		{PeerID: h1.ID()},
	}

	completions := make(chan WorkerResult, 1)
	errCh := make(chan error, 1)

	go func() {
		errCh <- sched.Run(ctx, tasks, sources, completions)
	}()

	// Wait 15ms so 1st attempt fails and task enters exponential backoff (ReadyAt = +30ms)
	time.Sleep(15 * time.Millisecond)

	// Recover network (set corruption = 0)
	chunk.TestCorruptProb = 0.0

	select {
	case res := <-completions:
		if res.Task.Index != 0 {
			t.Fatalf("Expected completion for task 0, got %d", res.Task.Index)
		}
		if res.Task.Attempts == 0 {
			t.Fatalf("Expected task attempts > 0 due to initial network corruption error")
		}
	case err := <-errCh:
		t.Fatalf("Scheduler.Run failed unexpectedly: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatalf("Timed out waiting for task completion after network recovery")
	}
}

func TestChunkTaskCoreID(t *testing.T) {
	var id core.ChunkID
	task := ChunkTask{ChunkID: id}
	if task.ChunkID != id {
		t.Fatalf("ChunkID mismatch")
	}
}
