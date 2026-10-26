package scheduler

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"cipher/internal/content/core"
	"cipher/internal/protocol/chunk"
)

func TestScheduler_CandidateMissAndRetry(t *testing.T) {
	// Test candidate miss logic with ChunkQueue
	tasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}

	queue := NewChunkQueue(tasks)
	defer queue.Close()

	if queue.Len() != 2 {
		t.Fatalf("expected 2 tasks in queue, got %d", queue.Len())
	}

	// Pop task 0
	task, ok := queue.Next()
	if !ok || task.Index != 0 {
		t.Fatalf("expected task 0, got %v", task)
	}

	// Simulate candidate miss on PeerA
	if task.MissedPeers == nil {
		task.MissedPeers = make(map[string]bool)
	}
	task.MissedPeers["PeerA"] = true
	queue.Push(task)

	if queue.Len() != 2 {
		t.Fatalf("expected 2 tasks in queue after push, got %d", queue.Len())
	}

	// Pop next task (task 1)
	task1, ok := queue.Next()
	if !ok || task1.Index != 1 {
		t.Fatalf("expected task 1, got %v", task1)
	}

	// Pop task 0 again
	task0Again, ok := queue.Next()
	if !ok || task0Again.Index != 0 {
		t.Fatalf("expected task 0 again, got %v", task0Again)
	}

	if !task0Again.MissedPeers["PeerA"] {
		t.Fatalf("expected MissedPeers['PeerA'] to be true")
	}
}

func TestScheduler_MaxAttemptsLimit(t *testing.T) {
	// Test exponential backoff calculation and max attempts threshold
	task := ChunkTask{
		Index:    0,
		ChunkID:  core.ChunkID{10},
		Attempts: 0,
	}

	maxAttempts := 3
	var delays []time.Duration

	for task.Attempts < maxAttempts {
		task.Attempts++
		if task.Attempts < maxAttempts {
			delay := CalculateBackoff(task.Attempts, 50*time.Millisecond)
			delays = append(delays, delay)
		}
	}

	if len(delays) != 2 {
		t.Fatalf("expected 2 backoff delays before hitting max attempts, got %d", len(delays))
	}

	if delays[0] != 50*time.Millisecond {
		t.Fatalf("expected first delay 50ms, got %v", delays[0])
	}

	if delays[1] != 100*time.Millisecond {
		t.Fatalf("expected second delay 100ms, got %v", delays[1])
	}
}

func TestScheduler_ErrRemoteChunkNotFoundHandling(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", chunk.ErrRemoteChunkNotFound)
	if !isRemoteChunkNotFound(err) {
		t.Fatalf("expected isRemoteChunkNotFound to identify ErrRemoteChunkNotFound")
	}
}

func isRemoteChunkNotFound(err error) bool {
	return errors.Is(err, chunk.ErrRemoteChunkNotFound)
}
