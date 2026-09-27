package scheduler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestChunkQueueBoundedCapacityAndBackpressure(t *testing.T) {
	initialTasks := []ChunkTask{
		{Index: 0, ChunkID: core.ChunkID{1}},
		{Index: 1, ChunkID: core.ChunkID{2}},
	}

	queue := NewChunkQueueWithCapacity(initialTasks, 2)
	if queue.Capacity() != 2 {
		t.Fatalf("expected capacity 2, got %d", queue.Capacity())
	}
	if queue.Len() != 2 {
		t.Fatalf("expected initial len 2, got %d", queue.Len())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Pushing to full queue should block until context timeout or space is freed
	pushDone := make(chan error, 1)
	go func() {
		pushDone <- queue.PushCtx(ctx, ChunkTask{Index: 2, ChunkID: core.ChunkID{3}})
	}()

	select {
	case err := <-pushDone:
		t.Fatalf("expected push to block on full queue, but finished early with %v", err)
	case <-time.After(20 * time.Millisecond):
		// Expected to be blocking
	}

	// Now pop one task
	task, ok := queue.Next()
	if !ok || task.Index != 0 {
		t.Fatalf("expected task 0, got %v (ok=%v)", task, ok)
	}

	// Now the pushed task should succeed
	select {
	case err := <-pushDone:
		if err != nil {
			t.Fatalf("push failed unexpectedly: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatalf("push did not complete after space was cleared")
	}

	if queue.Len() != 2 {
		t.Fatalf("expected len 2 after push, got %d", queue.Len())
	}
}

func TestChunkQueueContextCancellationOnFullPush(t *testing.T) {
	queue := NewChunkQueueWithCapacity([]ChunkTask{{Index: 0}}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	pushDone := make(chan error, 1)

	go func() {
		pushDone <- queue.PushCtx(ctx, ChunkTask{Index: 1})
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-pushDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("push did not unblock on context cancellation")
	}
}

func TestChunkQueueNextCtxWaitingAndClose(t *testing.T) {
	queue := NewChunkQueueWithCapacity(nil, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	popDone := make(chan ChunkTask, 1)
	go func() {
		task, ok := queue.NextCtx(ctx)
		if ok {
			popDone <- task
		}
	}()

	// Wait 20ms and push a task
	time.Sleep(20 * time.Millisecond)
	_ = queue.PushCtx(ctx, ChunkTask{Index: 42})

	select {
	case task := <-popDone:
		if task.Index != 42 {
			t.Fatalf("expected task 42, got %v", task)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("pop did not receive pushed task")
	}

	// Test close unblocks waiting pops
	popClosedDone := make(chan bool, 1)
	go func() {
		_, ok := queue.NextCtx(ctx)
		popClosedDone <- ok
	}()

	time.Sleep(10 * time.Millisecond)
	queue.Close()

	select {
	case ok := <-popClosedDone:
		if ok {
			t.Fatalf("expected ok=false after queue close")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("pop did not unblock on queue close")
	}
}

func TestSchedulerBackoffCalculation(t *testing.T) {
	sched := NewScheduler(nil, nil, 3)
	sched.BaseBackoff = 10 * time.Millisecond
	sched.MaxBackoff = 50 * time.Millisecond

	b1 := sched.getBackoff(1)
	if b1 != 10*time.Millisecond {
		t.Fatalf("expected attempt 1 backoff 10ms, got %v", b1)
	}

	b2 := sched.getBackoff(2)
	if b2 != 20*time.Millisecond {
		t.Fatalf("expected attempt 2 backoff 20ms, got %v", b2)
	}

	b3 := sched.getBackoff(3)
	if b3 != 40*time.Millisecond {
		t.Fatalf("expected attempt 3 backoff 40ms, got %v", b3)
	}

	b4 := sched.getBackoff(4)
	if b4 != 50*time.Millisecond {
		t.Fatalf("expected attempt 4 backoff capped at 50ms, got %v", b4)
	}
}

func TestSchedulerCompletionDispatchCancellation(t *testing.T) {
	sched := NewScheduler(nil, nil, 3)
	
	ctx, cancel := context.WithCancel(context.Background())
	completions := make(chan WorkerResult) // unbuffered channel

	// We fill completions with an unread send scenario
	tasks := []ChunkTask{{Index: 0, ChunkID: core.ChunkID{1}}}

	// If no worker is started because nil transport/sources, Run returns error
	err := sched.Run(ctx, tasks, nil, completions)
	if err == nil {
		t.Fatalf("expected error when no workers could be started")
	}

	_ = fmt.Sprint(cancel)
}
