package scheduler

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestScheduler_CalculateBackoff(t *testing.T) {
	sched := NewScheduler(nil, nil, 3)
	sched.BaseBackoff = 100 * time.Millisecond
	sched.MaxBackoff = 5 * time.Second

	tests := []struct {
		attempts int
		expected time.Duration
	}{
		{attempts: 1, expected: 100 * time.Millisecond},
		{attempts: 2, expected: 200 * time.Millisecond},
		{attempts: 3, expected: 400 * time.Millisecond},
		{attempts: 4, expected: 800 * time.Millisecond},
		{attempts: 10, expected: 5 * time.Second}, // Capped at MaxBackoff
	}

	for _, tt := range tests {
		got := sched.CalculateBackoff(tt.attempts)
		if got != tt.expected {
			t.Errorf("CalculateBackoff(%d) = %v, expected %v", tt.attempts, got, tt.expected)
		}
	}
}

func TestScheduler_CompletionForwarderNonBlocking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Unbuffered output channel with NO active reader
	unbufferedOut := make(chan WorkerResult)

	fwd := newCompletionForwarder(ctx, unbufferedOut)

	pushDone := make(chan struct{})
	go func() {
		// Push 50 results to completion forwarder
		for i := 0; i < 50; i++ {
			fwd.Push(WorkerResult{
				Task: ChunkTask{Index: i, ChunkID: core.ChunkID{byte(i)}},
			})
		}
		close(pushDone)
	}()

	select {
	case <-pushDone:
		// Push completed without blocking on unbufferedOut
	case <-time.After(2 * time.Second):
		t.Fatalf("completionForwarder.Push blocked on unbuffered completion channel")
	}

	// Now read all 50 results from unbufferedOut
	for i := 0; i < 50; i++ {
		select {
		case res := <-unbufferedOut:
			if res.Task.Index != i {
				t.Fatalf("expected task index %d, got %d", i, res.Task.Index)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out reading expected result %d from completion channel", i)
		}
	}

	fwd.Close()
}
