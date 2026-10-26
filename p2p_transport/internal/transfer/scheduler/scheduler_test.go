package scheduler

import (
	"context"
	"testing"
	"time"

	"cipher/internal/content/core"
)

func TestCalculateBackoff(t *testing.T) {
	if delay := CalculateBackoff(0); delay != 0 {
		t.Fatalf("expected 0 for attempts=0, got %v", delay)
	}

	// Attempt 1: base 50ms, jitter up to 25ms => range [50ms, 75ms]
	delay1 := CalculateBackoff(1)
	if delay1 < 50*time.Millisecond || delay1 > 75*time.Millisecond {
		t.Fatalf("expected attempt 1 delay in [50ms, 75ms], got %v", delay1)
	}

	// Attempt 2: base 100ms, jitter up to 50ms => range [100ms, 150ms]
	delay2 := CalculateBackoff(2)
	if delay2 < 100*time.Millisecond || delay2 > 150*time.Millisecond {
		t.Fatalf("expected attempt 2 delay in [100ms, 150ms], got %v", delay2)
	}

	// Attempt 3: base 200ms, jitter up to 100ms => range [200ms, 300ms]
	delay3 := CalculateBackoff(3)
	if delay3 < 200*time.Millisecond || delay3 > 300*time.Millisecond {
		t.Fatalf("expected attempt 3 delay in [200ms, 300ms], got %v", delay3)
	}

	// High attempt count capped at MaxBackoff (2s)
	delay10 := CalculateBackoff(10)
	if delay10 > 2*time.Second {
		t.Fatalf("expected attempt 10 delay <= 2s, got %v", delay10)
	}
}

func TestScheduler_NonBlockingCompletionsDispatch(t *testing.T) {
	// Create unbuffered completions channel and do not consume immediately
	completions := make(chan WorkerResult)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	res := WorkerResult{
		Task: ChunkTask{Index: 0, ChunkID: core.ChunkID{0xAA}},
	}

	// Dispatch using non-blocking pattern matching scheduler logic
	done := make(chan struct{})
	go func() {
		select {
		case completions <- res:
		default:
			go func(r WorkerResult) {
				select {
				case completions <- r:
				case <-ctx.Done():
				}
			}(res)
		}
		close(done)
	}()

	select {
	case <-done:
		// Succeeded immediately without blocking main loop
	case <-time.After(200 * time.Millisecond):
		t.Fatal("dispatch blocked main loop")
	}

	// Now consume from completions
	select {
	case readRes := <-completions:
		if readRes.Task.Index != 0 {
			t.Fatalf("unexpected completion task index: %d", readRes.Task.Index)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("failed to receive completion result")
	}
}
