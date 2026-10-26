package scheduler_test

import (
	"testing"
	"time"

	"cipher/internal/transfer/scheduler"
)

func TestSchedulerOptions(t *testing.T) {
	s1 := scheduler.NewScheduler(nil, nil, 3, scheduler.WithThrottle(100*time.Millisecond))
	if s1.Throttle != 100*time.Millisecond {
		t.Fatalf("Expected Throttle 100ms, got %v", s1.Throttle)
	}

	s2 := scheduler.NewScheduler(nil, nil, 3, scheduler.WithWorkerConfig(scheduler.WorkerConfig{Throttle: 200 * time.Millisecond}))
	if s2.Throttle != 200*time.Millisecond {
		t.Fatalf("Expected Throttle 200ms, got %v", s2.Throttle)
	}

	s3 := scheduler.NewScheduler(nil, nil, 3, scheduler.WithSchedulerConfig(scheduler.SchedulerConfig{Throttle: 300 * time.Millisecond}))
	if s3.Throttle != 300*time.Millisecond {
		t.Fatalf("Expected Throttle 300ms, got %v", s3.Throttle)
	}
}

func TestChunkQueue(t *testing.T) {
	tasks := []scheduler.ChunkTask{
		{Index: 0},
		{Index: 1},
	}
	queue := scheduler.NewChunkQueue(tasks)

	t1, ok1 := queue.Next()
	if !ok1 || t1.Index != 0 {
		t.Fatalf("Expected task index 0")
	}

	t2, ok2 := queue.Next()
	if !ok2 || t2.Index != 1 {
		t.Fatalf("Expected task index 1")
	}

	_, ok3 := queue.Next()
	if ok3 {
		t.Fatalf("Expected queue to be empty")
	}
}
