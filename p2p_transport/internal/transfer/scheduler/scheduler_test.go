package scheduler_test

import (
	"testing"
	"time"

	"cipher/internal/transfer/scheduler"
)

func TestSchedulerConfig_Option(t *testing.T) {
	s := scheduler.NewScheduler(nil, nil, 3, scheduler.WithThrottle(10*time.Millisecond))
	if s.Config.Throttle != 10*time.Millisecond {
		t.Fatalf("Expected throttle 10ms, got %v", s.Config.Throttle)
	}
}

func TestWorkerConfig_InstanceIsolation(t *testing.T) {
	w1 := scheduler.NewWorker(scheduler.Source{}, nil, nil, nil, scheduler.WorkerConfig{Throttle: 5 * time.Millisecond})
	w2 := scheduler.NewWorker(scheduler.Source{}, nil, nil, nil, scheduler.WorkerConfig{Throttle: 20 * time.Millisecond})

	if w1.Config.Throttle != 5*time.Millisecond {
		t.Errorf("w1 throttle mismatch: got %v, want 5ms", w1.Config.Throttle)
	}
	if w2.Config.Throttle != 20*time.Millisecond {
		t.Errorf("w2 throttle mismatch: got %v, want 20ms", w2.Config.Throttle)
	}
}
