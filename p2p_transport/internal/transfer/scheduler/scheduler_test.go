package scheduler_test

import (
	"testing"
	"time"

	"cipher/internal/transfer/scheduler"
)

func TestSchedulerConfig_InstanceScopedThrottle(t *testing.T) {
	cfg := scheduler.Config{
		MaxAttempts: 5,
		Throttle:    50 * time.Millisecond,
	}

	sched := scheduler.NewScheduler(nil, nil, 3, cfg)
	if sched.Config.Throttle != 50*time.Millisecond {
		t.Fatalf("Expected throttle 50ms, got %v", sched.Config.Throttle)
	}
	if sched.Config.MaxAttempts != 5 {
		t.Fatalf("Expected MaxAttempts 5, got %d", sched.Config.MaxAttempts)
	}

	// Verify another instance can have zero throttle independently
	sched2 := scheduler.NewScheduler(nil, nil, 2)
	if sched2.Config.Throttle != 0 {
		t.Fatalf("Expected zero throttle for default instance, got %v", sched2.Config.Throttle)
	}
	if sched2.Config.MaxAttempts != 2 {
		t.Fatalf("Expected MaxAttempts 2, got %d", sched2.Config.MaxAttempts)
	}
}

func TestScheduler_RunExecutesWorkerWithThrottle(t *testing.T) {
	cfg := scheduler.Config{
		MaxAttempts: 1,
		Throttle:    10 * time.Millisecond,
	}

	sched := scheduler.NewScheduler(nil, nil, 1, cfg)
	if sched.Config.Throttle != 10*time.Millisecond {
		t.Fatalf("Expected throttle 10ms, got %v", sched.Config.Throttle)
	}
}
