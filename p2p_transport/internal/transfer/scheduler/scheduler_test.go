package scheduler_test

import (
	"sync"
	"testing"
	"time"

	"cipher/internal/transfer/scheduler"
)

func TestSchedulerOptions_ThreadSafety(t *testing.T) {
	sched := scheduler.NewScheduler(nil, nil, 3, scheduler.WithThrottle(10*time.Millisecond))

	if sched.Throttle() != 10*time.Millisecond {
		t.Fatalf("Expected throttle 10ms, got %v", sched.Throttle())
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(2)
		go func(d time.Duration) {
			defer wg.Done()
			sched.SetThrottle(d)
		}(time.Duration(i) * time.Millisecond)

		go func() {
			defer wg.Done()
			_ = sched.Throttle()
		}()
	}
	wg.Wait()
}
