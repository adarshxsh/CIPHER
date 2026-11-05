package scheduler

import (
	"testing"
	"time"
)

func TestScheduler_CalculateBackoff(t *testing.T) {
	s := NewScheduler(nil, nil, 3)
	s.InitialBackoff = 100 * time.Millisecond
	s.BackoffFactor = 2.0
	s.MaxBackoff = 500 * time.Millisecond

	// Attempt 1: 100ms
	d1 := s.calculateBackoff(1)
	if d1 != 100*time.Millisecond {
		t.Fatalf("expected 100ms, got %v", d1)
	}

	// Attempt 2: 200ms
	d2 := s.calculateBackoff(2)
	if d2 != 200*time.Millisecond {
		t.Fatalf("expected 200ms, got %v", d2)
	}

	// Attempt 3: 400ms
	d3 := s.calculateBackoff(3)
	if d3 != 400*time.Millisecond {
		t.Fatalf("expected 400ms, got %v", d3)
	}

	// Attempt 4: capped at MaxBackoff (500ms)
	d4 := s.calculateBackoff(4)
	if d4 != 500*time.Millisecond {
		t.Fatalf("expected 500ms, got %v", d4)
	}
}
