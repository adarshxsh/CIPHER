package scheduler

import (
	"testing"
	"time"
)

func TestCalculateBackoff(t *testing.T) {
	minB := 100 * time.Millisecond
	maxB := 5 * time.Second

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1600 * time.Millisecond},
		{6, 3200 * time.Millisecond},
		{7, 5 * time.Second}, // Capped at maxB (6400ms > 5000ms)
		{10, 5 * time.Second},
		{100, 5 * time.Second},
	}

	for _, tt := range tests {
		got := CalculateBackoff(tt.attempt, minB, maxB)
		if got != tt.expected {
			t.Errorf("CalculateBackoff(attempt=%d): expected %v, got %v", tt.attempt, tt.expected, got)
		}
	}
}

func TestNewSchedulerDefaults(t *testing.T) {
	s := NewScheduler(nil, nil, 0)
	if s.MaxAttempts != 10 {
		t.Errorf("expected default MaxAttempts=10, got %d", s.MaxAttempts)
	}
	if s.MinBackoff != 100*time.Millisecond {
		t.Errorf("expected default MinBackoff=100ms, got %v", s.MinBackoff)
	}
	if s.MaxBackoff != 5*time.Second {
		t.Errorf("expected default MaxBackoff=5s, got %v", s.MaxBackoff)
	}
	if s.MaxQueueCap != DefaultMaxQueueCapacity {
		t.Errorf("expected default MaxQueueCap=10000, got %d", s.MaxQueueCap)
	}
}
