package chunk

import (
	"testing"
)

func TestStreamHandler_Throttling(t *testing.T) {
	handler := &StreamHandler{
		MaxErrorLogs: 2,
	}

	count := 0
	// First two should return true
	if !handler.shouldLogError(&count) {
		t.Errorf("expected 1st error log to be allowed")
	}
	if count != 1 {
		t.Errorf("expected count=1, got %d", count)
	}

	if !handler.shouldLogError(&count) {
		t.Errorf("expected 2nd error log to be allowed")
	}
	if count != 2 {
		t.Errorf("expected count=2, got %d", count)
	}

	// Subsequent calls beyond threshold (2) should return false
	for i := 3; i <= 10; i++ {
		if handler.shouldLogError(&count) {
			t.Errorf("call %d: expected error log to be throttled/suppressed", i)
		}
		if count != 2 {
			t.Errorf("count should remain capped at 2, got %d", count)
		}
	}
}

func TestStreamHandler_DefaultMaxErrorLogs(t *testing.T) {
	handler := &StreamHandler{
		MaxErrorLogs: 0, // Should default to DefaultMaxErrorLogsPerStream (5)
	}

	count := 0
	for i := 1; i <= DefaultMaxErrorLogsPerStream; i++ {
		if !handler.shouldLogError(&count) {
			t.Errorf("call %d: expected error log to be allowed under default limit (%d)", i, DefaultMaxErrorLogsPerStream)
		}
	}

	// 6th call should be throttled
	if handler.shouldLogError(&count) {
		t.Errorf("expected error log beyond default limit to be throttled")
	}
}
