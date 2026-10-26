package chunk

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"

	"golang.org/x/time/rate"

	"github.com/libp2p/go-libp2p/core/peer"
	"cipher/internal/content/core"
)

func TestSanitizeErrorMessage(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Normal message",
			input:    "invalid chunk hash",
			expected: "invalid chunk hash",
		},
		{
			name:     "ANSI escape sequences stripped",
			input:    "\x1b[31mRED ALERT\x1b[0m",
			expected: "RED ALERT",
		},
		{
			name:     "Log injection attempt with newlines and ANSI",
			input:    "Error\n[CRITICAL] System compromised\r\x1b[2J\x1b[H",
			expected: "Error[CRITICAL] System compromised",
		},
		{
			name:     "Control characters and null bytes",
			input:    "Hello\x00World\x07\x08\x09\x0b\x0c",
			expected: "HelloWorld",
		},
		{
			name:     "Truncated to 256 bytes",
			input:    strings.Repeat("A", 300),
			expected: strings.Repeat("A", 256),
		},
		{
			name:     "ANSI sequences in long message",
			input:    "\x1b[32m" + strings.Repeat("B", 300) + "\x1b[0m",
			expected: strings.Repeat("B", 256),
		},
		{
			name:     "Empty message",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeErrorMessage(tt.input)
			if result != tt.expected {
				t.Errorf("SanitizeErrorMessage(%q) = %q; expected %q", tt.input, result, tt.expected)
			}
			if len(result) > 256 {
				t.Errorf("SanitizeErrorMessage returned string of length %d > 256", len(result))
			}
		})
	}
}

func TestPeerErrorLogger_RateLimitingAndSanitization(t *testing.T) {
	fakePeer := peer.ID("test-peer-id-1234")
	logger := newPeerErrorLogger(fakePeer)

	// Capture log output
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	var chunkID core.ChunkID
	copy(chunkID[:], []byte("01234567890123456789012345678901"))

	// First 10 logs should be allowed (burst = 10)
	for i := 0; i < 10; i++ {
		logger.LogChunkError(chunkID, ErrChunkNotFound, "\x1b[31mError message\x1b[0m\n")
	}

	output := buf.String()
	logLines := strings.Split(strings.TrimSpace(output), "\n")
	if len(logLines) != 10 {
		t.Fatalf("expected 10 log lines for first 10 allowed errors, got %d", len(logLines))
	}

	// Verify ANSI escape sequences and newlines were stripped in all log lines
	for _, line := range logLines {
		if strings.Contains(line, "\x1b") {
			t.Errorf("log line contains ANSI escape code: %q", line)
		}
		if !strings.Contains(line, "Error message") {
			t.Errorf("log line missing sanitized content: %q", line)
		}
	}

	// Next 5 error logs should be throttled (rate limited)
	buf.Reset()
	for i := 0; i < 5; i++ {
		logger.LogChunkError(chunkID, ErrChunkNotFound, "Throttled Error")
	}

	// Output should be empty because messages were dropped without blocking
	if buf.Len() > 0 {
		t.Errorf("expected 0 log entries while rate limited, got: %q", buf.String())
	}

	if logger.droppedCount != 5 {
		t.Errorf("expected droppedCount to be 5, got %d", logger.droppedCount)
	}

	// Call Close() and verify drop count summary is logged
	buf.Reset()
	logger.Close()

	closeOutput := buf.String()
	if !strings.Contains(closeOutput, "Rate limit exceeded: dropped 5 error messages") {
		t.Errorf("expected drop count summary in log on Close(), got: %q", closeOutput)
	}

	if logger.droppedCount != 0 {
		t.Errorf("expected droppedCount to be reset to 0 after Close(), got %d", logger.droppedCount)
	}
}

func TestPeerErrorLogger_FlushesDropCountOnNextAllowed(t *testing.T) {
	fakePeer := peer.ID("test-peer-id-5678")
	logger := newPeerErrorLogger(fakePeer)

	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	var chunkID core.ChunkID

	// Exhaust tokens
	for i := 0; i < 10; i++ {
		logger.LogChunkError(chunkID, ErrInternal, "Error")
	}

	// Artificially simulate dropped count
	logger.droppedCount = 12

	// Replace limiter with an unlimited rate limiter to allow the next call immediately
	logger.limiter = rate.NewLimiter(rate.Inf, 1)
	buf.Reset()

	logger.LogChunkError(chunkID, ErrInternal, "Recovered Error")

	output := buf.String()
	if !strings.Contains(output, "Rate limit exceeded: dropped 12 error messages") {
		t.Errorf("expected log output to report 12 dropped error messages, got: %q", output)
	}
	if !strings.Contains(output, "Recovered Error") {
		t.Errorf("expected log output to contain allowed message, got: %q", output)
	}
	if logger.droppedCount != 0 {
		t.Errorf("expected droppedCount to be reset to 0, got %d", logger.droppedCount)
	}
}
