package chunk_test

import (
	"bytes"
	"context"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
)

func TestSanitizeErrorMessage(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "clean string",
			input:    "normal error message",
			expected: "normal error message",
		},
		{
			name:     "newlines converted to spaces",
			input:    "line 1\r\nline 2\nline 3\rline 4",
			expected: "line 1 line 2 line 3 line 4",
		},
		{
			name:     "control characters stripped",
			input:    "hello\x00world\x01\x07\x1f\x7f",
			expected: "helloworld",
		},
		{
			name:     "ANSI escape sequences stripped",
			input:    "\x1b[31mRed Error\x1b[0m and \x1b[1;32mGreen\x1b[0m",
			expected: "Red Error and Green",
		},
		{
			name:     "log injection attempt",
			input:    "Error occurred\n[INFO] 2026-09-11 Admin logged in\r\n[WARN] Password reset",
			expected: "Error occurred [INFO] 2026-09-11 Admin logged in [WARN] Password reset",
		},
		{
			name:     "truncation at 256 bytes",
			input:    strings.Repeat("A", 300),
			expected: strings.Repeat("A", 256),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := chunk.SanitizeErrorMessage(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeErrorMessage() = %q, expected %q", got, tc.expected)
			}
			if len(got) > 256 {
				t.Errorf("SanitizeErrorMessage() length = %d, expected <= 256", len(got))
			}
		})
	}
}

func TestParseError_Sanitization(t *testing.T) {
	msg := "\x1b[31mCritical Failure\r\nSystem compromised\x00\x1f"
	payload := append([]byte{byte(chunk.ErrInternal)}, []byte(msg)...)

	code, str, err := chunk.ParseError(payload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}

	if code != chunk.ErrInternal {
		t.Errorf("expected ErrorCode %v, got %v", chunk.ErrInternal, code)
	}

	expectedStr := "Critical Failure System compromised"
	if str != expectedStr {
		t.Errorf("ParseError() string = %q, expected %q", str, expectedStr)
	}
}

func TestParseError_LengthCap(t *testing.T) {
	longMsg := strings.Repeat("X", 300)
	payload := append([]byte{byte(chunk.ErrBadRequest)}, []byte(longMsg)...)

	_, str, err := chunk.ParseError(payload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}

	if len(str) != 256 {
		t.Errorf("expected string length 256, got %d", len(str))
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *safeBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestStreamHandler_RateLimiting(t *testing.T) {
	// Capture standard log output safely
	var logBuf safeBuffer
	origFlags := log.Flags()
	origOutput := log.Writer()
	log.SetFlags(0)
	log.SetOutput(&logBuf)
	defer func() {
		log.SetFlags(origFlags)
		log.SetOutput(origOutput)
	}()

	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)

	// Setup StreamHandler on h1
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}

	// Send 10 invalid message types over the stream in rapid succession
	for i := 0; i < 10; i++ {
		msg := &chunk.Message{
			Version: chunk.CurrentMessageVersion,
			Type:    0x99, // Unknown type to trigger error log
			Payload: []byte{},
		}
		if err := chunk.WriteMessage(s, msg); err != nil {
			t.Fatalf("WriteMessage failed on message %d: %v", i, err)
		}

		resp, err := chunk.ReadMessage(s)
		if err != nil {
			t.Fatalf("ReadMessage failed on message %d: %v", i, err)
		}
		if resp.Type != chunk.MsgError {
			t.Fatalf("Expected MsgError response, got type %d", resp.Type)
		}
	}

	s.Close()
	// Allow handler goroutine time to complete and flush summary
	time.Sleep(50 * time.Millisecond)

	logs := logBuf.String()

	// Count occurrences of "Unsupported message type: 153"
	unsupportedCount := strings.Count(logs, "Unsupported message type: 153")
	if unsupportedCount > 5 {
		t.Errorf("Expected at most 5 unsupported message logs, got %d. Logs:\n%s", unsupportedCount, logs)
	}

	// Verify that a summary notice for suppressed messages was logged
	if !strings.Contains(logs, "suppressed 5 log messages") && !strings.Contains(logs, "Rate limit reset") {
		t.Errorf("Expected log summary notice for suppressed messages, got:\n%s", logs)
	}
}
