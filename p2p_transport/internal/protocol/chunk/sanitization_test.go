package chunk_test

import (
	"strings"
	"testing"

	"cipher/internal/protocol/chunk"
)

func TestParseError_Sanitization(t *testing.T) {
	tests := []struct {
		name     string
		code     chunk.ErrorCode
		rawMsg   string
		contains string
		excludes []string
	}{
		{
			name:     "Newlines and Carriage Returns",
			code:     chunk.ErrBadRequest,
			rawMsg:   "line1\nline2\r\nline3",
			contains: "line1\\nline2\\r\\nline3",
			excludes: []string{"\n", "\r"},
		},
		{
			name:     "ANSI Control Sequences",
			code:     chunk.ErrInternal,
			rawMsg:   "\x1b[31mError:\x1b[0m System failure",
			contains: "\\x1b[31mError:\\x1b[0m System failure",
			excludes: []string{"\x1b"},
		},
		{
			name:     "Null Bytes and ASCII Control Characters",
			code:     chunk.ErrPermissionDenied,
			rawMsg:   "Access\x00Denied\x07\x08",
			contains: "Access\\x00Denied\\x07\\x08",
			excludes: []string{"\x00", "\x07", "\x08"},
		},
		{
			name:     "Invalid UTF-8 and Non-printable Bytes",
			code:     chunk.ErrChunkNotFound,
			rawMsg:   "Corrupted byte \x80\xff here",
			contains: "Corrupted byte \\x80\\xff here",
			excludes: []string{"\x80", "\xff"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := append([]byte{byte(tt.code)}, []byte(tt.rawMsg)...)
			code, parsedMsg, err := chunk.ParseError(payload)
			if err != nil {
				t.Fatalf("ParseError returned unexpected error: %v", err)
			}

			if code != tt.code {
				t.Errorf("Expected error code %d, got %d", tt.code, code)
			}

			if !strings.Contains(parsedMsg, tt.contains) {
				t.Errorf("Expected parsed string to contain %q, got %q", tt.contains, parsedMsg)
			}

			for _, excl := range tt.excludes {
				if strings.Contains(parsedMsg, excl) {
					t.Errorf("Parsed string %q still contains forbidden substring %q", parsedMsg, excl)
				}
			}
		})
	}
}

func TestParseError_Truncation(t *testing.T) {
	t.Run("Message exceeding 256 bytes", func(t *testing.T) {
		longMsg := strings.Repeat("A", 300)
		payload := append([]byte{byte(chunk.ErrInternal)}, []byte(longMsg)...)

		code, parsedMsg, err := chunk.ParseError(payload)
		if err != nil {
			t.Fatalf("ParseError returned unexpected error: %v", err)
		}

		if code != chunk.ErrInternal {
			t.Errorf("Expected error code %d, got %d", chunk.ErrInternal, code)
		}

		if !strings.HasSuffix(parsedMsg, "...") {
			t.Errorf("Expected truncated string to end with visual marker '...', got %q", parsedMsg)
		}

		// Length excluding marker should be capped at 256
		baseMsg := strings.TrimSuffix(parsedMsg, "...")
		if len(baseMsg) > 256 {
			t.Errorf("Expected base truncated message length <= 256, got %d", len(baseMsg))
		}
	})

	t.Run("Message within 256 bytes", func(t *testing.T) {
		shortMsg := strings.Repeat("B", 100)
		payload := append([]byte{byte(chunk.ErrInternal)}, []byte(shortMsg)...)

		_, parsedMsg, err := chunk.ParseError(payload)
		if err != nil {
			t.Fatalf("ParseError returned unexpected error: %v", err)
		}

		if strings.HasSuffix(parsedMsg, "...") {
			t.Errorf("Untruncated message should not end with truncation marker '...'")
		}

		if parsedMsg != shortMsg {
			t.Errorf("Expected %q, got %q", shortMsg, parsedMsg)
		}
	})
}

func TestParseError_InvalidPayload(t *testing.T) {
	_, _, err := chunk.ParseError([]byte{})
	if err == nil {
		t.Error("Expected error for empty payload, got nil")
	}
}
