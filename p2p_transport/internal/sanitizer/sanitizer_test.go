package sanitizer_test

import (
	"testing"

	"cipher/internal/sanitizer"
)

func TestSanitize(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Normal printable string",
			input:    "All systems operational: chunk 0x1234",
			expected: "All systems operational: chunk 0x1234",
		},
		{
			name:     "Newline injection",
			input:    "Error occurred\n[ADMIN] Granted superuser privileges",
			expected: "Error occurred\\n[ADMIN] Granted superuser privileges",
		},
		{
			name:     "Carriage return and line feed injection",
			input:    "Line 1\r\nLine 2",
			expected: "Line 1\\r\\nLine 2",
		},
		{
			name:     "Tab character",
			input:    "Column1\tColumn2",
			expected: "Column1\\tColumn2",
		},
		{
			name:     "Backslash escaping",
			input:    "Path C:\\Program Files\\App",
			expected: "Path C:\\\\Program Files\\\\App",
		},
		{
			name:     "Terminal ANSI escape codes",
			input:    "\x1b[31mCRITICAL ERROR\x1b[0m",
			expected: "\\x1b[31mCRITICAL ERROR\\x1b[0m",
		},
		{
			name:     "Null byte",
			input:    "Message\x00Truncated",
			expected: "Message\\x00Truncated",
		},
		{
			name:     "Invalid UTF-8 sequence",
			input:    "Bad byte: \xff\xfe",
			expected: "Bad byte: \\xff\\xfe",
		},
		{
			name:     "Non-printable unicode rune",
			input:    "Zero width space: \u200b",
			expected: "Zero width space: \\u200b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizer.Sanitize(tt.input)
			if got != tt.expected {
				t.Errorf("Sanitize(%q) = %q; expected %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestSanitizeBytes(t *testing.T) {
	input := []byte("Error\nPayload")
	expected := "Error\\nPayload"
	got := sanitizer.SanitizeBytes(input)
	if got != expected {
		t.Errorf("SanitizeBytes(%q) = %q; expected %q", input, got, expected)
	}
}
