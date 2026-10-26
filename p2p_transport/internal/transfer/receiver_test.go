package transfer

import (
	"testing"
)

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Standard filename",
			input:    "report.pdf",
			expected: "report.pdf",
		},
		{
			name:     "Filename with path",
			input:    "docs/sub/report.pdf",
			expected: "report.pdf",
		},
		{
			name:     "Relative path traversal",
			input:    "../../etc/passwd",
			expected: "passwd",
		},
		{
			name:     "Absolute path traversal",
			input:    "/etc/passwd",
			expected: "passwd",
		},
		{
			name:     "Windows path traversal",
			input:    "..\\..\\Windows\\System32\\cmd.exe",
			expected: "cmd.exe",
		},
		{
			name:     "Empty filename",
			input:    "",
			expected: defaultFilename,
		},
		{
			name:     "Whitespace filename",
			input:    "   ",
			expected: defaultFilename,
		},
		{
			name:     "Single dot filename",
			input:    ".",
			expected: defaultFilename,
		},
		{
			name:     "Double dot filename",
			input:    "..",
			expected: defaultFilename,
		},
		{
			name:     "Root directory",
			input:    "/",
			expected: defaultFilename,
		},
		{
			name:     "Multiple slashes",
			input:    "///",
			expected: defaultFilename,
		},
		{
			name:     "Parent resolution resulting in dot",
			input:    "foo/..",
			expected: defaultFilename,
		},
		{
			name:     "Parent resolution resulting in valid folder base",
			input:    "foo/bar/..",
			expected: "foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeFilename(tt.input)
			if got != tt.expected {
				t.Errorf("sanitizeFilename(%q) = %q; want %q", tt.input, got, tt.expected)
			}
		})
	}
}
