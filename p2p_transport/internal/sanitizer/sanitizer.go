package sanitizer

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Sanitize converts control characters, newlines, and unprintable bytes into safe escaped representations.
// It preserves readable diagnostic details while neutralizing line-break and log-formatting injection risks.
func Sanitize(s string) string {
	var sb strings.Builder
	sb.Grow(len(s))

	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			// Invalid UTF-8 byte
			fmt.Fprintf(&sb, "\\x%02x", s[i])
			i += size
			continue
		}

		switch r {
		case '\n':
			sb.WriteString("\\n")
		case '\r':
			sb.WriteString("\\r")
		case '\t':
			sb.WriteString("\\t")
		case '\\':
			sb.WriteString("\\\\")
		default:
			if unicode.IsPrint(r) {
				sb.WriteRune(r)
			} else if r <= 0xFF {
				fmt.Fprintf(&sb, "\\x%02x", r)
			} else {
				fmt.Fprintf(&sb, "\\u%04x", r)
			}
		}
		i += size
	}

	return sb.String()
}

// SanitizeBytes is a convenience wrapper around Sanitize for byte slice payloads.
func SanitizeBytes(b []byte) string {
	return Sanitize(string(b))
}
