package chunk_test

import (
	"bytes"
	"strings"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/protocol/chunk"
)

func TestMessageEnvelope_Serialization(t *testing.T) {
	// 1. Build a message
	var contentID core.ContentID
	contentID[0] = 0xAA
	contentID[31] = 0xBB

	msg := chunk.BuildRequestManifest(contentID)

	// 2. Write it
	var buf bytes.Buffer
	if err := chunk.WriteMessage(&buf, msg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	// 3. Read it
	parsedMsg, err := chunk.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}

	if parsedMsg.Version != chunk.CurrentMessageVersion {
		t.Errorf("expected version %d, got %d", chunk.CurrentMessageVersion, parsedMsg.Version)
	}
	if parsedMsg.Type != chunk.MsgRequestManifest {
		t.Errorf("expected type %d, got %d", chunk.MsgRequestManifest, parsedMsg.Type)
	}

	// 4. Parse payload
	parsedID, err := chunk.ParseRequestManifest(parsedMsg.Payload)
	if err != nil {
		t.Fatalf("ParseRequestManifest failed: %v", err)
	}
	if parsedID != contentID {
		t.Errorf("expected contentID %x, got %x", contentID, parsedID)
	}
}

func TestProtocolCompatibility_OldDecoder(t *testing.T) {
	// A new version comes in, we read it
	msg := &chunk.Message{
		Version: 2, // Newer version
		Type:    chunk.MsgRequestManifest,
		Payload: []byte("something"),
	}

	var buf bytes.Buffer
	if err := chunk.WriteMessage(&buf, msg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}

	// When reading, we could theoretically reject it inside ReadMessage if we strictly check version.
	// We didn't enforce it in ReadMessage yet, let's enforce it in the handler/application logic, 
	// or we can add it to ReadMessage. For now, let's just make sure we can parse the envelope and 
	// the application handler can reject `msg.Version != CurrentMessageVersion`.
	parsedMsg, err := chunk.ReadMessage(&buf)
	if err != nil {
		t.Fatalf("ReadMessage failed: %v", err)
	}
	if parsedMsg.Version != 2 {
		t.Errorf("Expected parsed version to remain intact")
	}
}

func TestProtocolCompatibility_MalformedMessage(t *testing.T) {
	// Empty payload for a REQUEST_MANIFEST
	msg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    chunk.MsgRequestManifest,
		Payload: []byte{0x00}, // Too short!
	}
	var buf bytes.Buffer
	chunk.WriteMessage(&buf, msg)

	parsedMsg, _ := chunk.ReadMessage(&buf)
	
	// Payload parser should reject it
	_, err := chunk.ParseRequestManifest(parsedMsg.Payload)
	if err == nil {
		t.Error("Expected error parsing malformed REQUEST_MANIFEST, got nil")
	}
}

func TestProtocolCompatibility_UnsupportedMessage(t *testing.T) {
	msg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    0x99, // Unknown type
		Payload: []byte{},
	}
	var buf bytes.Buffer
	chunk.WriteMessage(&buf, msg)

	parsedMsg, _ := chunk.ReadMessage(&buf)
	if parsedMsg.Type != 0x99 {
		t.Errorf("Expected type 0x99, got %v", parsedMsg.Type)
	}
	// Handler test will ensure it replies with ERR_UNSUPPORTED_MESSAGE
}

func TestSanitizeErrorMessage_ControlCharacters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "newlines and carriage returns",
			input:    "line1\nline2\rline3\r\nline4",
			expected: "line1\\nline2\\rline3\\r\\nline4",
		},
		{
			name:     "tabs",
			input:    "col1\tcol2",
			expected: "col1\\tcol2",
		},
		{
			name:     "ANSI escape sequences",
			input:    "\x1b[31mError message\x1b[0m",
			expected: "\\x1b[31mError message\\x1b[0m",
		},
		{
			name:     "multiline log forgery attack payload",
			input:    "Failed\n[INFO] User logged in as admin\r\n",
			expected: "Failed\\n[INFO] User logged in as admin\\r\\n",
		},
		{
			name:     "null bytes and non-printable control chars",
			input:    "err\x00\x07\x1f\x7f",
			expected: "err\\x00\\a\\x1f\\x7f",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := chunk.SanitizeErrorMessage(tc.input)
			if got != tc.expected {
				t.Errorf("SanitizeErrorMessage(%q):\n got  %q\n want %q", tc.input, got, tc.expected)
			}
			if strings.Contains(got, "\n") || strings.Contains(got, "\r") {
				t.Errorf("SanitizeErrorMessage output contains newline or carriage return: %q", got)
			}
		})
	}
}

func TestSanitizeErrorMessage_LengthTruncation(t *testing.T) {
	longMsg := strings.Repeat("A", 600)
	got := chunk.SanitizeErrorMessage(longMsg)

	if len(got) > chunk.MaxErrorMessageSize {
		t.Errorf("Expected max length %d, got length %d", chunk.MaxErrorMessageSize, len(got))
	}
	if got != strings.Repeat("A", chunk.MaxErrorMessageSize) {
		t.Errorf("Truncated string mismatched")
	}
}

func TestParseError_SanitizationAndTruncation(t *testing.T) {
	forgedPayload := append([]byte{byte(chunk.ErrBadRequest)}, []byte("Error:\n[FAKE LOG] Admin session granted\r\n")...)

	code, msg, err := chunk.ParseError(forgedPayload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}

	if code != chunk.ErrBadRequest {
		t.Errorf("expected code %d, got %d", chunk.ErrBadRequest, code)
	}

	if strings.Contains(msg, "\n") || strings.Contains(msg, "\r") {
		t.Errorf("Parsed error message contains raw newlines/carriage returns: %q", msg)
	}

	if msg != "Error:\\n[FAKE LOG] Admin session granted\\r\\n" {
		t.Errorf("Unexpected sanitized message: %q", msg)
	}
}

