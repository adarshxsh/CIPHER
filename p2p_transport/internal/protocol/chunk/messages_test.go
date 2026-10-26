package chunk_test

import (
	"bytes"
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

func TestParseError_Sanitization(t *testing.T) {
	tests := []struct {
		name     string
		rawMsg   string
		expected string
	}{
		{
			name:     "clean message",
			rawMsg:   "file not found",
			expected: "file not found",
		},
		{
			name:     "newlines and tabs converted to spaces",
			rawMsg:   "line1\nline2\r\nline3\ttab",
			expected: "line1 line2  line3 tab",
		},
		{
			name:     "ANSI escape sequences stripped",
			rawMsg:   "\x1b[31mRed Alert\x1b[0m",
			expected: "Red Alert",
		},
		{
			name:     "control characters stripped",
			rawMsg:   "bad\x00data\x07\x08here",
			expected: "baddatahere",
		},
		{
			name:     "invalid UTF-8 bytes stripped",
			rawMsg:   "hello\xff\xfe\xfdworld",
			expected: "helloworld",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := append([]byte{byte(chunk.ErrBadRequest)}, []byte(tt.rawMsg)...)
			code, parsedMsg, err := chunk.ParseError(payload)
			if err != nil {
				t.Fatalf("ParseError failed: %v", err)
			}
			if code != chunk.ErrBadRequest {
				t.Errorf("expected code %d, got %d", chunk.ErrBadRequest, code)
			}
			if parsedMsg != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, parsedMsg)
			}
		})
	}
}

func TestParseError_TruncatesOversizedMessage(t *testing.T) {
	oversized := make([]byte, 1000)
	for i := range oversized {
		oversized[i] = 'A'
	}
	payload := append([]byte{byte(chunk.ErrInternal)}, oversized...)
	_, parsedMsg, err := chunk.ParseError(payload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}
	if len(parsedMsg) != chunk.MaxErrorMessageSize {
		t.Errorf("expected sanitized message length %d, got %d", chunk.MaxErrorMessageSize, len(parsedMsg))
	}
}
