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

func TestSanitizeErrorString_ControlCharactersAndNewlines(t *testing.T) {
	input := "line1\nline2\rline3\t\x00\x07end"
	expected := "line1line2line3end"
	sanitized := chunk.SanitizeErrorString(input)
	if sanitized != expected {
		t.Errorf("expected %q, got %q", expected, sanitized)
	}
}

func TestSanitizeErrorString_ANSIEscapeSequences(t *testing.T) {
	input := "\x1b[31mCRITICAL ERROR\x1b[0m: \x1b[1;32mSystem Failure\x1b[0m"
	expected := "CRITICAL ERROR: System Failure"
	sanitized := chunk.SanitizeErrorString(input)
	if sanitized != expected {
		t.Errorf("expected %q, got %q", expected, sanitized)
	}
}

func TestSanitizeErrorString_Truncation(t *testing.T) {
	// Long message > MaxErrorLogLen (128)
	longMsg := ""
	for i := 0; i < 150; i++ {
		longMsg += "a"
	}
	sanitized := chunk.SanitizeErrorString(longMsg)

	if len([]rune(sanitized)) != chunk.MaxErrorLogLen {
		t.Errorf("expected sanitized length %d, got %d", chunk.MaxErrorLogLen, len([]rune(sanitized)))
	}
	if sanitized[len(sanitized)-3:] != "..." {
		t.Errorf("expected truncated string to end with '...', got %q", sanitized[len(sanitized)-3:])
	}

	// Exact length 128
	exactMsg := ""
	for i := 0; i < 128; i++ {
		exactMsg += "b"
	}
	sanitizedExact := chunk.SanitizeErrorString(exactMsg)
	if sanitizedExact != exactMsg {
		t.Errorf("expected exact length message to remain unmodified")
	}
}

func TestParseError_SanitizesPayload(t *testing.T) {
	msg := chunk.BuildError(chunk.ErrBadRequest, "bad\nrequest\x1b[31m!")
	code, msgStr, err := chunk.ParseError(msg.Payload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}
	if code != chunk.ErrBadRequest {
		t.Errorf("expected code %v, got %v", chunk.ErrBadRequest, code)
	}
	expectedMsg := "badrequest!"
	if msgStr != expectedMsg {
		t.Errorf("expected sanitized msg %q, got %q", expectedMsg, msgStr)
	}
}
