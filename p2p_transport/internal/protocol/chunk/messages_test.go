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

func TestParseError_ValidationAndSanitization(t *testing.T) {
	// 1. Empty payload
	_, _, err := chunk.ParseError([]byte{})
	if err == nil {
		t.Error("expected error for empty payload, got nil")
	}

	// 2. Unknown error code
	_, _, err = chunk.ParseError([]byte{0xFF, 't', 'e', 's', 't'})
	if err == nil {
		t.Error("expected error for unknown error code, got nil")
	}

	// 3. Oversized payload (> 512 bytes message)
	oversized := make([]byte, chunk.MaxErrorMessageSize+2)
	oversized[0] = byte(chunk.ErrBadRequest)
	for i := 1; i < len(oversized); i++ {
		oversized[i] = 'a'
	}
	_, _, err = chunk.ParseError(oversized)
	if err == nil {
		t.Error("expected error for oversized error payload, got nil")
	}

	// 4. Control characters and newlines in payload
	rawPayload := append([]byte{byte(chunk.ErrBadRequest)}, []byte("Error:\nLine 2\r\n\tTab\x00Null\x1b[31mRed")...)
	code, msg, err := chunk.ParseError(rawPayload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}
	if code != chunk.ErrBadRequest {
		t.Errorf("expected code %v, got %v", chunk.ErrBadRequest, code)
	}
	expected := `Error:\nLine 2\r\n\tTab\x00Null\x1b[31mRed`
	if msg != expected {
		t.Errorf("expected sanitized msg %q, got %q", expected, msg)
	}
}

func TestSanitizeErrorMessage(t *testing.T) {
	input := "Hello\nWorld\r\nFoo\tBar\x00\x1b[1m"
	sanitized := chunk.SanitizeErrorMessage(input)
	expected := `Hello\nWorld\r\nFoo\tBar\x00\x1b[1m`
	if sanitized != expected {
		t.Errorf("expected %q, got %q", expected, sanitized)
	}

	// Test truncation if > MaxErrorMessageSize
	longMsg := make([]byte, 600)
	for i := range longMsg {
		longMsg[i] = 'X'
	}
	sanitizedLong := chunk.SanitizeErrorMessage(string(longMsg))
	if len(sanitizedLong) != chunk.MaxErrorMessageSize {
		t.Errorf("expected length %d, got %d", chunk.MaxErrorMessageSize, len(sanitizedLong))
	}
}
