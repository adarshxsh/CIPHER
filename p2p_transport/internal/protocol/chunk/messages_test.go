package chunk_test

import (
	"bytes"
	"errors"
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

func TestParseError_Valid(t *testing.T) {
	msg := chunk.BuildError(chunk.ErrBadRequest, "invalid request parameter")
	code, msgStr, err := chunk.ParseError(msg.Payload)
	if err != nil {
		t.Fatalf("ParseError failed for valid payload: %v", err)
	}
	if code != chunk.ErrBadRequest {
		t.Errorf("Expected code %v, got %v", chunk.ErrBadRequest, code)
	}
	if msgStr != `"invalid request parameter"` {
		t.Errorf("Expected quoted error message, got %s", msgStr)
	}
}

func TestParseError_MultilineLogInjection(t *testing.T) {
	rawPayload := append([]byte{byte(chunk.ErrInternal)}, []byte("error\n[INJECTED] fake log entry\r\n\tadmin access granted")...)
	code, msgStr, err := chunk.ParseError(rawPayload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}
	if code != chunk.ErrInternal {
		t.Errorf("Expected code %v, got %v", chunk.ErrInternal, code)
	}
	if strings.Contains(msgStr, "\n") || strings.Contains(msgStr, "\r") {
		t.Errorf("Sanitized error message contains unescaped newline/carriage return: %s", msgStr)
	}
	if !strings.Contains(msgStr, "\\n") || !strings.Contains(msgStr, "\\r") {
		t.Errorf("Expected escaped newline/carriage return, got: %s", msgStr)
	}
}

func TestParseError_InvalidErrorCode(t *testing.T) {
	invalidPayload := []byte{0xFF, 'b', 'a', 'd'}
	_, _, err := chunk.ParseError(invalidPayload)
	if err == nil {
		t.Fatal("Expected error for invalid error code, got nil")
	}
	if !errors.Is(err, chunk.ErrInvalidErrorCode) {
		t.Errorf("Expected ErrInvalidErrorCode, got %v", err)
	}
}

func TestParseError_OversizedString(t *testing.T) {
	oversizedMsg := strings.Repeat("a", chunk.MaxErrorMessageSize+1)
	payload := append([]byte{byte(chunk.ErrBadRequest)}, []byte(oversizedMsg)...)
	_, _, err := chunk.ParseError(payload)
	if err == nil {
		t.Fatal("Expected error for oversized error message, got nil")
	}
	if !errors.Is(err, chunk.ErrInvalidErrorMessage) {
		t.Errorf("Expected ErrInvalidErrorMessage, got %v", err)
	}
}

func TestParseError_TruncatesSanitizedString(t *testing.T) {
	// Raw string length is 300 (<= 512), but when quoted, 300 newlines become 600 chars + 2 quotes = 602 chars (> 512).
	manyNewlines := strings.Repeat("\n", 300)
	payload := append([]byte{byte(chunk.ErrBadRequest)}, []byte(manyNewlines)...)
	_, msgStr, err := chunk.ParseError(payload)
	if err != nil {
		t.Fatalf("ParseError unexpectedly failed: %v", err)
	}
	if len(msgStr) > chunk.MaxErrorMessageSize {
		t.Errorf("Expected sanitized message length <= %d, got %d", chunk.MaxErrorMessageSize, len(msgStr))
	}
}

func TestParseError_EmptyPayload(t *testing.T) {
	_, _, err := chunk.ParseError([]byte{})
	if err == nil {
		t.Fatal("Expected error for empty payload, got nil")
	}
	if !errors.Is(err, chunk.ErrInvalidPayloadLength) {
		t.Errorf("Expected ErrInvalidPayloadLength, got %v", err)
	}
}

