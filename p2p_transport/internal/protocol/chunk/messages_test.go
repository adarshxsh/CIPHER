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

func TestParseError_SanitizesControlCharacters(t *testing.T) {
	// Attempted log injection attack via CR, LF, TAB, and null bytes
	injectionPayload := append([]byte{byte(chunk.ErrBadRequest)}, []byte("bad request\n2026/09/21 [Chunk Protocol] Fake log entry\r\t\x00")...)

	code, sanitizedMsg, err := chunk.ParseError(injectionPayload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}

	if code != chunk.ErrBadRequest {
		t.Errorf("expected error code %d, got %d", chunk.ErrBadRequest, code)
	}

	expected := `bad request\n2026/09/21 [Chunk Protocol] Fake log entry\r\t\u0000`
	if sanitizedMsg != expected {
		t.Errorf("expected sanitized message %q, got %q", expected, sanitizedMsg)
	}
}

func TestParseError_EnforcesMaxErrorMessageSize(t *testing.T) {
	// Create payload with message longer than MaxErrorMessageSize (512 bytes)
	longMsg := make([]byte, 1000)
	for i := range longMsg {
		longMsg[i] = 'a'
	}
	payload := append([]byte{byte(chunk.ErrInternal)}, longMsg...)

	code, sanitizedMsg, err := chunk.ParseError(payload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}

	if code != chunk.ErrInternal {
		t.Errorf("expected error code %d, got %d", chunk.ErrInternal, code)
	}

	if len(sanitizedMsg) != chunk.MaxErrorMessageSize {
		t.Errorf("expected message length %d, got %d", chunk.MaxErrorMessageSize, len(sanitizedMsg))
	}
}

func TestParseError_ValidMessagePreserved(t *testing.T) {
	msg := "manifest not found"
	errMsg := chunk.BuildError(chunk.ErrContentNotFound, msg)

	code, parsedMsg, err := chunk.ParseError(errMsg.Payload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}

	if code != chunk.ErrContentNotFound {
		t.Errorf("expected error code %d, got %d", chunk.ErrContentNotFound, code)
	}

	if parsedMsg != msg {
		t.Errorf("expected %q, got %q", msg, parsedMsg)
	}
}
