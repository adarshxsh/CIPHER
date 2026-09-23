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

func TestSanitizeErrorMessage_EscapesControlCharacters(t *testing.T) {
	input := "Line 1\nLine 2\r\tWith \x1b[31mANSI\x00 and null"
	expected := `Line 1\nLine 2\r\tWith \x1b[31mANSI\x00 and null`

	got := chunk.SanitizeErrorMessage(input)
	if got != expected {
		t.Errorf("SanitizeErrorMessage failed:\nexpected: %q\ngot:      %q", expected, got)
	}
}

func TestParseError_SanitizesControlCharacters(t *testing.T) {
	// Payload with ErrorCode (1 byte) + unsanitized string with newlines and control chars
	payload := append([]byte{byte(chunk.ErrBadRequest)}, []byte("Failed to process\n[LOG INJECTION ATTEMPT]\r\t\x00")...)

	code, msgStr, err := chunk.ParseError(payload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}

	if code != chunk.ErrBadRequest {
		t.Errorf("expected error code %d, got %d", chunk.ErrBadRequest, code)
	}

	expectedMsg := `Failed to process\n[LOG INJECTION ATTEMPT]\r\t\x00`
	if msgStr != expectedMsg {
		t.Errorf("expected sanitized message %q, got %q", expectedMsg, msgStr)
	}
}

func TestParseError_TruncatesOversizedErrorMessages(t *testing.T) {
	// Create string exceeding 512 bytes
	longMsg := bytes.Repeat([]byte("A"), 600)
	payload := append([]byte{byte(chunk.ErrInternal)}, longMsg...)

	_, msgStr, err := chunk.ParseError(payload)
	if err != nil {
		t.Fatalf("ParseError failed: %v", err)
	}

	if len(msgStr) != chunk.MaxErrorMessageSize {
		t.Errorf("expected truncated string length %d, got %d", chunk.MaxErrorMessageSize, len(msgStr))
	}

	expectedEnding := chunk.TruncationIndicator
	if !bytes.HasSuffix([]byte(msgStr), []byte(expectedEnding)) {
		t.Errorf("expected truncated string to end with %q, got ending %q", expectedEnding, msgStr[len(msgStr)-len(expectedEnding):])
	}
}
