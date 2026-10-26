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

func TestParseError_SanitizationAndTruncation(t *testing.T) {
	t.Run("valid message remains unchanged", func(t *testing.T) {
		msgBytes := []byte("chunk not found")
		payload := append([]byte{byte(chunk.ErrChunkNotFound)}, msgBytes...)
		code, msg, err := chunk.ParseError(payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if code != chunk.ErrChunkNotFound {
			t.Errorf("expected code %v, got %v", chunk.ErrChunkNotFound, code)
		}
		if msg != "chunk not found" {
			t.Errorf("expected 'chunk not found', got '%s'", msg)
		}
	})

	t.Run("truncates payload exceeding 256 bytes", func(t *testing.T) {
		largeMsg := make([]byte, 500)
		for i := range largeMsg {
			largeMsg[i] = 'A'
		}
		payload := append([]byte{byte(chunk.ErrInternal)}, largeMsg...)
		code, msg, err := chunk.ParseError(payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if code != chunk.ErrInternal {
			t.Errorf("expected code %v, got %v", chunk.ErrInternal, code)
		}
		if len(msg) > 256 {
			t.Errorf("expected msg length <= 256, got %d", len(msg))
		}
		if len(msg) != 256 {
			t.Errorf("expected msg length 256, got %d", len(msg))
		}
	})

	t.Run("escapes control characters and ANSI sequences", func(t *testing.T) {
		input := "line1\nline2\rline3\ttab\x1b[31mRed\x1b[0m\x00null\xffend"
		payload := append([]byte{byte(chunk.ErrBadRequest)}, []byte(input)...)
		_, msg, err := chunk.ParseError(payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if bytes.Contains([]byte(msg), []byte{'\n'}) {
			t.Errorf("sanitized msg contains unescaped newline: %q", msg)
		}
		if bytes.Contains([]byte(msg), []byte{'\r'}) {
			t.Errorf("sanitized msg contains unescaped carriage return: %q", msg)
		}
		if bytes.Contains([]byte(msg), []byte{0x1b}) {
			t.Errorf("sanitized msg contains unescaped ESC byte: %q", msg)
		}
		if bytes.Contains([]byte(msg), []byte{0x00}) {
			t.Errorf("sanitized msg contains unescaped NUL byte: %q", msg)
		}

		expectedSubstrings := []string{`line1\nline2\rline3\ttab`, `\x1b[31mRed\x1b[0m`, `\x00null`, `\xffend`}
		for _, sub := range expectedSubstrings {
			if !bytes.Contains([]byte(msg), []byte(sub)) {
				t.Errorf("expected sanitized msg to contain %q, got %q", sub, msg)
			}
		}
	})

	t.Run("rejects empty error payload", func(t *testing.T) {
		_, _, err := chunk.ParseError([]byte{})
		if err == nil {
			t.Error("expected error for empty payload, got nil")
		}
	})
}
