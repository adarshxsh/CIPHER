package chunk_test

import (
	"bytes"
	"encoding/binary"
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

func TestReadMessage_FrameSizeTooSmall(t *testing.T) {
	for size := uint32(0); size < 3; size++ {
		var buf bytes.Buffer
		binary.Write(&buf, binary.LittleEndian, size)

		_, err := chunk.ReadMessage(&buf)
		if err == nil {
			t.Fatalf("expected error for frame size %d < 3, got nil", size)
		}
	}
}

func TestReadMessage_FrameSizeExceedsMax(t *testing.T) {
	var buf bytes.Buffer
	oversized := uint32(chunk.MaxFrameSize + 1)
	binary.Write(&buf, binary.LittleEndian, oversized)

	_, err := chunk.ReadMessage(&buf)
	if err == nil {
		t.Fatalf("expected error for frame size %d > MaxFrameSize, got nil", oversized)
	}
	if !strings.Contains(err.Error(), "exceeds maximum frame size") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestReadMessage_RejectsInflatedControlMessage(t *testing.T) {
	testCases := []struct {
		name        string
		msgType     chunk.MessageType
		inflatedLen uint32 // payload length = inflatedLen - 3
	}{
		{
			name:        "MsgRequestManifest inflated to 1MB",
			msgType:     chunk.MsgRequestManifest,
			inflatedLen: 1024 * 1024,
		},
		{
			name:        "MsgAck inflated to 2MB",
			msgType:     chunk.MsgAck,
			inflatedLen: 2 * 1024 * 1024,
		},
		{
			name:        "MsgRequestChunk inflated to 64KB",
			msgType:     chunk.MsgRequestChunk,
			inflatedLen: 64 * 1024,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			// Write 4-byte size header
			binary.Write(&buf, binary.LittleEndian, tc.inflatedLen)
			// Write 2-byte Version header
			binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
			// Write 1-byte Type header
			binary.Write(&buf, binary.LittleEndian, tc.msgType)

			// Notice we do NOT write any payload bytes to buf.
			// ReadMessage should validate payloadSize against MaxPayloadSizeForMessage
			// and return an error before trying to read payload bytes or make []byte.

			_, err := chunk.ReadMessage(&buf)
			if err == nil {
				t.Fatalf("expected error for inflated message %s, got nil", tc.name)
			}
			if !strings.Contains(err.Error(), "exceeds limit") {
				t.Fatalf("expected 'exceeds limit' error, got %v", err)
			}
			// Verify that no bytes were read beyond the 7-byte header (4 size + 2 version + 1 type = 7 bytes total in buf)
			// Since buf had 7 bytes, after reading size (4) and header (3), buf is empty (Len() == 0).
			if buf.Len() != 0 {
				t.Fatalf("expected buffer to be completely read up to header, remaining len=%d", buf.Len())
			}
		})
	}
}

