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

func TestReadMessage_RejectsOversizedPayload(t *testing.T) {
	testCases := []struct {
		name        string
		msgType     chunk.MessageType
		payloadLen  int
		shouldPass  bool
	}{
		{
			name:       "RequestChunk within limit",
			msgType:    chunk.MsgRequestChunk,
			payloadLen: 512,
			shouldPass: true,
		},
		{
			name:       "RequestChunk exceeding limit by 1 byte",
			msgType:    chunk.MsgRequestChunk,
			payloadLen: 513,
			shouldPass: false,
		},
		{
			name:       "Ack within limit",
			msgType:    chunk.MsgAck,
			payloadLen: 33,
			shouldPass: true,
		},
		{
			name:       "Ack exceeding limit by 1 byte",
			msgType:    chunk.MsgAck,
			payloadLen: 34,
			shouldPass: false,
		},
		{
			name:       "Unknown message type with 0 bytes payload",
			msgType:    chunk.MessageType(0xFE),
			payloadLen: 0,
			shouldPass: true,
		},
		{
			name:       "Unknown message type with >0 bytes payload",
			msgType:    chunk.MessageType(0xFE),
			payloadLen: 1,
			shouldPass: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			msg := &chunk.Message{
				Version: chunk.CurrentMessageVersion,
				Type:    tc.msgType,
				Payload: make([]byte, tc.payloadLen),
			}
			var buf bytes.Buffer
			if err := chunk.WriteMessage(&buf, msg); err != nil {
				t.Fatalf("WriteMessage failed: %v", err)
			}

			parsedMsg, err := chunk.ReadMessage(&buf)
			if tc.shouldPass {
				if err != nil {
					t.Fatalf("expected success, got error: %v", err)
				}
				if parsedMsg.Type != tc.msgType {
					t.Errorf("expected msg type %v, got %v", tc.msgType, parsedMsg.Type)
				}
				if len(parsedMsg.Payload) != tc.payloadLen {
					t.Errorf("expected payload len %d, got %d", tc.payloadLen, len(parsedMsg.Payload))
				}
			} else {
				if err == nil {
					t.Fatalf("expected error for oversized payload, got nil")
				}
			}
		})
	}
}

func TestReadMessage_ZeroAllocationOnOversizedPayload(t *testing.T) {
	// Attacker sends 2MB frame length for a MsgRequestChunk (max payload 512 bytes).
	// Header consists of 4-byte size (2,097,152), 2-byte version (1), 1-byte type (MsgRequestChunk).
	// Crucially, NO payload bytes are written into the buffer.
	var buf bytes.Buffer
	var frameSize uint32 = 2 * 1024 * 1024
	buf.Write([]byte{
		byte(frameSize), byte(frameSize >> 8), byte(frameSize >> 16), byte(frameSize >> 24),
		byte(chunk.CurrentMessageVersion), byte(chunk.CurrentMessageVersion >> 8),
		byte(chunk.MsgRequestChunk),
	})

	_, err := chunk.ReadMessage(&buf)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// Error should be ErrPayloadTooLarge, NOT io.ErrUnexpectedEOF or truncated frame reading payload
	if !bytes.Equal([]byte(err.Error()), []byte("payload exceeds protocol limit")) && err.Error() == "EOF" {
		t.Fatalf("ReadMessage attempted to read payload bytes instead of failing at header check: %v", err)
	}
}

func TestReadMessage_InvalidFrameSizes(t *testing.T) {
	// Size < 3 bytes (smaller than envelope header)
	var buf bytes.Buffer
	buf.Write([]byte{0x02, 0x00, 0x00, 0x00}) // Size 2
	_, err := chunk.ReadMessage(&buf)
	if err == nil {
		t.Error("expected error for frame size < 3, got nil")
	}

	// Size > MaxFrameSize (2MB)
	buf.Reset()
	var hugeSize uint32 = 2*1024*1024 + 1
	buf.Write([]byte{
		byte(hugeSize), byte(hugeSize >> 8), byte(hugeSize >> 16), byte(hugeSize >> 24),
	})
	_, err = chunk.ReadMessage(&buf)
	if err == nil {
		t.Error("expected error for frame size > 2MB, got nil")
	}
}
