package chunk_test

import (
	"bytes"
	"encoding/binary"
	"errors"
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

func TestReadMessage_HeaderFirstPreParseLimitValidation(t *testing.T) {
	// Construct only 7 bytes header:
	// Frame size = 1000 + 3 = 1003 bytes
	// Version = 1
	// Type = MsgRequestChunk (limit is 512)
	// Notice that we supply NO payload bytes in buf!
	var buf bytes.Buffer
	frameSize := uint32(1003)
	binary.Write(&buf, binary.LittleEndian, frameSize)
	binary.Write(&buf, binary.LittleEndian, chunk.CurrentMessageVersion)
	buf.WriteByte(byte(chunk.MsgRequestChunk))

	// ReadMessage must inspect the 7-byte header first and reject the payload limit
	// before attempting to read 1000 payload bytes from buf.
	_, err := chunk.ReadMessage(&buf)
	if err == nil {
		t.Fatalf("expected error for oversized MsgRequestChunk payload, got nil")
	}
	if !errors.Is(err, chunk.ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got: %v", err)
	}
}

func TestReadMessage_PerMessageTypeLimits(t *testing.T) {
	tests := []struct {
		name        string
		msgType     chunk.MessageType
		payloadLen  int
		expectError bool
	}{
		{
			name:        "MsgRequestChunk within limit",
			msgType:     chunk.MsgRequestChunk,
			payloadLen:  512,
			expectError: false,
		},
		{
			name:        "MsgRequestChunk exceeding limit",
			msgType:     chunk.MsgRequestChunk,
			payloadLen:  513,
			expectError: true,
		},
		{
			name:        "MsgRequestManifest within limit",
			msgType:     chunk.MsgRequestManifest,
			payloadLen:  32,
			expectError: false,
		},
		{
			name:        "MsgRequestManifest exceeding limit",
			msgType:     chunk.MsgRequestManifest,
			payloadLen:  513,
			expectError: true,
		},
		{
			name:        "MsgAck within limit",
			msgType:     chunk.MsgAck,
			payloadLen:  33,
			expectError: false,
		},
		{
			name:        "MsgAck exceeding limit",
			msgType:     chunk.MsgAck,
			payloadLen:  34,
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			msg := &chunk.Message{
				Version: chunk.CurrentMessageVersion,
				Type:    tc.msgType,
				Payload: make([]byte, tc.payloadLen),
			}
			if err := chunk.WriteMessage(&buf, msg); err != nil {
				t.Fatalf("WriteMessage failed: %v", err)
			}

			parsed, err := chunk.ReadMessage(&buf)
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !errors.Is(err, chunk.ErrPayloadTooLarge) {
					t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if len(parsed.Payload) != tc.payloadLen {
					t.Fatalf("expected payload len %d, got %d", tc.payloadLen, len(parsed.Payload))
				}
			}
		})
	}
}

func TestReadMessage_UnsupportedMessageTypeGeneralCap(t *testing.T) {
	// Unknown message type 0x88
	unsupportedType := chunk.MessageType(0x88)

	// Case 1: Small payload for unknown message type succeeds
	smallMsg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    unsupportedType,
		Payload: []byte("hello unknown"),
	}
	var buf1 bytes.Buffer
	if err := chunk.WriteMessage(&buf1, smallMsg); err != nil {
		t.Fatalf("WriteMessage failed: %v", err)
	}
	parsed1, err := chunk.ReadMessage(&buf1)
	if err != nil {
		t.Fatalf("expected success for small unknown message, got %v", err)
	}
	if parsed1.Type != unsupportedType {
		t.Fatalf("expected type %v, got %v", unsupportedType, parsed1.Type)
	}

	// Case 2: Declared payload exceeding general maximum payload limit (MaxMessagePayloadSize)
	var buf2 bytes.Buffer
	oversizedLen := uint32(chunk.MaxMessagePayloadSize + 1)
	frameSize := oversizedLen + 3
	binary.Write(&buf2, binary.LittleEndian, frameSize)
	binary.Write(&buf2, binary.LittleEndian, chunk.CurrentMessageVersion)
	buf2.WriteByte(byte(unsupportedType))

	_, err = chunk.ReadMessage(&buf2)
	if err == nil {
		t.Fatalf("expected error for oversized unknown message, got nil")
	}
	if !errors.Is(err, chunk.ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestReadMessage_MalformedFrameHeaders(t *testing.T) {
	// Nil reader
	_, err := chunk.ReadMessage(nil)
	if err == nil {
		t.Fatalf("expected error for nil reader, got nil")
	}

	// Truncated header (< 7 bytes)
	bufTruncated := bytes.NewReader([]byte{0x01, 0x02, 0x03})
	_, err = chunk.ReadMessage(bufTruncated)
	if err == nil || !errors.Is(err, chunk.ErrTruncatedFrame) {
		t.Fatalf("expected ErrTruncatedFrame, got %v", err)
	}

	// Frame size < 3
	var bufInvalidSize bytes.Buffer
	binary.Write(&bufInvalidSize, binary.LittleEndian, uint32(2))
	binary.Write(&bufInvalidSize, binary.LittleEndian, chunk.CurrentMessageVersion)
	bufInvalidSize.WriteByte(byte(chunk.MsgRequestChunk))
	_, err = chunk.ReadMessage(&bufInvalidSize)
	if err == nil || !errors.Is(err, chunk.ErrInvalidPayloadSize) {
		t.Fatalf("expected ErrInvalidPayloadSize, got %v", err)
	}
}
