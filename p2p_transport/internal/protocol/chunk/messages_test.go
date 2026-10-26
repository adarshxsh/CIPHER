package chunk_test

import (
	"bytes"
	"io"
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

func TestWriteMessage_ZeroIntermediateAllocations(t *testing.T) {
	msg := &chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    chunk.MsgChunk,
		Payload: []byte("sample-payload-data-slice"),
	}

	allocs := testing.AllocsPerRun(100, func() {
		if err := chunk.WriteMessage(io.Discard, msg); err != nil {
			t.Fatalf("WriteMessage failed: %v", err)
		}
	})

	if allocs > 0 {
		t.Errorf("expected 0 dynamic intermediate allocations for WriteMessage, got %f", allocs)
	}
}

func TestParseChunk_ZeroByteArrayAllocations(t *testing.T) {
	chunkData := []byte("hello-world-ciphertext-chunk-data")
	origChunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			Index:      1,
			PlainSize:  uint32(len(chunkData)),
			CipherSize: uint32(len(chunkData)),
		},
		Data: chunkData,
	}
	msg, err := chunk.BuildChunk(origChunk)
	if err != nil {
		t.Fatalf("BuildChunk failed: %v", err)
	}

	parsed, err := chunk.ParseChunk(msg.Payload)
	if err != nil {
		t.Fatalf("ParseChunk failed: %v", err)
	}

	if &parsed.Data[0] != &msg.Payload[66] {
		t.Errorf("ParseChunk did not preserve slice reference (copied payload instead of slicing)")
	}
}

