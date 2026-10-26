package push

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"testing"

	"cipher/internal/content/core"
)

func TestPushMessageFraming(t *testing.T) {
	origMsg := BuildPushError(0x05, "disk full error")
	buf := new(bytes.Buffer)

	if err := WritePushMessage(buf, origMsg); err != nil {
		t.Fatalf("WritePushMessage failed: %v", err)
	}

	readMsg, err := ReadPushMessage(buf)
	if err != nil {
		t.Fatalf("ReadPushMessage failed: %v", err)
	}

	if readMsg.Version != origMsg.Version {
		t.Errorf("version mismatch: got %d, want %d", readMsg.Version, origMsg.Version)
	}
	if readMsg.Type != origMsg.Type {
		t.Errorf("type mismatch: got %d, want %d", readMsg.Type, origMsg.Type)
	}

	code, str, err := ParsePushError(readMsg.Payload)
	if err != nil {
		t.Fatalf("ParsePushError failed: %v", err)
	}
	if code != 0x05 || str != "disk full error" {
		t.Errorf("error payload mismatch: got code=%d, msg=%s", code, str)
	}
}

func TestPushManifestSerialization(t *testing.T) {
	var contentID core.ContentID
	_, _ = rand.Read(contentID[:])

	var cid1, cid2, cid3 core.ChunkID
	_, _ = rand.Read(cid1[:])
	_, _ = rand.Read(cid2[:])
	_, _ = rand.Read(cid3[:])

	assigned := []core.ChunkID{cid1, cid2, cid3}
	manifestJSON := []byte(`{"version":1,"descriptor":{"id":"abc"}}`)

	msg := BuildPushManifest(contentID, assigned, manifestJSON)
	parsedCID, parsedAssigned, parsedJSON, err := ParsePushManifest(msg.Payload)
	if err != nil {
		t.Fatalf("ParsePushManifest failed: %v", err)
	}

	if parsedCID != contentID {
		t.Errorf("contentID mismatch")
	}
	if len(parsedAssigned) != len(assigned) {
		t.Fatalf("assigned chunk count mismatch: got %d, want %d", len(parsedAssigned), len(assigned))
	}
	for i := range assigned {
		if parsedAssigned[i] != assigned[i] {
			t.Errorf("chunk %d mismatch", i)
		}
	}
	if string(parsedJSON) != string(manifestJSON) {
		t.Errorf("manifest json mismatch: got %s, want %s", string(parsedJSON), string(manifestJSON))
	}
}

func TestPushChunkSerialization(t *testing.T) {
	var contentID core.ContentID
	var chunkID core.ChunkID
	_, _ = rand.Read(contentID[:])
	_, _ = rand.Read(chunkID[:])

	chunk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID,
			Index:      4,
			Offset:     1024,
			PlainSize:  512,
			CipherSize: 528,
		},
		Data: []byte("encrypted-ciphertext-data-payload-example"),
	}

	msg, err := BuildPushChunk(contentID, chunk)
	if err != nil {
		t.Fatalf("BuildPushChunk failed: %v", err)
	}

	parsedCID, parsedChunk, err := ParsePushChunk(msg.Payload)
	if err != nil {
		t.Fatalf("ParsePushChunk failed: %v", err)
	}

	if parsedCID != contentID {
		t.Errorf("contentID mismatch")
	}
	if parsedChunk.Header.ID != chunkID {
		t.Errorf("chunk ID mismatch")
	}
	if parsedChunk.Header.Index != 4 {
		t.Errorf("chunk index mismatch")
	}
	if !bytes.Equal(parsedChunk.Data, chunk.Data) {
		t.Errorf("chunk data mismatch")
	}
}

func TestFrameSizeLimits(t *testing.T) {
	// Attempt to create a message larger than MaxMessageSize
	hugePayload := make([]byte, MaxMessageSize+1)
	msg := &PushMessage{
		Version: CurrentPushVersion,
		Type:    MsgPushChunk,
		Payload: hugePayload,
	}

	buf := new(bytes.Buffer)
	err := WritePushMessage(buf, msg)
	if err == nil {
		t.Fatalf("expected error for oversized message, got nil")
	}
}

func TestParsePushManifestValidation(t *testing.T) {
	t.Run("header too short", func(t *testing.T) {
		payload := make([]byte, 35)
		_, _, _, err := ParsePushManifest(payload)
		if err == nil {
			t.Fatal("expected error for header shorter than 36 bytes, got nil")
		}
	})

	t.Run("payload exceeds MaxManifestSize", func(t *testing.T) {
		payload := make([]byte, MaxManifestSize+1)
		_, _, _, err := ParsePushManifest(payload)
		if err == nil {
			t.Fatal("expected error for payload exceeding MaxManifestSize, got nil")
		}
	})

	t.Run("integer overflow count 0x08000000", func(t *testing.T) {
		// 32 bytes ContentID + uint32 count (0x08000000 = 134217728)
		payload := make([]byte, 36)
		binary.LittleEndian.PutUint32(payload[32:36], 0x08000000)

		_, _, _, err := ParsePushManifest(payload)
		if err == nil {
			t.Fatal("expected error for overflowing chunk count 0x08000000, got nil")
		}
	})

	t.Run("max uint32 count 0xFFFFFFFF", func(t *testing.T) {
		payload := make([]byte, 36)
		binary.LittleEndian.PutUint32(payload[32:36], 0xFFFFFFFF)

		_, _, _, err := ParsePushManifest(payload)
		if err == nil {
			t.Fatal("expected error for uint32 max chunk count 0xFFFFFFFF, got nil")
		}
	})

	t.Run("truncated payload for count", func(t *testing.T) {
		// Payload has 36 bytes header + 1 chunk (32 bytes) = 68 bytes,
		// but count is set to 5 (requires 36 + 5*32 = 196 bytes).
		payload := make([]byte, 68)
		binary.LittleEndian.PutUint32(payload[32:36], 5)

		_, _, _, err := ParsePushManifest(payload)
		if err == nil {
			t.Fatal("expected error for truncated payload, got nil")
		}
	})

	t.Run("valid manifest with zero chunks", func(t *testing.T) {
		payload := make([]byte, 36)
		binary.LittleEndian.PutUint32(payload[32:36], 0)

		cid, assigned, data, err := ParsePushManifest(payload)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(assigned) != 0 {
			t.Fatalf("expected 0 chunks, got %d", len(assigned))
		}
		if len(data) != 0 {
			t.Fatalf("expected 0 manifest data, got %d bytes", len(data))
		}
		_ = cid
	})
}

func BenchmarkParsePushManifest(b *testing.B) {
	var contentID core.ContentID
	assigned := make([]core.ChunkID, 100)
	manifestJSON := []byte(`{"version":1,"descriptor":{"id":"abc"}}`)

	msg := BuildPushManifest(contentID, assigned, manifestJSON)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := ParsePushManifest(msg.Payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}
