package chunk_test

import (
	"errors"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
)

func TestValidateMessage_AcceptsCurrentPayloads(t *testing.T) {
	var contentID core.ContentID
	contentID[0] = 0xAA

	if err := chunk.ValidateMessage(chunk.BuildRequestManifest(contentID)); err != nil {
		t.Fatalf("request manifest should validate: %v", err)
	}

	var chunkID core.ChunkID
	chunkID[0] = 0xBB

	if err := chunk.ValidateMessage(chunk.BuildRequestChunk(chunkID)); err != nil {
		t.Fatalf("request chunk should validate: %v", err)
	}

	if err := chunk.ValidateMessage(chunk.BuildAck(chunkID, 0)); err != nil {
		t.Fatalf("ack should validate: %v", err)
	}

	if err := chunk.ValidateMessage(chunk.BuildError(chunk.ErrBadRequest, "")); err != nil {
		t.Fatalf("empty error text should remain compatible: %v", err)
	}
}

func TestValidateMessage_RejectsUnsupportedEnvelope(t *testing.T) {
	err := chunk.ValidateMessage(&chunk.Message{
		Version: chunk.CurrentMessageVersion + 1,
		Type:    chunk.MsgRequestChunk,
		Payload: make([]byte, chunk.HashSize),
	})
	if !errors.Is(err, chunk.ErrInvalidMessageVersion) {
		t.Fatalf("expected ErrInvalidMessageVersion, got %v", err)
	}

	err = chunk.ValidateMessage(&chunk.Message{
		Version: chunk.CurrentMessageVersion,
		Type:    0x99,
		Payload: nil,
	})
	if !errors.Is(err, chunk.ErrInvalidMessageType) {
		t.Fatalf("expected ErrInvalidMessageType, got %v", err)
	}
}

func TestValidateChunkPayload_ChecksHeaderAndCipherSize(t *testing.T) {
	var chunkID core.ChunkID
	chunkID[0] = 0xCC

	msg, err := chunk.BuildChunk(&core.Chunk{
		Header: core.ChunkHeader{
			Version:    chunk.CurrentMessageVersion,
			ID:         chunkID,
			PlainSize:  3,
			CipherSize: 3,
		},
		Data: []byte{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("BuildChunk failed: %v", err)
	}

	if err := chunk.ValidateMessage(msg); err != nil {
		t.Fatalf("chunk should validate: %v", err)
	}

	badMsg, err := chunk.BuildChunk(&core.Chunk{
		Header: core.ChunkHeader{
			Version:    chunk.CurrentMessageVersion,
			ID:         chunkID,
			PlainSize:  3,
			CipherSize: 99,
		},
		Data: []byte{1, 2, 3},
	})
	if err != nil {
		t.Fatalf("BuildChunk failed: %v", err)
	}

	err = chunk.ValidateMessage(badMsg)
	if !errors.Is(err, chunk.ErrInvalidCiphertext) {
		t.Fatalf("expected ErrInvalidCiphertext, got %v", err)
	}
}

func TestValidateResponseForRequestHelpers(t *testing.T) {
	var requested core.ContentID
	requested[0] = 0x01

	var received core.ContentID
	received[0] = 0x02

	manifestMsg := chunk.BuildManifest(received, []byte("manifest"))
	err := chunk.ValidateManifestForRequest(requested, manifestMsg.Payload)
	if !errors.Is(err, chunk.ErrContentMismatch) {
		t.Fatalf("expected ErrContentMismatch, got %v", err)
	}

	var requestedChunk core.ChunkID
	requestedChunk[0] = 0x03

	var receivedChunk core.ChunkID
	receivedChunk[0] = 0x04

	chunkMsg, err := chunk.BuildChunk(&core.Chunk{
		Header: core.ChunkHeader{
			Version:    chunk.CurrentMessageVersion,
			ID:         receivedChunk,
			PlainSize:  1,
			CipherSize: 1,
		},
		Data: []byte{1},
	})
	if err != nil {
		t.Fatalf("BuildChunk failed: %v", err)
	}

	err = chunk.ValidateChunkForRequest(requestedChunk, chunkMsg.Payload)
	if !errors.Is(err, chunk.ErrChunkMismatch) {
		t.Fatalf("expected ErrChunkMismatch, got %v", err)
	}
}

func TestValidateManifestForRequest_DetectsSpoofing(t *testing.T) {
	m := &manifest.Manifest{
		Version: 1,
		Descriptor: manifest.ContentDescriptor{
			Type: manifest.TypeFile,
			Size: 100,
		},
		ChunkIDs: []core.ChunkID{{0x01}},
	}
	correctID, err := m.DeriveContentID()
	if err != nil {
		t.Fatalf("DeriveContentID failed: %v", err)
	}
	m.Descriptor.ID = correctID

	mBytes, err := m.Serialize()
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// 1. Valid manifest matching requested ContentID passes
	msgValid := chunk.BuildManifest(correctID, mBytes)
	if err := chunk.ValidateManifestForRequest(correctID, msgValid.Payload); err != nil {
		t.Fatalf("valid manifest should pass: %v", err)
	}

	// 2. Spoofed manifest: payload header claims correctID, but manifest body contains different content or wrong Descriptor.ID
	spoofedManifest := *m
	spoofedManifest.ChunkIDs = []core.ChunkID{{0xFF}} // Tampered chunks!
	// Even if descriptor ID is set to correctID, derived ID won't match!
	spoofedManifest.Descriptor.ID = correctID
	spoofedBytes, _ := spoofedManifest.Serialize()

	msgSpoofed := chunk.BuildManifest(correctID, spoofedBytes)
	err = chunk.ValidateManifestForRequest(correctID, msgSpoofed.Payload)
	if !errors.Is(err, chunk.ErrContentMismatch) {
		t.Fatalf("expected ErrContentMismatch for spoofed manifest content, got %v", err)
	}
}
