package storage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cipher/internal/content/core"
)

func TestFSStorage_GetChunk_Valid(t *testing.T) {
	dir := t.TempDir()
	store := NewFSStore(dir)
	ctx := context.Background()

	chunkID := core.ChunkID{1, 2, 3, 4, 5}
	payloadData := []byte("hello chunk storage world!")

	chk := &core.Chunk{
		Header: core.ChunkHeader{
			Version:    1,
			ID:         chunkID,
			Index:      0,
			Offset:     0,
			PlainSize:  uint32(len(payloadData)),
			CipherSize: uint32(len(payloadData)),
		},
		Data: payloadData,
	}

	if err := store.PutChunk(ctx, chk); err != nil {
		t.Fatalf("PutChunk failed: %v", err)
	}

	has, err := store.HasChunk(ctx, chunkID)
	if err != nil || !has {
		t.Fatalf("HasChunk expected true, got has=%v err=%v", has, err)
	}

	got, err := store.GetChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("GetChunk failed: %v", err)
	}

	if got.Header.CipherSize != chk.Header.CipherSize {
		t.Errorf("expected CipherSize %d, got %d", chk.Header.CipherSize, got.Header.CipherSize)
	}

	if string(got.Data) != string(payloadData) {
		t.Errorf("expected payload %q, got %q", string(payloadData), string(got.Data))
	}
}

func TestFSStorage_GetChunk_TooSmall(t *testing.T) {
	dir := t.TempDir()
	store := NewFSStore(dir)
	ctx := context.Background()

	chunkID := core.ChunkID{0xaa, 0xbb}
	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}

	// Write only 10 bytes (less than HeaderSize = 66)
	if err := os.WriteFile(path, []byte("1234567890"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for undersized chunk file, got nil")
	}
	if !strings.Contains(err.Error(), "chunk file too small") {
		t.Errorf("expected 'chunk file too small' in error, got: %v", err)
	}
}

func TestFSStorage_GetChunk_Oversized(t *testing.T) {
	dir := t.TempDir()
	store := NewFSStore(dir)
	ctx := context.Background()

	chunkID := core.ChunkID{0xcc, 0xdd}
	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	// Truncate file to HeaderSize + MaxCiphertextSize + 1 (32863 bytes)
	oversizedLen := int64(HeaderSize + MaxCiphertextSize + 1)
	if err := f.Truncate(oversizedLen); err != nil {
		f.Close()
		t.Fatal(err)
	}
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for oversized chunk file, got nil")
	}
	if !strings.Contains(err.Error(), "chunk file too large") {
		t.Errorf("expected 'chunk file too large' in error, got: %v", err)
	}
}

func TestFSStorage_GetChunk_HeaderPayloadMismatch(t *testing.T) {
	dir := t.TempDir()
	store := NewFSStore(dir)
	ctx := context.Background()

	chunkID := core.ChunkID{0xee, 0xff}
	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	hdr := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		CipherSize: 50, // Claims 50 bytes
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write only 20 bytes of actual payload instead of 50
	f.Write(make([]byte, 20))
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for header/payload size mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "does not match disk payload size") {
		t.Errorf("expected payload size mismatch error, got: %v", err)
	}
}

func TestFSStorage_GetChunk_TrailingBytes(t *testing.T) {
	dir := t.TempDir()
	store := NewFSStore(dir)
	ctx := context.Background()

	chunkID := core.ChunkID{0x11, 0x22}
	path := store.pathForChunk(chunkID)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	hdr := core.ChunkHeader{
		Version:    1,
		ID:         chunkID,
		CipherSize: 20, // Claims 20 bytes
	}
	if err := binary.Write(f, binary.LittleEndian, &hdr); err != nil {
		f.Close()
		t.Fatal(err)
	}

	// Write 20 bytes payload plus 5 trailing unvalidated bytes
	f.Write(make([]byte, 25))
	f.Close()

	_, err = store.GetChunk(ctx, chunkID)
	if err == nil {
		t.Fatalf("expected error for trailing bytes in chunk file, got nil")
	}
	if !strings.Contains(err.Error(), "does not match disk payload size") && !strings.Contains(err.Error(), "unexpected trailing bytes") {
		t.Errorf("expected error indicating mismatch/trailing bytes, got: %v", err)
	}
}
