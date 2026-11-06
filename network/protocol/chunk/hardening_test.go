package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"cipher/network/content/core"
	"cipher/network/content/manifest"
	"cipher/network/protocol"
	"cipher/network/protocol/chunk"
	"cipher/network/transport"
)

// A frame whose declared size exceeds the per-type limit is refused from the
// 7-byte header alone, before the payload is allocated or read.
func TestHandler_RejectsOversizedFrame(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	chunk.NewStreamHandler(h1, createTestEngine(t))

	s, err := h2.NewStream(context.Background(), h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(5 * time.Second))

	hdr := make([]byte, 7)
	binary.LittleEndian.PutUint32(hdr[0:4], chunk.MaxFrameSize) // far above MaxChunkRequestSize
	binary.LittleEndian.PutUint16(hdr[4:6], chunk.CurrentMessageVersion)
	hdr[6] = byte(chunk.MsgRequestChunk)
	if _, err := s.Write(hdr); err != nil {
		t.Fatal(err)
	}

	resp, err := chunk.ReadValidatedMessage(s)
	if err != nil {
		t.Fatalf("expected an ERROR reply, got read error: %v", err)
	}
	code, _, _ := chunk.ParseError(resp.Payload)
	if resp.Type != chunk.MsgError || code != chunk.ErrBadRequest {
		t.Fatalf("expected ERROR/BadRequest, got type=%d code=%d", resp.Type, code)
	}
}

// The provider serves exactly one transaction per stream.
func TestHandler_OneTransactionPerStream(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 64*1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatal(err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	s, err := h2.NewStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(5 * time.Second))

	for i := 0; i < 2; i++ {
		if err := chunk.WriteMessage(s, chunk.BuildRequestManifest(m.Descriptor.ID)); err != nil && i == 0 {
			t.Fatal(err)
		}
	}
	first, err := chunk.ReadValidatedMessage(s)
	if err != nil || first.Type != chunk.MsgManifest {
		t.Fatalf("first request: got %v, err=%v", first, err)
	}
	if second, err := chunk.ReadValidatedMessage(s); err == nil {
		t.Fatalf("second request on the same stream was served (type %d)", second.Type)
	}
}

// One Client still performs many requests; each runs on its own stream.
func TestClient_SequentialRequestsAndClose(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1, eng2 := createTestEngine(t), createTestEngine(t)
	chunk.NewStreamHandler(h1, eng1)

	ctx := context.Background()
	data := make([]byte, 1024*1024)
	rand.Read(data)
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatal(err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resolve(ctx, m.Descriptor.ID); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	for _, id := range m.ChunkIDs {
		if _, err := client.FetchChunk(ctx, id); err != nil {
			t.Fatalf("fetch %x: %v", id, err)
		}
	}

	client.Close()
	if _, err := client.FetchChunk(ctx, m.ChunkIDs[0]); !errors.Is(err, chunk.ErrClientClosed) {
		t.Fatalf("expected ErrClientClosed after Close, got %v", err)
	}
	var zero core.ContentID
	if _, err := client.Resolve(ctx, zero); !errors.Is(err, chunk.ErrClientClosed) {
		t.Fatalf("expected ErrClientClosed after Close, got %v", err)
	}
}
