package chunk_test

import (
	"bytes"
	"context"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func TestLimits_SecondRequestOnSameStream_Fails(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	var contentID core.ContentID
	contentID[0] = 0x12
	eng1.PutManifestBytes(ctx, contentID, []byte("test manifest data"))

	// Open raw stream manually from h2 to h1
	tp2 := transport.NewTransport(h2)
	s, err := tp2.OpenStream(ctx, h1.ID(), protocol.ChunkTransportProtocolID)
	if err != nil {
		t.Fatalf("Failed to open stream: %v", err)
	}
	defer s.Close()

	// 1. Send first request frame on stream
	req1 := chunk.BuildRequestManifest(contentID)
	if err := chunk.WriteMessage(s, req1); err != nil {
		t.Fatalf("Failed to write first request: %v", err)
	}

	// Read first response
	resp1, err := chunk.ReadMessage(s)
	if err != nil {
		t.Fatalf("Failed to read first response: %v", err)
	}
	if resp1.Type != chunk.MsgManifest {
		t.Fatalf("Expected MSG_MANIFEST for first response, got %d", resp1.Type)
	}

	// 2. Send second request frame on SAME stream
	req2 := chunk.BuildRequestManifest(contentID)
	if err := chunk.WriteMessage(s, req2); err != nil {
		// Writing might fail if stream was already closed by server, which is also valid behavior
		t.Logf("Write second message failed as stream closed: %v", err)
		return
	}

	// Read second response
	resp2, err := chunk.ReadMessage(s)
	if err != nil {
		// Server closing stream is acceptable, or returning ErrBadRequest
		return
	}

	if resp2.Type == chunk.MsgError {
		code, msgStr, _ := chunk.ParseError(resp2.Payload)
		if code != chunk.ErrBadRequest {
			t.Errorf("Expected ErrBadRequest (code %d), got code %d (%s)", chunk.ErrBadRequest, code, msgStr)
		}
	} else {
		t.Errorf("Expected MsgError for second request on same stream, got type %d", resp2.Type)
	}
}

func TestLimits_ClientOpensDedicatedStreams(t *testing.T) {
	h1, h2 := setupMockNetwork(t)
	eng1 := createTestEngine(t)
	eng2 := createTestEngine(t)

	chunk.NewStreamHandler(h1, eng1)
	chunk.NewStreamHandler(h2, eng2)

	ctx := context.Background()
	data := []byte("hello stream limit test data")
	m, err := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	if err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)

	client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	// Perform multiple transactions through the same Client instance
	// 1. Resolve manifest
	m2Data, err := client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("First Resolve failed: %v", err)
	}

	m2, err := manifest.Deserialize(m2Data)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// 2. Resolve manifest again (should succeed with new stream)
	_, err = client.Resolve(ctx, m.Descriptor.ID)
	if err != nil {
		t.Fatalf("Second Resolve failed: %v", err)
	}

	// 3. Download chunks (should open dedicated stream per chunk)
	if err := client.Download(ctx, m2.ChunkIDs); err != nil {
		t.Fatalf("Download failed: %v", err)
	}
}
