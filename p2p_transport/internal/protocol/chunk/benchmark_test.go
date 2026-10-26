package chunk_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/content/manifest"
	"cipher/internal/protocol/chunk"
	"cipher/internal/transport"
)

func BenchmarkChunkTransport_Sequential(b *testing.B) {
	h1, h2 := setupMockNetwork(b)
	eng1 := createTestEngine(b)
	eng2 := createTestEngine(b)
	
	chunk.NewStreamHandler(h1, eng1)
	
	ctx := context.Background()
	
	// Create 10MB of data to transfer per benchmark iteration
	dataSize := 10 * 1024 * 1024
	data := make([]byte, dataSize)
	rand.Read(data)
	
	m, _ := eng1.Ingest(ctx, bytes.NewReader(data), manifest.TypeFile)
	mBytes, _ := m.Serialize()
	eng1.PutManifestBytes(ctx, m.Descriptor.ID, mBytes)
	
	b.SetBytes(int64(dataSize))
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		client, err := chunk.NewClient(ctx, transport.NewTransport(h2), h1.ID(), eng2)
		if err != nil {
			b.Fatalf("Failed to create client: %v", err)
		}
		
		_, err = client.Resolve(ctx, m.Descriptor.ID)
		if err != nil {
			b.Fatalf("Resolve failed: %v", err)
		}
		
		err = client.Download(ctx, m.ChunkIDs)
		if err != nil {
			b.Fatalf("Download failed: %v", err)
		}
		
		client.Close()
	}
}

func BenchmarkWriteMessage(b *testing.B) {
	msg := chunk.BuildRequestManifest(core.ContentID{0x01})
	var nw nopWriter
	var w io.Writer = &nw

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = chunk.WriteMessage(w, msg)
	}
}

func BenchmarkReadMessage(b *testing.B) {
	msg := chunk.BuildRequestManifest(core.ContentID{0x01})
	var buf bytes.Buffer
	_ = chunk.WriteMessage(&buf, msg)
	data := buf.Bytes()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := bytes.NewReader(data)
		_, _ = chunk.ReadMessage(r)
	}
}

func BenchmarkParseChunk(b *testing.B) {
	chunkObj := &core.Chunk{
		Header: core.ChunkHeader{Version: 1, Index: 1},
		Data:   make([]byte, 32768),
	}
	msg, _ := chunk.BuildChunk(chunkObj)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = chunk.ParseChunk(msg.Payload)
	}
}
