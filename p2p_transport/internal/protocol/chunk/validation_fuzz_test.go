package chunk_test

import (
	"bytes"
	"encoding/binary"
	"testing"

	"cipher/internal/content/core"
	"cipher/internal/protocol/chunk"
)

func FuzzValidateChunkPayload(f *testing.F) {
	// Add some valid seeds
	validHeader := core.ChunkHeader{
		Version:    chunk.CurrentMessageVersion,
		Index:      0,
		CipherSize: 3,
	}
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, &validHeader)
	buf.Write([]byte{1, 2, 3})
	f.Add(buf.Bytes())

	invalidHeader := core.ChunkHeader{
		Version:    chunk.CurrentMessageVersion + 1,
		Index:      0,
		CipherSize: 3,
	}
	var buf2 bytes.Buffer
	binary.Write(&buf2, binary.LittleEndian, &invalidHeader)
	buf2.Write([]byte{1, 2, 3})
	f.Add(buf2.Bytes())

	f.Fuzz(func(t *testing.T, data []byte) {
		// Just ensure it doesn't panic on arbitrary data
		chunk.ValidateChunkPayload(data)
	})
}
