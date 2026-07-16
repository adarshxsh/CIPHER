package crypto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"cipher/internal/content/core"
)

var ErrUnsupportedVersion = errors.New("unsupported chunk header version")

func SerializeAAD(header *core.ChunkHeader) ([]byte, error) {
	if header.Version != 1 {
		return nil, ErrUnsupportedVersion
	}

	buf := new(bytes.Buffer)
	
	// Version
	if err := binary.Write(buf, binary.LittleEndian, header.Version); err != nil {
		return nil, err
	}
	
	// Index
	if err := binary.Write(buf, binary.LittleEndian, header.Index); err != nil {
		return nil, err
	}
	
	// Offset
	if err := binary.Write(buf, binary.LittleEndian, header.Offset); err != nil {
		return nil, err
	}
	
	// PlainSize
	if err := binary.Write(buf, binary.LittleEndian, header.PlainSize); err != nil {
		return nil, err
	}
	
	return buf.Bytes(), nil
}
