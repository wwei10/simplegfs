package chunkserver

import (
  "encoding/binary"
  "errors"
)

// Note: If header length changes, change ioutil.go accordingly.
const ChunkHeaderLength = 16 // Header occupies 16 bytes

type ChunkHeader struct {
  version uint64 // version number occupies 8 bytes
  checksum uint64 // Checksum occupies 8 bytes
}

func (header *ChunkHeader) Bytes() []byte {
  bytes := make([]byte, ChunkHeaderLength)
  putUint64(bytes[:8], header.version)
  putUint64(bytes[8:16], header.checksum)
  return bytes
}

func NewChunkHeader(bytes []byte) (*ChunkHeader, error) {
  header := &ChunkHeader{}
  if len(bytes) != ChunkHeaderLength {
    return header, errors.New("cannot decode")
  }
  header.version = readUint64(bytes[:8])
  header.checksum = readUint64(bytes[8:16])
  return header, nil
}

// Convert uint64 to 8 bytes representation and 
// fill it in to bytes
func putUint64(bytes []byte, x uint64) {
  binary.LittleEndian.PutUint64(bytes, x)
}

// Convert byte array to uint64
func readUint64(bytes []byte) uint64 {
  if (len(bytes) != 8) {
    return 0
  }
  return binary.LittleEndian.Uint64(bytes)
}
