package chunkserver

import (
  "fmt"
  "testing"
)

func testBytes(t *testing.T, v, cksum uint64) {
  h0 := ChunkHeader{
    version: v,
    checksum: cksum,
  }
  bytes := h0.Bytes()
  h1, err := NewChunkHeader(bytes)
  if err != nil {
    t.Error(err)
  }
  if h1.version != v || h1.checksum != cksum {
    fmt.Printf("version want: %v got %v\nchecksum want: %v got %v\n",
               v, h1.version, cksum, h1.checksum)
    t.Error("Bytes() and NewChunkHeader() don't work.")
  }
}

func TestBytes(t *testing.T) {
  testBytes(t, 123, 12345)
  testBytes(t, 1, 1)
  testBytes(t, 1000, 1000)
}
