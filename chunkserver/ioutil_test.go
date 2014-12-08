package chunkserver

import (
  "os"
  "testing"
)

func testReadWrite(t *testing.T, s string) {
  path := "/tmp/test"
  os.Remove(path)
  bytes := []byte(s)
  WriteDataAt(path, 0, bytes)
  got := make([]byte, 100)
  n, err := ReadDataAt(path, 0, got)
  if err != nil {
    t.Error(err)
  }
  if string(got[:n]) != s {
    t.Errorf("read write doesn't match: got %v want %s", string(got), s)
  }
}

func testReadWriteHeader(t *testing.T, v, cksum uint64) {
  path := "/tmp/test"
  os.Remove(path)
  h0 := ChunkHeader{
    version: v,
    checksum: cksum,
  }
  WriteHeader(path, h0)
  h1, err := ReadHeader(path)
  if err != nil {
    t.Error(err)
  }
  if h1.version != v || h1.checksum != cksum {
    t.Error("read write header failed")
  }
}

func TestReadWrite(t *testing.T) {
  testReadWrite(t, "hello, world.")
  testReadWrite(t, "fine thank you and you?")
  testReadWrite(t, "nice to meet you.")
}

func TestReadWriteHeader(t *testing.T) {
 testReadWriteHeader(t, 10, 100)
 testReadWriteHeader(t, 100, 1000)
 testReadWriteHeader(t, 1000, 10000)
}
