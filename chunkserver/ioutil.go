package chunkserver

import (
  "io"
  "os"
)

// Note: Can't handle concurrent read and write.
func WriteDataAt(path string, offset int64, bytes []byte) (int, error) {
  offset += ChunkHeaderLength // Take metadata into account.
  file, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE, 0666)
  if err != nil {
    return 0, err
  }
  defer file.Close()
  n, err := file.WriteAt(bytes, offset)
  if err != nil {
    return n, err
  }
  return n, nil
}

// Note: Can't handle concurrent read and write.
func ReadDataAt(path string, offset int64, bytes []byte) (int, error) {
  offset += ChunkHeaderLength // Take metadata into account.
  file, err := os.Open(path)
  if err != nil {
    return 0, err
  }
  defer file.Close()
  n, err := file.ReadAt(bytes, offset)
  if err != nil {
    switch err {
    case io.EOF:
      return n, nil
    default:
      return n, err
    }
  }
  return n, nil
}

func WriteHeader(path string, header ChunkHeader) error {
  file, err := os.OpenFile(path, os.O_WRONLY | os.O_CREATE, 0666)
  if err != nil {
    return err
  }
  defer file.Close()
  _, err = file.WriteAt(header.Bytes(), 0)
  if err != nil {
    return err
  }
  return nil
}

func ReadHeader(path string) (*ChunkHeader, error) {
  file, err := os.Open(path)
  header := &ChunkHeader{}
  if err != nil {
    return header, err
  }
  defer file.Close()
  bytes := make([]byte, ChunkHeaderLength)
  _, err = file.ReadAt(bytes, 0)
  if err != nil {
    return header, err
  }
  return NewChunkHeader(bytes)
}
