package simplegfs

import (
  "fmt"
  "time"
)

type Client struct {
  masterAddr string
  clientId uint64
  file2Lease map[string]time.Time
}

func NewClient(masterAddr string) *Client {
  c := &Client{
    masterAddr: masterAddr,
  }
  reply := &NewClientIdReply{}
  call(masterAddr, "MasterServer.NewClientId", struct{}{}, reply)
  c.clientId = reply.ClientId
  return c
}

// Client APIs

// Create a file
func (c *Client) Create(path string) bool {
  // TODO: Error handling
  reply := new(bool)
  call(c.masterAddr, "MasterServer.Create", path, reply)
  return *reply
}

// Write file at a specific offset
func (c *Client) Write(path string, offset uint64, bytes []byte) bool {
  // TODO: Split one write into multiple RPC
  length := uint64(len(bytes))
  startChunkIndex := offset / ChunkSize
  endChunkIndex := (offset + length - 1) / ChunkSize // inclusive
  startIdx := uint64(0)
  for i := startChunkIndex; i <= endChunkIndex; i++ {
    startOffset := uint64(0)
    endOffset := uint64(ChunkSize) // exclusive
    if i == startChunkIndex {
      startOffset = offset % ChunkSize
    }
    if i == endChunkIndex {
      endOffset = (offset + length) % ChunkSize
    }
    c.write(path, i, startOffset, endOffset, bytes[startIdx:startIdx+endOffset-startOffset])
    startIdx += endOffset - startOffset
  }
  return true
}

// Read file at a specific offset
func (c *Client) Read(path string, offset uint64, length uint64) ([]byte, error) {
  startChunkIndex := offset / ChunkSize
  endChunkIndex := (offset + length - 1) / ChunkSize // inclusive
  startIdx := uint64(0) // start index at a chunk
  bytes := make([]byte, length)
  for i := startChunkIndex; i <= endChunkIndex; i++ {
    startOffset := uint64(0)
    endOffset := uint64(ChunkSize) // exclusive
    if i == startChunkIndex {
      startOffset = offset % ChunkSize
    }
    if i == endChunkIndex {
      endOffset = (offset + length) % ChunkSize
    }
    b, err := c.read(path, i, startOffset, endOffset)
    if err != nil {
      return bytes, err
    }
    for j := 0; j < len(b); j++ {
      bytes[int(startIdx) + j] = b[j]
    }
    startIdx += endOffset - startOffset
  }
  return bytes, nil
}

func (c *Client) read(path string, chunkindex, start, end uint64) ([]byte, error) {
  // Get chunkhandle and locations
  // TODO: Cache chunk handle and location
  fmt.Println(c.clientId, "read", path, chunkindex, start, end)
  reply := c.findChunkLocations(path, chunkindex)
  cs := reply.ChunkLocations[0] // TODO: Use random location for load balance
  // TODO: Fault tolerance (e.g. chunk server down)
  args := ReadArgs{
    ChunkHandle: reply.ChunkHandle,
    Offset: start,
    Length: end - start,
  }
  resp := new(ReadReply)
  call(cs, "ChunkServer.Read", args, resp)
  return resp.Bytes, nil // TODO: Error handling
}

func (c *Client) write(path string, chunkindex, start, end uint64, bytes []byte) bool {
  // TODO: first try to get from cache.
  // Get chunkhandle and locations.
  fmt.Println(c.clientId, "write", path, chunkindex, start, end, string(bytes)) // For auditing
  reply := c.findChunkLocations(path, chunkindex)
  // Contact chunk location directly and apply the write
  for _, cs := range reply.ChunkLocations {
    args := WriteArgs{
      ChunkHandle: reply.ChunkHandle,
      Offset: start,
      Bytes: bytes,
    }
    reply := new(WriteReply)
    call(cs, "ChunkServer.Write", args, reply)
  }
  // Error handling.
  return true
}

// Find chunkhandle and chunk locations given filename and chunkindex
func (c *Client) findChunkLocations(path string, chunkindex uint64) FindLocationsReply {
  args := FindLocationsArgs{
    Path: path,
    ChunkIndex: chunkindex,
  }
  reply := new(FindLocationsReply)
  call(c.masterAddr, "MasterServer.FindLocations", args, reply)
  return *reply
}

// Helper functions declared here
