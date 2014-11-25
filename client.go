package simplegfs

import (
  "fmt"
)

type Client struct {
  masterAddr string
  clientId uint64
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
func (c *Client) Create(path string) bool {
  // TODO: Error handling
  reply := new(bool)
  call(c.masterAddr, "MasterServer.Create", path, reply)
  return *reply
}

func (c *Client) Write(path string, offset uint64, bytes []byte) bool {
  // TODO: Split one write into multiple RPC
  length := uint64(len(bytes))
  startChunkIndex := offset / ChunkSize
  endChunkIndex := (offset + length) / ChunkSize
  startIdx := uint64(0)
  for i := startChunkIndex; i <= endChunkIndex; i++ {
    startOffset := uint64(0)
    endOffset := uint64(ChunkSize)
    if i == startChunkIndex {
      startOffset = offset % ChunkSize
    }
    if i == endChunkIndex {
      endOffset = (offset + length) % ChunkSize
    }
    c.write(path, startOffset, endOffset, bytes[startIdx:startIdx+endOffset-startOffset])
    startIdx += endOffset - startOffset
  }
  return true
}

func (c *Client) write(path string, start uint64, end uint64, bytes []byte) bool {
  chunkindex := start / ChunkSize
  // TODO: first try to get from cache.
  // Get chunkhandle and locations.
  fmt.Println(c.clientId, "write", path, start, end, bytes) // For auditing
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
