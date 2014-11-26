package simplegfs

import (
  "errors"
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
      if rem := (offset + length) % ChunkSize; rem == 0 {
        endOffset = ChunkSize
      } else {
        endOffset = rem
      }
    }
    c.write(path, i, startOffset, endOffset, bytes[startIdx:startIdx+endOffset-startOffset])
    startIdx += endOffset - startOffset
  }
  return true
}

// Read file at a specific offset
func (c *Client) Read(path string, offset uint64, bytes []byte) (n int, err error) {
  info, ok := c.getFileInfo(path)
  if !ok {
    return 0, errors.New("file not found")
  }
  length := uint64(len(bytes))
  limit := min(offset + length, uint64(info.Length)) // Read should not exceed the boundary.
  startChunkIndex := offset / ChunkSize
  endChunkIndex := (limit - 1) / ChunkSize // inclusive
  startIdx := uint64(0) // start index at a chunk
  total := 0
  for i := startChunkIndex; i <= endChunkIndex; i++ {
    startOffset := uint64(0)
    endOffset := uint64(ChunkSize) // exclusive
    if i == startChunkIndex {
      startOffset = offset % ChunkSize
    }
    if i == endChunkIndex {
      if rem := limit % ChunkSize; rem == 0 {
        endOffset = ChunkSize
      } else {
        endOffset = rem
      }
    }
    n, err = c.read(path, i, startOffset, bytes[startIdx:startIdx+endOffset-startOffset])
    if err != nil {
      return total, err
    }
    total = int(startIdx) + n
    startIdx += endOffset - startOffset
  }
  return int(limit - offset), nil
}

func (c *Client) read(path string, chunkindex, start uint64, bytes []byte) (n int, err error) {
  // Get chunkhandle and locations
  // TODO: Cache chunk handle and location
  length := uint64(len(bytes))
  fmt.Println(c.clientId, "read", path, chunkindex, start, len(bytes))
  reply, ok := c.findChunkLocations(path, chunkindex)
  if !ok {
    // TODO: Error handling. Define error code or something.
    return 0, nil
  }
  cs := reply.ChunkLocations[0] // TODO: Use random location for load balance
  // TODO: Fault tolerance (e.g. chunk server down)
  args := ReadArgs{
    ChunkHandle: reply.ChunkHandle,
    Offset: int64(start),
    Length: length,
  }
  resp := new(ReadReply)
  resp.Bytes = bytes
  call(cs, "ChunkServer.Read", args, resp)
  return resp.Length, nil // TODO: Error handling
}

func (c *Client) write(path string, chunkindex, start, end uint64, bytes []byte) bool {
  // TODO: first try to get from cache.
  // Get chunkhandle and locations.
  fmt.Println(c.clientId, "write", path, chunkindex, start, end, string(bytes)) // For auditing
  reply, ok := c.findChunkLocations(path, chunkindex)
  var chunkHandle uint64
  var chunkLocations []string
  if !ok {
    reply, ok := c.addChunk(path, chunkindex)
    if !ok {
      return false
    }
    chunkHandle = reply.ChunkHandle
    chunkLocations = reply.ChunkLocations
  } else {
  // Contact chunk location directly and apply the write
    chunkHandle = reply.ChunkHandle
    chunkLocations = reply.ChunkLocations
  }
  for _, cs := range chunkLocations {
    args := WriteArgs {
      Path: path,
      ChunkIndex: chunkindex,
      ChunkHandle: chunkHandle,
      Offset: start,
      Bytes: bytes,
    }
    reply := new(WriteReply)
    call(cs, "ChunkServer.Write", args, reply)
  }
  return true
}

func (c *Client) addChunk(path string, chunkIndex uint64) (AddChunkReply, bool) {
  args := AddChunkArgs{
    Path: path,
    ChunkIndex: chunkIndex,
  }
  reply := new(AddChunkReply)
  ok := call(c.masterAddr, "MasterServer.AddChunk", args, reply)
  return *reply, ok
}

// Find chunkhandle and chunk locations given filename and chunkindex
func (c *Client) findChunkLocations(path string, chunkindex uint64) (FindLocationsReply, bool) {
  args := FindLocationsArgs{
    Path: path,
    ChunkIndex: chunkindex,
  }
  reply := new(FindLocationsReply)
  ok := call(c.masterAddr, "MasterServer.FindLocations", args, reply)
  return *reply, ok
}

func (c *Client) getFileInfo(path string) (FileInfo, bool) {
  args := GetFileInfoArgs{path}
  reply := new(GetFileInfoReply)
  ok := call(c.masterAddr, "MasterServer.GetFileInfo", args, reply)
  fmt.Println(path, "file information:", reply.Info)
  return reply.Info, ok
}
