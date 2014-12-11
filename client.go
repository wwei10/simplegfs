package simplegfs

import (
  "fmt"
  "github.com/wweiw/simplegfs/pkg/cache"
  log "github.com/Sirupsen/logrus"
  "time"
  sgfsErr "github.com/wweiw/simplegfs/error"
)

type Client struct {
  masterAddr string
  clientId uint64
  locationCache *cache.Cache
  leaseHolderCache *cache.Cache
}

func NewClient(masterAddr string) *Client {
  c := &Client{
    masterAddr: masterAddr,
    locationCache: cache.New(CacheTimeout, CacheGCInterval),
    leaseHolderCache: cache.New(CacheTimeout, CacheGCInterval),
  }
  reply := &NewClientIdReply{}
  call(masterAddr, "MasterServer.NewClientId", struct{}{}, reply)
  c.clientId = reply.ClientId
  return c
}

// Client APIs

// Create a file
func (c *Client) Create(path string) (bool, error) {
  // TODO: Error handling
  reply := new(bool)
  err := call(c.masterAddr, "MasterServer.Create", path, reply)
  return *reply, err
}

// Mkdir
func (c *Client) Mkdir(path string) (bool, error) {
  reply := new(bool)
  err := call(c.masterAddr, "MasterServer.Mkdir", path, reply)
  return *reply, err
}

// List dir
func (c *Client) List(path string) ([]string, error) {
  reply := new(ListReply)
  err := call(c.masterAddr, "MasterServer.List", path, reply)
  return reply.Paths, err
}

// Delete a directory or a file
func (c *Client) Delete(path string) (bool, error) {
  reply := new(bool)
  err := call(c.masterAddr, "MasterServer.Delete", path, reply)
  return *reply, err
}

// Append writes data to an offset chosen by the primary chunk server.
// Data is only appended if its size if less then AppendSize, which is one
// fourth of ChunkSize.
// Returns (offset chosen by primary, nil) if success, appropriate
// error otherwise.
// The caller must check return error before using the offset.
func (c *Client) Append(path string, data []byte) (uint64, error) {
  // First check if the size is valid.
  if len(data) > AppendSize {
    log.Println("ERROR: Data size exceeds append limit.")
    return 0, sgfsErr.ErrAppendLimitExceeded
  }

  // To calculate chunkIndex we must get the length.
  fileLength, err := c.getFileLength(path)
  if err != nil {
    log.Println("ERROR: Get file length failed.")
    return 0, err
  }
  chunkIndex := uint64(fileLength /  ChunkSize)

  // Get chunkHandle and chunkLocations
  chunkHandle, chunkLocations, err := c.guaranteeChunkLocations(path, chunkIndex)
  if err != nil {
    return 0, err
  }

  // Construct dataId with clientId and current timestamp.
  dataId := DataId{
    ClientId: c.clientId,
    Timestamp: time.Now(),
  }

  // Push data to all replicas' memory.
  err = c.pushData(chunkLocations, dataId, data)
  if err != nil {
    return 0, err
  }

  // Once data is pushed to all replicas, send append request to the primary.
  primary := c.findLeaseHolder(chunkHandle)
  if primary == "" {
    return 0, sgfsErr.ErrLeaseHolderNotFound
  }

  // Construct Append RPC arguments and replies.
  appendArgs := AppendArgs {
    DataId: dataId,
    ChunkHandle: chunkHandle,
    ChunkIndex: chunkIndex,
    Path: path,
    ChunkLocations: chunkLocations,
  }
  appendReply := new(AppendReply)

  // Send Append request.
  err = call(primary, "ChunkServer.Append", appendArgs, appendReply)
  if err != nil {
    // If not enough space on the target chunk for append, retry append
    // request on a new chunk.
    if err.Error() == sgfsErr.ErrNotEnoughSpace.Error() {
      c.Append(path, data)
    } else {
      return 0, err
    }
  }

  return appendReply.Offset, nil
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
    if ok := c.write(path, i, startOffset, endOffset, bytes[startIdx:startIdx+endOffset-startOffset]); !ok {
      return false
    }
    startIdx += endOffset - startOffset
  }
  return true
}

// Read file at a specific offset
func (c *Client) Read(path string, offset uint64, bytes []byte) (n int, err error) {
  fileLength, err := c.getFileLength(path)
  if err != nil {
    return 0, err
  }
  // If file length is zero, do nothing.
  if fileLength == 0 {
    return 0, err
  }
  length := uint64(len(bytes))
  limit := min(offset + length, uint64(fileLength)) // Read should not exceed the boundary.
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

// Release any resources held by client here.
func (c *Client) Stop() {
  c.locationCache.Stop()
  c.leaseHolderCache.Stop()
}

func (c *Client) read(path string, chunkIndex, start uint64,
                      bytes []byte) (n int, err error) {
  // Get chunkhandle and locations
  length := uint64(len(bytes))
  log.Debugln(c.clientId, "read", path, chunkIndex, start, len(bytes))
  chunkHandle, chunkLocations, err := c.findChunkLocations(path, chunkIndex)
  if err != nil {
    // TODO: Error handling. Define error code or something.
    return 0, nil
  }
  cs := chunkLocations[0] // TODO: Use random location for load balance
  // TODO: Fault tolerance (e.g. chunk server down)
  args := ReadArgs{
    ChunkHandle: chunkHandle,
    Offset: int64(start),
    Length: length,
  }
  resp := new(ReadReply)
  resp.Bytes = bytes
  call(cs, "ChunkServer.Read", args, resp)
  return resp.Length, nil // TODO: Error handling
}

// The pushData function pushes data to all replica's memory through RPC.
func (c *Client) pushData(chunkLocations []string, dataId DataId, data []byte) error {
  // Push data to each replica's memory.
  for _, cs := range chunkLocations {
    pushDataReply := new(PushDataReply)
    if err := call(cs, "ChunkServer.PushData", &PushDataArgs{dataId, data},
                   pushDataReply); err != nil {
      return err
    }
  }
  return nil
}

// The guaranteeChunkLocations takes in a path name and a chunkIndex and
// guarantees to return a chunkHandle and chunkLocations unless some
// unexpected error occurs and the operation is not retriable. It does this
// by first looking for chunk locations normally, if not found, tries to
// add the chunk to master server.
func (c *Client) guaranteeChunkLocations(path string, chunkIndex uint64) (uint64, []string, error) {
  // Find locations
  chunkHandle, chunkLocations, err := c.findChunkLocations(path, chunkIndex)

  // If cannot find chunk, add the chunk.
  if err != nil {
    if err.Error() == sgfsErr.ErrKeyNotFound.Error() {
      return 0, []string{}, err
    }
    chunkHandle, chunkLocations, err = c.addChunk(path, chunkIndex)
  }

  // Other client might have added the chunk simultaneously,
  // must check error code. If it already exists, find the location again.
  if err != nil {
    log.Debugln(err)
    if err.Error() == sgfsErr.ErrChunkExist.Error() {
      chunkHandle, chunkLocations, err = c.findChunkLocations(path, chunkIndex)
    }
  }

  // Either some other err occurred during add Chunk, or the second
  // findChunkLocation fails.
  if err != nil {
    return chunkHandle, chunkLocations, err
  }

  // If no unexpected error has occurred.
  return chunkHandle, chunkLocations, nil
}

func (c *Client) write(path string, chunkIndex, start, end uint64,
                       bytes []byte) bool {
  // Get chunkhandle and locations.
  // For auditing
  fmt.Println(c.clientId, "write", path, chunkIndex, start, end, string(bytes))

  // Get chunkHandle and chunkLocations
  chunkHandle, chunkLocations, err := c.guaranteeChunkLocations(path, chunkIndex)
  if err != nil {
    return false
  }

  // Construct dataId with clientId and current timestamp.
  dataId := DataId{
    ClientId: c.clientId,
    Timestamp: time.Now(),
  }

  // Push data to all replicas' memory.
  err = c.pushData(chunkLocations, dataId, bytes)
  if err != nil {
    log.Println("Data did not push to all replicas.")
    return false
  }

  // Once data is pushed to all replicas, send write request to the primary.
  primary := c.findLeaseHolder(chunkHandle)
  if primary == "" {
    log.Println("Primary chunk server not found.")
    return false
  }

  writeArgs := WriteArgs{
    DataId: dataId,
    Path: path,
    ChunkIndex: chunkIndex,
    ChunkHandle: chunkHandle,
    Offset: start,
    ChunkLocations: chunkLocations,
  }
  writeReply := new(WriteReply)
  if err := call(primary, "ChunkServer.Write", writeArgs,
                 writeReply); err != nil {
    return false
  }
  return true
}

func (c *Client) addChunk(path string, chunkIndex uint64) (uint64, []string,
                                                           error) {
  args := AddChunkArgs{
    Path: path,
    ChunkIndex: chunkIndex,
  }
  reply := new(AddChunkReply)
  err := call(c.masterAddr, "MasterServer.AddChunk", args, reply)
  return reply.ChunkHandle, reply.ChunkLocations, err
}

// Find chunkhandle and chunk locations given filename and chunkIndex
func (c *Client) findChunkLocations(path string, chunkIndex uint64) (uint64, []string, error) {
  key := fmt.Sprintf("%s,%d", path, chunkIndex)
  value, ok := c.locationCache.Get(key)
  if ok {
    reply := value.(*FindLocationsReply)
    return reply.ChunkHandle, reply.ChunkLocations, nil
  }
  args := FindLocationsArgs{
    Path: path,
    ChunkIndex: chunkIndex,
  }
  reply := new(FindLocationsReply)
  err := call(c.masterAddr, "MasterServer.FindLocations", args, reply)
  if err == nil {
    // Set cache entry to the answers we get.
    c.locationCache.Set(key, reply)
  }
  return reply.ChunkHandle, reply.ChunkLocations, err
}

// Client.findLeaseHolder
//
// First check with Client.leaseHolderCache, if not found, RPC master server
// with chunkhandle to find the current lease holder of the target chunk.
//
// params - chunkhandle: Unique ID of the target chunk.
// return - string: Location of the primary chunkserver if successful, nil
//                  otherwise.
func (c *Client) findLeaseHolder(chunkhandle uint64) string {
  // First check with the leaseHolderCache
  key := fmt.Sprintf("%d", chunkhandle)
  value, ok := c.leaseHolderCache.Get(key)
  if ok {
    reply := value.(*FindLeaseHolderReply)
    return reply.Primary
  }

  // If not found in cache, RPC the master server.
  args := FindLeaseHolderArgs{
    ChunkHandle: chunkhandle,
  }
  reply := new(FindLeaseHolderReply)
  err := call(c.masterAddr, "MasterServer.FindLeaseHolder", args, reply)
  if err == nil {
    // Cache lease holder, set cache entry expiration time to lease expiration
    // time.
    c.leaseHolderCache.SetWithTimeout(key, reply,
                                      reply.LeaseEnds.Sub(time.Now()))
    return reply.Primary
  }

  return ""
}

func (c *Client) getFileLength(path string) (int64, error) {
  args := path
  reply := new(int64)
  ok := call(c.masterAddr, "MasterServer.GetFileLength", args, reply)
  log.Debugln(path, "file length:", *reply)
  return *reply, ok
}
