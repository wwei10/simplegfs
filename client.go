package simplegfs

import (
  "errors"
  "fmt"
  "github.com/wweiw/simplegfs/pkg/cache"
  "time"
  "sync"
  "log"
)

type Client struct {
  masterAddr string
  clientId uint64
  locationCache *cache.Cache
  leaseHolderCache *cache.Cache
  file2Lease map[string]lease

  // Stores file name of the files that the client is trying to request lease
  // extension on.
  pendingExtension []string
  // Lease lock for pendingextension and file2Lease
  leaseMutex sync.RWMutex
}

type lease struct {
  softLimit time.Time
  hardLimit time.Time
}

func NewClient(masterAddr string) *Client {
  c := &Client{
    masterAddr: masterAddr,
    locationCache: cache.New(CacheTimeout, CacheGCInterval),
    leaseHolderCache: cache.New(CacheTimeout, CacheGCInterval),
    file2Lease: make(map[string]lease),
  }
  reply := &NewClientIdReply{}
  call(masterAddr, "MasterServer.NewClientId", struct{}{}, reply)
  c.clientId = reply.ClientId
  go c.leaseManager()
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

// Mkdir
func (c *Client) Mkdir(path string) bool {
  reply := new(bool)
  call(c.masterAddr, "MasterServer.Mkdir", path, reply)
  return *reply
}

// List dir
func (c *Client) List(path string) []string {
  reply := new(ListReply)
  call(c.masterAddr, "MasterServer.List", path, reply)
  return reply.Paths
}

// Delete a directory or a file
func (c *Client) Delete(path string) bool {
  reply := new(bool)
  call(c.masterAddr, "MasterServer.Delete", path, reply)
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

// Release any resources held by client here.
func (c *Client) Stop() {
  c.locationCache.Stop()
  c.leaseHolderCache.Stop()
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
    c.blockOnLease(&path)
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
  key := fmt.Sprintf("%s,%d", path, chunkindex)
  value, ok := c.locationCache.Get(key)
  if ok {
    reply := value.(*FindLocationsReply)
    return *reply, ok
  }
  args := FindLocationsArgs{
    Path: path,
    ChunkIndex: chunkindex,
  }
  reply := new(FindLocationsReply)
  ok = call(c.masterAddr, "MasterServer.FindLocations", args, reply)
  if ok {
    // Set cache entry to the answers we get.
    c.locationCache.Set(key, reply)
  }
  return *reply, ok
}

func (c *Client) getFileInfo(path string) (FileInfo, bool) {
  args := GetFileInfoArgs{path}
  reply := new(GetFileInfoReply)
  ok := call(c.masterAddr, "MasterServer.GetFileInfo", args, reply)
  fmt.Println(path, "file information:", reply.Info)
  return reply.Info, ok
}

// Client.blockOnLease
//
// A wrapper function for Client.requestLease, automatically blocks untill
// the caller has lease on a file.
//
// param  - path: A pointer to the name of the file.
// return - None.
func (c *Client) blockOnLease(path *string) {
  hasLease := c.requestLease(path)
  for !hasLease {
    time.Sleep(SoftLeaseTime)
    hasLease = c.requestLease(path)
  }
}

// Client.requestLease
//
// Client should call this funtion whenever it tries to modify a file.
// RequestLease first checks if the client already has the lease, if so,
// simply return to client. If the client doesn't hold the lease, it requests
// lease from master server, update file->lease mapping.
// This function does not block, instead the caller should block untill it
// has the lease.
//
// param  - path: A pointer to the name of the file.
// return - True if the client has the lease, false otherwise.
func (c *Client) requestLease(path *string) bool {
  // Return if client already holds the lease.
  if c.checkLease(path) {
    // Automatically extends the lease while the client is still writing
    // to the file.
    c.extendLease(path)
    return true
  }

  // The client does not hold the lease, request it from master.
  args := NewLeaseArgs {
    ClientId: c.clientId,
    Path: *path,
  }

  // Return false if we cannot get a new lease from master.
  reply := new(NewLeaseReply)
  if ok := call(c.masterAddr, "MasterServer.NewLease", args, reply); !ok {
    // Failed to request lease.
    return false
  }

  // Got the lease, now update file -> lease mapping in client.
  newLease := lease {
    softLimit: reply.SoftLimit,
    hardLimit: reply.HardLimit,
  }
  c.leaseMutex.Lock()
  defer c.leaseMutex.Unlock()
  c.file2Lease[*path] = newLease
  return true
}

// Client.checkLease
//
// Called by Client.requestLease to check if the client holds a lease to a file.
//
// param  - path: A pointer to the name of the file.
// return - True if client holds the lease, false otherwise.
func (c *Client) checkLease(path *string) bool {
  c.leaseMutex.RLock()
  defer c.leaseMutex.RUnlock()
  val, ok := c.file2Lease[*path]
  // The client does not hold the lease.
  if !ok {
    return false
  }
  // The client used to hold the lease, but it has expired.
  if val.softLimit.Before(time.Now()) {
    return false
  }
  return true
}

// Client.extendLease
//
// Called by Client.requestLease when the client already has a lease.
// This function adds the requested file into Client.pendingExtension, the
// extension requests will be batched and later be sent by client's regular
// lease extension RPCs.
//
// param  - path: A pointer to the name of the file.
// return - None.
func (c *Client) extendLease(path *string) {
  c.leaseMutex.Lock()
  defer c.leaseMutex.Unlock()
  // If a pending extension request already exists then we don't need to add
  // more.
  for _, val := range c.pendingExtension {
    if val == *path {
      return
    }
  }
  c.pendingExtension = append(c.pendingExtension, *path)
}

// Client.leaseManager
//
// A lease management thread that sends out lease extension requests every
// ExtensionRequestInterval to master server if there is any pending extension
// requests.
//
// param  - None.
// return - None.
func (c *Client) leaseManager() {
  for {
    time.Sleep(ExtensionRequestInterval)

    // Only sent RPC request to master if there is pending extension requests.
    c.leaseMutex.RLock()
    if len(c.pendingExtension) > 0 {
      c.leaseMutex.RUnlock()
      args := ExtendLeaseArgs{
        ClientId: c.clientId,
        Paths: c.pendingExtension,
      }
      reply := new(ExtendLeaseReply)
      call(c.masterAddr, "MasterServer.ExtendLease", args, reply)

      // Clear pending extension requests and update related lease
      c.leaseMutex.Lock()
      c.pendingExtension = nil
      for path, softLimit := range reply.File2SoftLimit {
        // Update lease only if extension requests were granted by master.
        if softLimit.After(time.Now()) {
          lease, ok := c.file2Lease[path]
          if !ok {
            // No mapping exist means the client has never had a lease on this
            // file, therefore there is no way we could be requesting an
            // extension on that file. Something went seriously wrong.
            log.Fatal("Lease extension granted on files without lease.")
          }
          lease.softLimit = softLimit
          c.file2Lease[path] = lease
        }
      }
      c.leaseMutex.Unlock()
    } else {
      c.leaseMutex.RUnlock()
    }
  }
}
