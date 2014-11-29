package simplegfs

import (
  "errors"
  "fmt"
  "log"
  "net"
  "net/rpc"
  "sync"
  "time"
  "os" // For os file operations
  "strconv" // For string conversion to/from basic data types
  "bufio" // For reading lines from a file
  "strings" // For parsing serverMeta
)

type MasterServer struct {
  dead bool
  l net.Listener
  me string // Server address
  clientId uint64 // Client ID
  chunkhandle uint64 // Chunkhandle ID
  mutex sync.RWMutex

  // Filename of a file that contains MasterServer metadata
  serverMeta string

  chunkservers map[string]time.Time
  file2chunkhandle map[string](map[uint64]uint64)
  chunkhandle2locations map[uint64][]string
  files map[string]*FileInfo // Stores file to information mapping

  // Filename -> version number, the highest version number of all the chunks
  // belong to that file
  file2VersionNumber map[string]uint64

  // Filename -> clientLease
  file2ClientLease map[string]clientLease
}

// Client lease management
type clientLease struct {
  clientId uint64 // The client who holds the lease before softLimit expires
  softLimit time.Time // The lease ends at softLimit, can be extended
  hardLimit time.Time // The hard limit on how long a client can have the lease
}

// RPC call handlers declared here

// Heartbeat RPC handler for interactions between master
// and chunkservers.
func (ms *MasterServer) Heartbeat(args *HeartbeatArgs,
                                  reply *HeartbeatReply) error {
  ms.chunkservers[args.Addr] = time.Now()
  reply.Reply = "Hello, world."
  return nil
}

// When a new client is attached to the master,
// it calls NewClientId to get a unique ID.
func (ms *MasterServer) NewClientId(args *struct{},
                                    reply *NewClientIdReply) error {
  ms.mutex.Lock()
  defer ms.mutex.Unlock()
  reply.ClientId = ms.clientId
  ms.clientId++
  storeServerMeta(ms)
  return nil
}

// Client calls Create to create a file in the namespace.
func (ms *MasterServer) Create(args string,
                               reply *bool) error {
  // TODO: error handling
  ms.mutex.Lock()
  defer ms.mutex.Unlock()
  _, ok := ms.file2chunkhandle[args]
  if ok {
    fmt.Println("Existing file.")
    *reply = false
    return nil
  }
  ms.file2chunkhandle[args] = make(map[uint64]uint64)
  ms.files[args] = &FileInfo{}
  *reply = true
  return nil
}

// Client calls FindLoations to get chunk locations
// given file name and chunk index.
func (ms *MasterServer) FindLocations(args FindLocationsArgs,
                                      reply *FindLocationsReply) error {
  ms.mutex.Lock()
  defer ms.mutex.Unlock()
  fmt.Println("Find Locations RPC")
  path := args.Path
  chunkindex := args.ChunkIndex
  val, ok := ms.file2chunkhandle[path]
  if !ok {
    // Filename not found
    return errors.New("file not found")
  }
  handle, ok2 := val[chunkindex]
  if !ok2 {
    // Chunk not found
    return errors.New("chunk locations not found")
  }
  reply.ChunkHandle = handle
  reply.ChunkLocations = ms.chunkhandle2locations[handle]
  return nil
}

// Client calls AddChunk to get a new chunk.
func (ms *MasterServer) AddChunk(args AddChunkArgs,
                                 reply *AddChunkReply) error {
  ms.mutex.Lock()
  defer ms.mutex.Unlock()
  fmt.Println(ms.me + " Add chunk RPC")
  path := args.Path
  chunkIndex := args.ChunkIndex
  chunks, ok := ms.file2chunkhandle[path]
  if !ok {
    return errors.New("file not found.")
  }
  _, ok = chunks[chunkIndex]
  if ok {
    return errors.New("chunk already exists")
  }
  handle := ms.chunkhandle
  ms.chunkhandle++
  chunks[chunkIndex] = handle
  reply.ChunkHandle = handle
  chunkLocations := getRandomLocations(ms.chunkservers, 3)
  ms.chunkhandle2locations[handle] = chunkLocations
  reply.ChunkLocations = chunkLocations
  return nil
}

// Chunk server calls ReportChunk to tell the master
// they have a certain chunk and the number of defined bytes in
// the chunk.
func (ms *MasterServer) ReportChunk(args ReportChunkArgs,
                                    reply *ReportChunkReply) error {
  fmt.Println("MasterServer: Report Chunk.")
  length := args.Length
  chunkIndex := args.ChunkIndex
  // chunkHandle := args.ChunkHandle
  // address := args.ServerAddress
  path := args.Path
  // Update file information
  ms.mutex.Lock()
  defer ms.mutex.Unlock()
  info, ok := ms.files[path]
  if !ok {
    return errors.New("file not found.")
  }
  calculated := int64(ChunkSize * chunkIndex) + length
  fmt.Println("Result", calculated, "index", chunkIndex, "length", length)
  if calculated > info.Length {
    info.Length = calculated
    fmt.Println("#### New length:", ms.files[path].Length)
  }
  return nil
}

// Get information about a file.
func (ms *MasterServer) GetFileInfo(args GetFileInfoArgs,
                                    reply *GetFileInfoReply) error {
  fmt.Println("MasterServer: GetFileInfo")
  ms.mutex.RLock()
  defer ms.mutex.RUnlock()
  info, ok := ms.files[args.Path]
  if !ok {
    return errors.New("file not found.")
  }
  reply.Info = *info
  return nil
}

// MasterServer.NewLease
//
// Handles RPC calls from client who requests a new lease on a file.
// A lease can only be granted if the client is not currently holding the
// lease, and no one else is currently holding the lease.
// A client who is currently holding the lease should call
// MasterServer.ExtendLease instead.
//
// param  - args: a NewLeaseArgs which contains a clientId and a filename.
//          reply: a NewLeaseReply which contains a softLimit indicating how
//                 long does the client has the lease, a hardLimit indicating
//                 the longest possible time the client can hold the lease.
// return - appropriate error if any, nil otherwise
func (ms *MasterServer) NewLease(args NewLeaseArgs,
                                 reply *NewLeaseReply) error {
  path := args.Path
  clientId := args.ClientId
  val, ok := ms.file2ClientLease[path]
  // Check to see if anyone(including the requesting client) is currently
  // holding a lease to the requested file.
  if ok && time.Now().Before(val.softLimit) {
    return errors.New("Lease held by other client.")
  }
  val = clientLease{
    clientId: clientId,
    softLimit: time.Now().Add(SoftLeaseTime),
    hardLimit: time.Now().Add(HardLeaseTime),
  }
  ms.file2ClientLease[path] = val
  reply.SoftLimit = val.softLimit
  reply.HardLimit = val.hardLimit
  return nil
}

// Master.ExtendLease
//
// Handles RPC calls from client who wishes to extent their existing lease.
// A lease can only be extended when the requesting client is currently holding
// the lease, and the extended lease does not exceed the hard limit on that
// lease.
//
// param  - args: ExtentLeaseArgs {ClientId, Paths}. Paths is an array of
//                filenames the client is requesting extensions on.
//          reply: ExtendLeaseReply {File2SoftLimit}. If the client is eligible
//                 for an extension on a file, then there will be a mapping
//                 from that file to a SoftLimit time, otherwise it will be
//                 mapped to an expired lease time.
// return - nil.
func (ms *MasterServer) ExtendLease(args ExtendLeaseArgs,
                                    reply *ExtendLeaseReply) error {
  reply.File2SoftLimit = make(map[string]time.Time)
  for _, path := range args.Paths {
    val, ok := ms.file2ClientLease[path]

    // If no file->clientLease mapping exists, or the current lease holder is
    // not the requesting client, or the hard limit on the client lease has
    // expired, or if granting extension would reseult in lease exceeding the
    // hard limit, map file to an exipired lease time.
    if !ok || val.clientId != args.ClientId ||
       val.hardLimit.Before(time.Now()) ||
       time.Now().Add(SoftLeaseTime).After(val.hardLimit) {
      reply.File2SoftLimit[path] = time.Now()
    }

    // Client is eligible for a lease extension, update soft limit
    val.softLimit = time.Now().Add(SoftLeaseTime)
    ms.file2ClientLease[path] = val
    reply.File2SoftLimit[path] = val.softLimit
  }
  return nil
}

// Tell the server to shut itself down
// for testing
func (ms *MasterServer) Kill() {
  ms.dead = true
  ms.l.Close()
}

// tick() is called once per PingInterval to
// handle background tasks
func (ms *MasterServer) tick() {
  // TODO: Scan in-memory data structures to find dead chunk servers
}

// Called whenever server's persistent meta data changes.
//
// param  - ms: pointer to a MasterServer instance
// return - none
func storeServerMeta(ms *MasterServer) {
  f, er := os.OpenFile(ms.serverMeta, os.O_RDWR|os.O_CREATE, FilePermRW)
  if er != nil {
    // TODO Use log instead
    fmt.Println("Open/Create file ", ms.serverMeta, " failed.")
  }
  defer f.Close()

  // Write out clientId
  storeClientId(ms, f)
  // Write out chunkhandle
  storeChunkhandle(ms, f)
}

// Store MasterServer.chunkhandle on to MasterServer.serverMeta
//
// parram  - ms: a pointer to a MasterServer instance
//           f: a pointer to os.File serverMeta
// return - none
func storeChunkhandle(ms *MasterServer, f *os.File) {
  n, err := f.WriteString("chunkhandle " +
                          strconv.FormatUint(ms.chunkhandle, 10) + "\n")
  if err != nil {
    fmt.Println(err)
  } else {
    fmt.Printf("Wrote %d bytes to serverMeta\n", n)
  }
}

// Store MasterServer.clientId on to MasterServer.serverMeta
//
// param  - ms: a pointer to a MasterServer instance
//          f: a pointer to os.File serverMeta
// return - none
func storeClientId(ms *MasterServer, f *os.File) {
  n, err := f.WriteString("clientId " +
                          strconv.FormatUint(ms.clientId, 10) + "\n")
  if err != nil {
    fmt.Println(err)
  } else {
    fmt.Printf("Wrote %d bytes to serverMeta\n", n)
  }
}

// Called by StartMasterServer when starting a new MasterServer instance ms,
// loads serverMeta files into ms.
//
// param  - ms: pointer to a MasterServer instance
// return - none
func loadServerMeta(ms *MasterServer) {
  f, err := os.OpenFile(ms.serverMeta, os.O_RDONLY, FilePermRW)
  if err != nil {
    fmt.Println("Open file ", ms.serverMeta, " failed.");
  }
  defer f.Close()
  parseServerMeta(ms, f)
}

// Called by loadServerMeta
// This function parses each line in serverMeta file and loads each value
// into its corresponding MasterServer fields
//
// param  - ms: pointer to a MasterServer instance
//          f: point to the file that contains serverMeta
// return - none
func parseServerMeta(ms *MasterServer, f *os.File) {
  scanner := bufio.NewScanner(f)
  for scanner.Scan() {
    fields := strings.Fields(scanner.Text())
    switch fields[0] {
    case "clientId":
      var err error
      ms.clientId, err = strconv.ParseUint(fields[1], 0, 64)
      if err != nil {
        log.Fatal("Failed to load clientId into ms.clientId")
      }
    case "chunkhandle":
      var err error
      ms.chunkhandle, err = strconv.ParseUint(fields[1], 0, 64)
      if err != nil {
        log.Fatal("Failed to load chunkhandle into ms.chunkhanle")
      }
    default:
      log.Fatal("Unknown serverMeta key.")
    }
  }
}

func StartMasterServer(me string) *MasterServer {
  ms := &MasterServer{
    me: me,
    serverMeta: "serverMeta" + me,
    clientId: 1,
    chunkhandle: 1,
    chunkservers: make(map[string]time.Time),
    file2chunkhandle: make(map[string](map[uint64]uint64)),
    chunkhandle2locations: make(map[uint64][]string),
    files: make(map[string]*FileInfo),
    file2ClientLease: make(map[string]clientLease),
  }

  loadServerMeta(ms)

  rpcs := rpc.NewServer()
  rpcs.Register(ms)

  l, e := net.Listen("tcp", ms.me)
  if e != nil {
    log.Fatal("listen error", e)
  }
  ms.l = l

  // RPC handler
  go func() {
    for ms.dead == false {
      conn, err := ms.l.Accept()
      if err == nil && ms.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      } else if err != nil && ms.dead == false {
        fmt.Println("Kill server.")
        ms.Kill()
      }
    }
  }()

  // Background tasks
  go func() {
    for ms.dead == false {
      ms.tick()
      time.Sleep(HeartbeatInterval)
    }
  }()

  return ms
}


// Helper functions

// Pre-condition: ms.mutex.Lock() is called.
func getRandomLocations(chunkservers map[string]time.Time, num uint) []string {
  ret := make([]string, num)
  // TODO: Better random algorithm
  i := uint(0)
  for cs := range chunkservers {
    if i == num {
      break
    }
    ret[i] = cs
    i++
  }
  return ret
}
