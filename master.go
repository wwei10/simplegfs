package simplegfs

import (
  "bufio" // For reading lines from a file
  "fmt"
  "github.com/wweiw/simplegfs/master"
  "log"
  "net"
  "net/rpc"
  "os" // For os file operations
  "strconv" // For string conversion to/from basic data types
  "strings" // For parsing serverMeta
  "sync"
  "time"
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

  // Namespace manager
  namespaceManager *master.NamespaceManager

  // Chunk manager
  chunkManager *master.ChunkManager
}

// Client lease management
type clientLease struct {
  clientId uint64 // The client who holds the lease before softLimit expires
  softLimit time.Time // The lease ends at softLimit, can be extended
  hardLimit time.Time // The hard limit on how long a client can have the lease
}

// Used for master's mapping from chunkhandle to locations and lease.
type locationsAndLease struct {
  primary string // Selected by master, holds and renews lease on the chunk.
  replicas []string // Chunkservers' addresses that store the chunk.
  leaseEnds time.Time // Time of when the lease expires.
}

// RPC call handlers declared here

// Heartbeat RPC handler for interactions between master
// and chunkservers.
func (ms *MasterServer) Heartbeat(args *HeartbeatArgs,
                                  reply *HeartbeatReply) error {
  // First process heartbeat message.
  ms.chunkManager.HandleHeartbeat(args.Addr)
  // Deal with lease extensions.
  if len(args.PendingExtensions) > 0 {
    ms.csExtendLease(args.Addr, args.PendingExtensions)
  }
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
  ok, err := ms.namespaceManager.Create(args)
  if !ok {
    *reply = false
    return err
  }
  *reply = true
  return nil
}

// Client calls Mkdir to make a new directory.
func (ms *MasterServer) Mkdir(args string,
                              reply *bool) error {
  ok, err := ms.namespaceManager.Mkdir(args)
  if !ok {
    *reply = false
    return err
  }
  *reply = true
  return nil
}

// List all files or directories under a specific directory.
// Returns empty []string when the argument is not a directory
// or it contains no files and directories.
func (ms *MasterServer) List(args string,
                             reply *ListReply) error {
  paths, err := ms.namespaceManager.List(args)
  reply.Paths = paths
  return err
}

// Delete a file or directory.
// This operation will succeeds only if it is a valid path and
// it contains no children.
func (ms *MasterServer) Delete(args string,
                               reply *bool) error {
  ok, err := ms.namespaceManager.Delete(args)
  *reply = ok
  return err
}

// MasterServer.FindLocations
//
// Client calls FindLoations to get chunk locations given file name and chunk
// index.
//
// param  - FindLocationsArgs: Path, name of the file.
//                             ChunkIndex, chunk index in file.
//          FindLocationsReply: ChunkHandle, unique id of a chunk.
//                              ChunkLocations, chunkserver address that hold
//                              the target chunk.
//                              PrimaryLocation, address of the primary replica.
// return - Appropriate error if any, nil otherwise.
func (ms *MasterServer) FindLocations(args FindLocationsArgs,
                                      reply *FindLocationsReply) error {
  fmt.Println("Find Locations RPC")
  path := args.Path
  chunkIndex := args.ChunkIndex
  info, err := ms.chunkManager.FindLocations(path, chunkIndex)
  if err != nil {
    return err
  }
  // Set reply message.
  reply.ChunkHandle = info.Handle
  reply.ChunkLocations = info.Locations
  return nil
}

// MasterServer.FindLeaseHolder
//
// Client calls FindLeaseHolder to get the primary chunkserver for a given
// chunkhandle. If there is no current lease holder, MasterServer.grantLease
// will automatically select one of the replicas to be the primary, and grant
// lease to that primary.
//
// params - FindLeaseHolderArgs: ChunkHandle, unique id of the target chunk.
//          FindLeaseHolderReply: Primary, the lease holder of the target chunk.
//                                LeaseEnds, the lease expiration time.
// return - nil.
func (ms *MasterServer) FindLeaseHolder(args FindLeaseHolderArgs,
                                        reply *FindLeaseHolderReply) error {
  fmt.Println("MasterServer: FindLeaseHolder RPC")
  lease, err := ms.chunkManager.FindLeaseHolder(args.ChunkHandle)
  if err != nil {
    return err
  }
  reply.Primary = lease.Primary
  reply.LeaseEnds = lease.Expiration
  return nil
}

// Client calls AddChunk to get a new chunk.
func (ms *MasterServer) AddChunk(args AddChunkArgs,
                                 reply *AddChunkReply) error {
  fmt.Println(ms.me + " Add chunk RPC")
  path := args.Path
  chunkIndex := args.ChunkIndex
  info, err := ms.chunkManager.AddChunk(path, chunkIndex)
  if err != nil {
    return err
  }
  reply.ChunkHandle = info.Handle
  reply.ChunkLocations = info.Locations
  return nil
}

// Chunk server calls ReportChunk to tell the master
// they have a certain chunk and the number of defined bytes in
// the chunk.
func (ms *MasterServer) ReportChunk(args ReportChunkArgs,
                                    reply *ReportChunkReply) error {
  fmt.Println("MasterServer: Report Chunk.")
  length := args.Length
  handle := args.ChunkHandle
  server := args.ServerAddress
  pathIndex, err := ms.chunkManager.GetPathIndexFromHandle(handle)
  if err != nil {
    return err
  }
  ms.chunkManager.SetChunkLocation(handle, server)
  // Update file information
  fileLength, err := ms.namespaceManager.GetFileLength(pathIndex.Path)
  if err != nil {
    return err
  }
  calculated := int64(ChunkSize * pathIndex.Index) + length
  fmt.Println("Result", calculated, "index", pathIndex.Index, "length", length)
  if calculated > fileLength {
    ms.namespaceManager.SetFileLength(pathIndex.Path, calculated)
    fmt.Println("#### New length:", calculated)
  }
  return nil
}

func (ms *MasterServer) GetFileLength(args string, reply *int64) error {
  fmt.Println("MasterServer: GetFileLength")
  length, err := ms.namespaceManager.GetFileLength(args)
  if err != nil {
    return err
  }
  *reply = length
  return nil
}

// Tell the server to shut itself down
// for testing
func (ms *MasterServer) Kill() {
  ms.dead = true
  ms.l.Close()
  ms.chunkManager.Stop()
}

func StartMasterServer(me string, servers []string) *MasterServer {
  ms := &MasterServer{
    me: me,
    serverMeta: "serverMeta" + me,
    clientId: 1,
    chunkhandle: 1,
    namespaceManager: master.NewNamespaceManager(),
    chunkManager: master.NewChunkManager(servers),
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

// tick() is called once per PingInterval to
// handle background tasks
func (ms *MasterServer) tick() {
  // TODO: Scan in-memory data structures to find dead chunk servers
  // Remove dead servers from in-memory data structures.
  ms.chunkManager.HeartbeatCheck()
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
    default:
      log.Fatal("Unknown serverMeta key.")
    }
  }
}

// MasterServer.csExtendLease
//
// Called by Heartbeat RPC handler, when chunkservers heartbeat message includes
// lease extension requests.
// Lease extensions are only granted when the requesting chunkserver is the
// primary replica.
// This function acquires MasterServer.mutex.
//
// params - cs: Chunkserver's address.
//          chunks: A list of chunkhandles the chunkserver wants lease
//                  extensions on.
// return - None.
func (ms *MasterServer) csExtendLease(cs string, chunks []uint64) {
  ms.chunkManager.ExtendLease(cs, chunks)
}
