package simplegfs

import (
  "bufio" // For reading lines from a file
  "errors"
  "fmt"
  "github.com/wweiw/simplegfs/master"
  sgfsErr "github.com/wweiw/simplegfs/error"
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

  chunkservers map[string]time.Time
  file2chunkhandle map[string](map[uint64]uint64)
  files map[string]*FileInfo // Stores file to information mapping

  // Filename -> clientLease
  file2ClientLease map[string]clientLease

  // Namespace manager
  namespaceManager *master.NamespaceManager

  // Chunk server lease management related fields.
  // Map chunkhandle to its replica locations, primary location, and lease time.
  ckhandle2locLease map[uint64]locationsAndLease
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
  ms.chunkservers[args.Addr] = time.Now()
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
  ms.file2chunkhandle[args] = make(map[uint64]uint64)
  ms.files[args] = &FileInfo{}
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
  ms.mutex.Lock()
  defer ms.mutex.Unlock()
  fmt.Println("Find Locations RPC")
  path := args.Path
  chunkindex := args.ChunkIndex
  val, ok := ms.file2chunkhandle[path]
  if !ok {
    // Filename not found.
    return errors.New("File not found.")
  }
  handle, ok2 := val[chunkindex]
  if !ok2 {
    // Chunk not found.
    return errors.New("Chunk locations not found.")
  }

  // Set reply message.
  reply.ChunkHandle = handle
  reply.ChunkLocations = ms.ckhandle2locLease[handle].replicas
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
  ms.mutex.Lock()
  defer ms.mutex.Unlock()
  fmt.Println("MasterServer: FindLeaseHolder RPC")

  // If no current lease holer, select primary and grant lease to the primary.
  ms.grantLease(args.ChunkHandle)

  // Set reply message.
  reply.Primary = ms.ckhandle2locLease[args.ChunkHandle].primary
  reply.LeaseEnds = ms.ckhandle2locLease[args.ChunkHandle].leaseEnds

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
    return sgfsErr.ErrChunkExist
  }
  handle := ms.chunkhandle
  ms.chunkhandle++
  chunks[chunkIndex] = handle
  reply.ChunkHandle = handle
  chunkLocations := getRandomLocations(ms.chunkservers, 3)
  ms.ckhandle2locLease[handle] = locationsAndLease{
    // Primary is unassigned untill client RPCs FindLocation.
    primary: "",
    replicas: chunkLocations,
    // Lease is invalid when chunk initially added.
    leaseEnds: time.Now(),
  }
  reply.ChunkLocations = chunkLocations
  storeServerMeta(ms)
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
    return errors.New("Cannot grant lease to client " +
                      strconv.FormatUint(clientId, 10) +
                      ". Lease held by client " +
                      strconv.FormatUint(val.clientId, 10) + ".")
  }
  val = clientLease{
    clientId: clientId,
    softLimit: time.Now().Add(SoftLeaseTime),
    hardLimit: time.Now().Add(HardLeaseTime),
  }
  ms.file2ClientLease[path] = val
  reply.SoftLimit = val.softLimit
  reply.HardLimit = val.hardLimit
  fmt.Println("New lease granted to client", clientId, "for file", path,
              "expires at", val.softLimit.Unix())
  return nil
}

// Tell the server to shut itself down
// for testing
func (ms *MasterServer) Kill() {
  ms.dead = true
  ms.l.Close()
}

func StartMasterServer(me string) *MasterServer {
  ms := &MasterServer{
    me: me,
    serverMeta: "serverMeta" + me,
    clientId: 1,
    chunkhandle: 1,
    chunkservers: make(map[string]time.Time),
    file2chunkhandle: make(map[string](map[uint64]uint64)),
    ckhandle2locLease: make(map[uint64]locationsAndLease),
    files: make(map[string]*FileInfo),
    namespaceManager: master.NewNamespaceManager(),
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

// MasterServer.grantLease
//
// Called by MasterServer.FindLeaseHolder. First checks if there is a valid
// lease on the chunkhandle, if so return. If not, picks one replica as
// primary, then grant lease to the primary.
//
// Note: ms.mutex is already held by MasterServer.FindLeaseHolder.
//
// params - chunkhandle: Unique ID of the chunk being requested lease on.
// return - None.
func (ms *MasterServer) grantLease(chunkhandle uint64) {
  // No more work to do if there is a valid lease already.
  if ms.checkLease(chunkhandle) {
    return
  }

  // No valid lease, must grant a new one.
  locLease := ms.ckhandle2locLease[chunkhandle]
  locLease.primary = locLease.replicas[0]
  locLease.leaseEnds = time.Now().Add(LeaseTimeout)
  ms.ckhandle2locLease[chunkhandle] = locLease
}

// MasterServer.checkLease
//
// Called by MasterServer.grantLease. Checks if the given chunk already has
// an valid unexpired lease.
//
// Note: ms.mutex is already held by MasterServer.FindLeaseHolder.
//
// params - chunkhandle: Unique ID of the chunk being requested lease on.
// return - True if there is a valid lease, false otherwise.
func (ms *MasterServer) checkLease(chunkhandle uint64) bool {
  lease, ok := ms.ckhandle2locLease[chunkhandle]
  if !ok {
    log.Fatal("Chunkhandle to locationsAndLease mapping not found.")
  }

  _, ok = ms.chunkservers[lease.primary]
  // If primary is not a valid chunkserver that connected with the master,
  // return false.
  if !ok {
    return false
  }

  // If lease on the primary has already expired, return false.
  if lease.leaseEnds.Before(time.Now()) {
    return false
  }

  return true
}

// MasterServer.csExtendLease
//
// Called by Hearbeat RPC handler, when chunkservers heartbeat message includes
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
  ms.mutex.Lock()
  defer ms.mutex.Unlock()
  // For each of the lease extension requests
  for _, chunkhandle := range chunks {
    locLease, ok := ms.ckhandle2locLease[chunkhandle]
    // If the entry exists and the current lease holder is the requesting
    // chunkserver, extend the lease.
    if ok && locLease.primary == cs {
      locLease.leaseEnds = time.Now().Add(LeaseTimeout)
      ms.ckhandle2locLease[chunkhandle] = locLease
    }
  }
}
