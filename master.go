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

// RPC call handler
func (ms *MasterServer) Heartbeat(args *HeartbeatArgs,
                                  reply *HeartbeatReply) error {
  ms.chunkservers[args.Addr] = time.Now()
  reply.Reply = "Hello, world."
  return nil
}

func (ms *MasterServer) NewClientId(args *struct{},
                                    reply *NewClientIdReply) error {
  ms.mutex.Lock()
  defer ms.mutex.Unlock()
  reply.ClientId = ms.clientId
  ms.clientId++
  storeServerMeta(ms)
  return nil
}

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
  *reply = true
  return nil
}

func (ms *MasterServer) FindLocations(args FindLocationsArgs,
                                      reply *FindLocationsReply) error {
  // TODO
  fmt.Println("Find Locations RPC")
  path := args.Path
  chunkindex := args.ChunkIndex
  if val, ok := ms.file2chunkhandle[path]; ok {
    if handle, ok2 := val[chunkindex]; ok2 {
      reply.ChunkHandle = handle
      reply.ChunkLocations = ms.chunkhandle2locations[handle]
      return nil
    } else {
      // Chunk index not found, create new entry
      ms.mutex.Lock()
      defer ms.mutex.Unlock()
      handle = ms.chunkhandle
      ms.chunkhandle++
      val[chunkindex] = handle
      reply.ChunkHandle = handle
      chunklocations := getRandomLocations(ms.chunkservers, 3)
      fmt.Println("Random chunk locations", chunklocations)
      ms.chunkhandle2locations[handle] = chunklocations
      reply.ChunkLocations = chunklocations
      fmt.Println("chunk index not found")
      return nil
    }
  } else {
    // Filename not found
    return errors.New("file not found")
  }
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
// param  - args: a NewLeaseArgs which contains a clientId and a filename
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
