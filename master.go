package simplegfs

import (
  "errors"
  "fmt"
  "log"
  "net"
  "net/rpc"
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
  chunkservers map[string]time.Time
  file2chunkhandle map[string](map[uint64]uint64)
  chunkhandle2locations map[uint64][]string
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

func StartMasterServer(me string) *MasterServer {
  ms := &MasterServer{
    me: me,
    clientId: 1,
    chunkhandle: 1,
    chunkservers: make(map[string]time.Time),
    file2chunkhandle: make(map[string](map[uint64]uint64)),
    chunkhandle2locations: make(map[uint64][]string),
  }

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
