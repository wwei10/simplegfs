package simplegfs

import (
  "fmt"
  "io"
  "time"
  "bytes"
  "encoding/gob"
  "bufio"
  "strings"
  "log"
  "net"
  "net/rpc"
  "os"
  "sync"
)

type ChunkServer struct {
  dead bool
  l net.Listener
  me string
  masterAddr string

  // Chunkhandle -> file version number, updated whenever the chunk is being
  // written to and has a lower file version number than client's file version
  // number.
  chunkhandle2VersionNum map[uint64]uint64

  // Filename of a file that contains chunkserver meta data
  // TODO Should later mutates into a log file of some sort
  chunkServerMeta string

  path string
  chunks map[uint64]ChunkInfo // Store a mapping from handle to information.
  mutex sync.RWMutex

  // Stores pending lease extension requests on chunkhandles.
  pendingExtensions []uint64
  // Only used by Hearbeat and ChunkServer.addChunkExtensionRequest.
  // Possible to have ChunkServer.mutex held first, therefore do not acquire
  // ChunkServer.mutex after acuqire this lock.
  pendingExtensionsLock sync.RWMutex

  // Stores client's data in memory before commit to disk.
  data map[dataId][]byte
  dataMutex sync.RWMutex
}

type dataId struct {
  clientId uint64
  timestamp time.Time
}

// RPC handler declared here

func (cs *ChunkServer) Write(args WriteArgs, reply *WriteReply) error {
  fmt.Println(cs.me, "Write RPC.")
  chunkhandle := args.ChunkHandle
  off := int64(args.Offset)
  bytes := args.Bytes
  length := int64(len(bytes))
  filename := fmt.Sprintf("%d", chunkhandle)
  file, err := os.OpenFile(cs.path + "/" + filename, os.O_WRONLY | os.O_CREATE, 0777)
  if err != nil {
    log.Fatal(err)
  }
  defer file.Close()
  file.WriteAt(bytes, off)
  cs.mutex.Lock()
  defer cs.mutex.Unlock()
  _, ok := cs.chunks[chunkhandle]
  // If we have never seen this chunk before,
  // or chunk size has changed, we should
  // report to Master immediately.
  if !ok {
    cs.chunks[chunkhandle] = ChunkInfo{
      Path: args.Path,
      ChunkHandle: chunkhandle,
      ChunkIndex: args.ChunkIndex,
    }
  }
  chunkInfo := cs.chunks[chunkhandle]
  if off + length > chunkInfo.Length {
    chunkInfo.Length = off + length
    reportChunk(cs, chunkInfo)
  }

  // Since we are still writing to the chunk, we must continue request
  // lease extensions on the chunk.
  cs.addChunkExtensionRequest(chunkhandle)
  return nil
}

func (cs *ChunkServer) Read(args ReadArgs, reply *ReadReply) error {
  fmt.Println(cs.me, "Read RPC.")
  chunkhandle := args.ChunkHandle
  off := args.Offset
  length := args.Length
  bytes := make([]byte, length)
  filename := fmt.Sprintf("%d", chunkhandle)
  file, err := os.Open(cs.path + "/" + filename)
  if err != nil {
    log.Fatal(err)
  }
  defer file.Close()
  n, err := file.ReadAt(bytes, off)
  if err != nil {
    switch err {
    case io.EOF:
      reply.Length = n
      reply.Bytes = bytes
    default:
      log.Fatal(err)
    }
  } else {
    reply.Length = n
    reply.Bytes = bytes
  }
  return nil
}

// Kill for testing.
func (cs *ChunkServer) Kill() {
  cs.dead = true
  cs.l.Close()
}

// ChunkServer.PushData
//
// Handles client RPC to store data in memory. Data is identified with a
// mapping from DataID:[ClientID, Timestamp] -> Data.
// This function acquires lock on ChunkServer.dataMutex
//
// params - PushDataArgs: ClientId, client's ID.
//                        Timestamp, client assigned timestamp, monotonically
//                        increasing on each client instance.
// return - nil.
func (cs *ChunkServer) PushData(args PushDataArgs) error {
  // Create dataId by clientid and timestampe.
  dataId := dataId{
    clientId: args.ClientId,
    timestamp: args.Timestamp,
  }

  // Acquire lock on ChunkServer.Data
  cs.dataMutex.Lock()
  defer cs.dataMutex.Unlock()

  // If data already exists, then just return.
  if _, ok := cs.data[dataId]; ok {
    return nil
  }

  // Otherwise push data into chunkserver's memory.
  cs.data[dataId] = args.Data
  return nil
}

func StartChunkServer(masterAddr string, me string, path string) *ChunkServer {
  cs := &ChunkServer{
    dead: false,
    me: me,
    masterAddr: masterAddr,
    chunkServerMeta: "chunkServerMeta" + me,
    path: path,
    chunks: make(map[uint64]ChunkInfo),
    data: make(map[dataId][]byte),
  }

  rpcs := rpc.NewServer()
  rpcs.Register(cs)

  l, e := net.Listen("tcp", cs.me)
  if e != nil {
    log.Fatal("listen error", e)
  }
  cs.l = l


  // RPC handler for client library.
  go func() {
    for cs.dead == false {
      conn, err := cs.l.Accept()
      if err == nil && cs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      } else if err != nil && cs.dead == false {
        fmt.Println("Kill chunk server.")
        cs.Kill()
      }
    }
  }()

  // Heartbeat
  go func() {
    for cs.dead == false {
      // Send lease extension requests with Hearbeat message, if any.
      cs.pendingExtensionsLock.Lock()
      args := &HeartbeatArgs{
        Addr: cs.me,
        PendingExtensions: cs.pendingExtensions,
      }
      // Clear pending extensions.
      cs.pendingExtensions = nil
      cs.pendingExtensionsLock.Unlock()
      var reply HeartbeatReply
      call(cs.masterAddr, "MasterServer.Heartbeat", args, &reply)
      time.Sleep(HeartbeatInterval)
    }
  }()

  return cs
}

// Helper functions
func reportChunk(cs *ChunkServer, info ChunkInfo) {
  args := ReportChunkArgs{
    ServerAddress: cs.me,
    ChunkHandle: info.ChunkHandle,
    ChunkIndex: info.ChunkIndex,
    Length: info.Length,
    Path: info.Path,
  }
  reply := new(ReportChunkReply)
  // TODO: What if this RPC call failed?
  go call(cs.masterAddr, "MasterServer.ReportChunk", args, reply)
}

// Chunkserver meta data is laoded in StartChunkServer
//
// param  - c: a pointer to a ChunkServer instance
// return - none
func loadChunkServerMeta (c *ChunkServer) {
  f, err := os.Open(c.chunkServerMeta)
  if err != nil {
    fmt.Println("Open file ", c.chunkServerMeta, " failed.");
  }
  defer f.Close()
  parseChunkServerMeta(f, c)
}

// Called by loadChunkServerMeta
// This function parses each line in chunkServerMeta file and loads each value
// into its correspoding ChunkServer fields
//
// param  - c: a pointer to a ChunkServer instance
//          f: a pointer to the file chunkServerMeta
// return - none
func parseChunkServerMeta(f *os.File, c *ChunkServer) {
  scanner := bufio.NewScanner(f)
  for scanner.Scan() {
    fields := strings.Fields(scanner.Text())
    switch fields[0] {
    case "chunkhandle2VersionNum":
      // The map was encoded into bytes.Buffer then to string, now we must
      // convert from string to bytes.Buffer then decode it back to map.
      b := bytes.NewBufferString(fields[1])
      c.chunkhandle2VersionNum = decodeMap(b)
    default:
      fmt.Println("Unknown ChunkServer key")
    }
  }
}

// To store chunk server meta data on disk.
//
// param  - c: a pointer to a ChunkServer instance
// return - none
func storeChunkServerMeta (c *ChunkServer) {
  f, er := os.OpenFile(c.chunkServerMeta, os.O_RDWR|os.O_CREATE, FilePermRW)
  if er != nil {
    fmt.Println("Open/Create file ", c.chunkServerMeta, " failed.")
  }
  defer f.Close()

  // Store map chunkhandle -> file version number
  storeChunkhandle2VersionNum(c, f)
}

// Store chunckhanle to file version number mapping from
// ChunkServer.chunkhandle2VersionNum to ChunkServer.chunkServerMeta.
//
// param  - c: a pointer to a ChunkServer instance
//          f: a pointer to the file chunkServerMeta
// return - none
func storeChunkhandle2VersionNum (c *ChunkServer, f *os.File) {
  b := encodeMap(&c.chunkhandle2VersionNum)
  n, err := f.WriteString("chunkhandle2VersionNum " + b.String() + "\n")
  if err != nil {
    fmt.Println(err)
  } else {
    fmt.Printf("Wrote %d bytes into %s\n", n, c.chunkServerMeta)
  }
}

// Encode a uint64 -> uint64 map into bytes.Buffer
//
// param  - m: the map to be encoded
// return - bytes.Buffer containing the encoded map
func encodeMap(m *map[uint64]uint64) *bytes.Buffer {
  b := new(bytes.Buffer)
  e := gob.NewEncoder(b)
  err := e.Encode(m)
  if err != nil {
    fmt.Println(err)
  }
  return b
}

// Decode a bytes.Buffer into a uint64 -> uint64 map
//
// param  - b: bytes.Buffer containing the encoded map
// return - the encoded map
func decodeMap(b *bytes.Buffer) map[uint64]uint64 {
  d := gob.NewDecoder(b)
  var decodedMap map[uint64]uint64
  err := d.Decode(&decodedMap)
  if err != nil {
    fmt.Println(err)
  }
  return decodedMap
}

// ChunkServer.addChunkExtensionRequest
//
// Called by ChunkServer.Write.
// Add chunkhandle and time of write request to Chunkserver.pendingExtensions.
// These requests are batched together and piggybacked onto Heartbeat messages
// with the master.
// Note: ChunkServer.mutex is held by ChunkServer.Write.
//
// params - chunkhandle: Unique ID for the chunk to be request extension on.
// return - None.
func (cs *ChunkServer) addChunkExtensionRequest(chunkhandle uint64) {
  cs.pendingExtensionsLock.Lock()
  defer cs.pendingExtensionsLock.Unlock()
  cs.pendingExtensions = append(cs.pendingExtensions, chunkhandle)
}
