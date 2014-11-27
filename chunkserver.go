package simplegfs

import (
  "fmt"
  "time"
  "bytes"
  "encoding/gob"
  "bufio"
  "strings"
  "log"
  "net"
  "net/rpc"
  "os"
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
}

// RPC handler declared here

func (cs *ChunkServer) Write(args WriteArgs, reply *WriteReply) error {
  fmt.Println(cs.me, "Write RPC.")
  chunkhandle := args.ChunkHandle
  off := int64(args.Offset)
  bytes := args.Bytes
  filename := fmt.Sprintf("%d", chunkhandle)
  file, err := os.OpenFile(cs.path + "/" + filename, os.O_WRONLY | os.O_CREATE, 0777)
  if err != nil {
    log.Fatal(err)
  }
  defer file.Close()
  file.WriteAt(bytes, off)
  return nil
}

func (cs *ChunkServer) Read(args ReadArgs, reply *ReadReply) error {
  fmt.Println(cs.me, "Read RPC.")
  chunkhandle := args.ChunkHandle
  off := int64(args.Offset)
  length := args.Length
  bytes := make([]byte, length)
  filename := fmt.Sprintf("%d", chunkhandle)
  file, err := os.Open(cs.path + "/" + filename)
  if err != nil {
    log.Fatal(err)
  }
  defer file.Close()
  _, err = file.ReadAt(bytes, off)
  if err != nil {
    log.Fatal(err)
  }
  reply.Bytes = bytes
  return nil
}

// Kill for testing.
func (cs *ChunkServer) Kill() {
  cs.dead = true
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

func StartChunkServer(masterAddr string, me string, path string) *ChunkServer {
  cs := &ChunkServer{
    dead: false,
    me: me,
    masterAddr: masterAddr,
    chunkServerMeta: "chunkServerMeta" + me,
    path: path,
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
      args := &HeartbeatArgs{cs.me}
      var reply HeartbeatReply
      call(cs.masterAddr, "MasterServer.Heartbeat", args, &reply)
      time.Sleep(HeartbeatInterval)
    }
  }()

  return cs
}
