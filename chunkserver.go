package simplegfs

import (
  "fmt"
  "io"
  "time"
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
  path string
  chunks map[uint64]ChunkInfo // Store a mapping from handle to information.
  mutex sync.RWMutex
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
}

func StartChunkServer(masterAddr string, me string, path string) *ChunkServer {
  cs := &ChunkServer{
    dead: false,
    me: me,
    masterAddr: masterAddr,
    path: path,
    chunks: make(map[uint64]ChunkInfo),
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
