package simplegfs

import (
  "fmt"
  "time"
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
