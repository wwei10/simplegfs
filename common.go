package simplegfs

import (
  "fmt"
  "net/rpc"
  "time"
)

const FilePermRW = 0666
const FilePermRWX = 0777
const ChunkSize = 64 * (1 << 20)
const HeartbeatInterval = 100 * time.Millisecond

// Message types

// Master server RPC
type HeartbeatArgs struct {
  Addr string
}

type HeartbeatReply struct {
  Reply string
}

type NewClientIdReply struct {
  ClientId uint64
}

type FindLocationsArgs struct {
  Path string
  ChunkIndex uint64
}

type FindLocationsReply struct {
  ChunkHandle uint64
  ChunkLocations []string
}

// Chunkserver RPC
type WriteArgs struct {
  ChunkHandle uint64
  Offset uint64
  Bytes []byte
}

type WriteReply struct {
}

type ReadArgs struct {
  ChunkHandle uint64
  Offset uint64
  Length uint64
}

type ReadReply struct {
  Bytes []byte
}

// Helper functions
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("tcp", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}
