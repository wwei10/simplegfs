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

// Useful data structures
type ChunkInfo struct {
  Path string
  ChunkHandle uint64
  ChunkIndex uint64
  Length int64
}

type FileInfo struct {
  Length int64
}

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

type AddChunkArgs struct {
  Path string
  ChunkIndex uint64
}

type AddChunkReply struct {
  ChunkHandle uint64
  ChunkLocations []string
}

type ReportChunkArgs struct {
  ServerAddress string
  ChunkHandle uint64
  ChunkIndex uint64
  Length int64
  Path string
}

type ReportChunkReply struct {
}

type GetFileInfoArgs struct {
  Path string
}

type GetFileInfoReply struct {
  Info FileInfo
}
// Chunkserver RPC
type WriteArgs struct {
  ChunkHandle uint64
  ChunkIndex uint64
  Offset uint64
  Bytes []byte
  Path string
}

type WriteReply struct {
}

type ReadArgs struct {
  ChunkHandle uint64
  Offset int64
  Length uint64
}

type ReadReply struct {
  Length int
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

func min(x, y uint64) uint64 {
  if x > y {
    return y
  } else {
    return x
  }
}
