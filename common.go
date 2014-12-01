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
const CacheTimeout = time.Minute
const CacheGCInterval = time.Minute

// Client lease related const
const ClockDrift = 5 * time.Millisecond
const SoftLeaseTime = 1 * time.Second
const HardLeaseTime = 30 * time.Second
const ExtensionRequestInterval = 500 * time.Millisecond

// ChunkServer lease related const
const LeaseTimeout = 60 * time.Second

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
  PrimaryLocation string
}

type NewLeaseArgs struct {
  ClientId uint64
  Path string
}

type NewLeaseReply struct {
  SoftLimit time.Time
  HardLimit time.Time
}

type ExtendLeaseArgs struct {
  ClientId uint64
  Paths []string
}

type ExtendLeaseReply struct {
  File2SoftLimit map[string]time.Time
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

type ListReply struct {
  Paths []string
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
