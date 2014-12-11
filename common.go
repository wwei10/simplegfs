package simplegfs

import (
  "net/rpc"
  "time"
)

const FilePermRW = 0666
const FilePermRWX = 0777
const ChunkSize = 64 * (1 << 20)
const HeartbeatInterval = 100 * time.Millisecond
const CacheTimeout = time.Minute
const CacheGCInterval = time.Minute
const AppendSize = ChunkSize / 4

// ChunkServer lease related const
const LeaseTimeout = 60 * time.Second
const SoftLeaseTime = 1 * time.Second
const HardLeaseTime = 30 * time.Second


// Start fresh or not
var StartFresh = true

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

type DataId struct {
  ClientId uint64
  Timestamp time.Time
}

// Message types

// Master server RPC
type HeartbeatArgs struct {
  Addr string
  PendingExtensions []uint64
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

type FindLeaseHolderArgs struct {
  ChunkHandle uint64
}

type FindLeaseHolderReply struct {
  Primary string
  LeaseEnds time.Time
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

type ListReply struct {
  Paths []string
}

// Chunkserver RPC
type WriteArgs struct {
  DataId DataId
  ChunkHandle uint64
  ChunkIndex uint64
  Offset uint64
  Path string
  ChunkLocations []string
  IsAppend bool
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

type PushDataArgs struct {
  DataId DataId
  Data []byte
}

type PushDataReply struct {
}

// Chunk server -> chunk server
type ReplicateChunkArgs struct {
  Handle uint64
  Data []byte
}

type ReplicateChunkReply struct {
}

// Master -> chunk server
type StartReplicateChunkArgs struct {
  Handle uint64
  Address string
}

type StartReplicateChunkReply struct {
}

type AppendArgs struct {
  DataId DataId
  ChunkHandle uint64
  ChunkIndex uint64
  Path string
  ChunkLocations []string
}

type AppendReply struct {
  Offset uint64
}

// Helper functions
func call(srv string, rpcname string,
          args interface{}, reply interface{}) error {
  c, errx := rpc.Dial("tcp", srv)
  if errx != nil {
    return errx
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err != nil {
    return err
  }

  return nil
}

func min(x, y uint64) uint64 {
  if x > y {
    return y
  } else {
    return x
  }
}
