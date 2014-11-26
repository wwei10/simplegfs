package simplegfs

import (
  "fmt"
  "net/rpc"
  "time"
)

const FilePermRW = 0666
const FilePermRWX = 0777
const HeartbeatInterval = 100 * time.Millisecond

// Message types
type HeartbeatArgs struct {
  Addr string
}

type HeartbeatReply struct {
  Reply string
}

type NewClientIdReply struct {
  ClientId uint64
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
