package simplegfs

import (
  "fmt"
  "time"
)

type ChunkServer struct {
  dead bool
  me string
  masterAddr string
}

func (cs *ChunkServer) Kill() {
  cs.dead = true
}

func StartChunkServer(masterAddr string, me string) *ChunkServer {
  cs := &ChunkServer{
    dead: false,
    me: me,
    masterAddr: masterAddr,
  }

  // RPC handler for client library.
  go func() {
    for cs.dead == false {
      time.Sleep(HeartbeatInterval)
    }
  }()

  // Heartbeat
  go func() {
    for cs.dead == false {
      args := &HeartbeatArgs{cs.me}
      var reply HeartbeatReply
      call(cs.masterAddr, "MasterServer.Heartbeat", args, &reply)
      fmt.Println(reply.Reply)
      time.Sleep(HeartbeatInterval)
    }
  }()

  return cs
}
