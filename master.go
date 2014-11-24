package simplegfs

import (
  "fmt"
  "log"
  "net"
  "net/rpc"
  "sync"
  "time"
)

type MasterServer struct {
  dead bool
  l net.Listener
  me string // Server address
  clientId uint64 // Client ID
  mutex sync.RWMutex
}

// RPC call handler
func (ms *MasterServer) Heartbeat(args *HeartbeatArgs,
                                  reply *HeartbeatReply) error {
  fmt.Println("RPC handler")
  reply.Reply = "Hello, world."
  return nil
}

func (ms *MasterServer) NewClientId(args *struct{},
                                    reply *NewClientIdReply) error {
  ms.mutex.Lock()
  defer ms.mutex.Unlock()
  reply.ClientId = ms.clientId
  ms.clientId++
  return nil
}

// Tell the server to shut itself down
// for testing
func (ms *MasterServer) Kill() {
  ms.dead = true
  ms.l.Close()
}

// tick() is called once per PingInterval to
// handle background tasks
func (ms *MasterServer) tick() {
  fmt.Println("Background tasks...")
}

func StartMasterServer(me string) *MasterServer {
  ms := &MasterServer{
    me: me,
  }

  rpcs := rpc.NewServer()
  rpcs.Register(ms)

  l, e := net.Listen("tcp", ms.me)
  if e != nil {
    log.Fatal("listen error", e)
  }
  ms.l = l

  // RPC handler
  go func() {
    for ms.dead == false {
      conn, err := ms.l.Accept()
      if err == nil && ms.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      } else if err != nil && ms.dead == false {
        fmt.Println("Kill server.")
        ms.Kill()
      }
    }
  }()

  // Background tasks
  go func() {
    for ms.dead == false {
      ms.tick()
      time.Sleep(HeartbeatInterval)
    }
  }()

  return ms
}
