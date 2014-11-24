package simplegfs

import (
  "time"
  "testing"
)

func TestNewClientId(t *testing.T) {
  ms := StartMasterServer(":4444")
  time.Sleep(HeartbeatInterval)
  c0 := NewClient(":4444")
  if c0.clientId != 0 {
    t.Error("c0's client id should start with 0.")
  }
  time.Sleep(HeartbeatInterval)
  c1 := NewClient(":4444")
  if c1.clientId != 1 {
    t.Error("c1's client id should be 1.")
  }
  time.Sleep(HeartbeatInterval)
  ms.Kill()
}
