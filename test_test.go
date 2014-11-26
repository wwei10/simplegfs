package simplegfs

import (
  "time"
  "os"
  "testing"
)

func TestNewClientId(t *testing.T) {
  ms := StartMasterServer(":4444")
  time.Sleep(HeartbeatInterval)
  c0 := NewClient(":4444")
  if c0.clientId != 1 {
    t.Error("c0's client id should start with 1.")
  }
  time.Sleep(HeartbeatInterval)
  c1 := NewClient(":4444")
  if c1.clientId != 2 {
    t.Error("c1's client id should be 2.")
  }
  time.Sleep(HeartbeatInterval)
  ms.Kill()
}

func TestCreate(t *testing.T) {
  ms := StartMasterServer(":4444")
  time.Sleep(HeartbeatInterval)
  c := NewClient(":4444")
  if c.Create("/a") == false {
    t.Error("c should create '/a' successfully.")
  }
  if c.Create("/a") == true {
    t.Error("c should not be able to create '/a' again.")
  }
  time.Sleep(HeartbeatInterval)
  ms.Kill()
}

func TestReadWrite(t *testing.T) {
  ms := StartMasterServer(":4444")
  time.Sleep(HeartbeatInterval)

  os.Mkdir("/var/tmp/ck1", 0777)
  os.Mkdir("/var/tmp/ck2", 0777)
  os.Mkdir("/var/tmp/ck3", 0777)

  cs1 := StartChunkServer(":4444", ":5555", "/var/tmp/ck1")
  cs2 := StartChunkServer(":4444", ":6666", "/var/tmp/ck2")
  cs3 := StartChunkServer(":4444", ":7777", "/var/tmp/ck3")

  c := NewClient(":4444")
  if c.Create("/a") != true {
    t.Error("c should create '/a' successfully.")
  }
  c.Write("/a", 0, []byte("hello, world. nice to meet you."))

  time.Sleep(HeartbeatInterval)

  if data, _ := c.Read("/a", 0, 31); string(data) != "hello, world. nice to meet you." {
    t.Error("c should read 'hello, world.' actually c reads", string(data))
  }

  time.Sleep(10 * HeartbeatInterval)
  cs1.Kill()
  cs2.Kill()
  cs3.Kill()
  ms.Kill()

  os.RemoveAll("/var/tmp/ck1")
  os.RemoveAll("/var/tmp/ck2")
  os.RemoveAll("/var/tmp/ck3")

}
