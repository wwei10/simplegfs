package simplegfs

import (
  "time"
  "os"
  "bufio"
  "testing"
  "strings"
  "strconv"
)

func TestNewClientId(t *testing.T) {
  ms := StartMasterServer(":4444")
  time.Sleep(HeartbeatInterval)

  // Read master server's meta data to determine what the next clientId
  // is suppose to be.
  f, err := os.OpenFile("serverMeta:4444", os.O_RDONLY, 0666)
  if err != nil {
    t.Error(err)
  }
  defer f.Close()

  var cid uint64
  scanner := bufio.NewScanner(f)
  for scanner.Scan() {
    fields := strings.Fields(scanner.Text())
    if fields[0] == "clientId" {
      cid, _ = strconv.ParseUint(fields[1], 0, 64)
      break
    }
  }

  c0 := NewClient(":4444")
  if c0.clientId != cid {
    t.Error("c0's client id should start with 0.")
  }
  time.Sleep(HeartbeatInterval)
  cid ++
  c1 := NewClient(":4444")
  if c1.clientId != cid {
    t.Error("c1's client id should be 1.")
  }
  time.Sleep(HeartbeatInterval)
  ms.Kill()
}
