package simplegfs

import (
  "fmt"
  "time"
  "fmt"
  "os"
  "bufio"
  "testing"
  "strings"
  "strconv"
  "runtime"
)

// Print a logging message indicatin the test has started.
//
// param  - none
// return - none
func testStart() {
  pc, _, _, ok := runtime.Caller(1)
  if ok {
    test := runtime.FuncForPC(pc)
    if test != nil {
      fmt.Println("+++++ Start\t", test.Name())
      return
    }
  }
  fmt.Println("+++++ Start\tUnknown")
}

// Print a logging message indicatin the test has finished.
//
// param  - none
// return - none
func testEnd() {
  pc, _, _, ok := runtime.Caller(1)
  if ok {
    test := runtime.FuncForPC(pc)
    if test != nil {
      fmt.Println("----- Finish\t", test.Name())
      return
    }
  }
  fmt.Println("----- Finish\tUnknown")
}

func TestNewClientId(t *testing.T) {
  testStart()
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
  cid++
  c1 := NewClient(":4444")
  if c1.clientId != cid {
    t.Error("c1's client id should be 1.")
  }
  time.Sleep(HeartbeatInterval)
  ms.Kill()
  testEnd()
}

func TestCreate(t *testing.T) {
  testStart()
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
  testEnd()
}

func TestReadWrite(t *testing.T) {
  testStart()
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

  data := make([]byte, 31)
  if n, _ := c.Read("/a", 0, data); n != 31 || string(data) != "hello, world. nice to meet you." {
    t.Error("c actually reads", string(data))
  }

  data = make([]byte, 100)
  if n, _ := c.Read("/a", 0, data); n != 31 || string(data[0:n]) != "hello, world. nice to meet you." {
    t.Error("c actually reads", n, "chars:", string(data))
  }

  if c.Create("/b") != true {
    t.Error("c should create '/b' successfully.")
  }
  test := "how are you. fine thank you and you? I'm fine too."
  c.Write("/b", 15, []byte(test))
  fmt.Println("#############", len(test))

  time.Sleep(HeartbeatInterval)
  data = make([]byte, 100)
  if n, _ := c.Read("/b", 10, data); n != len(test) + 5 || string(data[5:5 + len(test)]) != test {
    t.Error("c actually reads", n, "chars:", string(data))
  }

  test = "abcdefghijklmnopqrstuvwxyz"
  c.Write("/b", 85, []byte(test))

  time.Sleep(HeartbeatInterval)

  data = make([]byte, 50)
  if n, _ := c.Read("/b", 60, data); n != 50 {
    t.Error("c actually reads", n, "chars:", data[:n])
  }

  time.Sleep(10 * HeartbeatInterval)
  cs1.Kill()
  cs2.Kill()
  cs3.Kill()
  ms.Kill()

  os.RemoveAll("/var/tmp/ck1")
  os.RemoveAll("/var/tmp/ck2")
  os.RemoveAll("/var/tmp/ck3")

  testEnd()
}
