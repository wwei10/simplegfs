package simplegfs

import (
  "fmt"
  "time"
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
      fmt.Println()
      fmt.Println("+++++ Start\t", test.Name())
      return
    }
  }
  fmt.Println()
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
      fmt.Println()
      return
    }
  }
  fmt.Println("----- Finish\tUnknown")
  fmt.Println()
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

// TestClientLease
//
// Test for client request new lease, request extensino on lease, and master
// grant/reject new/extend lease.
func TestClientLease(t *testing.T) {
  testStart()

  // Master definitions.
  msAddr := ":4444"

  // Chunkserver definitions.
  ck1Path := "/var/tmp/ck1"
  ck2Path := "/var/tmp/ck2"
  ck3Path := "/var/tmp/ck3"
  ck1Addr := ":5555"
  ck2Addr := ":6666"
  ck3Addr := ":7777"

  // Client definitions.
  testFile := "/a"
  testData1 := "Client1: The quick brown fox jumps over the lazy dog.\n"
  testData2 := "Client2: The quick brown fox jumps over the lazy dog.\n"
  testData3 := "Client3: The quick brown fox jumps over the lazy dog.\n"
  readBuf := make([]byte, 1000)

  // Fire up master server.
  ms := StartMasterServer(msAddr)
  time.Sleep(2 * HeartbeatInterval)

  // Make space on local for chunkserver to store data.
  os.Mkdir(ck1Path, FilePermRWX)
  os.Mkdir(ck2Path, FilePermRWX)
  os.Mkdir(ck3Path, FilePermRWX)

  // Fire up chunk servers.
  cs1 := StartChunkServer(msAddr, ck1Addr, ck1Path)
  cs2 := StartChunkServer(msAddr, ck2Addr, ck2Path)
  cs3 := StartChunkServer(msAddr, ck3Addr, ck3Path)

  // Create client instances.
  c1 := NewClient(msAddr)
  c2 := NewClient(msAddr)
  c3 := NewClient(msAddr)
  time.Sleep(2 * HeartbeatInterval)

  // Create a test file
  if c1.Create(testFile) != true {
    t.Error("Failed to create testfile")
  }

  // Initiate concurrent writes to the same file from 3 clients.
  go func() {
    duration := time.Now().Add(5 * time.Second)
    offset := uint64(0)
    for time.Now().Before(duration) {
      fmt.Println("Client1 writing...");
      c1.Write(testFile, offset, []byte(testData1))
      offset += uint64(len(testData1))
    }
  }()
  go func() {
    time.Sleep(5 * time.Millisecond)
    duration := time.Now().Add(15 * time.Second)
    offset := uint64(0)
    for time.Now().Before(duration) {
      c2.Write(testFile, offset, []byte(testData2))
      offset += uint64(len(testData2))
    }
  }()
  go func() {
    time.Sleep(10 * time.Millisecond)
    duration := time.Now().Add(10 * time.Second)
    offset := uint64(0)
    for time.Now().Before(duration) {
      c3.Write(testFile, offset, []byte(testData3))
      offset += uint64(len(testData3))
    }
  }()

  // Read contents of the files.
  time.Sleep(20 * time.Second)
  n, err := c1.Read(testFile, 1000, readBuf)
  if err != nil {
    fmt.Println("ERROR:", err)
  } else {
    fmt.Println("Read", n, "from testFile", testFile)
    fmt.Println("Data:", string(readBuf))
  }

  // Shutdown master and chunk servers.
  ms.Kill()
  cs1.Kill()
  cs2.Kill()
  cs3.Kill()

  // Remove local disk space allocated for chunkserver.
  // os.RemoveAll(ck1Path)
  os.RemoveAll(ck2Path)
  os.RemoveAll(ck3Path)

  testEnd()
}
