package simplegfs

import (
  "fmt"
  "github.com/wweiw/simplegfs/pkg/testutil"
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
  defer c0.Stop()
  testutil.AssertEquals(t, c0.clientId, cid)
  time.Sleep(HeartbeatInterval)
  cid++
  c1 := NewClient(":4444")
  defer c1.Stop()
  testutil.AssertEquals(t, c1.clientId, cid)
  time.Sleep(HeartbeatInterval)
  ms.Kill()
  testEnd()
}

func testCreate(c *Client, path string) bool {
  ok, err := c.Create(path)
  if err != nil {
    fmt.Println(err)
  }
  return ok
}

func testMkdir(c *Client, path string) bool {
  ok, err := c.Mkdir(path)
  if err != nil {
    fmt.Println(err)
  }
  return ok
}

func testDelete(c *Client, path string) bool {
  ok, err := c.Delete(path)
  if err != nil {
    fmt.Println(err)
  }
  return ok
}

func TestNamespaceManagement(t *testing.T) {
  testStart()
  ms := StartMasterServer(":4444")
  time.Sleep(HeartbeatInterval)
  c := NewClient(":4444")
  defer c.Stop()
  testutil.AssertTrue(t, testCreate(c, "/a"), "create /a returns true.")
  testutil.AssertFalse(t, testCreate(c, "/a"), "create /a returns false.")
  testutil.AssertFalse(t, testMkdir(c, "/var/tmp"), "mkdir /var/tmp returns false.")
  testutil.AssertTrue(t, testMkdir(c, "/var"), "mkdir /var returns true.")
  testutil.AssertTrue(t, testMkdir(c, "/var/tmp"), "mkdir /var/tmp returns true.")
  testutil.AssertTrue(t, testCreate(c, "/var/tmp/a"), "create /var/tmp/a returns true.")
  testutil.AssertTrue(t, testCreate(c, "/var/tmp/b"), "create /var/tmp/b returns true.")
  testutil.AssertTrue(t, testCreate(c, "/var/tmp/c"), "create /var/tmp/c returns true.")
  testutil.AssertTrue(t, testCreate(c, "/var/tmp/d"), "create /var/tmp/d returns true.")
  fmt.Println(c.List("/var/tmp"))
  testutil.AssertFalse(t, testDelete(c, "/var"), "delete /var returns false.")
  testutil.AssertFalse(t, testDelete(c, "/var/tmp"), "delete /var/tmp returns false.")
  testutil.AssertTrue(t, testDelete(c, "/var/tmp/a"), "delete /var/tmp/a returns true.")
  testutil.AssertTrue(t, testDelete(c, "/var/tmp/b"), "delete /var/tmp/b returns true.")
  testutil.AssertTrue(t, testDelete(c, "/var/tmp/c"), "delete /var/tmp/c returns true.")
  testutil.AssertTrue(t, testDelete(c, "/var/tmp/d"), "delete /var/tmp/d returns true.")
  fmt.Println(c.List("/var/tmp"))
  testutil.AssertTrue(t, testDelete(c, "/var/tmp"), "delete /var/tmp returns true.")
  testutil.AssertTrue(t, testDelete(c, "/var"), "delete /var returns true.")
  fmt.Println(c.List("/var"))
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
  defer c.Stop()
  if ok, err := c.Create("/a"); err != nil || ok != true {
    t.Error("c should create '/a' successfully.")
  }
  if ok := c.Write("/a", 0, []byte("hello, world. nice to meet you.")); !ok {
    t.Error("Write request failed.")
  }

  time.Sleep(HeartbeatInterval)

  data := make([]byte, 31)
  if n, _ := c.Read("/a", 0, data); n != 31 || string(data) != "hello, world. nice to meet you." {
    t.Error("c actually reads", string(data))
  }

  data = make([]byte, 100)
  if n, _ := c.Read("/a", 0, data); n != 31 || string(data[0:n]) != "hello, world. nice to meet you." {
    t.Error("c actually reads", n, "chars:", string(data))
  }

  if ok, err := c.Create("/b"); err != nil || ok != true {
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

// 3 chunk servers + 3 clients sequantial read/write + concurrent read/write
// test.
func TestChunkServerLease(t *testing.T) {
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
  testFile1 := "/a"
  testFile2 := "/b"
  testData1 := "testData1: The quick brown fox jumps over the lazy dog.\n"
  testData2 := "testData2: The quick brown fox jumps over the lazy dog.\n"
  testData3 := "testData3: The quick brown fox jumps over the lazy dog.\n"
  testData4 := "testData4: The quick brown fox jumps over the lazy dog.\n"
  readBuf := make([]byte, len(testData1))
  readBuf2 := make([]byte, 7000)

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
  time.Sleep(HeartbeatInterval)

  // ----- Test sequential read and write -----

  // Create a test file.
  if ok, err := c1.Create(testFile1); err != nil || ok != true {
    t.Error("Failed to create testfile")
  }

  // Write once.
  offset := uint64(0)
  if ok := c1.Write(testFile1, offset, []byte(testData1)); !ok {
    t.Error("Write request failed")
  }

  // Read and verify.
  if n, _ := c1.Read(testFile1, 0, readBuf); n != len(testData1) ||
  string(readBuf) != testData1 {
    t.Error("Client 1 reads:", string(readBuf),". Should read:", testData1)
  }

  // Write once.
  offset += uint64(len(testData1))
  if ok := c2.Write(testFile1, offset, []byte(testData2)); !ok {
    t.Error("Write request failed")
  }

  // Read and verify.
  if n, _ := c2.Read(testFile1, offset, readBuf); n != len(testData2) ||
  string(readBuf) != testData2 {
    t.Error("Client 2 reads:", string(readBuf),". Should read:", testData2)
  }

  // Write once.
  offset += uint64(len(testData2))
  if ok := c3.Write(testFile1, offset, []byte(testData3)); !ok {
    t.Error("Write request failed")
  }

  // Read and verify.
  if n, _ := c3.Read(testFile1, offset, readBuf); n != len(testData3) ||
  string(readBuf) != testData3 {
    t.Error("Client 3 reads:", string(readBuf),". Should read:", testData3)
  }

  // Create a second test file.
  if ok, err := c2.Create(testFile2); err != nil || ok != true {
    t.Error("Failed to create testfile2")
  }

  // Write once.
  offset = uint64(0)
  if ok := c1.Write(testFile2, offset, []byte(testData4)); !ok {
    t.Error("Write request failed")
  }

  // Read and verify.
  if n, _ := c3.Read(testFile2, offset, readBuf); n != len(testData4) ||
  string(readBuf) != testData4 {
    t.Error("Client 3 reads:", string(readBuf),". Should read:", testData4)
  }

  // ----- Test concurrent write -----
  // Write concurrently, there is no way to deterministically the read output
  // against preset value, therefore we can only verify partial outputs are
  // valid and testDatas are not interleaving each other.
  //
  // Each client runs for 5 seconds, and writes to the same file conccurently,
  // starting from offset 0.
  go func() {
    duration := time.Now().Add(5 * time.Second)
    offset = uint64(0)
    for time.Now().Before(duration) {
      time.Sleep(HeartbeatInterval)
      c1.Write(testFile1, offset, []byte(testData1))
      offset += uint64(len(testData1))
    }
  }()
  go func() {
    duration := time.Now().Add(5 * time.Second)
    offset = uint64(0)
    for time.Now().Before(duration) {
      time.Sleep(HeartbeatInterval)
      c2.Write(testFile1, offset, []byte(testData2))
      offset += uint64(len(testData2))
    }
  }()
  go func() {
    duration := time.Now().Add(5 * time.Second)
    offset = uint64(0)
    for time.Now().Before(duration) {
      time.Sleep(HeartbeatInterval)
      c3.Write(testFile1, offset, []byte(testData3))
      offset += uint64(len(testData3))
    }
  }()

  // Read contents of the file while writes are still ongoing.
  time.Sleep(1 * time.Second)
  n, err := c1.Read(testFile1, 0, readBuf2)
  if err != nil {
    t.Error(err)
  } else {
    fmt.Println("Read", n, "from testFile", testFile1)
    fmt.Println(string(readBuf2))
  }

  // Read contents of the file after writes are finished.
  time.Sleep(5 * time.Second)
  n, err = c2.Read(testFile1, 0, readBuf2)
  if err != nil {
    t.Error(err)
  } else {
    fmt.Println("Read", n, "from testFile", testFile1)
    fmt.Println(string(readBuf2))
  }

  // Shutdown master and chunk servers.
  ms.Kill()
  cs1.Kill()
  cs2.Kill()
  cs3.Kill()

  // Remove local disk space allocated for chunkserver.
  os.RemoveAll(ck1Path)
  os.RemoveAll(ck2Path)
  os.RemoveAll(ck3Path)

  testEnd()
}
