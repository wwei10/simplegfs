package master

import (
  "fmt"
  "testing"
)

// See if add chunk and find location prints the same results.
func TestAddChunk(t *testing.T) {
  server := []string{"1", "2", "3", "4", "5"}
  m := NewChunkManager(server[:])
  fmt.Println(m.AddChunk("a", 1))
  fmt.Println(m.AddChunk("b", 1))
  fmt.Println(m.FindLocations("a", 1))
  fmt.Println(m.FindLocations("b", 1))
}

// Current implementation will only print results.
// It will not throw any errors if the result doesn't match.
func TestStoreAndLoad(t *testing.T) {
  server := []string{"1", "2", "3", "4", "5"}
  m0 := NewChunkManager(server[:])
  m0.AddChunk("a", 1)
  m0.AddChunk("a", 2)
  m0.AddChunk("b", 1)
  m0.AddChunk("b", 5)
  m0.AddChunk("c", 1)
  m0.Store("/var/tmp/chunkinfo")

  m1 := NewChunkManager(server[:])
  m1.Load("/var/tmp/chunkinfo")
  m1.AddChunk("d", 2)
  fmt.Println(m1)
}

// Test unexported method: insert().
func TestInsertLocation(t *testing.T) {
  arr := make([]string, 0)
  arr = insert(arr, ":4444")
  arr = insert(arr, ":5555")
  arr = insert(arr, ":6666")
  arr = insert(arr, ":4444")
  arr = insert(arr, ":5555")
  arr = insert(arr, ":6666")
  arr = insert(arr, ":7777")
  if len(arr) != 4 {
    t.Error("arr's length should be 4")
  }
  fmt.Println(arr)
}

// Test lease management.
func TestLease(t *testing.T) {
  fmt.Println("test lease")
  server := []string{"1", "2", "3"}
  m := NewChunkManager(server[:])
  m.AddChunk("a", 1)
  info, _ := m.FindLocations("a", 1)
  ok, _ := m.checkLease(info.Handle)
  if ok {
    t.Error("should not have lease")
  }
  lease, _ := m.FindLeaseHolder(info.Handle)
  fmt.Println("info", info)
  fmt.Println("lease", lease)
  ok, _ = m.checkLease(info.Handle)
  if !ok {
    t.Error("should have lease")
  }
}
