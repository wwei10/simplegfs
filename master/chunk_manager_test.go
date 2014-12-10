package master

import (
	"fmt"
	"testing"
	"time"
)

// See if add chunk and find location prints the same results.
func TestAddChunk(t *testing.T) {
	server := []string{"1", "2", "3", "4", "5"}
	m := NewChunkManager(server[:])
	defer m.Stop()
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
	defer m0.Stop()
	m0.AddChunk("a", 1)
	m0.AddChunk("a", 2)
	m0.AddChunk("b", 1)
	m0.AddChunk("b", 5)
	m0.AddChunk("c", 1)
	m0.Store("/var/tmp/chunkinfo")

	m1 := NewChunkManager(server[:])
	defer m1.Stop()
	m1.Load("/var/tmp/chunkinfo")
	m1.AddChunk("d", 2)
	fmt.Println(m1)
}

// Test unexported method: insert().
func TestInsertLocation(t *testing.T) {
	arr := make([]string, 0)
	arr = insertElem(arr, ":4444")
	arr = insertElem(arr, ":5555")
	arr = insertElem(arr, ":6666")
	arr = insertElem(arr, ":4444")
	arr = insertElem(arr, ":5555")
	arr = insertElem(arr, ":6666")
	arr = insertElem(arr, ":7777")
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
	defer m.Stop()
	m.AddChunk("a", 1)
	info, _ := m.FindLocations("a", 1)
	ok := m.checkLease(info.Handle)
	if ok {
		t.Error("should not have lease")
	}
	lease, _ := m.FindLeaseHolder(info.Handle)
	fmt.Println("info", info)
	fmt.Println("lease", lease)
	ok = m.checkLease(info.Handle)
	if !ok {
		t.Error("should have lease")
	}
}

func TestHeartbeat(t *testing.T) {
	heartbeatGC = time.Second * 2
	heartbeatExpiration = time.Second
	servers := []string{"1", "2", "3", "4", "5"}
	m := NewChunkManager(servers)
	defer m.Stop()
	time.Sleep(time.Second)
	m.HeartbeatCheck()
	if len(m.chunkServers) != 0 {
		t.Error("Servers that are alive:", m.chunkServers)
	}
	fmt.Println("Servers that are alive:", m.chunkServers)

	// Receive heartbeat messages from 1, 2, 3.
	m.HandleHeartbeat("1")
	m.HandleHeartbeat("2")
	m.HandleHeartbeat("3")
	m.HeartbeatCheck()
	if len(m.chunkServers) != 3 {
		t.Error("Servers that are alive:", m.chunkServers)
	}
	fmt.Println("Servers that are alive:", m.chunkServers)
}
