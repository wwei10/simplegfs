package master

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/wweiw/simplegfs/pkg/cache"
	"github.com/wweiw/simplegfs/pkg/set"
	"io/ioutil"
	"log"
	"math/rand"
	"sync"
	"time"
)

// Lease Expires in 1 minute
const LeaseTimeout = time.Minute

var heartbeatGC = 2 * time.Minute
var heartbeatExpiration = time.Minute

// Persistent information of a specific chunk.
type Chunk struct {
	ChunkHandle uint64
	// version number
}

// In-memory detailed information of a specific chunk.
type ChunkInfo struct {
	Handle    uint64 // Unique chunk handle.
	Locations []string
}

type PathIndex struct {
	Path  string
	Index uint64
}

type Lease struct {
	Primary    string    // Primary's location.
	Expiration time.Time // Lease expiration time.
}

// Chunk manager does the actual bookkeeping work for chunk servers.
type ChunkManager struct {
	lock           sync.RWMutex                   // Read write lock.
	chunkHandle    uint64                         // Increment by 1 when a new chunk is created.
	chunks         map[string](map[uint64]*Chunk) // (path, chunk index) -> chunk information (persistent)
	handles        map[uint64]*PathIndex          // chunk handle -> (path, chunk index) (inverse of chunks)
	locations      map[uint64]*ChunkInfo          // chunk handle -> chunk locations (in-memory)
	chunkServers   []string                       // Alive chunk servers.
	serverChunkMap map[string]*set.Set            // Server -> chunk mapping.
	heartbeats     *cache.Cache                   // Cache of updated-heartbeat servers.
	leases         map[uint64]*Lease              // chunk handle -> lease
}

func NewChunkManager(servers []string) *ChunkManager {
	m := &ChunkManager{
		chunkHandle:    uint64(0),
		chunks:         make(map[string](map[uint64]*Chunk)),
		handles:        make(map[uint64]*PathIndex),
		locations:      make(map[uint64]*ChunkInfo),
		leases:         make(map[uint64]*Lease),
		chunkServers:   servers,
		heartbeats:     cache.New(heartbeatExpiration, heartbeatGC),
		serverChunkMap: make(map[string]*set.Set),
	}
	return m
}

// Find chunk server locations associated given a file name and a chunk index.
func (m *ChunkManager) FindLocations(path string, chunkIndex uint64) (*ChunkInfo, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.getChunkInfo(path, chunkIndex)
}

// Find lease holder and return its location.
func (m *ChunkManager) FindLeaseHolder(handle uint64) (*Lease, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	ok := m.checkLease(handle)
	// If lease check is not passed, try to grant a new lease.
	if !ok {
		err := m.addLease(handle)
		// If add lease failed, return err.
		if err != nil {
			return &Lease{}, err
		}
	}
	// Return current lease holder for handle.
	lease, _ := m.leases[handle]
	return lease, nil
}

// Handle lease extension requests.
// Lease extensions are only granted when the requesting chunkserver is the primary
// replica.
func (m *ChunkManager) ExtendLease(cs string, handles []uint64) {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, handle := range handles {
		lease, ok := m.leases[handle]
		// If the entry exists and the current lease holder is requesting, extend its
		// current lease.
		if ok && lease.Primary == cs {
			lease.Expiration = time.Now().Add(LeaseTimeout)
		}
	}

}

// Allocate a new chunk handle and three random chunk servers
// for a given file's chunk.
func (m *ChunkManager) AddChunk(path string, chunkIndex uint64) (*ChunkInfo, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	return m.addChunk(path, chunkIndex)
}

// Get (file, chunk index) associated with the specified chunk handle.
func (m *ChunkManager) GetPathIndexFromHandle(handle uint64) (PathIndex, error) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	pathIndex, ok := m.handles[handle]
	if !ok {
		return PathIndex{}, errors.New("chunk handle not found")
	}
	return *pathIndex, nil
}

// Set the location associated with a chunk handle.
func (m *ChunkManager) SetChunkLocation(handle uint64, address string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	info, ok := m.locations[handle]
	if !ok {
		info = &ChunkInfo{
			Handle:    handle,
			Locations: make([]string, 0),
		}
		m.locations[handle] = info
		m.serverChunkMap[address].Add(handle)
	}
	// TODO: Add address into the locations array. Need to ensure the there are no
	// duplicates in the array.
	info.Locations = insertElem(info.Locations, address)
	return nil
}

// Heartbeat associated functions.

// Process heartbeat message and update server status.
func (m *ChunkManager) HandleHeartbeat(server string) {
	log.Println("handle heartbeat from", server)
	// If haven't seen this server before,
	// Insert server into elem.
	if _, ok := m.heartbeats.Get(server); !ok {
		m.lock.Lock()
		m.chunkServers = insertElem(m.chunkServers, server)
		fmt.Println(m.chunkServers)
		m.lock.Unlock()
	}
	m.heartbeats.Set(server, true)
}

// Update chunkServers so that it contains only servers that are alive.
// This function only supports single-threaded mode.
func (m *ChunkManager) HeartbeatCheck() {
	log.Println("heartbeat check")
	chunkServers := make([]string, 0)
	for _, server := range m.chunkServers {
		fmt.Println("server", server)
		_, ok := m.heartbeats.Get(server)
		if ok {
			fmt.Println("exist")
			chunkServers = append(chunkServers, server)
		} else {
			set, ok := m.serverChunkMap[server]
			if !ok {
				continue
			}
			handles := set.Slice()
			fmt.Println("slice", handles)
			// Acquire lock first.
			m.lock.Lock()
			for _, v := range handles {
				// Slice() returns interface{}, should first convert to uint64.
				handle, ok := v.(uint64)
				if ok {
					// Remove handle from chunk handle -> location mapping.
					m.locations[handle].Locations = removeElem(m.locations[handle].Locations, server)
				}
			}
			m.lock.Unlock()
		}
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.chunkServers = chunkServers
}

// Release any resources it holds.
func (m *ChunkManager) Stop() {
	m.heartbeats.Stop()
}

// Helper functions

// Pre-condition: call m.lock.Lock()
// Get chunk information associated with a file and a chunk index.
// Returns chunk information and errors.
func (m *ChunkManager) getChunkInfo(path string, chunkIndex uint64) (*ChunkInfo, error) {
	info := &ChunkInfo{}
	val, ok := m.chunks[path]
	if !ok {
		fmt.Println("File not found.")
		return info, errors.New("File not found.")
	}
	chunk, ok := val[chunkIndex]
	if !ok {
		fmt.Println("Chunk index not found.")
		return info, errors.New("Chunk index not found.")
	}
	chunkInfo, ok := m.locations[chunk.ChunkHandle]
	if !ok {
		fmt.Println("Locations not found.")
		return info, errors.New("Locations not available.")
	}
	return chunkInfo, nil
}

// unexported struct for serialization-use only.
type persistentData struct {
	Handle  uint64
	Chunks  *map[string](map[uint64]*Chunk)
	Handles *map[uint64]*PathIndex
}

// Store current chunk handle into path.
// Store (file, chunk index) -> chunk information into path.
func (m *ChunkManager) Store(path string) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	var data bytes.Buffer
	enc := gob.NewEncoder(&data)
	err := enc.Encode(&persistentData{
		Handle:  m.chunkHandle,
		Chunks:  &m.chunks,
		Handles: &m.handles,
	})
	if err != nil {
		log.Fatal("encode error:", err)
	}
	err = ioutil.WriteFile(path, data.Bytes(), FilePermRW)
	if err != nil {
		log.Fatal("write error:", err)
	}
}

func (m *ChunkManager) Load(path string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	var data persistentData
	b, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatal("read error:", err)
	}
	buffer := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(&data)
	if err != nil {
		log.Fatal("decode error:", err)
	}
	m.chunkHandle = data.Handle
	m.chunks = *data.Chunks
	m.handles = *data.Handles
}

// Pretty print ChunkManager instance.
func (m *ChunkManager) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("----- Chunk Manager -----\n")
	buffer.WriteString(fmt.Sprintf("Handle %v\n", m.chunkHandle))
	buffer.WriteString("Chunk information map:\n")
	for k, v := range m.chunks {
		buffer.WriteString(fmt.Sprintf("- File: %v\n", k))
		for i, handle := range v {
			buffer.WriteString(fmt.Sprintf("-- Index: %v, Handle: %v\n", i, handle))
		}
	}
	buffer.WriteString("Chunk handle inverse map:\n")
	for k, v := range m.handles {
		buffer.WriteString(fmt.Sprintf("%v: %v\n", k, v))
	}
	buffer.WriteString("----- Chunk Manager -----\n")
	return buffer.String()
}

// Pre-condition: call m.lock.Lock()
func (m *ChunkManager) addChunk(path string, chunkIndex uint64) (*ChunkInfo, error) {
	info := &ChunkInfo{}
	_, ok := m.chunks[path]
	if !ok {
		m.chunks[path] = make(map[uint64]*Chunk)
	}
	_, ok = m.chunks[path][chunkIndex]
	if ok {
		fmt.Println("Chunk index already exists.")
		return info, errors.New("Chunk index already exists.")
	}
	handle := m.chunkHandle
	m.chunkHandle++
	m.chunks[path][chunkIndex] = &Chunk{
		ChunkHandle: handle,
	}
	locations := random(m.chunkServers, 3)
	m.locations[handle] = &ChunkInfo{
		Handle:    handle,
		Locations: locations,
	}
	m.handles[handle] = &PathIndex{
		Path:  path,
		Index: chunkIndex,
	}
	return m.locations[handle], nil
}

// Pre-codition: m.lock is acquired.
// addLease will grant a lease to a randomly selected server as the primary.
func (m *ChunkManager) addLease(handle uint64) error {
	locations, ok := m.locations[handle]
	if !ok {
		return errors.New("chunk handle does not exist")
	}
	lease, ok := m.leases[handle]
	if !ok {
		// Entry not found, create a new one.
		lease = &Lease{}
		m.leases[handle] = lease
	}
	// If no chunk server is alive, can't grant a new lease.
	if len(locations.Locations) == 0 {
		return errors.New("no chunk server is alive")
	}
	// Assign new values to lease.
	lease.Primary = locations.Locations[0] // TODO: randomly select one.
	lease.Expiration = time.Now().Add(LeaseTimeout)
	m.leases[handle] = lease
	return nil
}

// Pre-condition: m.lock is acquired.
// checkLease will check whether the lease is still valid.
func (m *ChunkManager) checkLease(handle uint64) bool {
	lease, ok := m.leases[handle]
	// If lease doesn't exist, simply return false.
	if !ok {
		return false
	}
	// TODO: check if primary is still alive.
	// If lease on the primary has already expired, return false
	if lease.Expiration.Before(time.Now()) {
		return false
	}
	return true
}

// Pick num elements randomly from array.
func random(array []string, num int) []string {
	ret := make([]string, num)
	perm := rand.Perm(len(array))
	for i := 0; i < num; i++ {
		ret[i] = array[perm[i]]
	}
	return ret
}

// Add an element into an array. Need to ensure there are
// no dupliates.
func insertElem(array []string, elem string) []string {
	for _, s := range array {
		if s == elem {
			return array
		}
	}
	return append(array, elem)
}

// Remove an element in the array. Returns a new array.
func removeElem(array []string, elem string) []string {
	ret := make([]string, 0)
	for _, s := range array {
		if s != elem {
			ret = append(ret, s)
		}
	}
	return ret
}
