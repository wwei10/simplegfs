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

type pendingReplication struct {
	priority int      // priority of this replication task.
	target   []string // Target server addresses.
}

// Chunk manager does the actual bookkeeping work for chunk servers.
type ChunkManager struct {
	lock           sync.RWMutex                   // Read write lock.
	chunkHandle    uint64                         // Increment by 1 when a new chunk is created.
	chunks         map[string](map[uint64]*Chunk) // (path, chunk index) -> chunk information (persistent)
	handles        map[uint64]*PathIndex          // Chunk handle -> (path, chunk index) (inverse of chunks)
	locations      map[uint64]*ChunkInfo          // Chunk handle -> chunk locations (in-memory)
	chunkServers   []string                       // Alive chunk servers.
	serverChunkMap map[string]*set.Set            // Server -> chunk mapping.
	heartbeats     *cache.Cache                   // Cache of updated-heartbeat servers.
	leases         map[uint64]*Lease              // Chunk handle -> lease
	pendingRepMap  map[uint64]*pendingReplication // Chunk handle -> pending replication request.
	scheduledReps  []uint64                       // Scheduled replication.
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
		pendingRepMap:  make(map[uint64]*pendingReplication),
		scheduledReps:  make([]uint64, 0),
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
	}
	// TODO: Add address into the locations array. Need to ensure the there are no
	// duplicates in the array.
	info.Locations = insertElem(info.Locations, address)
	// Add inverse mapping from server address to chunk handle.
	if _, ok := m.serverChunkMap[address]; !ok {
		m.serverChunkMap[address] = set.New()
	}
	m.serverChunkMap[address].Add(handle)
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
		m.lock.Unlock()
	}
	m.heartbeats.Set(server, true)
}

// Update chunkServers so that it contains only servers that are alive.
// This function only supports single-threaded mode.
func (m *ChunkManager) HeartbeatCheck() {
	log.Println("heartbeat check")
	chunkServers := make([]string, 0)
	badChunkServers := make([]string, 0)
	for _, server := range m.chunkServers {
		_, ok := m.heartbeats.Get(server)
		if ok {
			chunkServers = append(chunkServers, server)
		} else {
			badChunkServers = append(badChunkServers, server)
		}
	}
	m.lock.Lock()
	m.chunkServers = chunkServers
	m.lock.Unlock()
	for _, server := range badChunkServers {
		set, ok := m.serverChunkMap[server]
		if !ok {
			continue
		}
		handles := set.Slice()
		// Acquire lock first.
		m.lock.Lock()
		for _, v := range handles {
			// Slice() returns interface{}, should first convert to uint64.
			handle, ok := v.(uint64)
			if ok {
				// Remove handle from chunk handle -> location mapping.
				locations := removeElem(m.locations[handle].Locations, server)
				m.locations[handle].Locations = locations
				// Make sure chunk handle actually means something.
				_, ok = m.handles[handle]
				if !ok {
					continue
				}
				// Add a pending replication task for future re-replication.
				m.addPendingReplication(handle, locations)
			}
		}
		m.lock.Unlock()
	}
}

// Pick one chunk that has the highest priority as the candidate to
// for re-replication.
func (m *ChunkManager) ScheduleReplication() {
  // Ensure there are at most m.scheduleReps is 1.
	if len(m.scheduledReps) > 1 {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	highestSoFar := 0
	highestHandle := uint64(0)
	for handle, pending := range m.pendingRepMap {
		if pending.priority > highestSoFar {
			highestSoFar = pending.priority
			highestHandle = handle
		}
	}
	m.scheduledReps = append(m.scheduledReps, highestHandle)
}

// Start replication.
// Returns handle, location, target location
func (m *ChunkManager) StartReplication() (uint64, string, []string, error) {
  handle := m.scheduledReps[0]
  if len(m.locations[handle].Locations) == 0 {
    return 0, "", []string{}, errors.New("no available locaiton")
  }
  location := m.locations[handle].Locations[0]
  return handle, location, m.pendingRepMap[handle].target, nil
}

// Clear replication.
func (m *ChunkManager) ClearReplication() {
  m.scheduledReps = make([]uint64, 0)
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
	// Construct serverChunkMap to record blocks associated with a server.
	for _, location := range locations {
		if _, ok := m.serverChunkMap[location]; !ok {
			m.serverChunkMap[location] = set.New()
		}
		m.serverChunkMap[location].Add(handle)
	}
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
	if find(m.scheduledReps, handle) {
		return errors.New("scheduled replication")
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

func (m *ChunkManager) addPendingReplication(handle uint64, locations []string) {
	pending, ok := m.pendingRepMap[handle]
	if ok {
		// If Pending replication already exists in the map,
		// Add a target address to it.
		newLocation, err := getNewLocation(m.chunkServers, locations, pending.target)
		if err != nil {
			return
		}
		pending.priority += 1
		pending.target = insertElem(pending.target, newLocation)
	} else {
		newLocation, err := getNewLocation(m.chunkServers, locations, []string{})
		if err != nil {
			return
		}
		m.pendingRepMap[handle] = &pendingReplication{
			priority: 1,
			target:   []string{newLocation},
		}
	}
}

// Get a new location to place a chunk.
func getNewLocation(servers, locations, targets []string) (string, error) {
	// First get 3 random alive servers out.
	candidates := random(servers, 3)
	for _, candidate := range candidates {
		notFound := true
		for _, location := range locations {
			// This check ensure that the new location is not the same
			// as previous locations.
			if candidate == location {
				notFound = false
				break
			}
		}
		if !notFound {
			continue
		}
		for _, target := range targets {
			if candidate == target {
				notFound = false
				break
			}
		}
		if notFound {
			return candidate, nil
		}
	}
	// Should not reach here.
	return "", errors.New("new location not available")
}

// Return true if handles contain handle.
func find(handles []uint64, handle uint64) bool {
	for _, h := range handles {
		if h == handle {
			return true
		}
	}
	return false
}

// Pick num elements randomly from array.
func random(array []string, num int) []string {
	limit := 0
	if num > len(array) {
		limit = len(array)
	} else {
		limit = num
	}
	ret := make([]string, limit)
	perm := rand.Perm(limit)
	for i := 0; i < limit; i++ {
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
