package simplegfs

import (
  "errors"
  "fmt"
  "io"
  "time"
  "bytes"
  "encoding/gob"
  "bufio"
  "strings"
  log "github.com/Sirupsen/logrus"
  "net"
  "net/rpc"
  "os"
  sgfsErr "github.com/wweiw/simplegfs/error"
  "sync"
)

type ChunkServer struct {
  dead bool
  l net.Listener
  me string
  masterAddr string

  // Chunkhandle -> file version number, updated whenever the chunk is being
  // written to and has a lower file version number than client's file version
  // number.
  chunkhandle2VersionNum map[uint64]uint64

  // Filename of a file that contains chunkserver meta data
  // TODO Should later mutates into a log file of some sort
  chunkServerMeta string

  path string
  chunks map[uint64]*ChunkInfo // Store a mapping from handle to information.
  mutex sync.RWMutex

  // Stores pending lease extension requests on chunkhandles.
  pendingExtensions []uint64
  // Only used by Hearbeat and ChunkServer.addChunkExtensionRequest.
  // Possible to have ChunkServer.mutex held first, therefore do not acquire
  // ChunkServer.mutex after acquire this lock.
  pendingExtensionsLock sync.RWMutex

  // Stores client's data in memory before commit to disk.
  data map[DataId][]byte
  dataMutex sync.RWMutex
}

// RPC handler declared here

// ChunkServer.Write
//
// Handles client RPC write requests to the primary chunk. The primary first
// applies requested write to its local state, serializes and records the order
// of application in ChunkServer.writeRequests, then sends the write requests
// to secondary replicas.
//
// params - WriteArgs: DataId, identifies the data stores in ChunkServer.data
//                     ChunkHanlde, unique ID of the chunk being write to.
//                     ChunkIndex, chunk's position in its file.
//                     Offset, offset within the chunk.
//                     Path, name of the file.
//                     Chunklocations, replicas that store the chunk.
//          WriteReply: None.
// return - Appropriate error if any, nil otherwise.
func (cs *ChunkServer) Write(args WriteArgs, reply *WriteReply) error {
  log.Debugln(cs.me, "ChunkServer: Write RPC.")
  cs.mutex.Lock()
  defer cs.mutex.Unlock()

  // Extract/define arguments.
  dataId := args.DataId
  chunkhandle := args.ChunkHandle
  off := int64(args.Offset)
  data, ok := cs.data[dataId]
  if !ok {
    return errors.New("ChunkServer.Write: requested data is not in memory")
  }
  length := int64(len(data))
  filename := fmt.Sprintf("%d", chunkhandle)

  // Apply write request to local state.
  if err := cs.applyWrite(filename, data, off); err != nil {
    return err
  }

  // Update chunkserver metadata.
  cs.reportChunkInfo(chunkhandle, length, off)

  // Apply the write to all secondary replicas.
  if err := cs.applyToSecondary(args, reply); err != nil {
    return err
  }

  // Since we are still writing to the chunk, we must continue request
  // lease extensions on the chunk.
  cs.addChunkExtensionRequest(chunkhandle)
  return nil
}

// ChunkServer.SerializedWrite
//
// RPC handler for primary chunkservers to send write requests to secondary
// chunkservers.
//
// params - WriteArgs: DataId, identifies the data stores in ChunkServer.data
//                     ChunkHanlde, unique ID of the chunk being write to.
//                     ChunkIndex, chunk's position in its file.
//                     Offset, offset within the chunk.
//                     Path, name of the file.
//          WriteReply: None.
// return - Appropriate error if any, nil otherwise.
func (cs *ChunkServer) SerializedWrite(args WriteArgs, reply *WriteReply) error {
  log.Debugln(cs.me, "ChunkServer: SerializedWrite RPC")
  cs.mutex.Lock()
  defer cs.mutex.Unlock()

  var data []byte
  var ok bool
  if args.IsAppend {
    // Padding chunk with zeros.
    padLength := ChunkSize - args.Offset
    data = make([]byte, padLength)
  } else {
    // Fetch data from ChunkServer.data
    data, ok = cs.data[args.DataId]
    if !ok {
      return errors.New("ChunkServer.SerializedWrite: requested data " +
                        "is not in memory")
    }
  }

  // Apply write reqeust to local state.
  filename := fmt.Sprintf("%d", args.ChunkHandle)
  if err := cs.applyWrite(filename, data, int64(args.Offset)); err != nil {
    return err
  }

  // Update chunkserver metadata.
  length := int64(len(data))
  cs.reportChunkInfo(args.ChunkHandle, length, int64(args.Offset))
  return nil
}

func (cs *ChunkServer) Read(args ReadArgs, reply *ReadReply) error {
  log.Debugln(cs.me, "Read RPC.")
  chunkhandle := args.ChunkHandle
  off := args.Offset
  length := args.Length
  bytes := make([]byte, length)
  filename := fmt.Sprintf("%d", chunkhandle)
  n, err := cs.read(filename, bytes, off)
  reply.Length = n
  reply.Bytes = bytes
  return err
}

// Kill for testing.
func (cs *ChunkServer) Kill() {
  cs.dead = true
  cs.l.Close()
}

// ChunkServer.PushData
//
// Handles client RPC to store data in memory. Data is identified with a
// mapping from DataID:[ClientID, Timestamp] -> Data.
// This function acquires lock on ChunkServer.dataMutex
//
// params - PushDataArgs: ClientId, client's ID.
//                        Timestamp, client assigned timestamp, monotonically
//                        increasing on each client instance.
// return - nil.
func (cs *ChunkServer) PushData(args PushDataArgs, reply *PushDataReply) error {
  log.Debugln("ChunkServer: PushData RPC")
  dataId := args.DataId

  // Acquire lock on ChunkServer.Data
  cs.dataMutex.Lock()
  defer cs.dataMutex.Unlock()

  // If data already exists, then just return.
  if _, ok := cs.data[dataId]; ok {
    return nil
  }

  // Otherwise push data into chunkserver's memory.
  cs.data[dataId] = args.Data
  return nil
}

// Handles chunk server RPC to replicate a chunk. Data is in replicate chunk args.
func (cs *ChunkServer) ReplicateChunk(args ReplicateChunkArgs, reply *ReplicateChunkReply) error {
  log.Debugln("ChunkServer: ReplicateChunk")
  filename := fmt.Sprintf("%d", args.Handle)
  log.Infoln("ChunkServer: Replication", filename)
  data := args.Data
  if err := cs.applyWrite(filename, data, 0); err != nil {
    return err
  }
  cs.reportChunkInfo(args.Handle, int64(len(data)), 0)
  return nil
}

// Master sends a command to chunk server to start replication of a chunk.
func (cs *ChunkServer) StartReplicateChunk(args StartReplicateChunkArgs, reply *StartReplicateChunkReply) error {
  handle := args.Handle
  address := args.Address
  argsRep := ReplicateChunkArgs{}
  bytes := make([]byte, ChunkSize)
  filename := fmt.Sprintf("%d", handle)
  n, err := cs.read(filename, bytes, 0)
  if err != nil {
    return err
  }
  log.Infoln("StartReplicateChunk:", filename, handle, address, argsRep)
  argsRep.Handle = handle
  argsRep.Data = bytes[:n]
  replyRep := new(StartReplicateChunkReply)
  err = call(address, "ChunkServer.ReplicateChunk", argsRep, replyRep)
  return err
}

// Append accepts client append request and append the data to an offset
// chosen by the primary replica. It then serializes the request just like
// Write request, and send it to all secondary replicas.
// If appending the record to the current chunk would cause the chunk to
// exceed the maximum size, append fails and the client must retry.
// It also puts the offset chosen in AppendReply so the client knows where
// the data is appended to.
func (cs *ChunkServer) Append(args AppendArgs, reply *AppendReply) error {
  log.Debugln(cs.me, "ChunkServer: Append RPC.")
  cs.mutex.Lock()
  defer cs.mutex.Unlock()

  // Extract/define arguments.
  data, ok := cs.data[args.DataId]
  if !ok {
    return sgfsErr.ErrDataNotInMem
  }
  length := int64(len(data))
  filename := fmt.Sprintf("%d", args.ChunkHandle)

  // Get length of the current chunk so we can calculate an offset.
  chunkInfo, ok := cs.chunks[args.ChunkHandle]
  // If we cannot find chunkInfo, means this is a new chunk, therefore offset
  // should be zero, otherwise the offset should be the chunk length.
  chunkLength := int64(0)
  if ok {
    chunkLength = chunkInfo.Length
  }

  // If appending the record to the current chunk would cause the chunk to
  // exceed the maximum size, report error for client to retry at another
  // chunk.
  if chunkLength + length >= ChunkSize {
    if err := cs.padChunk(&filename, chunkLength, &args); err != nil {
      return err
    }
    return sgfsErr.ErrNotEnoughSpace
  }

  // Apply write request to local state, with chunkLength as offset.
  if err := cs.applyWrite(filename, data, chunkLength); err != nil {
    return err
  }

  // Update chunkserver metadata.
  cs.reportChunkInfo(args.ChunkHandle, length, chunkLength)

  // Apply append to all secondary replicas.
  writeArgs := WriteArgs{
    DataId: args.DataId,
    ChunkHandle: args.ChunkHandle,
    ChunkIndex: args.ChunkIndex,
    ChunkLocations: args.ChunkLocations,
    Offset: uint64(chunkLength),
  }
  writeReply := new(WriteReply)
  if err := cs.applyToSecondary(writeArgs, writeReply); err != nil {
    return err
  }

  // Since we are still writing to the chunk, we must continue request
  // lease extensions on the chunk.
  cs.addChunkExtensionRequest(args.ChunkHandle)

  // Set offset for returning to client.
  reply.Offset = uint64(chunkLength) + args.ChunkIndex * ChunkSize
  return nil
}

func StartChunkServer(masterAddr string, me string, path string) *ChunkServer {
  cs := &ChunkServer{
    dead: false,
    me: me,
    masterAddr: masterAddr,
    chunkServerMeta: "chunkServerMeta" + me,
    path: path,
    chunks: make(map[uint64]*ChunkInfo),
    data: make(map[DataId][]byte),
  }

  rpcs := rpc.NewServer()
  rpcs.Register(cs)

  l, e := net.Listen("tcp", cs.me)
  if e != nil {
    log.Fatal("listen error", e)
  }
  cs.l = l


  // RPC handler for client library.
  go func() {
    for cs.dead == false {
      conn, err := cs.l.Accept()
      if err == nil && cs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      } else if err != nil && cs.dead == false {
        log.Debugln("Kill chunk server.")
        cs.Kill()
      }
    }
  }()

  // Heartbeat
  go func() {
    for cs.dead == false {
      // Send lease extension requests with Heartbeat message, if any.
      cs.pendingExtensionsLock.Lock()
      args := &HeartbeatArgs{
        Addr: cs.me,
        PendingExtensions: cs.pendingExtensions,
      }
      // Clear pending extensions.
      cs.pendingExtensions = nil
      cs.pendingExtensionsLock.Unlock()
      var reply HeartbeatReply
      call(cs.masterAddr, "MasterServer.Heartbeat", args, &reply)
      time.Sleep(HeartbeatInterval)
    }
  }()

  return cs
}

// Helper functions
func reportChunk(cs *ChunkServer, info *ChunkInfo) {
  args := ReportChunkArgs{
    ServerAddress: cs.me,
    ChunkHandle: info.ChunkHandle,
    ChunkIndex: info.ChunkIndex,
    Length: info.Length,
    Path: info.Path,
  }
  reply := new(ReportChunkReply)
  // TODO: What if this RPC call failed?
  go call(cs.masterAddr, "MasterServer.ReportChunk", args, reply)
}

// Chunkserver meta data is laoded in StartChunkServer
//
// param  - c: a pointer to a ChunkServer instance
// return - none
func loadChunkServerMeta (c *ChunkServer) {
  f, err := os.Open(c.chunkServerMeta)
  if err != nil {
    log.Debugln("Open file ", c.chunkServerMeta, " failed.");
  }
  defer f.Close()
  parseChunkServerMeta(f, c)
}

// Called by loadChunkServerMeta
// This function parses each line in chunkServerMeta file and loads each value
// into its correspoding ChunkServer fields
//
// param  - c: a pointer to a ChunkServer instance
//          f: a pointer to the file chunkServerMeta
// return - none
func parseChunkServerMeta(f *os.File, c *ChunkServer) {
  scanner := bufio.NewScanner(f)
  for scanner.Scan() {
    fields := strings.Fields(scanner.Text())
    switch fields[0] {
    case "chunkhandle2VersionNum":
      // The map was encoded into bytes.Buffer then to string, now we must
      // convert from string to bytes.Buffer then decode it back to map.
      b := bytes.NewBufferString(fields[1])
      c.chunkhandle2VersionNum = decodeMap(b)
    default:
      log.Debugln("Unknown ChunkServer key")
    }
  }
}

// To store chunk server meta data on disk.
//
// param  - c: a pointer to a ChunkServer instance
// return - none
func storeChunkServerMeta (c *ChunkServer) {
  f, er := os.OpenFile(c.chunkServerMeta, os.O_RDWR|os.O_CREATE, FilePermRW)
  if er != nil {
    log.Debugln("Open/Create file ", c.chunkServerMeta, " failed.")
  }
  defer f.Close()

  // Store map chunkhandle -> file version number
  storeChunkhandle2VersionNum(c, f)
}

// Store chunckhanle to file version number mapping from
// ChunkServer.chunkhandle2VersionNum to ChunkServer.chunkServerMeta.
//
// param  - c: a pointer to a ChunkServer instance
//          f: a pointer to the file chunkServerMeta
// return - none
func storeChunkhandle2VersionNum (c *ChunkServer, f *os.File) {
  b := encodeMap(&c.chunkhandle2VersionNum)
  n, err := f.WriteString("chunkhandle2VersionNum " + b.String() + "\n")
  if err != nil {
    log.Debugln(err)
  } else {
    log.Debugf("Wrote %d bytes into %s\n", n, c.chunkServerMeta)
  }
}

// Encode a uint64 -> uint64 map into bytes.Buffer
//
// param  - m: the map to be encoded
// return - bytes.Buffer containing the encoded map
func encodeMap(m *map[uint64]uint64) *bytes.Buffer {
  b := new(bytes.Buffer)
  e := gob.NewEncoder(b)
  err := e.Encode(m)
  if err != nil {
    log.Debugln(err)
  }
  return b
}

// Decode a bytes.Buffer into a uint64 -> uint64 map
//
// param  - b: bytes.Buffer containing the encoded map
// return - the encoded map
func decodeMap(b *bytes.Buffer) map[uint64]uint64 {
  d := gob.NewDecoder(b)
  var decodedMap map[uint64]uint64
  err := d.Decode(&decodedMap)
  if err != nil {
    log.Debugln(err)
  }
  return decodedMap
}

// ChunkServer.addChunkExtensionRequest
//
// Called by ChunkServer.Write.
// Add chunkhandle and time of write request to Chunkserver.pendingExtensions.
// These requests are batched together and piggybacked onto Heartbeat messages
// with the master.
// Note: ChunkServer.mutex is held by ChunkServer.Write.
//
// params - chunkHandle: Unique ID for the chunk to be request extension on.
// return - None.
func (cs *ChunkServer) addChunkExtensionRequest(chunkHandle uint64) {
  cs.pendingExtensionsLock.Lock()
  defer cs.pendingExtensionsLock.Unlock()
  // Do not add duplicate requests.
  for _, r := range cs.pendingExtensions {
    if r == chunkHandle {
      return
    }
  }
  cs.pendingExtensions = append(cs.pendingExtensions, chunkHandle)
}

// ChunkServer.applyWrite
//
// Helper function for ChunkServer.Write(primary chunkserver) and
// ChunkServer.SerializedWrite(secondary chunkserver) to apply writes from
// memory to local state.
// Note: ChunkServer.mutex is held by the caller.
//
// params - filename: Chunkhandle's string representation, on disk file name.
//          data: In memory data to be written to disk.
//          offset: Staring offset for writes.
// return - Appropriate error if any, nil otherwise.
func (cs *ChunkServer) applyWrite(filename string, data []byte, offset int64) error {
  // Open file that stores the chunk.
  file, err := os.OpenFile(cs.path + "/" + filename, os.O_WRONLY | os.O_CREATE,
                           FilePermRWX)
  if err != nil {
    return err
  }
  defer file.Close()

  // Write data to chunk, and serializes the order of concurrent write requests.
  file.WriteAt(data, offset)
  return nil
}

// Read filename from offset and fill the data into bytes.
func (cs *ChunkServer) read(filename string, bytes []byte, offset int64) (int, error) {
  file, err := os.Open(cs.path + "/" + filename)
  if err != nil {
    log.Fatal(err)
  }
  defer file.Close()
  n, err := file.ReadAt(bytes, offset)
  if err != nil {
    switch err {
    case io.EOF:
      return n, nil
    default:
      return 0, err
    }
  } else {
    return n, nil
  }
}

// reportChunkInfo is a helper function for ChunkServer.Write,
// ChunkServer.SerializedWrite and ChunkServer.Append to update chunkserver's
// metadata after a write request.
func (cs *ChunkServer) reportChunkInfo(chunkhandle uint64, length, offset int64) {
  // Update chunkserver metadata.
  _, ok := cs.chunks[chunkhandle]
  // If we have never seen this chunk before,
  // or chunk size has changed, we should
  // report to Master immediately.
  if !ok {
    cs.chunks[chunkhandle] = &ChunkInfo{
      ChunkHandle: chunkhandle,
    }
  }
  chunkInfo := cs.chunks[chunkhandle]
  if offset + length > chunkInfo.Length {
    chunkInfo.Length = offset + length
    reportChunk(cs, chunkInfo)
  }
}

// padChunk is called by the primary replica that pads the target chunk
// with zeros. After padding the chunk on local server, it also directs
// secondary chunk servers to do the same by sending serialized write requests.
// Note that cs.mutex must be held before calling this function.
// fileName - File name of the chunk on disk.
// offset - Location of the file to start padding.
func (cs *ChunkServer) padChunk(fileName *string, offset int64,
                                args *AppendArgs) error {
  log.Debugln("ChunkServer.padChunk:", args.Path, *fileName, offset)

  // Pad the chunk locally.
  padLength := ChunkSize - offset
  data := make([]byte, padLength)
  if err := cs.applyWrite(*fileName, data, offset); err != nil {
    return err
  }

  // Update chunkserver metadata.
  cs.reportChunkInfo(args.ChunkHandle, padLength, offset)

  // Apply pad to all secondary replicas.
  writeArgs := WriteArgs{
    DataId: args.DataId,
    ChunkHandle: args.ChunkHandle,
    ChunkIndex: args.ChunkIndex,
    Path: args.Path,
    ChunkLocations: args.ChunkLocations,
    Offset: uint64(offset),
    IsAppend: true,
  }
  writeReply := new(WriteReply)
  if err := cs.applyToSecondary(writeArgs, writeReply); err != nil {
    return err
  }

  // Since we are still writing to the chunk, we must continue request
  // lease extensions on the chunk.
  cs.addChunkExtensionRequest(args.ChunkHandle)

  return nil
}

// applyToSecondary is used by the primary replica to apply any modifications
// that are serialized by the replica, to all of its secondary replicas.
func (cs *ChunkServer) applyToSecondary(args WriteArgs, reply *WriteReply) error {
  // RPC each secondary chunkserver to apply the write.
  for _, chunkLocation := range args.ChunkLocations {
    if chunkLocation != cs.me {
       err := call(chunkLocation, "ChunkServer.SerializedWrite", args, reply)
       if err != nil {
        return err
      }
    }
  }
  return nil
}
