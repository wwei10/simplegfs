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

// Write handles client RPC write requests to the primary chunk. The primary
// first applies requested write to its local storage, serializes and records
// the order of application in ChunkServer.writeRequests, then sends the write
// requests to secondary replicas.
func (cs *ChunkServer) Write(args WriteArgs, reply *WriteReply) error {
  log.Debugln(cs.me, "ChunkServer: Write RPC.")
  cs.mutex.Lock()
  log.Debugln(cs.me, "ChunkServer: Write RPC. Lock Acquired")

  // Extract/define arguments.
  dataId := args.DataId
  chunkhandle := args.ChunkHandle
  off := int64(args.Offset)
  data, ok := cs.data[dataId]
  if !ok {
    log.Debugln(cs.me, "ChunkServer: Write RPC. Lock Released.")
    cs.mutex.Unlock()
    return errors.New("ChunkServer.Write: requested data is not in memory")
  }
  length := int64(len(data))
  filename := fmt.Sprintf("%d", chunkhandle)

  // Apply write request to local state.
  if err := cs.applyWrite(filename, data, off); err != nil {
    log.Debugln(cs.me, "ChunkServer: Write RPC. Lock Released.")
    cs.mutex.Unlock()
    return err
  } else {
    delete(cs.data, dataId)
  }

  // Update chunkserver metadata.
  cs.reportChunkInfo(chunkhandle, args.ChunkIndex, args.Path, length, off)

  cs.mutex.Unlock()
  // Apply the write to all secondary replicas.
  if err := cs.applyToSecondary(args, reply); err != nil {
    log.Debugln(cs.me, "ChunkServer: Write RPC. Lock Released.")
    cs.mutex.Unlock()
    return err
  }
  cs.mutex.Lock()

  // Since we are still writing to the chunk, we must continue request
  // lease extensions on the chunk.
  cs.addChunkExtensionRequest(chunkhandle)
  log.Debugln(cs.me, "ChunkServer: Write RPC. Lock Released.")
  cs.mutex.Unlock()
  return nil
}

// SerializedWrite handles RPC calls from primary replica's write requests to
// secondary replicas.
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
  } else if !args.IsAppend {
    delete(cs.data, args.DataId)
  }

  // Update chunkserver metadata.
  length := int64(len(data))
  cs.reportChunkInfo(args.ChunkHandle, args.ChunkIndex, args.Path,
                           length, int64(args.Offset))
  return nil
}

func (cs *ChunkServer) Read(args ReadArgs, reply *ReadReply) error {
  log.Debugln(cs.me, "Read RPC.")
  chunkhandle := args.ChunkHandle
  off := args.Offset
  length := args.Length
  bytes := make([]byte, length)
  filename := fmt.Sprintf("%d", chunkhandle)
  file, err := os.Open(cs.path + "/" + filename)
  if err != nil {
    log.Fatal(err)
  }
  defer file.Close()
  n, err := file.ReadAt(bytes, off)
  if err != nil {
    switch err {
    case io.EOF:
      reply.Length = n
      reply.Bytes = bytes
    default:
      log.Fatal(err)
    }
  } else {
    reply.Length = n
    reply.Bytes = bytes
  }
  return nil
}

// Kill for testing.
func (cs *ChunkServer) Kill() {
  cs.dead = true
  cs.l.Close()
}

// PushData handles client RPC to store data in memory. Data is identified with
// a mapping from DataId:[ClientID, Timestamep] -> Data.
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
  } else {
    delete(cs.data, args.DataId)
  }

  // Update chunkserver metadata.
  cs.reportChunkInfo(args.ChunkHandle, args.ChunkIndex, args.Path, length,
                     chunkLength)

  // Apply append to all secondary replicas.
  writeArgs := WriteArgs{
    DataId: args.DataId,
    ChunkHandle: args.ChunkHandle,
    ChunkIndex: args.ChunkIndex,
    Path: args.Path,
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

// loadChunkServerMeta loads chunk server's meta data when called by
// StartChunkServer.
func loadChunkServerMeta (c *ChunkServer) {
  f, err := os.Open(c.chunkServerMeta)
  if err != nil {
    log.Debugln("Open file ", c.chunkServerMeta, " failed.");
  }
  defer f.Close()
  parseChunkServerMeta(f, c)
}

// parseChunkServerMeta parses each line in chunkServerMeta file and loads
// each value into its corresponding ChunkServer instance fields.
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

// storeChunkServerMeta stores chunk server meta data on disk.
func storeChunkServerMeta (c *ChunkServer) {
  f, er := os.OpenFile(c.chunkServerMeta, os.O_RDWR|os.O_CREATE, FilePermRW)
  if er != nil {
    log.Debugln("Open/Create file ", c.chunkServerMeta, " failed.")
  }
  defer f.Close()

  // Store map chunkhandle -> file version number
  storeChunkhandle2VersionNum(c, f)
}

// storeChunkhandle2VersionNum Store chunckhandle to file version number mapping from
// ChunkServer.chunkhandle2VersionNum to ChunkServer.chunkServerMeta.
func storeChunkhandle2VersionNum (c *ChunkServer, f *os.File) {
  b := encodeMap(&c.chunkhandle2VersionNum)
  n, err := f.WriteString("chunkhandle2VersionNum " + b.String() + "\n")
  if err != nil {
    log.Debugln(err)
  } else {
    log.Debugf("Wrote %d bytes into %s\n", n, c.chunkServerMeta)
  }
}

// encodeMap encodes a uint64 -> uint64 map into bytes.Buffer
func encodeMap(m *map[uint64]uint64) *bytes.Buffer {
  b := new(bytes.Buffer)
  e := gob.NewEncoder(b)
  err := e.Encode(m)
  if err != nil {
    log.Debugln(err)
  }
  return b
}

// decodeMap decodes a bytes.Buffer into a uint64 -> uint64 map
func decodeMap(b *bytes.Buffer) map[uint64]uint64 {
  d := gob.NewDecoder(b)
  var decodedMap map[uint64]uint64
  err := d.Decode(&decodedMap)
  if err != nil {
    log.Debugln(err)
  }
  return decodedMap
}

// addChunkExtensionRequests add chunkhandle and time of write request to
// ChunkServer.pendingExtensions. These requests are batched together and
// piggybacked onto Heartbeat messages with the master.
// Note: ChunkServer.mutex must be held before calling this function.
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

// applyWrite is a helper function for Write and SerializedWrite to apply
// writes from memory to local storage.
// Note: ChunkServer.mutex must be held before calling this function.
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

// reportChunkInfo is a helper function for ChunkServer.Write,
// ChunkServer.SerializedWrite and ChunkServer.Append to update chunkserver's
// metadata after a write request.
func (cs *ChunkServer) reportChunkInfo(chunkhandle, chunkindex uint64,
                                       path string,
                                       length, offset int64) {
  // Update chunkserver metadata.
  _, ok := cs.chunks[chunkhandle]
  // If we have never seen this chunk before,
  // or chunk size has changed, we should
  // report to Master immediately.
  if !ok {
    cs.chunks[chunkhandle] = &ChunkInfo{
      Path: path,
      ChunkHandle: chunkhandle,
      ChunkIndex: chunkindex,
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
  cs.reportChunkInfo(args.ChunkHandle, args.ChunkIndex, args.Path, padLength,
                     offset)

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
