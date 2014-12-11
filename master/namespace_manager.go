package master

import (
  "bytes"
  "encoding/gob"
  "errors"
  sgfsErr "github.com/wweiw/simplegfs/error"
  "io/ioutil"
  log "github.com/Sirupsen/logrus"
  "strings"
  "sync"
)

type Info struct {
  IsDir bool
  Length int64
}

type NamespaceManager struct {
  mutex sync.RWMutex
  paths map[string]*Info
}

func NewNamespaceManager() *NamespaceManager {
  m := &NamespaceManager{
    paths: make(map[string]*Info),
  }
  m.paths["/"] = &Info{
    IsDir: true,
    Length: 0,
  }
  return m
}

func (m *NamespaceManager) Create(path string) (bool, error) {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  return m.add(path, false)
}

func (m *NamespaceManager) Mkdir(path string) (bool, error) {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  return m.add(path, true)
}

func (m *NamespaceManager) List(path string) ([]string, error) {
  m.mutex.RLock()
  defer m.mutex.RUnlock()
  return m.list(path)
}

func (m *NamespaceManager) Delete(path string) (bool, error) {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  return m.remove(path)
}

// Returns true if path exists and is not a directory
func (m *NamespaceManager) Exists(path string) bool {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  return m.exists(path) && !m.isDir(path)
}

// Get file length.
func (m *NamespaceManager) GetFileLength(path string) (int64, error) {
  m.mutex.RLock()
  defer m.mutex.RUnlock()
  info, ok := m.paths[path]
  if !ok {
    return 0, errors.New("File not found.")
  }
  return info.Length, nil
}

// Set file length.
func (m *NamespaceManager) SetFileLength(path string, length int64) {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  info, ok := m.paths[path]
  if !ok {
    // Do nothing.
  }
  info.Length = length
}

// Load data from path to reconstruct namespace.
func (m *NamespaceManager) Load(path string) {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  var paths map[string]*Info
  // ReadFile will read in entire file.
  b, err := ioutil.ReadFile(path)
  if err != nil {
    log.Warnln("NamespaceManager read error:", err)
    return
  }
  data := bytes.NewBuffer(b)
  dec := gob.NewDecoder(data)
  err = dec.Decode(&paths)
  if err != nil {
    log.Warnln("NamespaceManager decode error:", err)
    return
  }
  m.paths = paths
}

// Store entire namespace to path on disk.
func (m *NamespaceManager) Store(path string) {
  // Grab read lock before serialization.
  m.mutex.RLock()
  defer m.mutex.RUnlock()
  var data bytes.Buffer
  enc := gob.NewEncoder(&data)
  err := enc.Encode(m.paths)
  if err != nil {
    log.Warnln("NamespaceManager encode error:", err)
    return
  }
  // WriteFile will create a new file with specified
  // permission if file doesn't exist.
  err = ioutil.WriteFile(path, data.Bytes(), FilePermRW)
  if err != nil {
    log.Warnln("NamespaceManager write error:", err)
    return
  }
}

// Precondition: m.mutex.Lock() is called.
func (m *NamespaceManager) add(path string, isdir bool) (bool, error) {
  parent := getParent(path)
  // Returns false if its parent directory doesn't exist or itself exists
  if !m.exists(parent) {
    return false, &sgfsErr.PathError{"add", path, sgfsErr.ErrParentNotFound}
  }
  if !m.isDir(parent) {
    return false, &sgfsErr.PathError{"add", path, sgfsErr.ErrParentIsNotDir}
  }
  if m.exists(path) {
    return false, &sgfsErr.PathError{"add", path, sgfsErr.ErrKeyExist}
  }
  m.paths[path] = &Info{
    IsDir: isdir,
    Length: 0,
  }
  return true, nil
}

// Precondition: m.mutex.Lock() is called.
func (m *NamespaceManager) remove(path string) (bool, error) {
  // Returns false if path doesn't exist in the namespace
  if !m.exists(path) {
    return false, &sgfsErr.PathError{"remove", path, sgfsErr.ErrKeyNotFound}
  }
  // Return false if it has children
  if m.isDir(path) {
    ret, err := m.list(path)
    if err != nil {
      return false, err
    }
    if len(ret) > 0 {
      return false, &sgfsErr.PathError{"remove", path, sgfsErr.ErrDirNotEmpty}
    }
  }
  // Remove path from metadata
  delete(m.paths, path)
  return true, nil
}

// Precondition: m.mutex.RLock() is called.
func (m *NamespaceManager) list(path string) ([]string, error) {
  paths := make([]string, 0)
  // If path doesn't exist or it is not a directory,
  // return empty []string.
  if !m.exists(path) {
    return paths, &sgfsErr.PathError{"list", path, sgfsErr.ErrKeyNotFound}
  }
  if !m.isDir(path) {
    return paths, &sgfsErr.PathError{"list", path, sgfsErr.ErrIsNotDir}
  }
  for key := range(m.paths) {
    if key != "/" && getParent(key) == path {
      paths = append(paths, key)
    }
  }
  return paths, nil
}

// Precondition: m.mutex.RLock() is called.
// Returns true if path exists in the namespace.
func (m *NamespaceManager) exists(path string) bool {
  _, ok := m.paths[path]
  if !ok {
    return false
  }
  return true
}

// Precondition: m.mutex.RLock() is called.
// Returns true if path exists and it is a directory.
func (m *NamespaceManager) isDir(path string) bool {
  info, ok := m.paths[path]
  if !ok {
    return false
  }
  return info.IsDir
}

// Returns parent path.
func getParent(path string) string {
  idx := strings.LastIndex(path, "/")
  if idx == -1 || idx == 0 {
    return "/"
  }
  return path[:idx]
}
