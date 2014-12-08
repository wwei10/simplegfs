package master

import (
  "bytes"
  "encoding/gob"
  "errors"
  "io/ioutil"
  "log"
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

func (m *NamespaceManager) Create(path string) bool {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  return m.add(path, false)
}

func (m *NamespaceManager) Mkdir(path string) bool {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  return m.add(path, true)
}

func (m *NamespaceManager) List(path string) []string {
  m.mutex.RLock()
  defer m.mutex.RUnlock()
  return m.list(path)
}

func (m *NamespaceManager) Delete(path string) bool {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  return m.remove(path)
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
    log.Fatal("NamespaceManager read error:", err)
  }
  data := bytes.NewBuffer(b)
  dec := gob.NewDecoder(data)
  err = dec.Decode(&paths)
  if err != nil {
    log.Fatal("NamespaceManager decode error:", err)
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
    log.Fatal("NamespaceManager encode error:", err)
  }
  // WriteFile will create a new file with specified
  // permission if file doesn't exist.
  err = ioutil.WriteFile(path, data.Bytes(), FilePermRW)
  if err != nil {
    log.Fatal("NamespaceManager write error:", err)
  }
}

// Precondition: m.mutex.Lock() is called.
func (m *NamespaceManager) add(path string, isdir bool) bool {
  parent := getParent(path)
  // Returns false if its parent directory doesn't exist or itself exists
  if !m.exists(parent) || !m.isDir(parent) || m.exists(path) {
    return false
  }
  m.paths[path] = &Info{
    IsDir: isdir,
    Length: 0,
  }
  return true
}

// Precondition: m.mutex.Lock() is called.
func (m *NamespaceManager) remove(path string) bool {
  // Returns false if path doesn't exist in the namespace
  if !m.exists(path) {
    return false
  }
  // Return false if it has children
  if len(m.list(path)) > 0 {
    return false
  }
  // Remove path from metadata
  delete(m.paths, path)
  return true
}

// Precondition: m.mutex.RLock() is called.
func (m *NamespaceManager) list(path string) []string {
  paths := make([]string, 0)
  // If path doesn't exist or it is not a directory,
  // return empty []string.
  if !m.exists(path) || !m.isDir(path) {
    return paths
  }
  for key := range(m.paths) {
    if key != "/" && getParent(key) == path {
      paths = append(paths, key)
    }
  }
  return paths
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
