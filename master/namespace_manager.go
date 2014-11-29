package master

import (
  "strings"
  "sync"
)

type Info struct {
  isdir bool
  length int64
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
    isdir: true,
    length: 0,
  }
  return m
}

func (m *NamespaceManager) Create(path string) bool {
  return m.add(path, false)
}

func (m *NamespaceManager) Mkdir(path string) bool {
  return m.add(path, true)
}

func (m *NamespaceManager) add(path string, isdir bool) bool {
  m.mutex.Lock()
  defer m.mutex.Unlock()
  parent := getParent(path)
  // Returns false if its parent doesn't exist or itself exists
  if !m.exists(parent, true) || m.exists(path, true) || m.exists(path, false) {
    return false
  }
  m.paths[path] = &Info{
    isdir: isdir,
    length: 0,
  }
  return true
}

// Returns true if path exists in the namespace.
func (m *NamespaceManager) exists(path string, isdir bool) bool {
  info, ok := m.paths[path]
  if !ok {
    return false
  }
  if info.isdir != isdir {
    return false
  }
  return true
}

// Returns parent path.
func getParent(path string) string {
  idx := strings.LastIndex(path, "/")
  if idx == -1 || idx == 0 {
    return "/"
  }
  return path[:idx]
}
