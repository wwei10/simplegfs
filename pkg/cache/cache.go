package cache

import (
  "sync"
  "time"
)

// Cache entry that stores expiration time with a value
type Entry struct {
  Value interface{}
  Time time.Time // Expiration time
}

// TODO: Currently cache entry never expires
func (entry *Entry) Expired() bool {
  return false
}

type Cache struct {
  kv map[string]*Entry
  lock sync.RWMutex
  timeout time.Duration // Timeout for cache entries
}

func New(timeout time.Duration) *Cache {
  c := &Cache{
    kv: make(map[string]*Entry),
    timeout: timeout,
  }
  return c
}

func (c *Cache) Get(key string) (interface{}, bool) {
  c.lock.RLock()
  defer c.lock.RUnlock()
  entry, ok := c.kv[key]
  if !ok || entry.Expired() {
    // If key not found or time expired, return false
    return nil, false
  }
  return entry.Value, true
}

func (c *Cache) Set(key string, value interface{}) {
  c.lock.Lock()
  defer c.lock.Unlock()
  entry, ok := c.kv[key]
  if ok {
    // Set new value
    entry.Value = value
    // Extend expiration time
  } else {
    entry := &Entry{
      Value: value,
      Time: time.Now().Add(c.timeout),
    }
    c.kv[key] = entry
  }
}
