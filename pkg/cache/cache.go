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

// Returns true if cache entry expires
func (entry *Entry) Expired() bool {
  return time.Now().After(entry.Time)
}

type Cache struct {
  kv map[string]*Entry
  lock sync.RWMutex
  timeout time.Duration // Timeout for cache entries
  interval time.Duration // Time interval for garbage collection
  sweeper *Sweeper
}

type Sweeper struct {
  stop chan bool // Stop working after receiving a stop signal
  interval time.Duration // Time interval for garbage collection
}

func New(timeout time.Duration, interval time.Duration) *Cache {
  c := &Cache{
    kv: make(map[string]*Entry),
    timeout: timeout,
    interval: interval,
    sweeper: newSweeper(interval),
  }
  runSweeper(c)
  return c
}

// Call Stop() to stop sweeper.
func (c *Cache) Stop() {
  c.sweeper.stop <- true
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
  _, ok := c.kv[key]
  if ok {
    // Set new value
    c.kv[key].Value = value
    // Extend expiration time
    c.kv[key].Time = time.Now().Add(c.timeout)
  } else {
    entry := &Entry{
      Value: value,
      Time: time.Now().Add(c.timeout),
    }
    c.kv[key] = entry
  }
}

func (c *Cache) Delete(key string) {
  c.lock.Lock()
  defer c.lock.Unlock()
  delete(c.kv, key)
}

func (c *Cache) Size() int {
  c.lock.RLock()
  defer c.lock.RUnlock()
  return len(c.kv)
}

// Purges all entries which expires.
func sweep(c *Cache) {
  c.lock.Lock()
  defer c.lock.Unlock()
  for key, value := range c.kv {
    if value.Expired() {
      delete(c.kv, key)
    }
  }
}

// Stop Sweeper instance associated with Cache.
func stopSweeper(c *Cache) {
  c.sweeper.stop <- true
}

// Runs Sweeper instance on Cache.
func runSweeper(c *Cache) {
  s := c.sweeper
  ticks := time.Tick(s.interval)
  go func() {
    for {
      select {
      case <-s.stop:
        return
      case <-ticks:
        sweep(c)
      }
    }
  }()
}

// Returns a new Sweeper
func newSweeper(interval time.Duration) *Sweeper {
  s := &Sweeper{
    stop: make(chan bool),
    interval: interval,
  }
  return s
}
