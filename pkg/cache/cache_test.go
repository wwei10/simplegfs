package cache

import (
  "testing"
  "time"
)

type Info struct {
  name string
}

func testGet(t *testing.T, c *Cache, key string, found bool, want string) {
  info, ok := c.Get(key)
  if ok && !found {
    t.Error("should not find", key)
  }
  if !ok && found {
    t.Error("should have found", key)
  }
  if !ok && !found {
    return
  }
  if info == nil {
    t.Error("should not get nil")
  }
  got := info.(Info).name
  if got != want {
    t.Error("want", want, "got", got)
  }
}

func TestGetAndSet(t *testing.T) {
  c := New(time.Second * 60)
  c.Set("/usr", Info{"david"})
  c.Set("/usr/bin", Info{"dawson"})
  testGet(t, c, "/usr", true, "david")
  testGet(t, c, "/usr/bin", true, "dawson")
  testGet(t, c, "/abc", false, "")
}

func TestExpiration(t *testing.T) {
  c := New(time.Second * 1)
  c.Set("/usr", Info{"david"})
  c.Set("/usr/bin", Info{"dawson"})
  testGet(t, c, "/usr", true, "david")
  testGet(t, c, "/usr/bin", true, "dawson")
  time.Sleep(time.Second * 1)
  testGet(t, c, "/usr", false, "")
  testGet(t, c, "/usr/bin", false, "")
}
