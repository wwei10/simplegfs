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
  c := New(time.Second * 60, time.Minute)
  defer c.Stop()
  c.Set("/usr", Info{"david"})
  c.Set("/usr/bin", Info{"dawson"})
  testGet(t, c, "/usr", true, "david")
  testGet(t, c, "/usr/bin", true, "dawson")
  testGet(t, c, "/abc", false, "")
}

func TestSetWithTimeout(t *testing.T) {
  c := New(time.Millisecond * 1, time.Minute)
  defer c.Stop()
  c.Set("/usr", Info{"sam"})
  time.Sleep(time.Millisecond)
  testGet(t, c, "/usr", false, "")
  c.SetWithTimeout("/usr", Info{"david"}, time.Second)
  c.SetWithTimeout("/usr/bin", Info{"dawson"}, time.Second)
  testGet(t, c, "/usr", true, "david")
  testGet(t, c, "/usr/bin", true, "dawson")
  time.Sleep(time.Second)
  testGet(t, c, "/usr", false, "")
  testGet(t, c, "/usr/bin", false, "")
}

func TestExpiration(t *testing.T) {
  c := New(time.Second * 1, time.Minute)
  defer c.Stop()
  c.Set("/usr", Info{"david"})
  c.Set("/usr/bin", Info{"dawson"})
  testGet(t, c, "/usr", true, "david")
  testGet(t, c, "/usr/bin", true, "dawson")
  time.Sleep(time.Second * 1)
  testGet(t, c, "/usr", false, "")
  testGet(t, c, "/usr/bin", false, "")
}

func TestDelete(t *testing.T) {
  c := New(time.Second * 60, time.Minute)
  defer c.Stop()
  c.Set("a", Info{"1"})
  c.Set("b", Info{"2"})
  testGet(t, c, "a", true, "1")
  c.Delete("a")
  testGet(t, c, "a", false, "")
  testGet(t, c, "b", true, "2")
  c.Delete("b")
  c.Delete("c")
  testGet(t, c, "b", false, "")
}

func TestSweeper(t *testing.T) {
  c := New(time.Second, time.Second * 2)
  defer c.Stop()
  c.Set("a", Info{"1"})
  c.Set("b", Info{"2"})
  c.Set("c", Info{"2"})
  c.Set("d", Info{"2"})
  c.Set("e", Info{"1"})
  c.Set("f", Info{"2"})
  time.Sleep(time.Second * 2)
  if c.Size() != 0 {
    t.Error("sweep() should have delete all entries")
  }
}
