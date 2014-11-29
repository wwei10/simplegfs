package master

import (
  "fmt"
  "testing"
)

func assertEquals(t *testing.T, got, want string) {
  if got != want {
    t.Error("Got '" + got + "', want '" + want + "'")
  }
}

func assertTrue(t *testing.T, got bool, message string) {
  if !got {
    t.Error(message)
  }
}

func assertFalse(t *testing.T, got bool, message string) {
  if got {
    t.Error(message)
  }
}

func TestGetParent(t *testing.T) {
  assertEquals(t, getParent(""), "/") // Is this the expected behavior?
  assertEquals(t, getParent("/"), "/")
  assertEquals(t, getParent("/a/b"), "/a")
  assertEquals(t, getParent("/a/b/c"), "/a/b")
  assertEquals(t, getParent("/a/b/c"), "/a/b")
}

func TestCreateAndMkdir(t *testing.T) {
  m := NewNamespaceManager()
  assertTrue(t, m.Create("/a"), "Create /a should return true")
  assertTrue(t, m.Mkdir("/b"), "Mkdir /b should return true")
  assertTrue(t, m.Mkdir("/b/d"), "Mkdir /b/d should return true")
  assertTrue(t, m.Mkdir("/b/d/e"), "Create /b/d/e should return true")
  assertTrue(t, m.Create("/b/c"), "Create /b/c should return true")
  assertFalse(t, m.Mkdir("/a"), "Mkdir /a should return false")
  assertFalse(t, m.Create("/a/b"), "Create /a/b should return false") // /a is not a directory
  assertFalse(t, m.Create("/"), "Create / should return fasle")
}

func TestList(t *testing.T) {
  m := NewNamespaceManager()
  m.Mkdir("/a")
  m.Mkdir("/b")
  m.Mkdir("/c")
  m.Mkdir("/a/a")
  m.Mkdir("/a/b")
  m.Mkdir("/a/c")
  m.Mkdir("/a/a/b")
  m.Mkdir("/a/a/c")
  fmt.Println("List /a", m.List("/a"))
  fmt.Println("List /a/a", m.List("/a/a"))
  fmt.Println("List /", m.List("/"))
}
