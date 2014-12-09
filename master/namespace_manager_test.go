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

func create(m *NamespaceManager, path string) bool {
  ok, err := m.Create(path)
  if err != nil {
    fmt.Println(err)
  }
  return ok
}

func mkdir(m *NamespaceManager, path string) bool {
  ok, err := m.Mkdir(path)
  if err != nil {
    fmt.Println(err)
  }
  return ok
}

func remove(m *NamespaceManager, path string) bool {
  ok, err := m.Delete(path)
  if err != nil {
    fmt.Println(err)
  }
  return ok
}

func list(m *NamespaceManager, path string) []string {
  ret, err := m.List(path)
  if err != nil {
    fmt.Println(err)
  }
  return ret
}

func TestCreateAndMkdir(t *testing.T) {
  m := NewNamespaceManager()
  assertTrue(t, create(m, "/a"), "Create /a should return true")
  assertTrue(t, mkdir(m, "/b"), "Mkdir /b should return true")
  assertTrue(t, mkdir(m, "/b/d"), "Mkdir /b/d should return true")
  assertTrue(t, mkdir(m, "/b/d/e"), "Create /b/d/e should return true")
  assertTrue(t, create(m, "/b/c"), "Create /b/c should return true")
  assertFalse(t, mkdir(m, "/a"), "Mkdir /a should return false")
  assertFalse(t, create(m, "/a/b"), "Create /a/b should return false") // /a is not a directory
  assertFalse(t, create(m, "/"), "Create / should return fasle")
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
  fmt.Println("List /a", list(m, "/a"))
  fmt.Println("List /a/a", list(m, "/a/a"))
  fmt.Println("List /", list(m, "/"))
}

func TestDelete(t *testing.T) {
  m := NewNamespaceManager()
  m.Mkdir("/a")
  m.Mkdir("/a/b")
  m.Mkdir("/a/c")
  assertFalse(t, remove(m, "/a"), "Delete /a should return false")
  assertTrue(t, remove(m, "/a/b"), "Delete /a/b should return true")
  assertTrue(t, remove(m, "/a/c"), "Delete /a/c should return true")
  assertTrue(t, remove(m, "/a"), "Delete /a should return true")
  m.Mkdir("/a")
  m.Mkdir("/b")
  m.Mkdir("/b/c")
  m.Mkdir("/b/c/d")
  assertFalse(t, remove(m, "/b/c"), "Delete /b/c should return false")
  assertTrue(t, remove(m, "/b/c/d"), "Delete /b/c/d should return true")
  assertTrue(t, remove(m, "/b/c"), "Delete /b/c should return true")
  assertTrue(t, remove(m, "/b"), "Delete /b should return true")
}

func TestSaveAndLoad(t *testing.T) {
  // m0 first stores some data in its namespace, then stores all
  // its data into a file.
  m0 := NewNamespaceManager()
  m0.Mkdir("/a")
  m0.Mkdir("/a/b")
  m0.Mkdir("/a/c")
  path := fmt.Sprintf("/var/tmp/namespace")
  m0.Store(path)

  // m1 will load namespace information from a file and try to
  // perform some namespace operations. It should fail to create
  // some directories because they already exist in the namespace.
  m1 := NewNamespaceManager()
  m1.Load(path)
  assertFalse(t, mkdir(m1, "/a"), "m1 Mkdir /a should fail.")
  assertFalse(t, mkdir(m1, "/a/b"), "m1 Mkdir /a/b should fail." )
  assertFalse(t, mkdir(m1, "/a/c"), "m1 Mkdir /a/c should fail.")
  assertTrue(t, mkdir(m1, "/a/b/c"), "m1 Mkdir /a/b/c should succeed.")
}
