package error

import (
  "fmt"
  "testing"
)

func testHandleError(err *PathError) {
  fmt.Print(err.Error() + " |=| ")
  switch err.Err {
  case ErrKeyNotFound:
    fmt.Println("key not found")
  case ErrIsNotFile:
    fmt.Println("is not file")
  case ErrKeyExist:
    fmt.Println("key already exists")
  default:
    fmt.Println("default: " + err.Error())
  }
}

func TestPathError(t *testing.T) {
  pe0 := &PathError{"write", "/usr/local", ErrKeyNotFound}
  pe1 := &PathError{"read", "/usr/bin", ErrIsNotFile}
  pe2 := &PathError{"create", "/var/tmp", ErrKeyExist}
  testHandleError(pe0)
  testHandleError(pe1)
  testHandleError(pe2)
}
