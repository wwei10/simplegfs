package error

import (
  "errors"
)

var (
  // Namespace path related error handling.
  ErrKeyNotFound = errors.New("file not found")
  ErrKeyExist = errors.New("file already exists")
  ErrParentNotFound = errors.New("parent not found")
  ErrIsNotDir = errors.New("is not directory")
  ErrIsNotFile = errors.New("is not file")


  // Chunk associated error handling.
  ErrChunkExist = errors.New("chunk already exists")
)

type PathError struct {
  Op string
  Path string
  Err error
}

func (e *PathError) Error() string {
  return e.Op + " " + e.Path + " " + e.Err.Error()
}
