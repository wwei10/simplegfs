package error

import (
  "errors"
)

var (
  // Namespace path related error handling.
  // Note: Key is a file or directory.
  ErrKeyNotFound = errors.New("key not found")
  ErrKeyExist = errors.New("key already exists")
  ErrParentNotFound = errors.New("parent not found")
  ErrParentIsNotDir = errors.New("parent is not a directory")
  ErrIsNotDir = errors.New("is not directory")
  ErrIsNotFile = errors.New("is not file")
  ErrDirNotEmpty = errors.New("directory is not empty")

  // Chunk associated error handling.
  ErrChunkExist = errors.New("Chunk already exists")
  ErrDataNotInMem = errors.New("Target data is not in memory")
  ErrNotEnoughSpace = errors.New("Append to chunk with not enough space")
)

type PathError struct {
  Op string
  Path string
  Err error
}

func (e *PathError) Error() string {
  return e.Op + " " + e.Path + " " + e.Err.Error()
}
