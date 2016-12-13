package cache

import (
    "errors"
    "io"

    "github.com/xaevman/log"
)

const (
    GetLengthUnknown = -2
    CacheFillTimeout = -3
    CacheFillError   = -4
)

var ErrDataNotFound = errors.New("Requested data not found in cache")

var Log log.DebugLogger

type ReadCache interface {
    Get(key string, metadata interface{}) (int64, io.Reader, error)
}
type WriteCache interface {
    Delete(key string, metadata interface{}) error
    Put(key string, metadata interface{}, data io.Reader) (int64, error)
}
type RWCache interface {
    Delete(key string, metadata interface{}) error
    Get(key string, metadata interface{}) (int64, io.Reader, error)
    Put(key string, metadata interface{}, data io.Reader) (int64, error)
}

func init() {
    Log = log.NullLogger
}
