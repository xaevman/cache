package cache

import (
    "errors"
    "io"

    "github.com/xaevman/log"
)

var ErrDataNotFound = errors.New("Requested data not found in cache")

var Log log.DebugLogger

type ReadCache interface {
    Get(key string, metadata interface{}) (io.Reader, error)
}
type WriteCache interface {
    Delete(key string, metadata interface{}) error
    Put(key string, metadata interface{}, data io.Reader) error
}
type RWCache interface {
    Delete(key string, metadata interface{}) error
    Get(key string, metadata interface{}) (io.Reader, error)
    Put(key string, metadata interface{}, data io.Reader) error
}

func init() {
    Log = log.NullLogger
}
