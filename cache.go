package cache

import (
    "errors"
    "io"
)

var ErrDataNotFound = errors.New("Requested data not found in cache")

type ReadCache interface {
    Get(key string, metadata interface{}) (io.Reader, error)
}
type WriteCache interface {
    Put(key string, metadata interface{}, data io.Reader) error
}
type RWCache interface {
    Get(key string, metadata interface{}) (io.Reader, error)
    Put(key string, metadata interface{}, data io.Reader) error
}
