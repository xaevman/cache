package cache

import (
    "bytes"
    "io"
)

type CacheFiller struct {
    buffer   bytes.Buffer
    cache    WriteCache
    data     io.Reader
    metadata interface{}
    path     string
    tee      io.Reader
}

func NewCacheFiller(
    path string,
    metadata interface{},
    parent WriteCache,
    child io.Reader,
) *CacheFiller {
    cr := &CacheFiller{
        cache:    parent,
        data:     child,
        metadata: metadata,
        path:     path,
    }

    cr.tee = io.TeeReader(cr.data, &cr.buffer)

    return cr
}

func (cf *CacheFiller) Read(p []byte) (int, error) {
    c, err := cf.tee.Read(p)
    if err == io.EOF {
        cf.cache.Put(cf.path, cf.metadata, &cf.buffer)
    }

    return c, err
}
