package cache

import (
    "bytes"
    "io"
    "sync"

    "github.com/xaevman/crash"
)

type CacheFiller struct {
    buffer   bytes.Buffer
    cache    WriteCache
    data     io.Reader
    metadata interface{}
    path     string
    tee      io.Reader
    sync     bool
}

func NewCacheFiller(
    path string,
    metadata interface{},
    parent WriteCache,
    child io.Reader,
    synchronous bool,
) *CacheFiller {
    cr := &CacheFiller{
        cache:    parent,
        data:     child,
        metadata: metadata,
        path:     path,
        sync:     synchronous,
    }

    cr.tee = io.TeeReader(cr.data, &cr.buffer)

    return cr
}

func (cf *CacheFiller) Read(p []byte) (int, error) {
    c, err := cf.tee.Read(p)
    if err == io.EOF {
        Log.Debug("EOF reached after %d bytes", cf.buffer.Len())

        var wg sync.WaitGroup
        if cf.sync {
            wg.Add(1)
        }

        go func() {
            defer crash.HandleAll()
            cf.cache.Put(cf.path, cf.metadata, &cf.buffer)

            if cf.sync {
                wg.Done()
            }
        }()

        wg.Wait()
    }

    return c, err
}
