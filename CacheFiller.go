package cache

import (
    "bytes"
    "io"
    "time"

    "github.com/xaevman/crash"
)

type CacheFiller struct {
    buffer       bytes.Buffer
    cache        WriteCache
    data         io.Reader
    fillComplete chan int64
    metadata     interface{}
    path         string
    tee          io.Reader
}

func NewCacheFiller(
    path string,
    metadata interface{},
    parent WriteCache,
    child io.Reader,
) *CacheFiller {
    cr := &CacheFiller{
        cache:        parent,
        data:         child,
        fillComplete: make(chan int64, 0),
        metadata:     metadata,
        path:         path,
    }

    cr.tee = io.TeeReader(cr.data, &cr.buffer)

    return cr
}

func WaitForCacheFill(reader io.Reader) int64 {
    cf, ok := reader.(*CacheFiller)
    if !ok {
        return 0
    }

    return <-cf.fillComplete
}

func (cf *CacheFiller) Read(p []byte) (int, error) {
    c, err := cf.tee.Read(p)
    if err == io.EOF {
        Log.Debug("EOF reached after %d bytes", cf.buffer.Len())

        go func() {
            defer crash.HandleAll()
            defer close(cf.fillComplete)

            c, err := cf.cache.Put(cf.path, cf.metadata, &cf.buffer)
            if err != nil {
                Log.Debug("CacheFiller fill error %s: %v", cf.path, err)
                return
            }

            select {
            case cf.fillComplete <- c:
                Log.Debug("fillComplete %s: %d bytes", cf.path, c)
                break
            case <-time.After(3 * time.Second):
                Log.Debug("fillComplete event timeout (%s)", cf.path)
                break
            }
        }()
    }

    return c, err
}
