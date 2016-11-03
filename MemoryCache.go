package cache

import (
    "bytes"
    "compress/zlib"
    "io"
    "sync"
)

type MemoryCache struct {
    data map[string]*bytes.Buffer
    lock sync.RWMutex
}

func NewMemoryCache() *HierarchicalCache {
    return NewHierarchicalCache(&MemoryCache{
        data: make(map[string]*bytes.Buffer),
    })
}

func (mc *MemoryCache) Delete(key string, metadata interface{}) error {
    Log.Debug("MemoryCache::Delete %s", key)

    mc.lock.Lock()
    defer mc.lock.Unlock()

    delete(mc.data, key)

    return nil
}

func (mc *MemoryCache) Get(key string, metadata interface{}) (io.Reader, error) {
    Log.Debug("MemoryCache::Get %s", key)

    mc.lock.RLock()
    defer mc.lock.RUnlock()

    data, ok := mc.data[key]
    if ok {
        dst := make([]byte, data.Len())
        copy(dst, data.Bytes())

        zr, err := zlib.NewReader(bytes.NewReader(dst))
        if err != nil {
            return nil, err
        }

        return NewSafeReader(zr, nil), nil
    }

    return nil, ErrDataNotFound
}

func (mc *MemoryCache) Put(key string, metadata interface{}, data io.Reader) error {
    Log.Debug("MemoryCache::Put %s", key)

    var buffer bytes.Buffer
    writer := zlib.NewWriter(&buffer)
    _, err := io.Copy(writer, data)
    writer.Close()

    if err != nil {
        return err
    }

    mc.lock.Lock()
    defer mc.lock.Unlock()

    mc.data[key] = &buffer

    return nil
}
