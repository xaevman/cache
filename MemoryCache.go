package cache

import (
    "bytes"
    "compress/zlib"
    "io"
    "sync"
)

type MemoryCache struct {
    data     map[string]*bytes.Buffer
    dataSize map[string]int64
    lock     sync.RWMutex
}

func NewMemoryCache() *HierarchicalCache {
    return NewHierarchicalCache(&MemoryCache{
        data:     make(map[string]*bytes.Buffer),
        dataSize: make(map[string]int64),
    })
}

func (mc *MemoryCache) Delete(key string, metadata interface{}) error {
    Log.Debug("MemoryCache::Delete %s", key)

    mc.lock.Lock()
    defer mc.lock.Unlock()

    delete(mc.data, key)
    delete(mc.dataSize, key)

    return nil
}

func (mc *MemoryCache) Get(key string, metadata interface{}) (int64, io.Reader, error) {
    Log.Debug("MemoryCache::Get %s", key)

    mc.lock.RLock()
    defer mc.lock.RUnlock()

    data, ok := mc.data[key]
    if !ok {
        return GetLengthUnknown, nil, ErrDataNotFound
    }

    dataSize, ok := mc.dataSize[key]
    if !ok {
        return GetLengthUnknown, nil, ErrDataNotFound
    }

    dst := make([]byte, data.Len())
    copy(dst, data.Bytes())

    zr, err := zlib.NewReader(bytes.NewReader(dst))
    if err != nil {
        return GetLengthUnknown, nil, err
    }

    Log.Debug("Returning reader for %s (len %d)", key, dataSize)

    return dataSize, NewSafeReader(dataSize, zr, nil), nil
}

func (mc *MemoryCache) Put(key string, metadata interface{}, data io.Reader) (int64, error) {
    Log.Debug("MemoryCache::Put %s", key)

    var buffer bytes.Buffer
    writer := zlib.NewWriter(&buffer)
    c, err := io.Copy(writer, data)
    writer.Close()

    if err != nil {
        return 0, err
    }

    mc.lock.Lock()
    defer mc.lock.Unlock()

    mc.data[key] = &buffer
    mc.dataSize[key] = c

    return c, nil
}
