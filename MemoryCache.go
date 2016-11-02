package cache

import (
    "io"
    "sync"
    "compress/zlib"
    "bytes"
)

type CacheObject struct {
    Data []byte
    Lock sync.RWMutex
}

type MemoryCache struct {
    data map[string]*CacheObject
    lock sync.RWMutex
}

func NewMemoryCache() *HierarchicalCache {
    return NewHierarchicalCache(&MemoryCache {
        data : make(map[string]*CacheObject),
    })
}

func (mc *MemoryCache) Delete(key string, metadata interface{}) error {
    mc.lock.Lock()
    defer mc.lock.Unlock()

    delete(mc.data, key)

    return nil
}

func (mc *MemoryCache) Get(key string, metadata interface{}) (io.Reader, error) {
    mc.lock.RLock()
    defer mc.lock.RUnlock()

    obj, ok := mc.data[key]
    if ok {
        obj.Lock.RLock()
        dst := make([]byte, len(obj.Data))
        copy(dst, obj.Data)
        obj.Lock.RUnlock()

        zr, err := zlib.NewReader(bytes.NewReader(dst))
        if err != nil {
            return nil, err
        }

        return zr, nil
    }

    return nil, ErrDataNotFound
}

func (mc *MemoryCache) Put(key string, metadata interface{}, data io.Reader) error {
    var buffer bytes.Buffer
    writer := zlib.NewWriter(&buffer)
    _, err := io.Copy(writer, data)
    writer.Close()
    if err != nil {
        return err
    }

    mc.lock.Lock()
    defer mc.lock.Unlock()

    mc.data[key] = &CacheObject {
        Data : buffer.Bytes(),
    }

    return nil
}
