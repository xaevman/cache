package cache

import (
    "bytes"
    "fmt"
    "io"
    "sync"
)

type HierarchicalCache struct {
    deletethrough bool
    parentCache   RWCache
    readers       []ReadCache
    writers       []WriteCache
    writethrough  bool
    lock          sync.RWMutex
}

func NewHierarchicalCache(cache RWCache) *HierarchicalCache {
    return &HierarchicalCache{
        deletethrough: false,
        parentCache:   cache,
        readers:       make([]ReadCache, 0),
        writers:       make([]WriteCache, 0),
        writethrough:  true,
    }
}

func (hc *HierarchicalCache) SetDeleteThrough(enabled bool) {
    hc.lock.Lock()
    defer hc.lock.Unlock()

    hc.deletethrough = enabled
}

func (hc *HierarchicalCache) SetWriteThrough(enabled bool) {
    hc.lock.Lock()
    defer hc.lock.Unlock()

    hc.writethrough = enabled
}

func (hc *HierarchicalCache) AddChild(child interface{}) error {
    hc.lock.Lock()
    defer hc.lock.Unlock()

    match := false
    reader, ok := child.(ReadCache)
    if ok {
        match = true
        hc.readers = append(hc.readers, reader)
    }

    writer, ok := child.(WriteCache)
    if ok {
        match = true
        hc.writers = append(hc.writers, writer)
    }

    if !match {
        return fmt.Errorf("Failed to add child cache. Not a ReadCache, WriterCache, or RWCache")
    }

    return nil
}

func (hc *HierarchicalCache) RemoveChild(child interface{}) {
    hc.lock.Lock()
    defer hc.lock.Unlock()

    for i := range hc.readers {
        if hc.readers[i] == child {
            hc.readers = append(hc.readers[:i], hc.readers[i+1:]...)
        }
    }

    for i := range hc.writers {
        if hc.writers[i] == child {
            hc.writers = append(hc.writers[:i], hc.writers[i+1:]...)
        }
    }
}

func (hc *HierarchicalCache) Delete(key string, metadata interface{}) error {
    err := hc.parentCache.Delete(key, metadata)
    if err != nil {
        return err
    }

    if hc.deletethrough {
        hc.lock.Lock()
        defer hc.lock.Unlock()

        for i := range hc.writers {
            err := hc.writers[i].Delete(key, metadata)
            if err != nil {
                return err
            }
        }
    }

    return nil
}

func (hc *HierarchicalCache) Get(key string, metadata interface{}) (io.Reader, error) {
    // try main cache
    data, err := hc.parentCache.Get(key, metadata)
    if err == nil {
        return data, err
    }

    hc.lock.RLock()
    defer hc.lock.RUnlock()

    // failed - try children
    for i := range hc.readers {
        data, err := hc.readers[i].Get(key, metadata)
        if err == nil {
            return NewCacheFiller(key, metadata, hc, data), nil
        }
    }

    // not found
    return nil, ErrDataNotFound
}

func (hc *HierarchicalCache) Put(path string, metadata interface{}, data io.Reader) error {
    hc.lock.Lock()
    defer hc.lock.Unlock()

    var buffer bytes.Buffer
    _, err := io.Copy(&buffer, data)
    if err != nil {
        return err
    }

    err = hc.parentCache.Put(path, metadata, bytes.NewReader(buffer.Bytes()))
    if err != nil {
        return err
    }

    if hc.writethrough {
        for i := range hc.writers {
            err = hc.writers[i].Put(path, metadata, bytes.NewReader(buffer.Bytes()))
            if err != nil {
                return err
            }
        }
    }

    return nil
}
