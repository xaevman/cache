package cache

import (
    "bytes"
    "fmt"
    "io"
    "sync"

    "github.com/xaevman/crash"
)

type HierarchicalCache struct {
    deletethrough bool
    parentCache   RWCache
    readers       []ReadCache
    readerLock    sync.Mutex
    writers       []WriteCache
    writerLock    sync.Mutex
}

func NewHierarchicalCache(cache RWCache) *HierarchicalCache {
    return &HierarchicalCache{
        deletethrough: false,
        parentCache:   cache,
        readers:       make([]ReadCache, 0),
        writers:       make([]WriteCache, 0),
    }
}

func (hc *HierarchicalCache) SetDeleteThrough(enabled bool) {
    hc.deletethrough = enabled
}

func (hc *HierarchicalCache) AddChild(child interface{}) error {
    hc.readerLock.Lock()
    hc.writerLock.Lock()
    defer hc.readerLock.Unlock()
    defer hc.writerLock.Unlock()

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
    hc.readerLock.Lock()
    hc.writerLock.Lock()
    defer hc.readerLock.Unlock()
    defer hc.writerLock.Unlock()

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
    Log.Debug("HierarchicalCache::Delete %s", key)

    err := hc.parentCache.Delete(key, metadata)
    if err != nil {
        return err
    }

    if hc.deletethrough {
        hc.writerLock.Lock()
        defer hc.writerLock.Unlock()

        var wg sync.WaitGroup
        wg.Add(len(hc.writers))

        for i := range hc.writers {
            go func() {
                defer crash.HandleAll()
                Log.Debug("HierarchicalCache::Delete %s, child %d", key, i)
                err := hc.writers[i].Delete(key, metadata)
                if err != nil {
                    Log.Debug("Cache writethrough DELETE error: %v", err)
                }

                wg.Done()
            }()
        }

        wg.Wait()
    }

    return nil
}

func (hc *HierarchicalCache) Get(key string, metadata interface{}) (io.Reader, error) {
    Log.Debug("HierarchicalCache::Get %s", key)

    data, err := hc.parentCache.Get(key, metadata)
    if err == nil {
        return data, err
    }

    hc.readerLock.Lock()
    defer hc.readerLock.Unlock()

    // failed - try children
    for i := range hc.readers {
        data, err := hc.readers[i].Get(key, metadata)
        if err == nil {
            Log.Debug("HierarchicalCache::Get %s, child %d", key, i)
            return NewCacheFiller(key, metadata, hc.parentCache, data), nil
        }
    }

    // not found
    return nil, ErrDataNotFound
}

func (hc *HierarchicalCache) GetParent() RWCache {
    return hc.parentCache
}

func (hc *HierarchicalCache) Put(key string, metadata interface{}, data io.Reader) (int64, error) {
    Log.Debug("HierarchicalCache::Put %s", key)

    hc.writerLock.Lock()
    defer hc.writerLock.Unlock()

    var buffer bytes.Buffer
    _, err := io.Copy(&buffer, data)
    if err != nil {
        return 0, err
    }

    c, err := hc.parentCache.Put(key, metadata, bytes.NewReader(buffer.Bytes()))
    if err != nil {
        return 0, err
    }

    var wg sync.WaitGroup
    wg.Add(len(hc.writers))

    for i := range hc.writers {
        go func() {
            defer crash.HandleAll()

            Log.Debug("HierarchicalCache::Put %s, child %d", key, i)
            hc.writers[i].Put(key, metadata, bytes.NewReader(buffer.Bytes()))
            if err != nil {
                Log.Debug("Cache writethrough PUT error: %v", err)
            }

            wg.Done()
        }()
    }

    wg.Wait()

    return c, nil
}
