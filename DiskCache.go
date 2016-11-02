package cache

import (
    "io"
    "io/ioutil"
    "os"
    "path/filepath"
    "sync"
)

type DiskCache struct {
    Root    string // the root directory of the file cache
    TmpRoot string // the path where files are staged to before commiting to cache

    children []ReadCache
    lock     sync.RWMutex
}

func (dc *DiskCache) AddChild(child ReadCache) {
    dc.lock.Lock()
    defer dc.lock.Unlock()

    dc.children = append(dc.children, child)
}

func (dc *DiskCache) RemoveChild(child ReadCache) {
    dc.lock.Lock()
    defer dc.lock.Unlock()

    for i := range dc.children {
        if dc.children[i] == child {
            dc.children = append(dc.children[:i], dc.children[i+1:]...)
        }
    }
}

func (dc *DiskCache) ChildrenCount() int {
    dc.lock.RLock()
    defer dc.lock.RUnlock()

    return len(dc.children)
}

func (dc *DiskCache) Get(path string, metadata interface{}) (io.Reader, error) {
    // try getting from this cache
    fullPath := filepath.Join(dc.Root, path)
    f, err := os.Open(fullPath)
    if err == nil {
        return NewSafeReader(f), err
    }

    dc.lock.RLock()
    defer dc.lock.RUnlock()

    // failed - try children
    for i := range dc.children {
        data, err := dc.children[i].Get(path, metadata)
        if err == nil {
            return NewCacheFiller(path, metadata, dc, data), nil
        }
    }

    // not found
    return nil, ErrDataNotFound
}

func (dc *DiskCache) Put(path string, metadata interface{}, data io.Reader) error {
    dc.lock.Lock()
    defer dc.lock.Unlock()

    // write to a tmp file first
    err := os.MkdirAll(dc.TmpRoot, 0770)
    if err != nil {
        return err
    }

    f, err := ioutil.TempFile(dc.TmpRoot, "")
    if err != nil {
        return err
    }

    _, err = io.Copy(f, data)
    if err != nil {
        f.Close()
        return err
    }

    f.Close()
    return dc.commit(f.Name(), path)
}

func (dc *DiskCache) commit(tmpPath, path string) error {
    fullPath := filepath.Join(dc.Root, path)

    err := os.MkdirAll(filepath.Dir(fullPath), 0770)
    if err != nil {
        return err
    }

    err = os.Rename(tmpPath, fullPath)
    defer os.Remove(tmpPath)

    return err
}
