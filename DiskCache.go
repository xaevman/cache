package cache

import (
    "compress/zlib"
    "fmt"
    "io"
    "io/ioutil"
    "os"
    "path/filepath"
    "time"
)

const (
    FsMaxRetries       = 3
    FsRetryIntervalSec = 1
)

type DiskCache struct {
    compress bool
    root     string // the root directory of the file cache
    tmpRoot  string // the path where files are staged to before commiting to cache
}

func NewDiskCache(root, tmp string, compress bool) *HierarchicalCache {
    return NewHierarchicalCache(&DiskCache{
        root:     root,
        tmpRoot:  tmp,
        compress: compress,
    })
}

func (dc *DiskCache) Delete(path string, metadta interface{}) error {
    fullPath := filepath.Join(dc.root, path)

    Log.Debug("DiskCache::Delete %s", fullPath)

    retries := 0
    var err error

    for retries < FsMaxRetries {
        err = os.Remove(fullPath)
        if err == nil || os.IsNotExist(err) {
            return nil
        }

        Log.Debug("Delete %s failed (retry %d): %v", path, retries, err)
        retries++

        <-time.After(time.Duration(FsRetryIntervalSec) * time.Second)
    }

    return err
}

func (dc *DiskCache) Get(path string, metadata interface{}) (io.Reader, error) {
    Log.Debug("DiskCache::Get %s", path)

    // try getting from this cache
    fullPath := filepath.Join(dc.root, path)

    retries := 0

    for retries < FsMaxRetries {
        f, err := os.Open(fullPath)
        if err == nil || os.IsNotExist(err) {
            if dc.compress {
                zr, err := zlib.NewReader(f)
                if err != nil {
                    f.Close()
                    return nil, fmt.Errorf(
                        "Cache file found (%s) but decompression failed: %v",
                        path,
                        err,
                    )
                } else {
                    return NewSafeReader(-1, zr, f), err
                }
            } else {
                fi, err := f.Stat()
                if err != nil {
                    f.Close()
                    return nil, fmt.Errorf(
                        "Cache file found (%s) but stat failed: %v",
                        path,
                        err,
                    )
                }

                Log.Debug("DiskCache data size %d", fi.Size())
                return NewSafeReader(fi.Size(), f, nil), err
            }
        }

        Log.Debug("Open %s failed (retry %d): %v", path, retries, err)
        retries++

        <-time.After(time.Duration(FsRetryIntervalSec) * time.Second)
    }

    // not found
    return nil, ErrDataNotFound
}

func (dc *DiskCache) GetRoot() string {
    return dc.root
}

func (dc *DiskCache) GetTmpRoot() string {
    return dc.tmpRoot
}

func (dc *DiskCache) Put(path string, metadata interface{}, data io.Reader) (int64, error) {
    Log.Debug("DiskCache::Put %s", path)

    // write to a tmp file first
    err := os.MkdirAll(dc.tmpRoot, 0770)
    if err != nil {
        return 0, err
    }

    f, err := ioutil.TempFile(dc.tmpRoot, "")
    if err != nil {
        return 0, err
    }

    var count int64

    if dc.compress {
        writer := zlib.NewWriter(f)

        count, err = io.Copy(writer, data)
        if err != nil {
            writer.Close()
            f.Close()
            return 0, err
        }

        writer.Close()
        f.Close()
    } else {
        count, err = io.Copy(f, data)
        if err != nil {
            f.Close()
            return 0, err
        }

        f.Close()
    }

    return count, dc.commit(f.Name(), path)
}

func (dc *DiskCache) commit(tmpPath, path string) error {
    fullPath := filepath.Join(dc.root, path)

    err := os.MkdirAll(filepath.Dir(fullPath), 0770)
    if err != nil {
        return err
    }
    defer os.Remove(tmpPath)

    retries := 0

    for retries < FsMaxRetries {
        err = os.Rename(tmpPath, fullPath)
        if err == nil || os.IsNotExist(err) {
            return nil
        }

        Log.Debug("Commit %s failed (retry %d): %v", path, retries, err)
        retries++

        <-time.After(time.Duration(FsRetryIntervalSec) * time.Second)
    }

    return err
}
