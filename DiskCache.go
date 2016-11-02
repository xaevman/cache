package cache

import (
    "compress/zlib"
    "fmt"
    "io"
    "io/ioutil"
    "os"
    "path/filepath"
)

type DiskCache struct {
    root     string // the root directory of the file cache
    tmpRoot  string // the path where files are staged to before commiting to cache
    compress bool
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

    return os.Remove(fullPath)
}

func (dc *DiskCache) Get(path string, metadata interface{}) (io.Reader, error) {
    Log.Debug("DiskCache::Get %s", path)

    // try getting from this cache
    fullPath := filepath.Join(dc.root, path)
    f, err := os.Open(fullPath)
    if err == nil {
        if dc.compress {
            zr, err := zlib.NewReader(f)
            if err != nil {
                return nil, fmt.Errorf(
                    "Cache file found (%s) but decompression failed: %v",
                    path,
                    err,
                )
            } else {
                return NewSafeReader(zr, f), err
            }
        } else {
            return NewSafeReader(f), err
        }
    }

    // not found
    return nil, ErrDataNotFound
}

func (dc *DiskCache) Put(path string, metadata interface{}, data io.Reader) error {
    Log.Debug("DiskCache::Put %s", path)

    // write to a tmp file first
    err := os.MkdirAll(dc.tmpRoot, 0770)
    if err != nil {
        return err
    }

    f, err := ioutil.TempFile(dc.tmpRoot, "")
    if err != nil {
        return err
    }

    if dc.compress {
        writer := zlib.NewWriter(f)

        _, err = io.Copy(writer, data)
        if err != nil {
            writer.Close()
            f.Close()
            return err
        }

        writer.Close()
        f.Close()
    } else {
        _, err = io.Copy(f, data)
        if err != nil {
            f.Close()
            return err
        }

        f.Close()
    }

    return dc.commit(f.Name(), path)
}

func (dc *DiskCache) commit(tmpPath, path string) error {
    fullPath := filepath.Join(dc.root, path)

    err := os.MkdirAll(filepath.Dir(fullPath), 0770)
    if err != nil {
        return err
    }

    err = os.Rename(tmpPath, fullPath)
    defer os.Remove(tmpPath)

    return err
}
