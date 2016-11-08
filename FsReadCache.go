package cache

import (
    "io"
    "os"
)

type FsReadCache struct{}

func (fc *FsReadCache) Get(path string, metadata interface{}) (io.Reader, error) {
    Log.Debug("FsCache::Get %s", path)

    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }

    return NewSafeReader(f, nil), nil
}