package cache

import (
    "io"
    "os"
)

type FsReadCache struct{}

func (fc *FsReadCache) Get(path string, metadata interface{}) (int64, io.Reader, error) {
    Log.Debug("FsCache::Get %s", path)

    f, err := os.Open(path)
    if err != nil {
        return GetLengthUnknown, nil, err
    }

    fi, err := f.Stat()
    if err != nil {
        f.Close()
        return GetLengthUnknown, nil, err
    }

    Log.Debug("Returning reader for %s (len %d)", path, fi.Size())

    return fi.Size(), NewSafeReader(fi.Size(), f, nil), nil
}
