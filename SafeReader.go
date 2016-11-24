package cache

import (
    "fmt"
    "io"
)

type SafeReader struct {
    srcSize  int64
    source   io.Reader
    parent   io.Reader
    readSize int64
}

func NewSafeReader(srcSize int64, src, parent io.Reader) io.Reader {
    return &SafeReader{
        srcSize:  srcSize,
        source:   src,
        parent:   parent,
        readSize: int64(0),
    }
}

func (sr *SafeReader) Read(p []byte) (int, error) {
    c, err := sr.source.Read(p)
    sr.readSize += int64(c)

    if err == io.EOF {
        rc, ok := sr.source.(io.ReadCloser)
        if ok {
            rc.Close()
        }

        if sr.parent != nil {
            rc, ok = sr.parent.(io.ReadCloser)
            if ok {
                rc.Close()
            }
        }

        if sr.srcSize > -1 && sr.readSize != sr.srcSize {
            return c, fmt.Errorf(
                "read size mismatch (%d != %d)",
                sr.readSize,
                sr.srcSize,
            )
        }
    }

    return c, err
}
