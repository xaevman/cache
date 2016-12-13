package cache

import (
    "fmt"
    "io"
)

type SafeReader struct {
    ReadSize int64
    SrcSize  int64
    source   io.Reader
    parent   io.Reader
}

func NewSafeReader(srcSize int64, src, parent io.Reader) io.Reader {
    return &SafeReader{
        ReadSize: int64(0),
        SrcSize:  srcSize,
        source:   src,
        parent:   parent,
    }
}

func (sr *SafeReader) Read(p []byte) (int, error) {
    c, err := sr.source.Read(p)
    sr.ReadSize += int64(c)

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

        if sr.SrcSize > -1 && sr.ReadSize != sr.SrcSize {
            return c, fmt.Errorf(
                "read size mismatch (%d != %d)",
                sr.ReadSize,
                sr.SrcSize,
            )
        }
    }

    return c, err
}
