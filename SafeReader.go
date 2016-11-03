package cache

import (
    "io"
)

type SafeReader struct {
    source io.ReadCloser
    parent io.ReadCloser
}

func NewSafeReader(src, parent io.ReadCloser) io.Reader {
    return &SafeReader{
        source: src,
        parent: parent,
    }
}

func (sr *SafeReader) Read(p []byte) (int, error) {
    c, err := sr.source.Read(p)
    if err == io.EOF {
        sr.source.Close()

        if sr.parent != nil {
            sr.parent.Close()
        }
    }

    return c, err
}
