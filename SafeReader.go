package cache

import (
    "io"
)

type SafeReader struct {
    reader io.Reader
}

func NewSafeReader(src io.Reader) io.Reader {
    return &SafeReader{
        reader: src,
    }
}

func (sr *SafeReader) Read(p []byte) (int, error) {
    c, err := sr.reader.Read(p)
    if err == io.EOF {
        closer, ok := sr.reader.(io.Closer)
        if ok {
            closer.Close()
        }
    }

    return c, err
}
