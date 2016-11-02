package cache

import (
    "io"
)

type SafeReader struct {
    readers []io.Reader
}

func NewSafeReader(src ...io.Reader) io.Reader {
    return &SafeReader{
        readers: src,
    }
}

func (sr *SafeReader) Read(p []byte) (int, error) {
    c, err := sr.readers[0].Read(p)
    if err == io.EOF {
        for i := range sr.readers {
            closer, ok := sr.readers[i].(io.Closer)
            if ok {
                closer.Close()
            }
        }
    }

    return c, err
}
