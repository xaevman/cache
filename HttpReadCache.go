package cache

import (
    "errors"
    "fmt"
    "io"
    "net/http"
    "time"
)

const (
    HttpMaxRetries       = 3
    HttpRetryIntervalSec = 3
)

var (
    ErrInvalidHttpRequest = errors.New("Unable to cast metadata to valid HttpCacheRequest")
)

type HttpReadCache struct{}

func (hc *HttpReadCache) Get(path string, metadata interface{}) (int64, io.Reader, error) {
    Log.Debug("HttpReadCache::Get %s", path)

    header, ok := metadata.(http.Header)
    if !ok {
        return GetLengthUnknown, nil, ErrInvalidHttpRequest
    }

    retryTxt := ""
    var resp *http.Response
    var err error

    proxyReq, err := http.NewRequest("GET", path, nil)
    if err != nil {
        return GetLengthUnknown, nil, err
    }

    for k, v := range header {
        proxyReq.Header[k] = v
    }

    for i := 0; i < HttpMaxRetries; i++ {
        if i > 0 {
            retryTxt = fmt.Sprintf(" (retry %d)", i)
        }

        Log.Debug("HTTP GET: %s%s", path, retryTxt)

        resp, err = http.DefaultClient.Do(proxyReq)
        if err == nil && resp.StatusCode < 500 {
            break
        }

        if err != nil {
            Log.Debug("HTTP error: %v", err)
        } else {
            Log.Debug("HTTP error %d (%s)", resp.StatusCode, resp.Status)
        }

        <-time.After(HttpRetryIntervalSec * time.Second)
    }

    if resp == nil {
        Log.Debug("HTTP GET %s: nil response received", path)
        return GetLengthUnknown, nil, http.ErrMissingFile
    }

    if resp.StatusCode >= 400 {
        Log.Debug("HTTP error %d (%s)", resp.StatusCode, resp.Status)
        return GetLengthUnknown, nil, http.ErrMissingFile
    }

    Log.Debug("Returning reader for %s (len %d)", path, resp.ContentLength)

    return resp.ContentLength, NewSafeReader(resp.ContentLength, resp.Body, nil), nil
}
