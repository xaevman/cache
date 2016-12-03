package cache

import (
    "io"
    "strings"
    "time"

    "github.com/Azure/azure-sdk-for-go/storage"
)

type AzureReadCache struct {
    container string
    cli       storage.BlobStorageClient
}

func NewAzureReadCache(account, key, container string) (*AzureReadCache, error) {
    azure, err := storage.NewBasicClient(account, key)
    if err != nil {
        return nil, err
    }

    blobService := azure.GetBlobService()

    _, err = blobService.CreateContainerIfNotExists(
        container,
        storage.ContainerAccessTypePrivate,
    )
    if err != nil {
        return nil, err
    }

    arc := &AzureReadCache{
        container: container,
        cli:       blobService,
    }

    return arc, nil
}

func (arc *AzureReadCache) Get(path string, metadata interface{}) (io.Reader, error) {
    Log.Debug("AzureReadCache::Get %s", path)

    var err error
    var reader io.ReadCloser
    var srcSize int64

    var props *storage.BlobProperties
    for i := 0; i < HttpMaxRetries; i++ {
        props, err = arc.cli.GetBlobProperties(arc.container, path)
        if err == nil {
            srcSize = props.ContentLength
            break
        }

        storErr, ok := err.(storage.AzureStorageServiceError)
        if ok {
            if storErr.StatusCode == 404 {
                return arc.lcGet(strings.ToLower(path), metadata)
            }
        } else if strings.Contains(err.Error(), "404") {
            return arc.lcGet(strings.ToLower(path), metadata)
        } else {
            Log.Debug("AzureReadCache get error (%s): %T %v", path, err, err)
        }

        <-time.After(HttpRetryIntervalSec * time.Second)
    }

    for i := 0; i < HttpMaxRetries; i++ {
        reader, err = arc.cli.GetBlob(arc.container, path)
        if err == nil {
            break
        }

        storErr, ok := err.(storage.AzureStorageServiceError)
        if ok {
            if storErr.StatusCode == 404 {
                return arc.lcGet(strings.ToLower(path), metadata)
            }
        } else if strings.Contains(err.Error(), "404") {
            return arc.lcGet(strings.ToLower(path), metadata)
        } else {
            Log.Debug("AzureReadCache get error (%s): %T %v", path, err, err)
        }

        <-time.After(HttpRetryIntervalSec * time.Second)
    }

    if err != nil {
        return nil, err
    }

    Log.Debug("Returning reader for %s (len %d)", path, srcSize)

    return NewSafeReader(srcSize, reader, nil), nil
}

func (arc *AzureReadCache) lcGet(path string, metadata interface{}) (io.Reader, error) {
    Log.Debug("AzureReadCache::lcGet %s", path)

    var err error
    var reader io.ReadCloser
    var srcSize int64

    var props *storage.BlobProperties
    for i := 0; i < HttpMaxRetries; i++ {
        props, err = arc.cli.GetBlobProperties(arc.container, path)
        if err == nil {
            srcSize = props.ContentLength
            break
        }

        storErr, ok := err.(storage.AzureStorageServiceError)
        if ok {
            if storErr.StatusCode == 404 {
                break
            }
        } else if strings.Contains(err.Error(), "404") {
            break
        } else {
            Log.Debug("AzureReadCache get error (%s): %T %v", path, err, err)
        }

        <-time.After(HttpRetryIntervalSec * time.Second)
    }

    if err != nil {
        return nil, err
    }

    for i := 0; i < HttpMaxRetries; i++ {
        reader, err = arc.cli.GetBlob(arc.container, path)
        if err == nil {
            break
        }

        storErr, ok := err.(storage.AzureStorageServiceError)
        if ok {
            if storErr.StatusCode == 404 {
                break
            }
        } else if strings.Contains(err.Error(), "404") {
            break
        } else {
            Log.Debug("AzureReadCache get error (%s): %T %v", path, err, err)
        }

        <-time.After(HttpRetryIntervalSec * time.Second)
    }

    if err != nil {
        return nil, err
    }

    return NewSafeReader(srcSize, reader, nil), nil
}
