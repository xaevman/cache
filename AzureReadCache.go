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

        storErr := err.(storage.AzureStorageServiceError)
        if storErr.StatusCode == 404 {
            return arc.lcGet(strings.ToLower(path), metadata)
        }

        <-time.After(HttpRetryIntervalSec * time.Second)
    }

    for i := 0; i < HttpMaxRetries; i++ {
        reader, err = arc.cli.GetBlob(arc.container, path)
        if err == nil {
            break
        }

        storErr := err.(storage.AzureStorageServiceError)

        if storErr.StatusCode == 404 {
            // try forcing lower case for case-insensitive search
            return arc.lcGet(strings.ToLower(path), metadata)
        }

        <-time.After(HttpRetryIntervalSec * time.Second)
    }

    if err != nil {
        return nil, err
    }

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

        storErr := err.(storage.AzureStorageServiceError)
        if storErr.StatusCode == 404 {
            break
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

        storErr := err.(storage.AzureStorageServiceError)

        if storErr.StatusCode == 404 {
            break
        }

        <-time.After(HttpRetryIntervalSec * time.Second)
    }

    if err != nil {
        return nil, err
    }

    return NewSafeReader(srcSize, reader, nil), nil
}
