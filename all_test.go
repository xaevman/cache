//  ---------------------------------------------------------------------------
//
//  all_test.go
//
//  Copyright (c) 2016, Jared Chavez.
//  All rights reserved.
//
//  Use of this source code is governed by a BSD-style
//  license that can be found in the LICENSE file.
//
//  -----------

package cache

import (
    "bytes"
    "crypto/rand"
    "crypto/sha1"
    "fmt"
    "io"
    "io/ioutil"
    "net/http"
    "os"
    "path/filepath"
    "strings"
    "testing"
)

const (
    TestFileSize = 64 * 1024
    ScavMaxSize  = int64(1 * 1024 * 1024) // 1MB
)

var (
    TestFilePath     string
    TestFileHash     string
    TestCachePath    = filepath.Join("dir", "test.file")
    BigFilePath      = "test.data"
    BigFileHash      string
    BigFileSize      = int64(50331648)
    BigFileCachePath = filepath.Join("dir", BigFilePath)
)

type TestLogger struct{}

func (tl *TestLogger) Debug(format string, v ...interface{}) {
    fmt.Printf(fmt.Sprintf("%s\n", format), v...)
}

func TestMain(m *testing.M) {
    Log = &TestLogger{}

    for i := 1; i <= 3; i++ {
        err := clean(i)
        if err != nil {
            cleanup()
            os.Exit(i)
        }
    }

    err := makeTestFile()
    if err != nil {
        cleanup()
        os.Exit(1)
    }

    err = bigFileSetup()
    if err != nil {
        cleanup()
        os.Exit(1)
    }

    os.Exit(m.Run())
}

func TestFileRead(t *testing.T) {
    f, err := os.Open(TestFilePath)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    defer f.Close()

    checkData(f, t)
}

func TestSafeReader(t *testing.T) {
    f, err := os.Open(TestFilePath)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    fi, err := f.Stat()
    if err != nil {
        f.Close()
        t.Fatalf("Error: %v", err)
    }

    reader := NewSafeReader(fi.Size(), f, nil)

    checkData(reader, t)
}

func TestDiskCacheGetRoots(t *testing.T) {
    err := clean(1)
    if err != nil {
        t.Fatalf("Error cleaning: %v", err)
    }

    root := "root1"
    tmpRoot := "tmp1"

    c := NewDiskCache(root, tmpRoot, false)
    dc := c.GetParent().(*DiskCache)

    if dc.GetRoot() != root {
        t.Fatalf("Invalid cache root (%s != %s)", dc.GetRoot(), root)
    }

    if dc.GetTmpRoot() != tmpRoot {
        t.Fatalf("Invalid cache root (%s != %s)", dc.GetTmpRoot(), tmpRoot)
    }
}

func TestDiskCacheChildren(t *testing.T) {
    dc1 := NewDiskCache("cache1", "tmp1", false)
    dc2 := NewDiskCache("cache2", "tmp2", false)

    err := dc1.AddChild(dc2)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    dc1.RemoveChild(dc2)
    if len(dc1.readers) != 0 {
        t.Fatal()
    }
    if len(dc1.writers) != 0 {
        t.Fatal()
    }
}

func TestDiskCachePut(t *testing.T) {
    dc := NewDiskCache("cache1", "tmp1", false)

    f, err := os.Open(TestFilePath)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    fi, err := f.Stat()
    if err != nil {
        f.Close()
        t.Fatalf("Error: %v", err)
    }

    reader := NewSafeReader(fi.Size(), f, nil)

    _, err = dc.Put(TestCachePath, nil, reader)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    fullPath := filepath.Join("cache1", TestCachePath)
    f2, err := os.Open(fullPath)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    fi, err = f2.Stat()
    if err != nil {
        f2.Close()
        t.Fatalf("Error: %v", err)
    }

    checkData(NewSafeReader(fi.Size(), f2, nil), t)

    data, err := dc.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    checkData(data, t)
}

func TestDiskCacheGet(t *testing.T) {
    dc := NewDiskCache("cache1", "tmp1", false)

    reader, err := dc.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    checkData(reader, t)
}

func TestDiskCacheRetries(t *testing.T) {
    dc := NewDiskCache("cache1", "tmp1", false)
    tmp := dc.GetParent().(*DiskCache)

    data := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
    reader := bytes.NewReader(data)

    // hold file lock
    f, err := os.OpenFile(filepath.Join(tmp.GetRoot(), TestCachePath), os.O_RDWR, 0660)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    defer f.Close()

    err = dc.Delete(TestCachePath, nil)
    if err == nil {
        t.Fatalf("Deleting cache file wasn't supposed to succeed!", err)
    }

    _, err = dc.Put(TestCachePath, nil, reader)
    if err == nil {
        t.Fatalf("Error: %v", err)
    }
}

func TestFsReadCache(t *testing.T) {
    cache := &FsReadCache{}
    path := filepath.Join("cache1", TestCachePath)
    reader, err := cache.Get(path, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    checkData(reader, t)
}

func TestHttpReadCache(t *testing.T) {
    go http.ListenAndServe(":8888", http.FileServer(http.Dir("./cache1")))

    uri := fmt.Sprintf("http://localhost:8888/%s", TestCachePath)
    uri = strings.Replace(uri, "\\", "/", -1)

    cache := &HttpReadCache{}
    reader, err := cache.Get(uri, http.Header{})
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    checkData(reader, t)

    _, err = cache.Get(uri, nil)
    if err == nil {
        t.Fatalf("Error: should have thrown error on nil header")
    }
}

func TestChildFill(t *testing.T) {
    dc1 := NewDiskCache("cache1", "tmp1", false)
    dc3 := NewDiskCache("cache3", "tmp3", false)

    dc3.AddChild(dc1)

    // previous tests should have populated the dc1 cache
    // this call should fall through to r1 and return both
    // valid data and populate the dc3 cache
    d1, err := dc3.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    // check the returned data
    checkData(d1, t)

    WaitForCacheFill(d1)

    dc3.RemoveChild(dc1)
    d2, err := dc3.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    // check the data that was populated into dc3 cache
    checkData(d2, t)
}

func TestCompression(t *testing.T) {
    err := clean(1)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    f, err := os.Open(TestFilePath)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    fi, err := f.Stat()
    if err != nil {
        f.Close()
        t.Fatalf("Error: %v", err)
    }

    reader := NewSafeReader(fi.Size(), f, nil)

    dc1 := NewDiskCache("cache1", "tmp1", true)

    _, err = dc1.Put(TestCachePath, nil, reader)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    data, err := dc1.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    checkData(data, t)
}

func TestDataNotFound(t *testing.T) {
    dc1 := NewDiskCache("cache1", "tmp1", false)

    _, err := dc1.Get("notreal.file", nil)
    if err == nil {
        t.Fatal()
    }
}

func TestMemCache(t *testing.T) {
    dc := NewDiskCache("cache1", "tmp1", true)
    mc := NewMemoryCache()

    mc.AddChild(dc)

    // should bubble up from file cache
    d1, err := mc.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    // check the returned data
    checkData(d1, t)

    WaitForCacheFill(d1)

    // should come from RAM
    mc.RemoveChild(dc)
    d2, err := mc.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    // check the data that was populated into dc3 cache
    checkData(d2, t)
}

func TestWriteThrough(t *testing.T) {
    err := clean(1)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    dc := NewDiskCache("cache1", "tmp1", true)
    mc := NewMemoryCache()

    mc.AddChild(dc)

    f, err := os.Open(TestFilePath)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    fi, err := f.Stat()
    if err != nil {
        f.Close()
        t.Fatalf("Error: %v", err)
    }

    reader := NewSafeReader(fi.Size(), f, nil)

    _, err = mc.Put(TestCachePath, nil, reader)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    data, err := dc.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    checkData(data, t)

    data, err = mc.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    checkData(data, t)
}

func TestScavenger(t *testing.T) {
    err := clean(1)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    dc := NewDiskCache("cache1", "tmp1", false)
    mc := NewMemoryCache()

    mc.SetDeleteThrough(true)
    mc.AddChild(dc)

    cache := NewScavenger(
        mc,
        ScavMaxSize,
    )

    fd, err := ioutil.ReadFile(TestFilePath)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    origPath := fmt.Sprintf("%s0", TestCachePath)

    cache.Touch(origPath, 1)
    if cache.Size() != 1 {
        t.Fatalf("Error, Touch of size 1 failed. Size mismatch (%d != %d)", cache.Size(), 1)
    }

    if !cache.Find(origPath) {
        t.Fatalf("Error. Should have found key %s in cache", origPath)
    }

    cache.Delete(origPath, nil)
    if cache.Size() != 0 {
        t.Fatalf("Error, Touch of size 1 failed. Size mismatch (%d != %d)", cache.Size(), 0)
    }

    // should only be able to fit 16 copies of data in the 1MB cache before
    // our first eviction occurs
    for i := 0; i < 17; i++ {
        cachePath := fmt.Sprintf("%s%d", TestCachePath, i)
        reader := bytes.NewReader(fd)

        // put a record in the cache
        _, err = cache.Put(cachePath, nil, reader)
        if err != nil {
            t.Fatalf("Error: %v", err)
        }

        // check to make sure it made it everywhere via write-through
        data, err := dc.Get(cachePath, nil)
        if err != nil {
            t.Fatalf("Error: %v", err)
        }
        checkData(data, t)

        data, err = mc.Get(cachePath, nil)
        if err != nil {
            t.Fatalf("Error: %v", err)
        }
        checkData(data, t)

        data, err = cache.Get(cachePath, nil)
        if err != nil {
            t.Fatalf("Error: %v", err)
        }
        checkData(data, t)

        if i < 16 {
            testSize := int64((i + 1) * TestFileSize)
            if cache.Size() != testSize {
                t.Fatalf("Error cache size mismatch (%d != %d)", cache.Size(), testSize)
            }
        } else {
            // record should be gone now
            _, err = dc.Get(origPath, nil)
            if err == nil {
                t.Fatal()
            }

            _, err = mc.Get(origPath, nil)
            if err == nil {
                t.Fatal()
            }

            _, err = cache.Get(origPath, nil)
            if err == nil {
                t.Fatal()
            }
        }
    }
}

func TestScavengerDelete(t *testing.T) {
    err := clean(1)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    dc := NewDiskCache("cache1", "tmp1", false)
    mc := NewMemoryCache()

    mc.SetDeleteThrough(true)
    mc.AddChild(dc)

    cache := NewScavenger(
        mc,
        ScavMaxSize,
    )

    f, err := os.Open(TestFilePath)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    fi, err := f.Stat()
    if err != nil {
        f.Close()
        t.Fatalf("Error: %v", err)
    }

    reader := NewSafeReader(fi.Size(), f, nil)

    _, err = cache.Put(TestCachePath, nil, reader)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    data, err := dc.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    checkData(data, t)

    data, err = mc.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    checkData(data, t)

    data, err = cache.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    checkData(data, t)

    // manual delete
    cache.Delete(TestCachePath, nil)

    // record should be gone now
    _, err = dc.Get(TestCachePath, nil)
    if err == nil {
        t.Fatal()
    }

    _, err = mc.Get(TestCachePath, nil)
    if err == nil {
        t.Fatal()
    }

    _, err = cache.Get(TestCachePath, nil)
    if err == nil {
        t.Fatal()
    }
}

func TestBigFile(t *testing.T) {
    for i := 1; i < 3; i++ {
        err := clean(i)
        if err != nil {
            t.Fatalf("Error: %v", err)
        }
    }

    f, err := os.Open(BigFilePath)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    fi, err := f.Stat()
    if err != nil {
        f.Close()
        t.Fatalf("Error: %v", err)
    }

    reader := NewSafeReader(fi.Size(), f, nil)

    dc1 := NewDiskCache("cache1", "tmp1", false)
    dc2 := NewDiskCache("cache2", "tmp2", false)

    dc1.AddChild(dc2)

    _, err = dc2.Put(BigFileCachePath, nil, reader)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    data, err := dc1.Get(BigFileCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    hash := sha1.New()
    c, err := io.Copy(hash, data)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    if c != BigFileSize {
        t.Fatalf("Error: Invalid file size (%d != %d)", c, BigFileSize)
    }

    c2 := WaitForCacheFill(data)
    if c2 != BigFileSize {
        t.Fatalf("Error: Invalid fill size (%d != %d)", c2, BigFileSize)
    }

    hashStr := fmt.Sprintf("%X", hash.Sum(nil))
    if hashStr != BigFileHash {
        t.Fatalf("Error: Sha mismatch (%s != %s)", hashStr, BigFileHash)
    }

    f2, err := os.Open(filepath.Join(dc2.parentCache.(*DiskCache).GetRoot(), BigFileCachePath))
    if err != nil {
        t.Fatalf("Error: %v", err)
    }
    defer f2.Close()

    fi, err = f2.Stat()
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    if fi.Size() != BigFileSize {
        t.Fatalf("Error validating write-through size (%d != %d)", fi.Size(), BigFileSize)
    }
}

func bigFileSetup() error {
    f, err := os.Open(BigFilePath)
    if err != nil {
        return err
    }
    fi, err := f.Stat()
    if err != nil {
        f.Close()
        return err
    }

    reader := NewSafeReader(fi.Size(), f, nil)

    hash := sha1.New()
    c, err := io.Copy(hash, reader)
    if err != nil {
        return err
    }

    if c != BigFileSize {
        return fmt.Errorf("Wrong big file size (%d != %d)", c, BigFileSize)
    }

    BigFileHash = fmt.Sprintf("%X", hash.Sum(nil))
    return nil
}

func makeTestFile() error {
    f, err := ioutil.TempFile("", "")
    if err != nil {
        return err
    }
    defer f.Close()

    buffer := make([]byte, TestFileSize)
    c, err := rand.Read(buffer)
    if err != nil {
        return err
    }

    if c != TestFileSize {
        return fmt.Errorf("Size mismatch (%d != %d)", c, TestFileSize)
    }

    sha := sha1.New()
    mw := io.MultiWriter(f, sha)
    mw.Write(buffer)

    TestFilePath = f.Name()
    TestFileHash = fmt.Sprintf("%X", sha.Sum(nil))

    fmt.Printf("Generated %s (%s)\n", TestFilePath, TestFileHash)

    return nil
}

func checkData(reader io.Reader, t *testing.T) {
    hash := sha1.New()
    c, err := io.Copy(hash, reader)

    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    if c != TestFileSize {
        t.Fatal()
    }

    hashStr := fmt.Sprintf("%X", hash.Sum(nil))

    if hashStr != TestFileHash {
        t.Fatalf("Hash mismatch (%s != %s)", hashStr, TestFileHash)
    }
}

func cleanup() {
    for i := 1; i <= 3; i++ {
        err := clean(i)
        if err != nil {
            fmt.Println(err)
        }
    }

    err := os.Remove(TestFilePath)
    if err != nil {
        fmt.Printf("Error cleaning up temp file %s: %v\n", err)
    }
}

func clean(index int) error {
    fmt.Printf("Clean %d\n", index)

    root := fmt.Sprintf("cache%d", index)
    tmp := fmt.Sprintf("tmp%d", index)

    err := os.RemoveAll(root)
    if err != nil {
        return err
    }

    err = os.RemoveAll(tmp)
    if err != nil {
        return err
    }

    return nil
}
