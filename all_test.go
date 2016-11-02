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
    "crypto/rand"
    "crypto/sha1"
    "fmt"
    "io"
    "io/ioutil"
    "os"
    "path/filepath"
    "testing"
)

const TestFileSize = 64 * 1024

var (
    TestFilePath  string
    TestFileHash  string
    TestCachePath = filepath.Join("dir", "test.file")
)

func TestMain(m *testing.M) {
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

    reader := NewSafeReader(f)

    checkData(reader, t)
}

func TestDiskCacheChildren(t *testing.T) {
    dc1 := NewDiskCache("cache1", "tmp1", false)
    dc2 := NewDiskCache("cache2", "tmp2", false)

    dc1.AddChild(dc2)
    if dc1.ChildrenCount() != 1 {
        t.Fatal()
    }

    dc1.RemoveChild(dc2)
    if dc1.ChildrenCount() != 0 {
        t.Fatal()
    }
}

func TestDiskCachePut(t *testing.T) {
    dc := NewDiskCache("cache1", "tmp1", false)

    f, err := os.Open(TestFilePath)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    reader := NewSafeReader(f)

    err = dc.Put(TestCachePath, nil, reader)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    fullPath := filepath.Join("cache1", TestCachePath)
    f2, err := os.Open(fullPath)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    checkData(NewSafeReader(f2), t)
}

func TestDiskCacheGet(t *testing.T) {
    dc := NewDiskCache("cache1", "tmp1", false)

    reader, err := dc.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    checkData(reader, t)
}

func TestCacheFiller(t *testing.T) {
    dc1 := NewDiskCache("cache1", "tmp1", false)
    dc2 := NewDiskCache("cache2", "tmp2", false)

    // previous tests should have populated the r1 cache
    r1, err := dc1.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    // use CacheFiller to fill the r2 cache with the reader
    // from r1
    cf := NewCacheFiller(TestCachePath, nil, dc2, r1)

    checkData(cf, t)

    // r2 cache should be full at this point
    data, err := dc2.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    checkData(data, t)
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

    d2, err := dc3.Get(TestCachePath, nil)
    if err != nil {
        t.Fatalf("Error: %v", err)
    }

    // check the data that was populated into dc3 cache
    checkData(d2, t)
}

func TestDataNotFound(t *testing.T) {
    dc1 := NewDiskCache("cache1", "tmp1", false)

    _, err := dc1.Get("notreal.file", nil)
    if err == nil {
        t.Fatal()
    }
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
