package cache

import (
    "io"
    "sort"
    "sync"
    "time"
)

type DataRecord struct {
    Key      string
    LastRead time.Time
    Size     int64
}

type ByLastReadAsc []*DataRecord

func (dr ByLastReadAsc) Len() int           { return len(dr) }
func (dr ByLastReadAsc) Swap(i, j int)      { dr[i], dr[j] = dr[j], dr[i] }
func (dr ByLastReadAsc) Less(i, j int) bool { return dr[i].LastRead.Before(dr[j].LastRead) }

type Scavenger struct {
    data        map[string]*DataRecord
    dataList    []*DataRecord
    lock        sync.RWMutex
    maxSize     int64
    currentSize int64
    parentCache RWCache
}

func NewScavenger(parent RWCache, maxSize int64) *Scavenger {
    ns := &Scavenger{
        data:        make(map[string]*DataRecord),
        dataList:    make([]*DataRecord, 0),
        maxSize:     maxSize,
        currentSize: 0,
        parentCache: parent,
    }

    return ns
}

func (s *Scavenger) Touch(key string, size int64) {
    s.lock.Lock()
    defer s.lock.Unlock()

    val, ok := s.data[key]
    if !ok {
        s.data[key] = &DataRecord{
            Key:  key,
            Size: size,
        }
        val = s.data[key]
        s.dataList = append(s.dataList, val)
    }

    val.LastRead = time.Now()
    s.currentSize += size

    if s.currentSize > s.maxSize {
        s.scavenge()
    }
}

func (s *Scavenger) Delete(key string, metadata interface{}) error {
    Log.Debug("Scavenger::Delete %s", key)

    s.lock.Lock()
    defer s.lock.Unlock()

    return s.delete(key, metadata)
}

func (s *Scavenger) Find(key string) bool {
    Log.Debug("Scavenger::Find %s", key)

    s.lock.RLock()
    defer s.lock.RUnlock()

    _, exists := s.data[key]
    return exists
}

func (s *Scavenger) Get(key string, metadata interface{}) (int64, io.Reader, error) {
    Log.Debug("Scavenger::Get %s (cache size %d)", key, s.Size())

    s.lock.RLock()
    defer s.lock.RUnlock()

    count, reader, err := s.parentCache.Get(key, metadata)
    if err != nil {
        return GetLengthUnknown, nil, err
    }

    val, ok := s.data[key]
    if ok {
        val.LastRead = time.Now()
    }
    // if !ok {
    //     s.data[key] = &DataRecord{
    //         Key: key,
    //     }
    //     val = s.data[key]
    //     s.dataList = append(s.dataList, val)
    //     s.currentSize += count
    // }

    return count, reader, err
}

func (s *Scavenger) Put(key string, metadata interface{}, data io.Reader) (int64, error) {
    Log.Debug("Scavenger::Put %s", key)

    s.lock.Lock()
    defer s.lock.Unlock()

    c, err := s.parentCache.Put(key, metadata, data)
    if err != nil {
        return 0, err
    }

    val, ok := s.data[key]
    if !ok {
        s.data[key] = &DataRecord{
            Key: key,
        }
        val = s.data[key]
        s.dataList = append(s.dataList, val)
    }

    val.LastRead = time.Now()
    val.Size = c
    s.currentSize += c

    Log.Debug("Scavenger::CurrentSize %d (%d max)", s.currentSize, s.maxSize)

    if s.currentSize > s.maxSize {
        s.scavenge()
    }

    return c, nil
}

func (s *Scavenger) Size() int64 {
    s.lock.RLock()
    defer s.lock.RUnlock()

    return s.currentSize
}

func (s *Scavenger) delete(key string, metadata interface{}) error {
    err := s.parentCache.Delete(key, metadata)

    if err != nil {
        return err
    }

    val, ok := s.data[key]
    if !ok {
        return nil
    }

    index := -1
    for i := range s.dataList {
        if s.dataList[i] == val {
            index = i
        }
    }

    if index > -1 {
        s.dataList = append(s.dataList[:index], s.dataList[index+1:]...)
    }

    s.currentSize -= val.Size

    delete(s.data, key)

    return nil
}

func (s *Scavenger) scavenge() {
    Log.Debug("Scavenging cache records...")

    deletes := make([]string, 0)

    // sort by last read time
    sort.Sort(ByLastReadAsc(s.dataList))

    // delete until below maxSize
    targetSize := s.currentSize - s.maxSize
    deleteSize := int64(0)
    for i := range s.dataList {
        deleteSize += s.dataList[i].Size
        deletes = append(deletes, s.dataList[i].Key)
        if deleteSize >= targetSize {
            break
        }
    }

    for i := range deletes {
        s.delete(deletes[i], nil)
    }
}
