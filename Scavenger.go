package cache

import (
    "io"
    "sync"
    "time"

    "github.com/xaevman/crash"
)

type Scavenger struct {
    data        map[string]time.Time
    intervalSec int
    lock        sync.RWMutex
    maxAgeSec   int
    parentCache RWCache
    shutdown    chan chan interface{}
}

func NewScavenger(parent RWCache, intervalSec, maxAgeSec int) *Scavenger {
    ns := &Scavenger{
        data:        make(map[string]time.Time),
        intervalSec: intervalSec,
        maxAgeSec:   maxAgeSec,
        parentCache: parent,
        shutdown:    make(chan chan interface{}, 0),
    }

    ns.run()

    return ns
}

func (s *Scavenger) Delete(key string, metadata interface{}) error {
    Log.Debug("Scavenger::Delete %s", key)

    s.lock.Lock()
    defer s.lock.Unlock()

    delete(s.data, key)

    return s.parentCache.Delete(key, metadata)
}

func (s *Scavenger) Get(key string, metadata interface{}) (io.Reader, error) {
    Log.Debug("Scavenger::Get %s", key)

    s.lock.RLock()
    defer s.lock.RUnlock()

    s.data[key] = time.Now()

    return s.parentCache.Get(key, metadata)
}

func (s *Scavenger) Put(key string, metadata interface{}, data io.Reader) error {
    Log.Debug("Scavenger::Put %s", key)

    s.lock.Lock()
    defer s.lock.Unlock()

    s.data[key] = time.Now()

    return s.parentCache.Put(key, metadata, data)
}

func (s *Scavenger) Shutdown() {
    scc := make(chan interface{}, 0)

    // signal shutdown
    Log.Debug("Signaling shutdown")
    s.shutdown <- scc

    // wait for shutdown to complete
    Log.Debug("Shutdown complete")
    <-scc
}

func (s *Scavenger) scavenge() {
    s.lock.Lock()
    s.lock.Unlock()

    Log.Debug("Scavenging cache records")

    deletes := make([]string, 0)
    cutoff := time.Now().Add(-time.Duration(s.maxAgeSec) * time.Second)

    for k := range s.data {
        if s.data[k].Before(cutoff) {
            deletes = append(deletes, k)
        }
    }

    for i := range deletes {
        Log.Debug("Removing record %s", deletes[i])
        delete(s.data, deletes[i])
        s.parentCache.Delete(deletes[i], nil)
    }
}

func (s *Scavenger) run() {
    go func() {
        defer crash.HandleAll()

        // shutdown complete channel
        run := true
        var scc chan interface{}

        for run {
            select {
            case <-time.After(time.Duration(s.intervalSec) * time.Second):
                s.scavenge()
            case scc = <-s.shutdown:
                run = false
            }
        }

        scc <- nil
    }()
}
