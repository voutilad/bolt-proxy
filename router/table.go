package router

import (
	"container/ring"
	"errors"
	"sync"
	//	"time"
	//	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type RoutingTable struct {
	readers *ring.Ring
	writers *ring.Ring
	//	ttl        time.Duration
	//	lastUpdate time.Time
	mutex sync.Mutex
}

func NewRoutingTable() *RoutingTable {
	rt := RoutingTable{
		readers: ring.New(3),
		writers: ring.New(3),
		//	ttl:        time.Second * 300,
		//	lastUpdate: time.Now(),
	}

	return &rt
}

func (rt *RoutingTable) NextReader() (string, error) {
	rt.mutex.Lock()
	defer rt.mutex.Unlock()

	// Rings can be empty or have gaps
	for i := 0; i < rt.readers.Len(); i++ {
		rt.readers = rt.readers.Next()
		if rt.readers.Value != nil {
			// Assume we only use strings
			return rt.readers.Value.(string), nil
		}
	}
	return "", errors.New("empty readers ring!")
}

func (rt *RoutingTable) NextWriter() (string, error) {
	rt.mutex.Lock()
	defer rt.mutex.Unlock()

	// Rings can be empty or have gaps
	for i := 0; i < rt.writers.Len(); i++ {
		rt.writers = rt.writers.Next()
		if rt.writers.Value != nil {
			// Assume we only use strings
			return rt.writers.Value.(string), nil
		}
	}
	return "", errors.New("empty writers ring!")
}

func (rt *RoutingTable) ReplaceReaders(readers []string) {
	rt.mutex.Lock()
	defer rt.mutex.Unlock()

	rt.readers = ring.New(len(readers))

	for _, reader := range readers {
		rt.readers = rt.readers.Next()
		rt.readers.Value = reader
	}
}

func (rt *RoutingTable) ReplaceWriters(writers []string) {
	rt.mutex.Lock()
	defer rt.mutex.Unlock()

	rt.writers = ring.New(len(writers))

	for _, writer := range writers {
		rt.writers = rt.writers.Next()
		rt.writers.Value = writer
	}
}
