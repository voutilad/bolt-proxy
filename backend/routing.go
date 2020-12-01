package backend

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type RoutingTable struct {
	defaultDb  string
	readers    map[string][]string
	writers    map[string][]string
	ttl        time.Duration
	lastUpdate time.Time
	mutex      sync.Mutex
}

func (rt *RoutingTable) String() string {
	return fmt.Sprintf(
		"RoutingTable{ defaultDb: %s, "+
			"readerMap: %v, "+
			"writerMap: %v, "+
			"ttl: %s, "+
			"lastUpdate: %s }",
		rt.defaultDb, rt.readers, rt.writers, rt.ttl, rt.lastUpdate,
	)
}

func (rt *RoutingTable) DefaultDb() string {
	return rt.defaultDb
}

func NewRoutingTable(defaultDb string, ttl time.Duration) *RoutingTable {
	readers := make(map[string][]string)
	writers := make(map[string][]string)

	rt := RoutingTable{
		defaultDb:  defaultDb,
		readers:    readers,
		writers:    writers,
		ttl:        ttl,
		lastUpdate: time.Unix(0, 0),
	}
	return &rt
}

func (rt *RoutingTable) ReadersFor(db string) ([]string, error) {
	rt.mutex.Lock()
	defer rt.mutex.Unlock()

	readers, found := rt.readers[db]
	if found {
		result := make([]string, len(readers))
		copy(result, readers)
		return result, nil
	}
	return nil, errors.New("no such database")
}

func (rt *RoutingTable) WritersFor(db string) ([]string, error) {
	rt.mutex.Lock()
	defer rt.mutex.Unlock()

	writers, found := rt.writers[db]
	if found {
		result := make([]string, len(writers))
		copy(result, writers)
		return result, nil
	}
	return nil, errors.New("no such database")
}

func (rt *RoutingTable) SetRoutes(db string, readers, writers []string) {
	rt.mutex.Lock()
	defer rt.mutex.Unlock()

	rt.readers[db] = readers
	rt.writers[db] = writers
	rt.lastUpdate = time.Now()
}

func (rt *RoutingTable) SetTtl(ttl time.Duration) {
	rt.mutex.Lock()
	defer rt.mutex.Unlock()

	rt.ttl = ttl
	rt.lastUpdate = time.Now()
}
