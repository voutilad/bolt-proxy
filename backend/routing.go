package backend

import (
	"errors"
	"fmt"
	"time"
)

type RoutingTable struct {
	readers   map[string][]string
	writers   map[string][]string
	DefaultDb string
	Ttl       time.Duration
	CreatedAt time.Time
}

func (rt *RoutingTable) Expired() bool {
	now := time.Now()
	return rt.CreatedAt.Add(rt.Ttl).Before(now)
}

func (rt *RoutingTable) String() string {
	return fmt.Sprintf(
		"RoutingTable{ DefaultDb: %s, "+
			"readerMap: %v, "+
			"writerMap: %v, "+
			"Ttl: %s, "+
			"CreatedAt: %s }",
		rt.DefaultDb, rt.readers, rt.writers, rt.Ttl, rt.CreatedAt,
	)
}

func (rt *RoutingTable) ReadersFor(db string) ([]string, error) {
	readers, found := rt.readers[db]
	if found {
		result := make([]string, len(readers))
		copy(result, readers)
		return result, nil
	}
	return nil, errors.New("no such database")
}

func (rt *RoutingTable) WritersFor(db string) ([]string, error) {
	writers, found := rt.writers[db]
	if found {
		result := make([]string, len(writers))
		copy(result, writers)
		return result, nil
	}
	return nil, errors.New("no such database")
}
