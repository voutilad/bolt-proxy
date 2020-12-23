package backend

import (
	"fmt"
	"time"
)

type RoutingTable struct {
	Name                      string
	Readers, Writers, Routers []string
	CreatedAt                 time.Time
	Ttl                       time.Duration
}

func (t RoutingTable) String() string {
	return fmt.Sprintf("DbTable{ Name: %s, "+
		"Readers: %s, "+
		"Writers: %s, "+
		"Routers: %s, "+
		"CreatedAt: %s, "+
		"Ttl: %s}",
		t.Name, t.Readers, t.Writers, t.Routers,
		t.CreatedAt, t.Ttl)
}

type ClusterInfo struct {
	DefaultDb string
	Ttl       time.Duration
	Hosts     []string
	CreatedAt time.Time
}

func (rt RoutingTable) Expired() bool {
	return rt.CreatedAt.Add(rt.Ttl).Before(time.Now())
}

func (i ClusterInfo) String() string {
	return fmt.Sprintf(
		"ClusterInfo{ DefaultDb: %s, Ttl: %v, Hosts: %v, CreatedAt: %v }",
		i.DefaultDb, i.Ttl, i.Hosts, i.CreatedAt)
}
