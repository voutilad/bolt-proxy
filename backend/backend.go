package backend

import (
	"container/ring"
	"log"
	"time"
)

type Backend struct {
	sessions     map[string][]Session
	monitor      *Monitor
	routingTable *RoutingTable
}

func NewBackend(username, password string, hosts ...string) (*Backend, error) {
	monitor, err := NewMonitor(username, password, hosts...)
	if err != nil {
		return nil, err
	}
	routingTable := <-monitor.C
	sessions := make(map[string][]Session)

	return &Backend{sessions, monitor, routingTable}, nil
}

func (b *Backend) RoutingTable() *RoutingTable {
	if b.routingTable == nil {
		panic("attempting to use uninitialized BackendClient")
	}

	if b.routingTable.Expired() {
		select {
		case rt := <-b.monitor.C:
			b.routingTable = rt
		case <-time.After(60 * time.Second):
			log.Fatal("timeout waiting for new routing table!")
		}
	}

	return b.routingTable
}

type Session struct {
	principal   string
	credentials []byte
	pool        *Pool
}

type Pool struct {
	readers, writers *ring.Ring
}
