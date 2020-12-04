package backend

import (
	"log"
	"net"
	"time"

	"github.com/voutilad/bolt-proxy/bolt"
)

type Backend struct {
	monitor      *Monitor
	routingTable *RoutingTable
}

func NewBackend(username, password string, hosts ...string) (*Backend, error) {
	monitor, err := NewMonitor(username, password, hosts...)
	if err != nil {
		return nil, err
	}
	routingTable := <-monitor.C

	return &Backend{monitor, routingTable}, nil
}

func (b *Backend) RoutingTable() *RoutingTable {
	if b.routingTable == nil {
		panic("attempting to use uninitialized BackendClient")
	}

	log.Println("checking routing table...")
	if b.routingTable.Expired() {
		select {
		case rt := <-b.monitor.C:
			b.routingTable = rt
		case <-time.After(60 * time.Second):
			log.Fatal("timeout waiting for new routing table!")
		}
	}

	log.Println("using routing table")
	return b.routingTable
}

// For now, try auth'ing to the default db "Writer"
func (b *Backend) Authenticate(hello []byte) (net.Conn, error) {

	// TODO: clean up this api...push the dirt into Bolt package
	msg, _, err := bolt.ParseTinyMap(hello[4:])
	principal := msg["principal"].(string)

	log.Println("found principal:", principal)

	// refresh routing table
	// TODO: this api seems backwards...push down into table?
	rt := b.RoutingTable()
	writers, _ := rt.WritersFor(rt.DefaultDb)

	log.Printf("trying to auth %s to backend host %s\n", principal, writers[0])
	conn, err := authClient(hello, "tcp", writers[0])
	return conn, err
}
