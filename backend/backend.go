package backend

import (
	"log"
	"strings"
	"sync"
	"time"

	"github.com/voutilad/bolt-proxy/bolt"
)

type Backend struct {
	monitor      *Monitor
	routingTable *RoutingTable
	tls          bool
	log          *log.Logger
	// map of principals -> hosts -> connections
	connectionPool map[string]map[string]bolt.BoltConn
}

func NewBackend(logger *log.Logger, username, password string, uri string, hosts ...string) (*Backend, error) {
	monitor, err := NewMonitor(username, password, uri, hosts...)
	if err != nil {
		return nil, err
	}
	routingTable := <-monitor.C

	tls := false
	switch strings.Split(uri, ":")[0] {
	case "bolt+s", "bolt+ssc", "neo4j+s", "neo4j+ssc":
		tls = true
	default:
	}

	return &Backend{
		monitor:      monitor,
		routingTable: routingTable,
		tls:          tls,
		log:          logger,
	}, nil
}

func (b *Backend) Version() Version {
	return b.monitor.Version
}

func (b *Backend) RoutingTable() *RoutingTable {
	if b.routingTable == nil {
		panic("attempting to use uninitialized BackendClient")
	}

	b.log.Println("checking routing table...")
	if b.routingTable.Expired() {
		select {
		case rt := <-b.monitor.C:
			b.routingTable = rt
		case <-time.After(60 * time.Second):
			b.log.Fatal("timeout waiting for new routing table!")
		}
	}

	b.log.Println("using routing table")
	return b.routingTable
}

// For now, we'll authenticate to all known hosts up-front to simplify things.
// So for a given Hello message, use it to auth against all hosts known in the
// current routing table.
//
// Returns an map[string] of hosts to bolt.BoltConn's if successful, an empty
// map and an error if not.
func (b *Backend) Authenticate(hello *bolt.Message) (map[string]bolt.BoltConn, error) {
	if hello.T != bolt.HelloMsg {
		panic("authenticate requires a Hello message")
	}

	// TODO: clean up this api...push the dirt into Bolt package
	msg, pos, err := bolt.ParseTinyMap(hello.Data[4:])
	if err != nil {
		b.log.Printf("XXX pos: %d, hello map: %#v\n", pos, msg)
		panic(err)
	}
	principal, ok := msg["principal"].(string)
	if !ok {
		panic("principal in Hello message was not a string")
	}
	b.log.Println("found principal:", principal)

	// refresh routing table
	// TODO: this api seems backwards...push down into table?
	rt := b.RoutingTable()

	// Try authing first with the default db writer before we try others
	// this way we can fail fast and not spam a bad set of credentials
	writers, _ := rt.WritersFor(rt.DefaultDb)
	defaultWriter := writers[0]

	b.log.Printf("trying to auth %s to host %s\n", principal, defaultWriter)
	conn, err := authClient(hello.Data, b.Version().Bytes(),
		"tcp", defaultWriter, b.tls)
	if err != nil {
		return nil, err
	}

	// Ok, now to get the rest
	conns := make(map[string]bolt.BoltConn, len(rt.Hosts))
	conns[defaultWriter] = bolt.NewDirectConn(conn)

	// We'll need a channel to collect results as we're going to auth
	// to all hosts asynchronously
	type pair struct {
		conn bolt.BoltConn
		host string
	}
	c := make(chan pair, len(rt.Hosts)+1)
	var wg sync.WaitGroup
	for host := range rt.Hosts {
		// skip the host we already used to test auth
		if host != defaultWriter {
			wg.Add(1)
			go func(h string) {
				defer wg.Done()
				conn, err := authClient(hello.Data, b.Version().Bytes(), "tcp", h, b.tls)
				if err != nil {
					b.log.Printf("failed to auth %s to %s!?\n", principal, h)
					return
				}
				b.log.Printf("auth'd %s to host %s\n", principal, h)
				c <- pair{bolt.NewDirectConn(conn), h}
			}(host)
		}
	}

	wg.Wait()
	close(c)

	for p := range c {
		conns[p.host] = p.conn
	}

	b.log.Printf("auth'd principal to %d hosts\n", len(conns))
	return conns, err
}
