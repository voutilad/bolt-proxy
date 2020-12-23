package backend

import (
	"errors"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/voutilad/bolt-proxy/bolt"
)

type Backend struct {
	monitor *Monitor
	tls     bool
	log     *log.Logger
	// map of principals -> hosts -> connections
	connectionPool map[string]map[string]bolt.BoltConn
	routingCache   map[string]RoutingTable
	info           ClusterInfo
}

func NewBackend(logger *log.Logger, username, password string, uri string, hosts ...string) (*Backend, error) {
	tls := false
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "bolt+s", "bolt+ssc", "neo4j+s", "neo4j+ssc":
		tls = true
	case "bolt", "neo4j":
		// ok
	default:
		return nil, errors.New("invalid neo4j connection scheme")
	}

	monitor, err := NewMonitor(username, password, uri, hosts...)
	if err != nil {
		return nil, err
	}

	return &Backend{
		monitor:        monitor,
		tls:            tls,
		log:            logger,
		connectionPool: make(map[string]map[string]bolt.BoltConn),
		routingCache:   make(map[string]RoutingTable),
		info:           <-monitor.Info,
	}, nil
}

func (b *Backend) Version() Version {
	return b.monitor.Version
}

func (b *Backend) RoutingTable(db string) (RoutingTable, error) {
	table, found := b.routingCache[db]
	if found && !table.Expired() {
		return table, nil
	}

	table, err := b.monitor.UpdateRoutingTable(db)
	if err != nil {
		return RoutingTable{}, err
	}

	b.log.Printf("got routing table for %s: %s", db, table)
	return table, nil
}

func (b *Backend) ClusterInfo() (ClusterInfo, error) {
	// XXX: this technically isn't thread safe as we mutate b.info

	if b.info.CreatedAt.Add(30 * time.Second).Before(time.Now()) {
		select {
		case <-time.After(30 * time.Second):
			return ClusterInfo{}, errors.New("timeout waiting for updated ClusterInfo")
		case info := <-b.monitor.Info:
			b.info = info
		}
	}

	return b.info, nil
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

	// TODO: clean up this api...push the dirt into Bolt package?
	msg, pos, err := bolt.ParseMap(hello.Data[4:])
	if err != nil {
		b.log.Printf("XXX pos: %d, hello map: %#v\n", pos, msg)
		panic(err)
	}
	principal, ok := msg["principal"].(string)
	if !ok {
		panic("principal in Hello message was not a string")
	}
	b.log.Println("found principal:", principal)

	// Try authing first with a Core cluster member before we try others
	// this way we can fail fast and not spam a bad set of credentials
	info, err := b.ClusterInfo()
	if err != nil {
		return nil, err
	}
	defaultHost := info.Hosts[0]

	b.log.Printf("trying to auth %s to host %s\n", principal, defaultHost)
	conn, err := authClient(hello.Data, b.Version().Bytes(),
		"tcp", defaultHost, b.tls)
	if err != nil {
		return nil, err
	}

	// Ok, now to get the rest
	conns := make(map[string]bolt.BoltConn, len(info.Hosts))
	conns[defaultHost] = bolt.NewDirectConn(conn)

	// We'll need a channel to collect results as we're going to auth
	// to all hosts asynchronously
	type pair struct {
		conn bolt.BoltConn
		host string
	}
	c := make(chan pair, len(info.Hosts)+1)
	var wg sync.WaitGroup
	for _, host := range info.Hosts {
		// skip the host we already used to test auth
		if host != defaultHost {
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
