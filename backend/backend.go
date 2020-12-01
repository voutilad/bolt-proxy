package backend

import (
	"container/ring"
	"log"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type Config struct {
	Hosts              []string
	Username, Password string
	AuthTtl            time.Duration
}

type Backend struct {
	Config       Config
	RoutingTable *RoutingTable
	Driver       *neo4j.Driver
	Sessions     map[string][]Session
}

func NewBackend(config Config) Backend {
	sessions := make(map[string][]Session)
	ttl := time.Duration(300) * time.Second

	auth := neo4j.BasicAuth(config.Username, config.Password, "")

	// XXX: Lots of funny business here to handle multiple hosts
	configurer := func(c *neo4j.Config) {
		c.AddressResolver = func(_ neo4j.ServerAddress) []neo4j.ServerAddress {
			addrs := make([]neo4j.ServerAddress, len(config.Hosts))
			for i, host := range config.Hosts {
				parts := strings.Split(host, ":")
				if len(parts) != 2 {
					panic("invalid host: " + host)
				}
				addrs[i] = neo4j.NewServerAddress(parts[0], parts[1])
			}
			return addrs
		}
		c.UserAgent = "bolt-proxy/v0"
	}

	driver, err := neo4j.NewDriver("bolt://"+config.Hosts[0], auth, configurer)
	if err != nil {
		log.Fatal(err)
	}

	return Backend{
		Config:       config,
		RoutingTable: NewRoutingTable("neo4j", ttl),
		Driver:       &driver,
		Sessions:     sessions,
	}
}

type Session struct {
	principal   string
	credentials []byte
	pool        *Pool
}

type Pool struct {
	readers, writers *ring.Ring
}
