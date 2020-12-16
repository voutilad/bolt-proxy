package backend

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// Modeled after time.Ticker, a Monitor will keep tabs on the Neo4j routing
// table behind the scenes. It auto-adjusts the refresh interval to match
// the server's declared TTL recommendation.
//
// As it creates new RoutingTable instances on the heap, it will put pointers
// to new instances into the channel C. (Similar to how time.Ticker puts the
// current time into its channel.)
//
// Before it puts a new pointer in the channel, it tries to empty it, which
// hopefully reduces the chance of receiving stale entries.
type Monitor struct {
	C       <-chan *RoutingTable
	halt    chan bool
	driver  *neo4j.Driver
	Version Version
}

type Version struct {
	Major, Minor, Patch uint8
}

func ParseVersion(buf []byte) (Version, error) {
	if len(buf) < 4 {
		return Version{}, errors.New("buffer too short (< 4)")
	}

	version := Version{}
	version.Major = uint8(buf[3])
	version.Minor = uint8(buf[2])
	version.Patch = uint8(buf[1])
	return version, nil
}

func (v Version) String() string {
	return fmt.Sprintf("Bolt{major: %d, minor: %d, patch: %d}",
		v.Major,
		v.Minor,
		v.Patch)
}

func (v Version) Bytes() []byte {
	return []byte{
		0x00, 0x00,
		v.Minor, v.Major,
	}
}

// Our default Driver configuration provides:
// - custom user-agent name
// - ability to add in specific list of hosts to use for address resolution
func newConfigurer(hosts []string) func(c *neo4j.Config) {
	return func(c *neo4j.Config) {
		c.AddressResolver = func(addr neo4j.ServerAddress) []neo4j.ServerAddress {
			if len(hosts) == 0 {
				return []neo4j.ServerAddress{addr}
			}

			addrs := make([]neo4j.ServerAddress, len(hosts))
			for i, host := range hosts {
				parts := strings.Split(host, ":")
				if len(parts) != 2 {
					panic(fmt.Sprintf("invalid host: %s", host))
				}
				addrs[i] = neo4j.NewServerAddress(parts[0], parts[1])
			}
			return addrs
		}
		c.UserAgent = "bolt-proxy/v0"
	}
}

const VERSION_QUERY = `
CALL dbms.components() YIELD name, versions
WITH name, versions WHERE name = "Neo4j Kernel"
RETURN [x IN split(head(versions), ".") | toInteger(x)] AS version
`

// Get the backend Neo4j version based on the output of the VERSION_QUERY,
// which should provide an array of int64s corresponding to the Version.
// Return the Version on success, otherwise return an empty Version and
// and error.
func getVersion(driver *neo4j.Driver) (Version, error) {
	version := Version{}
	session := (*driver).NewSession(neo4j.SessionConfig{})
	defer session.Close()

	result, err := session.Run(VERSION_QUERY, nil)
	if err != nil {
		return version, nil
	}

	record, err := result.Single()
	if err != nil {
		return version, nil
	}

	val, found := record.Get("version")
	if !found {
		return version, errors.New("couldn't find version in query results")
	}
	data, ok := val.([]interface{})
	if !ok {
		return version, errors.New("version isn't an array")
	}
	if len(data) != 3 {
		return version, errors.New("version array doesn't contain 3 values")
	}

	// yolo for now
	version.Major = uint8(data[0].(int64))
	version.Minor = uint8(data[1].(int64))
	version.Patch = uint8(data[2].(int64))

	return version, nil
}

// Construct and start a new routing table Monitor using the provided user,
// password, and uri as arguments to the underlying neo4j.Driver. Returns a
// pointer to the Monitor on success, or nil and an error on failure.
//
// Any additional hosts provided will be used as part of a custom address
// resolution function via the neo4j.Driver.
func NewMonitor(user, password, uri string, hosts ...string) (*Monitor, error) {
	c := make(chan *RoutingTable, 1)
	h := make(chan bool, 1)

	// Try immediately to connect to Neo4j
	auth := neo4j.BasicAuth(user, password, "")
	driver, err := neo4j.NewDriver(uri, auth, newConfigurer(hosts))
	if err != nil {
		return nil, err
	}

	version, err := getVersion(&driver)
	if err != nil {
		panic(err)
	}
	// log.Printf("found neo4j version %v\n", version)

	// TODO: check if in SINGLE, CORE, or READ_REPLICA mode
	// We can run `CALL dbms.listConfig('dbms.mode') YIELD value` and
	// check if we're clustered or not. Ideally, if not clustered, we
	// simplify the monitor considerably to just health checks and no
	// routing table.

	// Get the first routing table and ttl details
	rt, err := getNewRoutingTable(&driver)
	if err != nil {
		panic(err)
	}
	c <- rt

	monitor := Monitor{c, h, &driver, version}
	go func() {
		// preset the initial ticker to use the first ttl measurement
		ticker := time.NewTicker(rt.Ttl)
		for {
			select {
			case <-ticker.C:
				rt, err := getNewRoutingTable(monitor.driver)
				if err != nil {
					panic(err)
				}
				ticker.Reset(rt.Ttl)

				// empty the channel and put the new value in
				// this looks odd, but even though it's racy,
				// it should be racy in a safe way since it
				// doesn't matter if another go routine takes
				// the value first
				select {
				case <-c:
				default:
				}
				select {
				case c <- rt:
				default:
					panic("monitor channel full")
				}
			case <-h:
				ticker.Stop()
				// log.Println("monitor stopped")
			case <-time.After(10 * rt.Ttl):
				msg := fmt.Sprintf("monitor timeout of 10*%v reached\n", rt.Ttl)
				panic(msg)
			}
		}
	}()

	return &monitor, nil
}

func (m *Monitor) Stop() {
	select {
	case (*m).halt <- true:
	default:
	}
}

// local data structure for passing the raw routing table details
// TODO: ttl could be pulled direct via a check of dbms.routing_ttl
// since it's not a dynamic config value as of v4.2
type table struct {
	db      string
	ttl     time.Duration
	readers []string
	writers []string
}

// Denormalize the routing table to make post-processing easier
const ROUTING_QUERY = `
UNWIND $names AS name
CALL dbms.routing.getRoutingTable({}, name)
  YIELD ttl, servers
WITH name, ttl, servers
UNWIND servers AS server
WITH name, ttl, server
UNWIND server["addresses"] AS address
RETURN name, ttl, server["role"] AS role, address
`

// Dump the list of databases. We need to keep this simple to support v4.0,
// v4.1, and v4.2 since this has been a moving target in how it works
const SHOW_DATABASES = "SHOW DATABASES"

// Use SHOW DATABASES to dump the current list of databases with the first
// database name being the default (based on the query logic)
func queryDbNames(driver *neo4j.Driver) ([]string, error) {
	session := (*driver).NewSession(neo4j.SessionConfig{
		DatabaseName: "system",
	})
	defer session.Close()

	result, err := session.Run(SHOW_DATABASES, nil)
	if err != nil {
		return nil, err
	}
	rows, err := result.Collect()
	if err != nil {
		return nil, err
	}

	// create a basic set structure
	nameSet := make(map[string]bool)
	for _, row := range rows {
		val, found := row.Get("name")
		if !found {
			return nil, errors.New("couldn't find name field in result")
		}
		name, ok := val.(string)
		if !ok {
			panic("name isn't a string")
		}

		val, found = row.Get("currentStatus")
		if !found {
			return nil, errors.New("couldn't find currentStatus field in result")
		}
		status, ok := val.(string)
		if !ok {
			panic("currentStatus isn't a string")
		}

		if status == "online" {
			nameSet[name] = true
		}
	}

	names := make([]string, 0, len(nameSet))
	for key := range nameSet {
		names = append(names, key)
	}
	return names, nil
}

func queryRoutingTable(driver *neo4j.Driver, names []string) (map[string]table, error) {
	session := (*driver).NewSession(neo4j.SessionConfig{})
	defer session.Close()

	result, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		return routingTableTx(tx, names)
	})
	if err != nil {
		return map[string]table{}, err
	}

	tableMap, ok := result.(map[string]table)
	if !ok {
		return map[string]table{}, errors.New("invalid type for routing table response")
	}

	return tableMap, nil
}

// Given a neo4j.Transaction tx, collect the routing table maps for each of
// the databases in names. Since this should run in a transaction work function
// we return a generic interface{} on success, or nil and an error if failed.
//
// The true data type is a map[string]table, mapping database names to their
// respective tables.
func routingTableTx(tx neo4j.Transaction, names []string) (interface{}, error) {
	params := make(map[string]interface{}, 1)
	params["names"] = names
	result, err := tx.Run(ROUTING_QUERY, params)
	if err != nil {
		return nil, err
	}

	rows, err := result.Collect()
	if err != nil {
		return nil, err
	}

	// expected fields: [name, ttl, role, address]
	tableMap := make(map[string]table, len(rows))
	for _, row := range rows {
		val, found := row.Get("address")
		if !found {
			return nil, errors.New("missing address field in result")
		}
		addr, ok := val.(string)
		if !ok {
			panic("addr isn't a string!")
		}

		val, found = row.Get("ttl")
		if !found {
			return nil, errors.New("missing ttl field in result")
		}
		ttl, ok := val.(int64)
		if !ok {
			panic("ttl isn't an integer!")
		}

		val, found = row.Get("name")
		if !found {
			return nil, errors.New("missing name field in result")
		}
		name, ok := val.(string)
		if !ok {
			panic("name isn't a string!")
		}

		t, found := tableMap[name]
		if !found {
			t = table{
				db:  name,
				ttl: time.Duration(ttl) * time.Second,
			}
		}

		val, found = row.Get("role")
		if !found {
			return nil, errors.New("missing role field in result")
		}
		role, ok := val.(string)
		if !ok {
			panic("role isn't a string")
		}

		switch role {
		case "READ":
			t.readers = append(t.readers, addr)
		case "WRITE":
			t.writers = append(t.writers, addr)
		case "ROUTE":
			continue
		default:
			return nil, errors.New("invalid role")
		}

		tableMap[name] = t
	}

	return tableMap, nil
}

// Using a pointer to a connected neo4j.Driver, orchestrate fetching the
// database names and get the current routing table for each.
//
// XXX: this is pretty heavy weight :-(
func getNewRoutingTable(driver *neo4j.Driver) (*RoutingTable, error) {
	names, err := queryDbNames(driver)
	if err != nil {
		msg := fmt.Sprintf("error getting database names: %v\n", err)
		return nil, errors.New(msg)
	}

	tableMap, err := queryRoutingTable(driver, names)
	if err != nil {
		msg := fmt.Sprintf("error getting routing table: %v\n", err)
		return nil, errors.New(msg)
	}

	// build the new routing table instance
	// TODO: clean this up...seems smelly..
	readers := make(map[string][]string)
	writers := make(map[string][]string)
	rt := RoutingTable{
		DefaultDb: names[0],
		readers:   readers,
		writers:   writers,
		CreatedAt: time.Now(),
		Hosts:     make(map[string]bool),
	}
	for db, t := range tableMap {
		r := make([]string, len(t.readers))
		copy(r, t.readers)
		w := make([]string, len(t.writers))
		copy(w, t.writers)
		rt.readers[db] = r
		rt.writers[db] = w

		// yes, this is redundant...
		rt.Ttl = t.ttl

		// yes, this is also wasteful...construct host sets
		for _, host := range r {
			rt.Hosts[host] = true
		}
		for _, host := range w {
			rt.Hosts[host] = true
		}
	}

	// log.Printf("updated routing table: %s\n", &rt)
	// log.Printf("known hosts look like: %v\n", rt.Hosts)

	return &rt, nil
}
