package backend

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// TODO: what the hell are we doing here?
type Monitor struct {
	Info    <-chan ClusterInfo
	halt    chan bool
	driver  *neo4j.Driver
	Version Version
	Ttl     time.Duration
	Host    string
}

type Version struct {
	Major, Minor, Patch uint8
	Extra               string
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
	return fmt.Sprintf("Bolt{major: %d, minor: %d, patch: %d, extra: %s}",
		v.Major,
		v.Minor,
		v.Patch,
		v.Extra)
}

func (v Version) Bytes() []byte {
	return []byte{
		0x00, 0x00,
		v.Minor, v.Major,
	}
}

func (m Monitor) UpdateRoutingTable(db string) (RoutingTable, error) {
	return getRoutingTable(m.driver, db, m.Host)
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
		// TODO: wire into global version string
		c.UserAgent = "bolt-proxy/v0.3.0"
	}
}

const VERSION_QUERY = `CALL dbms.components() YIELD name, versions
WITH name, versions WHERE name = "Neo4j Kernel"
WITH head(versions) AS version
RETURN [x IN split(head(split(version, "-")), ".") | toInteger(x)] AS version,
  coalesce(split(version, "-")[1], "") AS extra
`

// Get the backend Neo4j version based on the output of the VERSION_QUERY,
// which should provide an array of int64s corresponding to the Version.
// Return the Version on success, otherwise return an empty Version and
// and error.
//
// Note: Aura provides a special version string because Aura is special.
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
		return version, errors.New("couldn't find 'version' in query results")
	}
	data, ok := val.([]interface{})
	if !ok {
		return version, errors.New("'version' isn't an array")
	}
	if len(data) < 2 {
		return version, errors.New("'version' array is empty or too small")
	}

	val, found = record.Get("extra")
	if !found {
		return version, errors.New("couldn't find 'extra' version info")
	}
	extra, ok := val.(string)
	if !ok {
		return version, errors.New("'extra' value isn't a string")
	}

	// yolo for now
	version.Major = uint8(data[0].(int64))
	version.Minor = uint8(data[1].(int64))

	if len(data) > 2 {
		version.Patch = uint8(data[2].(int64))
	}
	version.Extra = extra

	return version, nil
}

// Construct and start a new routing table Monitor using the provided user,
// password, and uri as arguments to the underlying neo4j.Driver. Returns a
// pointer to the Monitor on success, or nil and an error on failure.
//
// Any additional hosts provided will be used as part of a custom address
// resolution function via the neo4j.Driver.
func NewMonitor(user, password, uri string, hosts ...string) (*Monitor, error) {
	infoChan := make(chan ClusterInfo, 1)
	haltChan := make(chan bool, 1)

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

	// TODO: check if in SINGLE, CORE, or READ_REPLICA mode
	// We can run `CALL dbms.listConfig('dbms.mode') YIELD value` and
	// check if we're clustered or not. Ideally, if not clustered, we
	// simplify the monitor considerably to just health checks and no
	// routing table.

	// Get the cluster members and ttl details
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	host := u.Host
	if u.Port() == "" {
		host = host + ":7687"
	}

	info, err := getClusterInfo(&driver, host)
	if err != nil {
		return nil, err
	}
	infoChan <- info

	monitor := Monitor{
		Info:    infoChan,
		halt:    haltChan,
		driver:  &driver,
		Version: version,
		Ttl:     info.Ttl, // since right now it's not dynamic
		Host:    host,
	}

	go func() {
		// TODO: configurable cluster info update frequency
		ticker := time.NewTicker(time.Second * 30)
		for {
			select {
			case <-ticker.C:
				info, err := getClusterInfo(monitor.driver, monitor.Host)
				if err != nil {
					// TODO: how do we handle faults???
					panic(err)
				}
				ticker.Reset(time.Second * 30)
				// empty the channel and put the new value in
				// this looks odd, but even though it's racy,
				// it should be racy in a safe way since it
				// doesn't matter if another go routine takes
				// the value first
				select {
				case <-infoChan:
				default:
				}
				select {
				case infoChan <- info:
				default:
					panic("monitor channel full")
				}
			case <-haltChan:
				ticker.Stop()
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

// Denormalize the routing table to make post-processing easier
const ROUTING_QUERY = `
CALL dbms.routing.getRoutingTable({address: $host}, $db)
  YIELD ttl, servers
UNWIND servers AS server
UNWIND server["addresses"] AS address
RETURN server["role"] AS role, address
`

// Given a neo4j.Transaction tx, collect the routing table maps for each of
// the databases in names. Since this should run in a transaction work function
// we return a generic interface{} on success, or nil and an error if failed.
//
// The true data type is a table struct, mapping providing arrays of readers,
// writers, and routers for the given db
func routingTableTx(tx neo4j.Transaction, host string, db string) (interface{}, error) {
	result, err := tx.Run(ROUTING_QUERY, map[string]interface{}{
		"db":   db,
		"host": host,
	})
	if err != nil {
		return nil, err
	}

	rows, err := result.Collect()
	if err != nil {
		return nil, err
	}

	// expected fields: [role, address]
	t := RoutingTable{
		Name:    db,
		Readers: []string{},
		Writers: []string{},
		Routers: []string{},
	}
	for _, row := range rows {
		val, found := row.Get("address")
		if !found {
			return nil, errors.New("missing address field in result")
		}
		addr, ok := val.(string)
		if !ok {
			panic("addr isn't a string!")
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
			t.Readers = append(t.Readers, addr)
		case "WRITE":
			t.Writers = append(t.Writers, addr)
		case "ROUTE":
			t.Routers = append(t.Routers, addr)
		default:
			return nil, errors.New("invalid role")
		}
	}

	return t, nil
}

// Using a pointer to a connected neo4j.Driver, orchestrate fetching the
// routing table for a given database while using the provided host
// routing context.
func getRoutingTable(driver *neo4j.Driver, db, host string) (RoutingTable, error) {
	session := (*driver).NewSession(neo4j.SessionConfig{})
	defer session.Close()

	result, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		return routingTableTx(tx, host, db)
	})
	if err != nil {
		return RoutingTable{}, err
	}
	table, ok := result.(RoutingTable)
	if !ok {
		panic("invalid return type: expected RoutingTable")
	}

	return table, nil
}

// Populate a ClusterInfo instance with critical details on our backend
func getClusterInfo(driver *neo4j.Driver, host string) (ClusterInfo, error) {
	session := (*driver).NewSession(neo4j.SessionConfig{
		DatabaseName: "system",
	})
	defer session.Close()

	// Inline TX function here for building the ClusterInfo on the fly
	result, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		info := ClusterInfo{CreatedAt: time.Now()}
		result, err := tx.Run("SHOW DATABASES", nil)
		if err != nil {
			return info, err
		}
		rows, err := result.Collect()
		if err != nil {
			return info, err
		}

		for _, row := range rows {
			val, found := row.Get("default")
			if !found {
				return info, errors.New("missing 'default' field")
			}
			defaultDb, ok := val.(bool)
			if !ok {
				return info, errors.New("default field isn't a boolean")
			}
			if defaultDb {
				val, found = row.Get("name")
				if !found {
					return info, errors.New("missing 'name' field")
				}
				name, ok := val.(string)
				if !ok {
					return info, errors.New("name field isn't a string")
				}
				info.DefaultDb = name
			}
		}

		return info, nil
	})
	if err != nil {
		return ClusterInfo{}, err
	}

	info, ok := result.(ClusterInfo)
	if !ok {
		panic("result isn't a ClusterInfo struct")
	}

	// For now get details for System db...
	rt, err := getRoutingTable(driver, "system", host)
	if err != nil {
		return info, err
	}
	hosts := map[string]bool{}
	for _, host := range append(rt.Readers, rt.Writers...) {
		hosts[host] = true
	}
	for host := range hosts {
		info.Hosts = append(info.Hosts, host)
	}
	return info, nil
}
