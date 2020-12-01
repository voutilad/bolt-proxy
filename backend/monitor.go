package backend

import (
	"errors"
	"log"
	"time"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type table struct {
	db      string
	ttl     time.Duration
	readers []string
	writers []string
}

// Denormalize the routing table
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

// Dump the list of databases with the first result being the default db
const SHOW_DATABASES = `
SHOW DATABASES YIELD name, default, currentStatus WHERE currentStatus = 'online'
RETURN name, default, currentStatus ORDER BY default DESC
`

// Use SHOW DATABASES to dump the current list of databases with the first
// database name being the default (based on the query logic)
func queryDbNames(s neo4j.Session) ([]string, error) {
	result, err := s.Run(SHOW_DATABASES, nil)
	if err != nil {
		return nil, err
	}
	rows, err := result.Collect()
	if err != nil {
		return nil, err
	}

	names := make([]string, len(rows))
	for i, row := range rows {
		val, found := row.Get("name")
		if !found {
			return nil, errors.New("couldn't find name field in result")
		}
		name, ok := val.(string)
		if !ok {
			panic("name isn't a string")
		}
		names[i] = name
	}
	log.Printf("found db names! %v\n", names)
	return names, nil
}

func queryRoutingTable(tx neo4j.Transaction, names []string) (interface{}, error) {
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

func UpdateRoutingTable(driver *neo4j.Driver, rt *RoutingTable) error {
	session := (*driver).NewSession(neo4j.SessionConfig{})
	defer session.Close()

	names, err := queryDbNames(session)
	if err != nil {
		log.Println("error querying database names")
		return err
	}

	result, err := session.ReadTransaction(func(tx neo4j.Transaction) (interface{}, error) {
		return queryRoutingTable(tx, names)
	})
	if err != nil {
		log.Println("error retrieving updated routing table")
		return err
	}

	tableMap, ok := result.(map[string]table)
	if !ok {
		panic(err)
	}

	log.Printf("[DEBUG] tableMap: %v\n", tableMap)

	// XXX: this is sloppy an inefficient...needs some rework
	for db, t := range tableMap {
		rt.SetRoutes(db, t.readers, t.writers)
		rt.SetTtl(t.ttl)
	}
	rt.defaultDb = names[0]

	return nil
}
