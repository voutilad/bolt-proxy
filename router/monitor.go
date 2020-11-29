package router

import (
	"errors"
	"log"

	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

func configurer(config *neo4j.Config) {
	config.UserAgent = "bolt-proxy/v0"
}

func queryRoutingTable(tx neo4j.Transaction) (interface{}, error) {
	result, err := tx.Run("SHOW DATABASES YIELD name, currentStatus WHERE currentStatus = 'online'", nil)
	if err != nil {
		return nil, err
	}
	rows, err := result.Collect()
	if err != nil {
		return nil, err
	}

	names := make([]string, len(rows))
	for i, row := range rows {
		name, found := row.Get("name")
		if !found {
			return nil, errors.New("couldn't find name field in result")
		}
		names[i] = name.(string)
	}
	log.Printf("names! %v\n", names)
	return names, nil
}

func Probe(uri, username, password string) error {
	auth := neo4j.BasicAuth(username, password, "")
	driver, err := neo4j.NewDriver(uri, auth, configurer)
	if err != nil {
		return err
	}

	session := driver.NewSession(neo4j.SessionConfig{
		DatabaseName: "system",
	})

	_, err = session.ReadTransaction(queryRoutingTable)
	return err
}
