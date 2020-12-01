package main

import (
	"log"
	"time"

	"github.com/voutilad/bolt-proxy/backend"
)

func main() {

	log.Println(time.Duration(300) * time.Second)
	config := backend.Config{[]string{"alpine:7687"}, "neo4j", "password", time.Duration(5) * time.Minute}

	b := backend.NewBackend(config)

	timer := time.NewTicker(5 * time.Second)
	for {
		err := backend.UpdateRoutingTable(b.Driver, b.RoutingTable)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(b.RoutingTable)
		<-timer.C
	}
}
