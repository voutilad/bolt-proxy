package main

import (
	"log"
	"time"

	"github.com/voutilad/bolt-proxy/backend"
)

func main() {
	b, err := backend.NewBackend("neo4j", "password", "alpine:7687")
	if err != nil {
		log.Fatal(err)
	}
	var rt *backend.RoutingTable
	for {
		rt = b.RoutingTable()
		log.Printf("got routing table: %s\n", rt)
		time.Sleep(2 * time.Minute)
	}
}
