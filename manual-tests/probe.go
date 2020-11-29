package main

import (
	"log"

	"github.com/voutilad/bolt-proxy/router"
)

func main() {
	err := router.Probe("bolt://alpine:7687", "neo4j", "password")
	if err != nil {
		log.Fatal(err)
	}
}
