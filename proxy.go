package main

import (
	"flag"
	"io"
	"log"
	"net"

	"github.com/voutilad/bolt-proxy/backend"
	"github.com/voutilad/bolt-proxy/bolt"
)

// "Splice" together a write-to net.Conn with a read-from net.Conn with
// the given name (for logging purposes). Reads data from r, parses into
// Bolt Messages, validates some state, and relayws the data to w.
//
// Before aborting, sends a message via the provided done channel.
func splice(w, r net.Conn, name string, done chan<- bool) {
	buf := make([]byte, 4*1024)
	finished := false

	for !finished {
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				log.Println("EOF detected for", name)
				break
			}
			log.Fatalf("Read failure on %s splice: %s\n", name, err.Error())
		}

		messages, _, err := bolt.Parse(buf[:n])
		if err != nil {
			panic(err)
		}

		// LOG EARLY FOR DEBUGGING
		bolt.LogMessages(name, messages)

		// try to inspect for messages
		for _, message := range messages {
			switch message.T {
			case bolt.GoodbyeMsg:
				finished = true
			case bolt.SuccessMsg:
				success, _, err := bolt.ParseTinyMap(message.Data[4:])
				if err != nil {
					panic(err)
				}
				val, found := success["bookmark"]
				if found {
					log.Printf("got bookmark: %s\n", val)
					finished = true
				}
			}
		}

		_, err = w.Write(buf[:n])
		if err != nil {
			log.Fatalf("Write failure on %s splice: %s\n", name, err.Error())
		}

	}
	done <- true
}

// Primary Client connection handler
func handleClient(client net.Conn, b *backend.Backend) {
	defer client.Close()
	buf := make([]byte, 512)

	// peek and check for magic and handshake
	_, err := client.Read(buf[:20])
	if err != nil {
		log.Printf("error peeking at client (%v): %v\n", client, err)
		return
	}

	// slice out and validate magic and handshake
	magic, handshake := buf[:4], buf[4:20]
	log.Printf("magic: %#v, handshake: %#v\n", magic, handshake)

	valid, err := bolt.ValidateMagic(magic)
	if !valid {
		log.Fatal(err)
	}

	// XXX hardcoded to bolt 4.2 for now
	hardcodedVersion := []byte{0x0, 0x0, 0x02, 0x04}
	match, err := bolt.ValidateHandshake(handshake, hardcodedVersion)
	if err != nil {
		log.Fatal(err)
	}
	_, err = client.Write(match)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("handshake complete: %#v\n", match)

	// intercept HELLO message for authentication
	n, err := client.Read(buf)
	if err != nil {
		log.Fatal(err)
	}
	messages, _, err := bolt.Parse(buf[:n])
	if err != nil {
		panic(err)
	}
	bolt.LogMessages("CLIENT", messages)

	// get backend connection
	log.Println("trying to auth...")
	server, err := b.Authenticate(buf[:n])
	if err != nil {
		log.Fatal(err)
	}

	// TODO: for now send our own Success Message
	_, err = client.Write([]byte{0x0, 0x2b, 0xb1, 0x70, 0xa2, 0x86, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x8b, 0x4e, 0x65, 0x6f, 0x34, 0x6a, 0x2f, 0x34, 0x2e,
		0x32, 0x2e, 0x30, 0x8d, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x86, 0x62, 0x6f, 0x6c, 0x74, 0x2d, 0x34, 0x00, 0x00})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("sent login Success to client")

	// ****************
	// zero our buf since it might have secrets
	for i, _ := range buf {
		buf[i] = 0
	}

	serverChan := make(chan bool)
	// loop over transactions
	for {
		// wait for the client to make the first move so we can react
		n, err = client.Read(buf)
		if err != nil {
			if err == io.EOF {
				log.Println("premature EOF detected?!")
				return
			}
			log.Fatal(err)
		}
		messages, _, err := bolt.Parse(buf[:n])
		if err != nil {
			panic(err)
		}
		bolt.LogMessages("CLIENT", messages)

		msg := messages[0]
		if msg.T == bolt.RunMsg || msg.T == bolt.BeginMsg {
			mode, _ := bolt.ValidateMode(msg.Data)
			log.Printf("[!!!]: NEW TRANSACTION, MODE = %s\n", mode)
			go splice(client, server, "SERVER", serverChan)
		}

		// flush message to server
		_, err = server.Write(buf[:n])
		if err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	var bindOn string
	var proxyTo string
	var username, password string

	flag.StringVar(&bindOn, "bind", "localhost:8888", "host:port to bind to")
	flag.StringVar(&proxyTo, "host", "alpine:7687", "remote neo4j host")
	flag.StringVar(&username, "user", "neo4j", "Neo4j username")
	flag.StringVar(&password, "pass", "", "Neo4j password")
	flag.Parse()

	// ---------- BACK END
	log.Println("Starting bolt-proxy back-end...")
	backend, err := backend.NewBackend(username, password, proxyTo)
	if err != nil {
		log.Fatal(err)
	}

	// ---------- FRONT END
	log.Println("Starting bolt-proxy front-end...")
	listener, err := net.Listen("tcp", bindOn)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening on %s\n", bindOn)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("error: %v\n", err)
		} else {
			log.Printf("got connection %v\n", conn)
			go handleClient(conn, backend)
		}
	}
}
