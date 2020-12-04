package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"net"
	"time"

	"github.com/voutilad/bolt-proxy/backend"
	"github.com/voutilad/bolt-proxy/bolt"
)

// Try to split a buffer into Bolt messages based on double zero-byte delims
func logMessages(who string, data []byte) {
	// then, deal with N-number of bolt messages
	for i, buf := range bytes.Split(data, []byte{0x00, 0x00}) {
		if len(buf) > 0 {
			msgType := bolt.ParseBoltMsg(buf)
			if msgType != bolt.HelloMsg {
				log.Printf("[%s]{%d}: %s %#v\n", who, i, msgType, buf)
			} else {
				// don't log HELLO payload...it has credentials
				log.Printf("[%s]{%d}: %s <redacted>\n", who, i, msgType)
			}
		}
	}
}

// Take 2 net.Conn's (writer and reader) and copy all bytes from the
// reader to the writer using io.Copy(). This hopefully will leverage
// any OS-level optimizations like zero-copy data transfer.
//
// The provided provided name is used for identifying the splice in logging.
//
// If finished without error, send a "true" flag on the provided channel.
func splice(w, r net.Conn, name string, done chan<- bool) {
	buf := make([]byte, 4*1024)
	for {
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				log.Println("EOF detected for", name)
				break
			}
			log.Fatalf("Read failure on %s splice: %s\n", name, err.Error())
		}

		_, err = w.Write(buf[:n])
		if err != nil {
			log.Fatalf("Write failure on %s splice: %s\n", name, err.Error())
		}

		logMessages(name, buf[:n])
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
	logMessages("CLIENT", buf[:n])

	// get backend connection
	log.Println("trying to auth...")
	server, err := b.Authenticate(buf[:n])
	if err != nil {
		log.Fatal(err)
	}

	// TODO: send our own Success Message
	_, err = client.Write([]byte{0x0, 0x2b, 0xb1, 0x70, 0xa2, 0x86, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x8b, 0x4e, 0x65, 0x6f, 0x34, 0x6a, 0x2f, 0x34, 0x2e,
		0x32, 0x2e, 0x30, 0x8d, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x86, 0x62, 0x6f, 0x6c, 0x74, 0x2d, 0x34, 0x00, 0x00})
	if err != nil {
		log.Fatal(err)
	}
	log.Println("sent login Success to client")

	// wait for the client to make the first move so we can react
	n, err = client.Read(buf)
	if err != nil {
		if err == io.EOF {
			log.Println("premature EOF detected?!")
			return
		}
		log.Fatal(err)
	}
	logMessages("CLIENT", buf[:n])

	mode, err := bolt.ValidateMode(buf[:n])
	log.Printf("XXX: TRANSACTION MODE DETECTED = %s\n", mode)

	// flush the buffer to the server before splicing
	_, err = server.Write(buf[:n])
	if err != nil {
		log.Fatal(err)
	}

	// set up some channels, splice, and go!
	clientChan := make(chan bool, 1)
	serverChan := make(chan bool, 1)

	go splice(server, client, "CLIENT", clientChan)
	go splice(client, server, "SERVER", serverChan)

	for {
		select {
		case <-clientChan:
			log.Println("Client pipe closed.")
		case <-serverChan:
			log.Println("Server pipe closed.")
		case <-time.After(5 * time.Minute):
			log.Println("no data received in 5 mins")
		}
	}

	log.Println("Client dead.")
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
