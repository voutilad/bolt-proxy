package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"net"
	"time"

	"github.com/voutilad/bolt-proxy/router"
)

// Try to split a buffer into Bolt messages based on double zero-byte delims
func logMessages(who string, data []byte) {
	// then, deal with N-number of bolt messages
	for i, buf := range bytes.Split(data, []byte{0x00, 0x00}) {
		if len(buf) > 0 {
			msgType := ParseBoltMsg(buf)
			if msgType != HelloMsg {
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
func handleClient(client net.Conn, config Config) {
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

	valid, err := ValidateMagic(magic)
	if !valid {
		log.Fatal(err)
	}

	// TODONEXT: MOVE THE OUTGOING CONNECTION TO AFTER TX START
	// open outgoing connection
	addr, err := net.ResolveTCPAddr("tcp", config.ProxyTo)
	if err != nil {
		log.Fatal(err)
	}

	server, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Close()

	// immediately relay the magic
	_, err = server.Write(buf[:20])
	if err != nil {
		log.Fatal(err)
	}
	log.Println("wrote magic to server")

	// validate server handshake response
	_, err = server.Read(buf[:4])
	log.Printf("SERVER Handshake: %#v\n", buf[:4])

	// server wrote to first 4 bytes, so look to for client match
	match, err := ValidateHandshake(handshake, buf[:4])
	if err != nil {
		log.Fatal(err)
	}
	_, err = client.Write(match)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("handshake complete: %#v\n", match)

	// intercept HELLO message for authentication
	// TODO: not sure what to do here...just intercept in future?
	n, err := client.Read(buf)
	if err != nil {
		log.Fatal(err)
	}
	logMessages("CLIENT", buf[:n])

	// XXXXX -- test proxy auth
	authHost, err := config.RoutingTable.NextReader()
	if err != nil {
		log.Fatal(err)
	}
	_, err = authClient(buf[:n], "tcp", authHost)
	if err != nil {
		log.Fatal(err)
	}
	// TODO: actually care about the result :-)
	// XXXXX

	_, err = server.Write(buf[:n])
	if err != nil {
		log.Fatalf("failed to write auth bytes to server: %s\n", err.Error())
	}
	// zero buf to drop credentials
	for i, _ := range buf {
		buf[i] = 0
	}
	// get server response to auth
	n, err = server.Read(buf)
	if err != nil {
		log.Fatalf("failed to read auth response from server: %s\n", err.Error())
	}
	logMessages("SERVER", buf[:n])
	_, err = client.Write(buf[:n])
	if err != nil {
		log.Fatal(err)
	}

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

	mode, err := ValidateMode(buf[:n])
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

type Config struct {
	Debug           bool
	BindOn, ProxyTo string
	RoutingTable    *router.RoutingTable
}

func main() {
	var debug bool
	var bindOn string
	var proxyTo string

	flag.BoolVar(&debug, "debug", false, "enable debug logging for traffic")
	flag.StringVar(&bindOn, "bind", "localhost:8888", "host/port to bind to")
	flag.StringVar(&proxyTo, "remote", "alpine:7687", "remote proxy target")
	flag.Parse()

	config := Config{debug, bindOn, proxyTo, router.NewRoutingTable()}

	// hardcode routing table for now
	config.RoutingTable.ReplaceWriters([]string{"alpine:7687"})
	config.RoutingTable.ReplaceReaders([]string{"alpine:7687"})

	log.Println("Starting bolt-proxy...")
	defer log.Println("finished.")

	listener, err := net.Listen("tcp", config.BindOn)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening on %s\n", config.BindOn)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("error: %v\n", err)
		} else {
			log.Printf("got connection %v\n", conn)
			go handleClient(conn, config)
		}
	}
}
