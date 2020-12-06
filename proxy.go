package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/voutilad/bolt-proxy/backend"
	"github.com/voutilad/bolt-proxy/bolt"

	"github.com/gobwas/ws"
)

// "Splice" together a write-to BoltConn to a read-from BoltConn with
// the given name (for logging purposes). Reads Messages from r,
// validates some state, and relays the Messages to w.
//
// Before aborting, sends a bool via the provided done channel to
// signal any waiting go routine.
func splice(w, r bolt.BoltConn, name string, done chan<- bool) {
	//buf := make([]byte, 4*1024)
	finished := false

	for !finished {
		message, err := r.ReadMessage()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		bolt.LogMessage(name, message)

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

		w.WriteMessage(message)
	}
	done <- true
}

// Identify if a new connection is valid Bolt or Bolt-on-Websockets.
// If so, pass it to the proper handler. Otherwise, close the connection.
func handleConn(conn net.Conn, b *backend.Backend) {
	defer conn.Close()

	// XXX why 1024? I've observed long user-agents that make this
	// pass the 512 mark easily, so let's be safe and go a full 1kb
	buf := make([]byte, 1024)

	n, err := conn.Read(buf[:4])
	if err != nil || n != 4 {
		log.Println("bad connection from", conn.RemoteAddr())
		return
	}

	if bytes.Equal(buf[:4], []byte{0x60, 0x60, 0xb0, 0x17}) {
		// regular bolt
		handleClient(conn, b)
	} else if bytes.Equal(buf[:4], []byte{0x47, 0x45, 0x54, 0x20}) {
		// "GET ", so websocket? :-(
		n, _ = conn.Read(buf[4:])

		// build a ReadWriter to pass to the upgrader
		iobuf := bytes.NewBuffer(buf[:n+4])
		_, err := ws.Upgrade(iobuf)
		if err != nil {
			log.Printf("failed to upgrade websocket client %s: %s\n",
				conn.RemoteAddr(), err)
			return
		}
		// relay the upgrade response
		_, err = io.Copy(conn, iobuf)
		if err != nil {
			log.Printf("failed to copy upgrade to client %s\n",
				conn.RemoteAddr())
			return
		}

		// TODO: finish handling logic, for now try to read a header
		// and initial payload
		header, err := ws.ReadHeader(conn)
		if err != nil {
			log.Printf("failed to read ws header from client %s: %s\n",
				conn.RemoteAddr(), err)
			return
		}
		log.Printf("XXX [ws] got header: %v\n", header)
		n, err := conn.Read(buf[:header.Length])
		if err != nil {
			log.Printf("failed to read payload from client %s\n",
				conn.RemoteAddr())
			return
		}
		if header.Masked {
			log.Println("unmasking payload")
			ws.Cipher(buf[:n], header.Mask, 0)
			header.Masked = false
		}
		log.Printf("GOT WS PAYLOAD: %#v\n", buf[:n])

	} else {
		log.Printf("client %s is speaking gibberish: %#v\n",
			conn.RemoteAddr(), buf[:4])
	}
}

// Primary Client connection handler
func handleClient(client net.Conn, b *backend.Backend) {
	buf := make([]byte, 1024)

	// read bytes for handshake message
	n, err := client.Read(buf[:20])
	if err != nil {
		log.Printf("error peeking at client (%v): %v\n", client, err)
		return
	}

	// XXX hardcoded to bolt 4.2 for now
	hardcodedVersion := []byte{0x0, 0x0, 0x02, 0x04}
	match, err := bolt.ValidateHandshake(buf[:n], hardcodedVersion)
	if err != nil {
		log.Fatal(err)
	}
	_, err = client.Write(match)
	if err != nil {
		log.Fatal(err)
	}

	// TODO: Keep pulling this BoltConn upwards!!!
	client2 := bolt.NewDirectConn(client)

	// intercept HELLO message for authentication
	hello, err := client2.ReadMessage()
	if err != nil {
		log.Fatal(err)
	}
	bolt.LogMessage("CLIENT", hello)

	if hello.T != bolt.HelloMsg {
		panic(fmt.Sprint("unexpected bolt message:", hello.T))
	}

	// get backend connection
	log.Println("trying to auth...")
	server, err := b.Authenticate(hello)
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
		message, err := client2.ReadMessage()
		if err != nil {
			if err == io.EOF {
				log.Println("premature EOF detected?!")
				return
			}
			log.Fatal(err)
		}
		bolt.LogMessage("CLIENT", message)

		if message.T == bolt.RunMsg || message.T == bolt.BeginMsg {
			mode, _ := bolt.ValidateMode(message.Data)
			log.Printf("[!!!]: NEW TRANSACTION, MODE = %s\n", mode)
			go splice(client2, server, "SERVER", serverChan)
		}

		err = server.WriteMessage(message)
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
			go handleConn(conn, backend)
		}
	}
}
