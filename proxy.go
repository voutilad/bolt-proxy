package main

import (
	"bytes"
	"flag"
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
func splice(w, r bolt.BoltConn, name string, reset chan bool, done chan<- bool) {
	//buf := make([]byte, 4*1024)
	finished := false

	for !finished {
		message, err := r.ReadMessage()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Println("PROBLEMS READING ", name)
			panic(err)
		}
		bolt.LogMessage(name, message)

		switch message.T {
		case bolt.GoodbyeMsg:
			finished = true
		case bolt.SuccessMsg:
			success, _, err := bolt.ParseTinyMap(message.Data[4:])
			if err == nil {
				val, found := success["bookmark"]
				if found {
					log.Printf("got bookmark: %s\n", val)
					finished = true
				}
			}
		}

		w.WriteMessage(message)
	}
	done <- true
}

// Identify if a new connection is valid Bolt or Bolt-on-Websockets.
// If so, pass it to the proper handler. Otherwise, close the connection.
func handleClient(conn net.Conn, b *backend.Backend) {
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
		// read bytes for handshake message
		n, err := conn.Read(buf[:20])
		if err != nil {
			log.Println("error peeking at connection from", conn.RemoteAddr())
			return
		}

		// XXX hardcoded to bolt 4.2 for now
		hardcodedVersion := []byte{0x0, 0x0, 0x02, 0x04}
		match, err := bolt.ValidateHandshake(buf[:n], hardcodedVersion)
		if err != nil {
			log.Fatal(err)
		}
		_, err = conn.Write(match)
		if err != nil {
			log.Fatal(err)
		}

		// regular bolt
		handleBoltConn(bolt.NewDirectConn(conn), b)

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
		n, err := conn.Read(buf[:header.Length])
		if err != nil {
			log.Printf("failed to read payload from client %s\n",
				conn.RemoteAddr())
			return
		}
		if header.Masked {
			ws.Cipher(buf[:n], header.Mask, 0)
		}
		// log.Printf("GOT WS PAYLOAD: %#v\n", buf[:n])

		// we expect we can now do the initial Bolt handshake
		magic, handshake := buf[:4], buf[4:20] // blaze it
		valid, err := bolt.ValidateMagic(magic)
		if !valid {
			log.Fatal(err)
		}

		// Browser uses an older 4.1 driver?!
		hardcodedVersion := []byte{0x0, 0x0, 0x1, 0x4}
		match, err := bolt.ValidateHandshake(handshake, hardcodedVersion)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("XXX sending match back: %#v\n", match)
		frame := ws.NewBinaryFrame(match)

		log.Printf("XXX sending Frame: %#v\n", frame)
		if err = ws.WriteFrame(conn, frame); err != nil {
			log.Fatal(err)
		}

		// websocket bolt
		handleBoltConn(bolt.NewWsConn(conn), b)
	} else {
		log.Printf("client %s is speaking gibberish: %#v\n",
			conn.RemoteAddr(), buf[:4])
	}
}

// Primary Client connection handler
func handleBoltConn(client bolt.BoltConn, b *backend.Backend) {
	// intercept HELLO message for authentication
	hello, err := client.ReadMessage()
	if err != nil {
		log.Fatal(err)
	}
	bolt.LogMessage("CLIENT", hello)

	if hello.T != bolt.HelloMsg {
		log.Println("unexpected bolt message:", hello.T)
		return
	}

	// get backend connection
	log.Println("trying to auth...")
	server, err := b.Authenticate(hello)
	if err != nil {
		log.Fatal(err)
	}

	// TODO: Replace hardcoded Success message with dynamic one
	success := bolt.Message{
		T: bolt.SuccessMsg,
		Data: []byte{
			0x0, 0x2b, 0xb1, 0x70,
			0xa2,
			0x86, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
			0x8b, 0x4e, 0x65, 0x6f, 0x34, 0x6a, 0x2f, 0x34, 0x2e,
			0x32, 0x2e, 0x30,
			0x8d, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64,
			0x86, 0x62, 0x6f, 0x6c, 0x74, 0x2d, 0x34,
			0x00, 0x00}}
	err = client.WriteMessage(&success)
	if err != nil {
		log.Fatal(err)
	}

	doneChan := make(chan bool, 1)
	resetChan := make(chan bool, 1)
	startingTx := false
	var lastMessageType bolt.Type
	emptySuccess := bolt.Message{
		T:    bolt.SuccessMsg,
		Data: []byte{0x0, 0x3, 0xb1, 0x70, 0xa0, 0x0, 0x0},
	}
	// loop over transactions
	for {
		message, err := client.ReadMessage()
		if err != nil {
			if err == io.EOF {
				log.Println("premature EOF detected?!")
				return
			}
			log.Fatal(err)
		}
		bolt.LogMessage("CLIENT", message)

		switch message.T {
		case bolt.BeginMsg:
			startingTx = true
		case bolt.RunMsg:
			if lastMessageType != bolt.BeginMsg {
				startingTx = true
			}
		case bolt.ResetMsg:
			log.Println("asked to reset by client")
			select {
			case resetChan <- true:
				log.Println("told server to reset")
				client.WriteMessage(&emptySuccess)
				continue
			default:
			}
		}

		if startingTx {
			mode, _ := bolt.ValidateMode(message.Data)
			log.Printf("[!!!]: NEW TRANSACTION, MODE = %s\n", mode)
			go splice(client, server, "SERVER", resetChan, doneChan)
			startingTx = false
		}

		err = server.WriteMessage(message)
		if err != nil {
			log.Fatal(err)
		}

		lastMessageType = message.T
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
			go handleClient(conn, backend)
		}
	}
}
