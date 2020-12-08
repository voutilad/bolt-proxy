package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"net"

	// debuggin'
	"net/http"
	_ "net/http/pprof"

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
func handleTx(client, server bolt.BoltConn, reset <-chan bool) {

	msgs := make(chan *bolt.Message, 5)
	done := make(chan bool, 1)
	go func() {
		defer func() {
			done <- true
		}()
		for {
			message, err := server.ReadMessage()
			if err != nil {
				if err == io.EOF {
					log.Println("EOF reading server")
					return
				}
				// panic(err)
				return
			}
			bolt.LogMessage("SERVER", message)
			msgs <- message
		}
	}()

	finished := false
	for !finished {
		select {
		case msg := <-msgs:
			client.WriteMessage(msg)
			if msg.T == bolt.GoodbyeMsg {
				finished = true
			}
		case <-reset:
			// Hang up on the server
			m := bolt.Message{
				T:    bolt.GoodbyeMsg,
				Data: []byte{0x0, 0x2, 0xb0, 0x2, 0x0, 0x0},
			}
			server.WriteMessage(&m)
			finished = true
		case <-done:
			log.Println("closing server tx handler")
			finished = true
		}
	}
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
		log.Printf("failed to read expected Hello: %v\n", err)
		return
	}
	bolt.LogMessage("CLIENT", hello)

	if hello.T != bolt.HelloMsg {
		log.Println("expected HelloMsg, got:", hello.T)
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

	// Some channels:
	//  - msgs: our Message queue for client Messages
	//  - reset: for signaling to our tx handler our client Reset
	msgs := make(chan *bolt.Message, 10)
	reset := make(chan bool, 1)
	done := make(chan bool, 1)

	go func() {
		defer func() {
			done <- true
		}()
		for {
			m, err := client.ReadMessage()
			if err != nil {
				if err == io.EOF {
					log.Println("EOF reading client")
					return
				}
				panic(err)
			}
			bolt.LogMessage("CLIENT", m)
			msgs <- m
		}
	}()

	// event loop...we enter in a state of HELLO being accepted
	startingTx := false
	manualTx := false
	for {
		// get the next event
		var msg *bolt.Message
		select {
		case m := <-msgs:
			msg = m
		case <-done:
			log.Println("closing client handler")
			return
		}

		switch msg.T {
		case bolt.BeginMsg:
			startingTx = true
			manualTx = true
		case bolt.RunMsg:
			if !manualTx {
				startingTx = true
			}
		case bolt.CommitMsg, bolt.RollbackMsg, bolt.ResetMsg:
			log.Println("client asked for", msg.T)
			manualTx = false
			startingTx = false
		}

		if startingTx {
			mode, _ := bolt.ValidateMode(msg.Data)
			log.Printf("[!!!]: NEW TRANSACTION, MODE = %s\n", mode)

			// get backend connection
			log.Println("trying to auth new server connection...")
			server, err = b.Authenticate(hello)
			if err != nil {
				log.Fatal(err)
			}

			go handleTx(client, server, reset)
			startingTx = false
		}

		err = server.WriteMessage(msg)
		if err != nil && msg.T != bolt.ResetMsg {
			log.Fatal(err)
		}
	}
}

func main() {
	var bindOn string
	var useTls bool
	var proxyTo string
	var username, password string

	flag.StringVar(&bindOn, "bind", "localhost:8888", "host:port to bind to")
	flag.BoolVar(&useTls, "tls", false, "use TLS (on listen side)")
	flag.StringVar(&proxyTo, "uri", "bolt://localhost:7687", "bolt uri for remote Neo4j")
	flag.StringVar(&username, "user", "neo4j", "Neo4j username")
	flag.StringVar(&password, "pass", "", "Neo4j password")
	flag.Parse()

	// ---------- pprof debugger
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

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
