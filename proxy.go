package main

import (
	"bytes"
	"flag"
	"io"
	"log"
	"net"
	"time"

	// debuggin'
	"net/http"
	_ "net/http/pprof"

	"github.com/voutilad/bolt-proxy/backend"
	"github.com/voutilad/bolt-proxy/bolt"

	"github.com/gobwas/ws"
)

// XXX temp monitor
var ADD_HANDLER chan int = make(chan int)
var DEL_HANDLER chan int = make(chan int)

// "Splice" together a write-to BoltConn to a read-from BoltConn with
// the given name (for logging purposes). Reads Messages from r,
// validates some state, and relays the Messages to w.
//
// Before aborting, sends a bool via the provided done channel to
// signal any waiting go routine.
func handleTx(client, server bolt.BoltConn, ack chan<- bool, reset, halt <-chan bool) {
	finished := false
	resetting := false

	success := []byte{0x0, 0x3, 0xb1, 0x70, 0xa0, 0x0, 0x0}

	ADD_HANDLER <- 1
	defer func() {
		DEL_HANDLER <- 1
	}()

	for !finished {
		select {
		case msg, ok := <-server.R():
			if ok {
				bolt.LogMessage("P<-S", msg)
				err := client.WriteMessage(msg)
				if err != nil {
					panic(err)
				}
				bolt.LogMessage("C<-P", msg)
				if msg.T == bolt.GoodbyeMsg {
					finished = true
				} else if resetting && bytes.Equal(success, msg.Data) {
					log.Println("tx handler saw SUCCESS and was asked to reset")
					finished = true
				}
			} else {
				log.Println("potential server hangup")
				finished = true
			}
		case <-reset:
			resetting = true
		case <-halt:
			finished = true
		case <-time.After(5 * time.Minute):
			log.Println("timeout reading server!")
			finished = true
		}
	}
	log.Println("!! txhandler stopped")

	select {
	case ack <- true:
		log.Println("tx handler stop ACK sent")
	default:
		log.Println("couldn't put value in ack channel?!")
	}
}

// Identify if a new connection is valid Bolt or Bolt-on-Websockets.
// If so, pass it to the proper handler. Otherwise, close the connection.
func handleClient(conn net.Conn, b *backend.Backend) {
	defer func() {
		log.Println("closing client connection", conn)
		conn.Close()
	}()

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

		// XXX hardcoded to bolt 4.1 for now
		hardcodedVersion := []byte{0x0, 0x0, 0x01, 0x04}
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

		frame := ws.NewBinaryFrame(match)
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
	var hello *bolt.Message
	select {
	case msg, ok := <-client.R():
		if !ok {
			log.Println("failed to read expected Hello from client")
			return
		}
		hello = msg
	case <-time.After(30 * time.Second):
		log.Println("timed out waiting for client to auth")
		return
	}
	bolt.LogMessage("C->P", hello)

	if hello.T != bolt.HelloMsg {
		log.Println("expected HelloMsg, got:", hello.T)
		return
	}

	// get backend connection
	log.Println("trying to authenticate with backend...")
	pool, err := b.Authenticate(hello)
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
	bolt.LogMessage("P->C", &success)
	err = client.WriteMessage(&success)
	if err != nil {
		log.Fatal(err)
	}

	// Some channels:
	//  - reset: for signaling to our tx handler our client Reset
	reset := make(chan bool)
	ack := make(chan bool)

	// Event loop: we enter in a state of HELLO being accepted
	startingTx := false
	manualTx := false
	resetting := false
	done := make(chan bool)
	var server bolt.BoltConn
	for {
		// get the next event
		var msg *bolt.Message
		select {
		case m, ok := <-client.R():
			if ok {
				msg = m
				bolt.LogMessage("C->P", msg)
			} else {
				log.Println("potential client hangup")
				select {
				case done <- true:
					log.Println("client hungup, asking tx to stop")
				default:
					log.Println("failed to send done message to tx handler")
				}
				return
			}
		case <-time.After(5 * time.Minute):
			log.Println("client idle timeout of 5 mins")
			continue
		}

		if msg == nil {
			// happens during websocket timeout?
			panic("msg is nil")
		}

		switch msg.T {
		case bolt.BeginMsg:
			startingTx = true
			manualTx = true
		case bolt.RunMsg:
			if !manualTx {
				startingTx = true
			}
		case bolt.CommitMsg, bolt.RollbackMsg:
			log.Println("client asked for", msg.T)
			manualTx = false
			startingTx = false
			/*		case bolt.ResetMsg:
					log.Println("resetting")
					manualTx = false
					startingTx = false
					resetting = true
					select {
					case reset <- true:
						log.Println("requested reset")
					default:
						panic("reset queue full!?")
					}*/
		}

		if startingTx {
			mode, _ := bolt.ValidateMode(msg.Data)
			rt := b.RoutingTable()

			db := rt.DefaultDb

			// get the db name, if any. otherwise, use default
			m, _, err := bolt.ParseTinyMap(msg.Data[4:])
			if err == nil {
				val, found := m["db"]
				if found {
					ok := false
					db, ok = val.(string)
					if !ok {
						panic("db name wasn't a string?!")
					}
				}
			}

			// just choose the first one for now...something simple
			var hosts []string
			if mode == bolt.ReadMode {
				hosts, err = rt.ReadersFor(db)
			} else {
				hosts, err = rt.WritersFor(db)
			}
			if err != nil {
				log.Printf("couldn't find host for '%s' in routing table", db)
			}

			// at some point we have to figure out how to pick a host
			if len(hosts) < 1 {
				log.Println("empty hosts lists for database", db)
				return
			}
			host := hosts[0]

			// are we already using a host?
			// if so try to stop the tx handler
			if server != nil {
				select {
				case done <- true:
					log.Println("asking current tx handler to halt")
					select {
					case <-ack:
						log.Println("tx handler ack'd stop")
					case <-time.After(15 * time.Second):
						log.Println("!!! timeout waiting for ack from tx handler")
					}
				default:
					log.Println("!!! couldn't send done to tx handler!")
				}
			}

			// grab our host from our local pool
			ok := false
			server, ok = pool[host]
			if !ok {
				log.Println("no established connection for host", host)
				return
			}

			log.Printf("grabbed conn for %s-access to db %s on host %s\n", mode, db, host)
			go handleTx(client, server, ack, reset, done)
			startingTx = false
		}

		// TODO: better connection state tracking?
		if server != nil {
			log.Printf(">>> writing %d byte message of type %s\n", len(msg.Data), msg.T)
			err = server.WriteMessage(msg)
			if err != nil {
				panic(err)
			}
			bolt.LogMessage("P->S", msg)
		}

		if resetting {
			// wait here for ack
			select {
			case <-ack:
				log.Println("server acked reset")
			case <-time.After(30 * time.Second):
				log.Println("no ack in 30s?!")
				return
			}
			resetting = false
		}
	}
}

func main() {
	var bindOn string
	var proxyTo string
	var username, password string

	flag.StringVar(&bindOn, "bind", "localhost:8888", "host:port to bind to")
	flag.StringVar(&proxyTo, "uri", "bolt://localhost:7687", "bolt uri for remote Neo4j")
	flag.StringVar(&username, "user", "neo4j", "Neo4j username")
	flag.StringVar(&password, "pass", "", "Neo4j password")
	flag.Parse()

	// ---------- pprof debugger
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// ------- handler reporter
	go func() {
		cnt := 0
		for {
			select {
			case i := <-ADD_HANDLER:
				cnt = cnt + i
				log.Println("[REPORT: tx handler count =", cnt)
			case i := <-DEL_HANDLER:
				cnt = cnt - i
				log.Println("[REPORT: tx handler count =", cnt)
			case <-time.After(1 * time.Minute):
				// timeout
			}
		}
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
