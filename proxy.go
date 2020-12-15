package main

import (
	"bytes"
	"crypto/tls"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"time"

	// debuggin' -- used for runtime profiling/debugging
	"net/http"
	_ "net/http/pprof"

	"github.com/voutilad/bolt-proxy/backend"
	"github.com/voutilad/bolt-proxy/bolt"

	"github.com/gobwas/ws"
)

// A basic idle timeout duration for now
const MAX_IDLE_MINS int = 30

// Primary Transaction server-side event handler, collecting Messages from
// the backend Bolt server and writing them to the given client.
//
// Since this should be running async to process server Messages as they
// arrive, two channels are provided for signaling:
//
//  ack: used for letting this handler to signal that it's completed and
//       stopping execution, basically a way to confirm the requested halt
//
// halt: used by an external routine to request this handler to cleanly
//       stop execution
//
func handleTx(client, server bolt.BoltConn, ack chan<- bool, halt <-chan bool) {
	finished := false

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

				// if know the server side is saying goodbye,
				// we abort the loop
				if msg.T == bolt.GoodbyeMsg {
					finished = true
				}
			} else {
				log.Println("potential server hangup")
				finished = true
			}

		case <-halt:
			finished = true

		case <-time.After(time.Duration(MAX_IDLE_MINS) * time.Minute):
			log.Println("timeout reading server!")
			finished = true
		}
	}

	select {
	case ack <- true:
		log.Println("tx handler stop ACK sent")
	default:
		log.Println("couldn't put value in ack channel?!")
	}
}

// Identify if a new connection is valid Bolt or Bolt-over-Websocket
// connection based on handshakes.
//
// If so, wrap the incoming conn into a BoltConn and pass it off to
// a client handler
func handleClient(conn net.Conn, b *backend.Backend) {
	defer func() {
		log.Printf("closing client connection from %s\n",
			conn.RemoteAddr())
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
		// First case: we have a direct bolt client connection
		n, err := conn.Read(buf[:20])
		if err != nil {
			log.Println("error peeking at connection from", conn.RemoteAddr())
			return
		}

		// Make sure we try to use the version we're using the best
		// version based on the backend server
		serverVersion := b.Version().Bytes()
		clientVersion, err := bolt.ValidateHandshake(buf[:n], serverVersion)
		if err != nil {
			log.Fatal(err)
		}
		_, err = conn.Write(clientVersion)
		if err != nil {
			log.Fatal(err)
		}

		// regular bolt
		handleBoltConn(bolt.NewDirectConn(conn), clientVersion, b)

	} else if bytes.Equal(buf[:4], []byte{0x47, 0x45, 0x54, 0x20}) {
		// Second case, we have an HTTP connection that might just
		// be a WebSocket upgrade
		n, _ = conn.Read(buf[4:])

		// Build something implementing the io.ReadWriter interface
		// to pass to the upgrader routine
		iobuf := bytes.NewBuffer(buf[:n+4])
		_, err := ws.Upgrade(iobuf)
		if err != nil {
			log.Printf("failed to upgrade websocket client %s: %s\n",
				conn.RemoteAddr(), err)
			return
		}
		// Relay the upgrade response
		_, err = io.Copy(conn, iobuf)
		if err != nil {
			log.Printf("failed to copy upgrade to client %s\n",
				conn.RemoteAddr())
			return
		}

		// After upgrade, we should get a WebSocket message with header
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

		// We expect we can now do the initial Bolt handshake
		magic, handshake := buf[:4], buf[4:20] // blaze it
		valid, err := bolt.ValidateMagic(magic)
		if !valid {
			log.Fatal(err)
		}

		// negotiate client & server side bolt versions
		serverVersion := b.Version().Bytes()
		clientVersion, err := bolt.ValidateHandshake(handshake, serverVersion)
		if err != nil {
			log.Fatal(err)
		}

		// Complete Bolt handshake via WebSocket frame
		frame := ws.NewBinaryFrame(clientVersion)
		if err = ws.WriteFrame(conn, frame); err != nil {
			log.Fatal(err)
		}

		// Let there be Bolt-via-WebSockets!
		handleBoltConn(bolt.NewWsConn(conn), clientVersion, b)
	} else {
		// not bolt, not http...something else?
		log.Printf("client %s is speaking gibberish: %#v\n",
			conn.RemoteAddr(), buf[:4])
	}
}

// Primary Transaction client-side event handler, collecting Messages from
// the Bolt client and finding ways to switch them to the proper backend.
//
// The event loop...
//
// TOOD: this logic should be split out between the authentication and the
// event loop. For now, this does both.
func handleBoltConn(client bolt.BoltConn, clientVersion []byte, b *backend.Backend) {
	// Intercept HELLO message for authentication and hold onto it
	// for use in backend authentication
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

	// Time to begin the client-side event loop!
	startingTx := false
	manualTx := false
	halt := make(chan bool, 1)
	ack := make(chan bool, 1)

	var server bolt.BoltConn
	for {
		var msg *bolt.Message
		select {
		case m, ok := <-client.R():
			if ok {
				msg = m
				bolt.LogMessage("C->P", msg)
			} else {
				log.Println("potential client hangup")
				select {
				case halt <- true:
					log.Println("client hungup, asking tx to halt")
				default:
					log.Println("failed to send halt message to tx handler")
				}
				return
			}
		case <-time.After(time.Duration(MAX_IDLE_MINS) * time.Minute):
			log.Println("client idle timeout")
			return
		}

		if msg == nil {
			// happens during websocket timeout?
			panic("msg is nil")
		}

		// Inspect the client's message to discern transaction state
		// We need to figure out if a transaction is starting and
		// what kind of transaction (manual, auto, etc.) it might be.
		switch msg.T {
		case bolt.BeginMsg:
			startingTx = true
			manualTx = true
		case bolt.RunMsg:
			if !manualTx {
				startingTx = true
			}
		case bolt.CommitMsg, bolt.RollbackMsg:
			manualTx = false
			startingTx = false
		}

		// XXX: This is a mess, but if we're starting a new transaction
		// we need to find a new connection to switch to
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

			// Just choose the first one for now...something simple
			var hosts []string
			if mode == bolt.ReadMode {
				hosts, err = rt.ReadersFor(db)
			} else {
				hosts, err = rt.WritersFor(db)
			}
			if err != nil {
				log.Printf("couldn't find host for '%s' in routing table", db)
			}

			if len(hosts) < 1 {
				log.Println("empty hosts lists for database", db)
				// TODO: return FailureMsg???
				return
			}
			host := hosts[0]

			// Are we already using a host? If so try to stop the
			// current tx handler before we create a new one
			if server != nil {
				select {
				case halt <- true:
					log.Println("...asking current tx handler to halt")
					select {
					case <-ack:
						log.Println("tx handler ack'd stop")
					case <-time.After(5 * time.Second):
						log.Println("!!! timeout waiting for ack from tx handler")
					}
				default:
					// this shouldn't happen!
					panic("couldn't send halt to tx handler!")
				}
			}

			// Grab our host from our local pool
			ok := false
			server, ok = pool[host]
			if !ok {
				log.Println("no established connection for host", host)
				return
			}
			log.Printf("grabbed conn for %s-access to db %s on host %s\n", mode, db, host)

			// TODO: refactor channel handling...probably have handleTx() return new ones
			// instead of reusing the same ones. If we don't create new ones, there could
			// be lingering halt/ack messages. :-(
			halt = make(chan bool, 1)
			ack = make(chan bool, 1)

			// kick off a new tx handler routine
			go handleTx(client, server, ack, halt)
			startingTx = false
		}

		// TODO: this connected/not-connected handling looks messy
		if server != nil {
			err = server.WriteMessage(msg)
			if err != nil {
				// TODO: figure out best way to handle failed writes
				panic(err)
			}
			bolt.LogMessage("P->S", msg)
		} else {
			// we have no connection since there's no tx...
			// handle only specific, simple messages
			switch msg.T {
			case bolt.ResetMsg:
				// XXX: Neo4j Desktop does this when defining a
				// remote dbms connection.
				// simply send empty success message
				client.WriteMessage(&bolt.Message{
					T: bolt.SuccessMsg,
					Data: []byte{
						0x00, 0x03,
						0xb1, 0x70,
						0xa0,
						0x00, 0x00,
					},
				})
			case bolt.GoodbyeMsg:
				// bye!
				return
			}
		}
	}
}

const (
	DEFAULT_BIND string = "localhost:8888"
	DEFAULT_URI  string = "bolt://localhost:7687"
	DEFAULT_USER string = "neo4j"
)

func main() {
	var (
		bindOn             string
		proxyTo            string
		username, password string
		certFile, keyFile  string
	)

	bindOn, found := os.LookupEnv("BOLT_PROXY_BIND")
	if !found {
		bindOn = DEFAULT_BIND
	}
	proxyTo, found = os.LookupEnv("BOLT_PROXY_URI")
	if !found {
		proxyTo = DEFAULT_URI
	}
	username, found = os.LookupEnv("BOLT_PROXY_USER")
	if !found {
		username = DEFAULT_USER
	}
	password = os.Getenv("BOLT_PROXY_PASSWORD")
	certFile = os.Getenv("BOLT_PROXY_CERT")
	keyFile = os.Getenv("BOLT_PROXY_KEY")

	// to keep it easy, let the defaults be populated by the env vars
	flag.StringVar(&bindOn, "bind", bindOn, "host:port to bind to")
	flag.StringVar(&proxyTo, "uri", proxyTo, "bolt uri for remote Neo4j")
	flag.StringVar(&username, "user", username, "Neo4j username")
	flag.StringVar(&password, "pass", password, "Neo4j password")
	flag.StringVar(&certFile, "cert", certFile, "x509 certificate")
	flag.StringVar(&keyFile, "key", keyFile, "x509 private key")
	flag.Parse()

	// We log to stdout because our parents raised us right
	log.SetOutput(os.Stdout)

	// ---------- pprof debugger
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// ---------- BACK END
	log.Println("Starting bolt-proxy backend...")
	backend, err := backend.NewBackend(username, password, proxyTo)
	if err != nil {
		log.Fatal(err)
	}

	// ---------- FRONT END
	log.Println("Starting bolt-proxy frontend...")
	var listener net.Listener
	if certFile == "" || keyFile == "" {
		// non-tls
		listener, err = net.Listen("tcp", bindOn)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Listening on %s\n", bindOn)
	} else {
		// tls
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatal(err)
		}
		config := &tls.Config{Certificates: []tls.Certificate{cert}}
		listener, err = tls.Listen("tcp", bindOn, config)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Listening for TLS connections on %s\n", bindOn)
	}

	// ---------- Event Loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("error: %v\n", err)
		} else {
			go handleClient(conn, backend)
		}
	}
}
