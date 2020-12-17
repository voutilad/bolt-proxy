package main

import (
	"bytes"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"time"

	// debuggin' -- used for runtime profiling/debugging
	"net/http"
	_ "net/http/pprof"

	"github.com/voutilad/bolt-proxy/backend"
	"github.com/voutilad/bolt-proxy/bolt"
	"github.com/voutilad/bolt-proxy/health"

	"github.com/gobwas/ws"
)

const (
	// A basic idle timeout duration for now
	MAX_IDLE_MINS int = 30
	// max bytes to display in logs in debug mode
	MAX_BYTES int = 4096
)

var (
	debug *log.Logger
	info  *log.Logger
	warn  *log.Logger
)

// Crude logging routine for helping debug bolt Messages. Tries not to clutter
// output too much due to large messages while trying to deliniate who logged
// the message.
func logMessage(who string, msg *bolt.Message) {
	end := MAX_BYTES
	suffix := fmt.Sprintf("...+%d bytes", len(msg.Data))
	if len(msg.Data) < MAX_BYTES {
		end = len(msg.Data)
		suffix = ""
	}

	switch msg.T {
	case bolt.HelloMsg:
		// make sure we don't print the secrets in a Hello!
		debug.Printf("[%s] <%s>: %#v\n%s\n", who, msg.T, msg.Data, msg.Data)
	case bolt.BeginMsg, bolt.FailureMsg:
		debug.Printf("[%s] <%s>: %#v\n%s\n", who, msg.T, msg.Data[:end], msg.Data)
	default:
		debug.Printf("[%s] <%s>: %#v%s\n", who, msg.T, msg.Data[:end], suffix)
	}
}

func logMessages(who string, messages []*bolt.Message) {
	for _, msg := range messages {
		logMessage(who, msg)
	}
}

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
				logMessage("P<-S", msg)
				err := client.WriteMessage(msg)
				if err != nil {
					panic(err)
				}
				logMessage("C<-P", msg)

				// if know the server side is saying goodbye,
				// we abort the loop
				if msg.T == bolt.GoodbyeMsg {
					finished = true
				}
			} else {
				debug.Println("potential server hangup")
				finished = true
			}

		case <-halt:
			finished = true

		case <-time.After(time.Duration(MAX_IDLE_MINS) * time.Minute):
			warn.Println("timeout reading server!")
			finished = true
		}
	}

	select {
	case ack <- true:
		debug.Println("tx handler stop ACK sent")
	default:
		warn.Println("couldn't put value in ack channel?!")
	}
}

// Identify if a new connection is valid Bolt or Bolt-over-Websocket
// connection based on handshakes.
//
// If so, wrap the incoming conn into a BoltConn and pass it off to
// a client handler
func handleClient(conn net.Conn, b *backend.Backend) {
	defer func() {
		debug.Printf("closing client connection from %s\n",
			conn.RemoteAddr())
		conn.Close()
	}()

	// XXX why 1024? I've observed long user-agents that make this
	// pass the 512 mark easily, so let's be safe and go a full 1kb
	buf := make([]byte, 1024)

	n, err := conn.Read(buf[:4])
	if err != nil || n != 4 {
		warn.Println("bad connection from", conn.RemoteAddr())
		return
	}

	if bytes.Equal(buf[:4], []byte{0x60, 0x60, 0xb0, 0x17}) {
		// First case: we have a direct bolt client connection
		n, err := conn.Read(buf[:20])
		if err != nil {
			warn.Println("error peeking at connection from", conn.RemoteAddr())
			return
		}

		// Make sure we try to use the version we're using the best
		// version based on the backend server
		serverVersion := b.Version().Bytes()
		clientVersion, err := bolt.ValidateHandshake(buf[:n], serverVersion)
		if err != nil {
			warn.Fatal(err)
		}
		_, err = conn.Write(clientVersion)
		if err != nil {
			warn.Fatal(err)
		}

		// regular bolt
		handleBoltConn(bolt.NewDirectConn(conn), clientVersion, b)

	} else if bytes.Equal(buf[:4], []byte{0x47, 0x45, 0x54, 0x20}) {
		// Second case, we have an HTTP connection that might just
		// be a WebSocket upgrade OR a health check.

		// Read the rest of the request
		n, err = conn.Read(buf[4:])
		if err != nil {
			warn.Printf("failed reading rest of GET request: %s\n", err)
			return
		}

		// Health check, maybe? If so, handle and bail.
		if health.IsHealthCheck(buf[:n+4]) {
			err = health.HandleHealthCheck(conn, buf[:n+4])
			if err != nil {
				warn.Println(err)
			}
			info.Printf("healthcheck for %s\n", conn.RemoteAddr())
			return
		}

		// Build something implementing the io.ReadWriter interface
		// to pass to the upgrader routine
		iobuf := bytes.NewBuffer(buf[:n+4])
		_, err := ws.Upgrade(iobuf)
		if err != nil {
			warn.Printf("failed to upgrade websocket client %s: %s\n",
				conn.RemoteAddr(), err)
			return
		}
		// Relay the upgrade response
		_, err = io.Copy(conn, iobuf)
		if err != nil {
			warn.Printf("failed to copy upgrade to client %s\n",
				conn.RemoteAddr())
			return
		}

		// After upgrade, we should get a WebSocket message with header
		header, err := ws.ReadHeader(conn)
		if err != nil {
			warn.Printf("failed to read ws header from client %s: %s\n",
				conn.RemoteAddr(), err)
			return
		}
		n, err := conn.Read(buf[:header.Length])
		if err != nil {
			warn.Printf("failed to read payload from client %s\n",
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
			warn.Fatal(err)
		}

		// negotiate client & server side bolt versions
		serverVersion := b.Version().Bytes()
		clientVersion, err := bolt.ValidateHandshake(handshake, serverVersion)
		if err != nil {
			warn.Fatal(err)
		}

		// Complete Bolt handshake via WebSocket frame
		frame := ws.NewBinaryFrame(clientVersion)
		if err = ws.WriteFrame(conn, frame); err != nil {
			warn.Fatal(err)
		}

		// Let there be Bolt-via-WebSockets!
		handleBoltConn(bolt.NewWsConn(conn), clientVersion, b)
	} else {
		// not bolt, not http...something else?
		info.Printf("client %s is speaking gibberish: %#v\n",
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
			warn.Println("failed to read expected Hello from client")
			return
		}
		hello = msg
	case <-time.After(30 * time.Second):
		warn.Println("timed out waiting for auth from", client)
		return
	}
	logMessage("C->P", hello)

	if hello.T != bolt.HelloMsg {
		// some clients are impolite...*cough*browser*cough*
		if hello.T != bolt.GoodbyeMsg {
			warn.Println("expected HelloMsg, got:", hello.T)
		} else {
			debug.Println("goodbye without a tx from", client)
		}
		return
	}

	// XXX SPLICE
	if b.Splicing {
		// molest our hello message
		tinymap, _, err := bolt.ParseTinyMap(hello.Data[4:])
		if err != nil {
			panic(err)
		}
		val, found := tinymap["routing"]
		if found {
			info.Printf("HACK...found existing routing: %v\n", val)
		}
		tinymap["routing"] = map[string]interface{}{
			"address": "localhost:8888",
		}
		info.Printf("HACK...hot-wired routing to be %v\n", tinymap["routing"])
		raw, err := bolt.TinyMapToBytes(tinymap)
		if err != nil {
			panic(err)
		}
		data := make([]byte, len(raw)+6) // prefix + 00,00
		copy(data, hello.Data[:4])
		copy(data[4:], raw)
		copy(data[len(raw)+4:], []byte{0x00, 0x00})

		hey := bolt.Message{bolt.HelloMsg, data}
		info.Printf("HACK...new hello message:\n%#v\n", hey)

		serverConn, err := b.TheHorror(hey.Data, clientVersion)
		if err != nil {
			panic(err)
		}
		server := bolt.NewDirectConn(serverConn)

		// inject a SUCCESS
		successMap := map[string]interface{}{
			"server":        "bolt-proxy/v0.2.0",
			"connection_id": fmt.Sprintf("%s", client),
		}
		successMapBytes, err := bolt.TinyMapToBytes(successMap)
		if err != nil {
			panic(err)
		}
		success := bytes.NewBuffer([]byte{0x00})
		// yolo
		success.WriteByte(byte(uint8(len(successMapBytes))))
		success.Write([]byte{0xb1, 0x70})
		success.Write(successMapBytes)
		success.Write([]byte{0x00, 0x00})
		info.Printf("HACK crafted success message:\n%#v\n", success.Bytes())
		err = client.WriteMessage(&bolt.Message{
			T: bolt.SuccessMsg,
			Data: []byte{
				0x0, 0x2b, 0xb1, 0x70,
				0xa2,
				0x86, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72,
				0x8b, 0x4e, 0x65, 0x6f, 0x34, 0x6a, 0x2f, 0x34, 0x2e,
				0x32, 0x2e, 0x30,
				0x8d, 0x63, 0x6f, 0x6e, 0x6e, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64,
				0x86, 0x62, 0x6f, 0x6c, 0x74, 0x2d, 0x34,
				0x00, 0x00},
		})
		if err != nil {
			panic(err)
		}
		die := make(chan bool)
		go func(client, server bolt.BoltConn) {
			defer func() { die <- true }()

			for {
				select {
				case m, ok := <-client.R():
					if ok {
						logMessage("C->S", m)
						server.WriteMessage(m)
					} else {
						return
					}
				case <-time.After(15 * time.Minute):
					info.Println("idle timeout, killing")
					return
				}
			}
		}(client, server)

		go func(client, server bolt.BoltConn) {
			defer func() { die <- true }()

			for {
				select {
				case m, ok := <-server.R():
					if ok {
						logMessage("C<-S", m)
						client.WriteMessage(m)
					} else {
						return
					}
				case <-time.After(15 * time.Minute):
					info.Println("idle timeout, killing")
					return
				}
			}
		}(client, server)

		info.Println("SPLICED CLIENT TO SERVER!")

		<-die
		info.Println("splice is dead, long live the splice")
		return
	}

	// get backend connection
	pool, err := b.Authenticate(hello)
	if err != nil {
		warn.Fatal(err)
	}

	// TODO: SPLICE HERE BABY!!!

	// TODO: this seems odd...move parser and version stuff to bolt pkg
	v, _ := backend.ParseVersion(clientVersion)
	info.Printf("authenticated client %s speaking %s to %d host(s)\n",
		client, v, len(pool))
	defer func() {
		info.Printf("goodbye to client %s\n", client)
	}()

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
	logMessage("P->C", &success)
	err = client.WriteMessage(&success)
	if err != nil {
		warn.Fatal(err)
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
				logMessage("C->P", msg)
			} else {
				debug.Println("potential client hangup")
				select {
				case halt <- true:
					debug.Println("client hungup, asking tx to halt")
				default:
					warn.Println("failed to send halt message to tx handler")
				}
				return
			}
		case <-time.After(time.Duration(MAX_IDLE_MINS) * time.Minute):
			warn.Println("client idle timeout")
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
				warn.Printf("couldn't find host for '%s' in routing table", db)
			}

			if len(hosts) < 1 {
				warn.Println("empty hosts lists for database", db)
				// TODO: return FailureMsg???
				return
			}
			host := hosts[0]

			// Are we already using a host? If so try to stop the
			// current tx handler before we create a new one
			if server != nil {
				select {
				case halt <- true:
					debug.Println("...asking current tx handler to halt")
					select {
					case <-ack:
						debug.Println("tx handler ack'd stop")
					case <-time.After(5 * time.Second):
						warn.Println("!!! timeout waiting for ack from tx handler")
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
				warn.Println("no established connection for host", host)
				return
			}
			debug.Printf("grabbed conn for %s-access to db %s on host %s\n", mode, db, host)

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
			logMessage("P->S", msg)
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
		debugMode, splicing bool
		bindOn              string
		proxyTo             string
		username, password  string
		certFile, keyFile   string
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
	_, debugMode = os.LookupEnv("BOLT_PROXY_DEBUG")
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
	flag.BoolVar(&debugMode, "debug", debugMode, "enable debug logging")
	flag.BoolVar(&splicing, "splice", false, "yolo")
	flag.Parse()

	// We log to stdout because our parents raised us right
	info = log.New(os.Stdout, "INFO ", log.Ldate|log.Ltime|log.Lmsgprefix)
	if debugMode {
		debug = log.New(os.Stdout, "DEBUG ", log.Ldate|log.Ltime|log.Lmsgprefix)
	} else {
		debug = log.New(ioutil.Discard, "DEBUG ", 0)
	}
	warn = log.New(os.Stderr, "WARN ", log.Ldate|log.Ltime|log.Lmsgprefix)

	// ---------- pprof debugger
	go func() {
		info.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// ---------- BACK END
	info.Println("starting bolt-proxy backend")
	backend, err := backend.NewBackend(debug, username, password, proxyTo, splicing)
	if err != nil {
		warn.Fatal(err)
	}
	info.Println("connected to backend", proxyTo)
	info.Printf("found backend version %s\n", backend.Version())

	// ---------- FRONT END
	info.Println("starting bolt-proxy frontend")
	var listener net.Listener
	if certFile == "" || keyFile == "" {
		// non-tls
		listener, err = net.Listen("tcp", bindOn)
		if err != nil {
			warn.Fatal(err)
		}
		info.Printf("listening on %s\n", bindOn)
	} else {
		// tls
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			warn.Fatal(err)
		}
		config := &tls.Config{Certificates: []tls.Certificate{cert}}
		listener, err = tls.Listen("tcp", bindOn, config)
		if err != nil {
			warn.Fatal(err)
		}
		info.Printf("listening for TLS connections on %s\n", bindOn)
	}

	// ---------- Event Loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			warn.Printf("error: %v\n", err)
		} else {
			go handleClient(conn, backend)
		}
	}
}
