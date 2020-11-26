package main

import (
	"bytes"
	"io"
	"log"
	"net"
	"time"
)

const SERVER = "alpine:7687"

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

// notes:
//  - if we see a RUN before we see a BEGIN, we know it's RW
//  - if we see a BEGIN, it may or may not have a 'mode' set to 'r' for R
//  - otherwise, RW!

//  BEGIN { 'mode': 'r' }
//  []byte{0x0, 0xa, 0xb1, 0x11, 0xa1, 0x84, 0x6d, 0x6f, 0x64, 0x65, 0x81, 0x72}

// Take 2 net.Conn's...a reader and a writer...and pipe the
// data from the reader to the writer.
func splice(r, w net.Conn, name string, done chan<- bool) {
	buf := make([]byte, 4*1024)
	for {
		n, err := r.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}

		_, err = w.Write(buf[:n])
		if err != nil {
			log.Fatal(err)
		}

		logMessages(name, buf[:n])
	}
	done <- true
}

// Primary Client connection handler
func handleClient(client net.Conn) {
	defer client.Close()

	// peek and check for magic and handshake
	log.Println("peeking at client buffer...")
	buf := make([]byte, 512)
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

	// open outgoing connection
	addr, err := net.ResolveTCPAddr("tcp", SERVER)
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
	_, err = server.Write(buf[:n])
	if err != nil {
		log.Fatal(err)
	}
	// zero buf to drop credentials
	for i, _ := range buf {
		buf[i] = 0
	}
	// get server response to auth
	n, err = server.Read(buf)
	if err != nil {
		log.Fatal(err)
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

	go splice(client, server, "CLIENT", clientChan)
	go splice(server, client, "SERVER", serverChan)

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
	log.Println("Starting bolt-proxy...")
	defer log.Println("finished.")

	listener, err := net.Listen("tcp", "localhost:8888")
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("error: %v\n", err)
		} else {
			log.Printf("got connection %v\n", conn)
			go handleClient(conn)
		}
	}
}
