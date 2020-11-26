package main

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"time"
)

const SERVER = "alpine:7687"

func validateMagic(magic []byte) (bool, error) {
	// 0x60, 0x60, 0xb0, 0x17
	if len(magic) < 4 {
		return false, errors.New("magic too short")
	}
	if bytes.Equal(magic, []byte{0x60, 0x60, 0xb0, 0x17}) {
		return true, nil
	}

	return false, errors.New("invalid magic bytes")
}

func validateHandshake(client, server []byte) ([]byte, error) {
	if len(server) != 4 {
		return nil, errors.New("server handshake wrong size")
	}

	if len(client) != 16 {
		return nil, errors.New("client handshake wrong size")
	}

	for i := 0; i < 4; i++ {
		part := client[i*4 : i*4+4]
		if bytes.Equal(server, part) {
			return part, nil
		}
	}
	return nil, errors.New("couldn't find handshake match!")
}

func tokenToType(token []byte) string {
	if len(token) < 4 {
		return "NOP"
	}

	var msgCode byte
	if token[0] == 0x0 {
		msgCode = token[3]
	} else {
		msgCode = token[2]
	}

	switch msgCode {
	case 0x0f:
		return "RESET"
	case 0x10:
		return "RUN"
	case 0x2f:
		return "DISCARD"
	case 0x3f:
		return "PULL"
	case 0x71:
		return "RECORD"
	case 0x70:
		return "SUCCESS"
	case 0x7e:
		return "IGNORED"
	case 0x7f:
		return "FAILURE"
	case 0x01:
		return "HELLO"
	case 0x02:
		return "GOODBYE"
	case 0x11:
		return "BEGIN"
	case 0x12:
		return "COMMIT"
	case 0x13:
		return "ROLLBACK"
	default:
		return "?UNKNOWN?"
	}
}

func logMessages(who string, data []byte) {
	// then, deal with N-number of bolt messages
	for i, token := range bytes.Split(data, []byte{0, 0}) {
		if len(token) > 0 {
			msgType := tokenToType(token)
			if msgType != "HELLO" {
				log.Printf("[%s]{%d}: %s %#v\n", who, i, msgType, token)
			} else {
				// don't log HELLO payload...it has credentials
				log.Printf("[%s]{%d}: %s <redacted>\n", who, i, msgType, token)
			}
		}
	}
}

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
	buf := make([]byte, 1024)
	_, err := client.Read(buf[:20])
	if err != nil {
		log.Printf("error peeking at client (%v): %v\n", client, err)
		return
	}

	// slice out and validate magic and handshake
	magic, handshake := buf[:4], buf[4:20]
	log.Printf("magic: %#v, handshake: %#v\n", magic, handshake)

	valid, err := validateMagic(magic)
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
	match, err := validateHandshake(handshake, buf[:4])
	if err != nil {
		log.Fatal(err)
	}
	_, err = client.Write(match)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("handshake complete: %#v\n", match)

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
		case <-time.After(120 * time.Second):
			log.Println("Timed out!")
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
