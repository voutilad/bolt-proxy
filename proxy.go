package main

import (
	"bytes"
	"errors"
	"io"
	"log"
	"net"
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

func logMessages(who string, data []byte) {
	// first, handle special messages
	if bytes.HasPrefix(data, []byte{0, 0x94}) ||
		bytes.HasPrefix(data, []byte{0, 0x2c}) {
		log.Printf("[%s]: %s\n", who, data)
		return
	}

	// then, deal with N-number of bolt messages
	for i, token := range bytes.Split(data, []byte{0, 0}) {
		if len(token) > 0 {
			log.Printf("[%s]{%d}: %#v\n", who, i, token)
		}
	}
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

	// lazy splice
	for {
		n, err := client.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		logMessages("CLIENT", buf[:n])

		n, err = server.Write(buf[:n])
		if err != nil {
			log.Fatal(err)
		}
		log.Println("wrote message to server")

		n, err = server.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatal(err)
		}
		logMessages("SERVER", buf[:n])

		n, err = client.Write(buf[:n])
		if err != nil {
			log.Fatal(err)
		}
		log.Println("wrote message to client")
	}

	log.Println("Client dead.")
}

func main() {
	log.Println("Starting bolt-proxy...")
	defer log.Println("finished.")

	addr, err := net.ResolveTCPAddr("tcp", "localhost:8888")
	if err != nil {
		log.Fatal(err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Printf("error: %v\n", err)
		} else {
			log.Printf("got connection %v\n", conn)
			go handleClient(conn)
		}
	}
}
