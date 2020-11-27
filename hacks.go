package main

// This is horrible...don't look here yet

import (
	"errors"
	"log"
	"net"
)

var handshake = []byte{
	0x60, 0x60, 0xb0, 0x17,
	0x00, 0x00, 0x02, 0x04,
	0x00, 0x00, 0x01, 0x04,
	0x00, 0x00, 0x00, 0x04,
	0x00, 0x00, 0x00, 0x03}

// horrible check to see if a client is able to auth
func authClient(auth []byte, network, address string) (bool, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	_, err = conn.Write(handshake)
	if err != nil {
		log.Println("couldn't send handshake to auth server", address)
		return false, err
	}

	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil || n != 4 {
		log.Println("didn't get valid handshake response from auth server", address)
		return false, err
	}

	_, err = conn.Write(auth)
	if err != nil {
		log.Println("failed to send auth buffer to auth server", address)
		return false, err
	}

	n, err = conn.Read(buf)
	if err != nil {
		log.Println("failed to get auth response from auth server", address)
		return false, err
	}

	msg := ParseBoltMsg(buf)
	if msg == FailureMsg {
		// see if we can extract the error message
		r, err := parseTinyMap(buf[4:n])
		if err != nil {
			log.Println("auth failure, but failed to parse tiny map")
			return false, nil
		}

		val, ok := r["message"]
		if ok {
			// XXX danger! just blindly bringing bytes as string
			log.Printf("auth proxy failure: %s\n", val)
			return false, nil
		}
		log.Printf("auth failure, but no message in tiny map: %v\n", r)
		return false, nil
	} else if msg == SuccessMsg {
		return true, nil
	}

	// try to be polite and say goodbye
	conn.Write([]byte{0x00, 0x02, 0xb0, 0x02})

	return false, errors.New("unknown error from auth server")
}
