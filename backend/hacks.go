package backend

// This is horrible...don't look here yet

import (
	"errors"
	"log"
	"net"

	"github.com/voutilad/bolt-proxy/bolt"
)

var handshake = []byte{
	0x60, 0x60, 0xb0, 0x17,
	0x00, 0x00, 0x02, 0x04,
	0x00, 0x00, 0x01, 0x04,
	0x00, 0x00, 0x00, 0x04,
	0x00, 0x00, 0x00, 0x03}

// horrible check to see if a client is able to auth
func authClient(auth []byte, network, address string) (net.Conn, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	_, err = conn.Write(handshake)
	if err != nil {
		log.Println("couldn't send handshake to auth server", address)
		conn.Close()
		return nil, err
	}

	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil || n != 4 {
		log.Println("didn't get valid handshake response from auth server", address)
		conn.Close()
		return nil, err
	}

	_, err = conn.Write(auth)
	if err != nil {
		log.Println("failed to send auth buffer to auth server", address)
		conn.Close()
		return nil, err
	}

	n, err = conn.Read(buf)
	if err != nil {
		log.Println("failed to get auth response from auth server", address)
		conn.Close()
		return nil, err
	}

	msg := bolt.ParseBoltMsg(buf)
	if msg == bolt.FailureMsg {
		// see if we can extract the error message
		r, _, err := bolt.ParseTinyMap(buf[4:n])
		if err != nil {
			conn.Close()
			return nil, err
		}

		val, found := r["message"]
		if found {
			failmsg, ok := val.(string)
			if ok {
				conn.Close()
				return nil, errors.New(failmsg)
			}
		}
		log.Printf("!!! auth failure, but could not parse response: %v\n", r)
		conn.Close()
		return nil, errors.New("unknown auth failure")
	} else if msg == bolt.SuccessMsg {
		// the only happy outcome! keep conn open.
		return conn, nil
	}

	// try to be polite and say goodbye
	conn.Write([]byte{0x00, 0x02, 0xb0, 0x02})
	conn.Close()
	return nil, errors.New("unknown error from auth server")
}
