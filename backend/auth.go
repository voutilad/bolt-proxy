package backend

// This is horrible...don't look here yet

import (
	"crypto/tls"
	"errors"
	"log"
	"net"

	"github.com/voutilad/bolt-proxy/bolt"
)

var magic = []byte{0x60, 0x60, 0xb0, 0x17}

// Use the provided []byte as a Hello message to try authenticating with the
// provided address, forcing the use of the given version []byte.
//
// If useTls, dial the address with the TLS dialer routine.
//
// On success, return a net.Conn that's pass the bolt handshake and has been
// authenticated and is ready for transactions. Otherwise, return nil and the
// error.
func authClient(hello, version []byte, network, address string, useTls bool) (net.Conn, error) {
	var conn net.Conn
	var err error

	// XXX: For now, we use the default TLS config, so probably won't work
	// with self-signed certificates.
	if useTls {
		conf := &tls.Config{}
		conn, err = tls.Dial(network, address, conf)
	} else {
		conn, err = net.Dial(network, address)
	}
	if err != nil {
		return nil, err
	}

	// Bolt handshake (bolt magic + version list)
	handshake := append(magic, version...)
	handshake = append(handshake, []byte{
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00}...)
	_, err = conn.Write(handshake)
	if err != nil {
		log.Println("couldn't send handshake to auth server", address)
		conn.Close()
		return nil, err
	}

	// Server should pick a version and provide as 4-byte array
	// TODO: we eventually need version handling...for now ignore :-/
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil || n != 4 {
		log.Println("didn't get valid handshake response from auth server", address)
		conn.Close()
		return nil, err
	}

	// Try performing the bolt auth the given hello message
	_, err = conn.Write(hello)
	if err != nil {
		log.Println("failed to send hello buffer to server", address)
		conn.Close()
		return nil, err
	}

	n, err = conn.Read(buf)
	if err != nil {
		log.Println("failed to get auth response from auth server", address)
		conn.Close()
		return nil, err
	}

	msg := bolt.IdentifyType(buf)
	if msg == bolt.FailureMsg {
		// See if we can extract the error message
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
		// The only happy outcome! Keep conn open.
		return conn, nil
	}

	// Try to be polite and say goodbye if we know we failed.
	conn.Write([]byte{0x00, 0x02, 0xb0, 0x02})
	conn.Close()
	return nil, errors.New("unknown error from auth server")
}
