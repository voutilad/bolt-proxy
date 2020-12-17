package backend

// This is horrible...don't look here yet

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"

	"github.com/voutilad/bolt-proxy/bolt"
)

var magic = []byte{0x60, 0x60, 0xb0, 0x17}

// XXX: SPLICE
func (b *Backend) TheHorror(hello, version []byte) (net.Conn, error) {
	return authClient(hello, version, "tcp", b.Host, b.tls)
}

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
		msg := fmt.Sprintf("couldn't send handshake to auth server %s: %s", address, err)
		conn.Close()
		return nil, errors.New(msg)
	}

	// XXX: SPLICE
	fmt.Println("xxxx handh has been shook")

	// Server should pick our version and provide as 4-byte array
	buf := make([]byte, 256)
	n, err := conn.Read(buf)
	if err != nil || n != 4 {
		msg := fmt.Sprintf("didn't get valid handshake response from auth server %s: %s", address, err)
		conn.Close()
		return nil, errors.New(msg)
	}

	// Try performing the bolt auth the given hello message
	n, err = conn.Write(hello)
	if err != nil {
		msg := fmt.Sprintf("failed to send hello buffer to server %s: %s", address, err)
		conn.Close()
		return nil, errors.New(msg)
	}
	if n != len(hello) {
		panic("under write of hello msg")
	}

	// XXX: SPLICE
	fmt.Println("xxxxx auth has been sent")

	n, err = conn.Read(buf)
	if err != nil {
		msg := fmt.Sprintf("failed to get auth response from auth server %s: %s", address, err)
		conn.Close()
		return nil, errors.New(msg)
	}

	// XXX: SPLICE
	fmt.Printf("xxxxxx auth response:\n%#v\n", buf[:n])

	msg := bolt.IdentifyType(buf[:n])
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
		conn.Close()
		return nil, errors.New("could not parse auth server response")
	} else if msg == bolt.SuccessMsg {
		// The only happy outcome! Keep conn open.
		return conn, nil
	}

	// Try to be polite and say goodbye if we know we failed.
	conn.Write([]byte{0x00, 0x02, 0xb0, 0x02})
	conn.Close()
	return nil, errors.New("unknown error from auth server")
}
