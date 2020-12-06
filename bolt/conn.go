package bolt

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/gobwas/ws"
)

// An abstraction of a Bolt-aware io.ReadWriterCloser. Allows for sending and
// receiving Messages, abstracting away the nuances of the transport.
type BoltConn interface {
	ReadMessage() (*Message, error)
	WriteMessage(*Message) error
	io.Closer
}

// Designed for operating direct (e.g. TCP/IP-only) Bolt connections
type DirectConn struct {
	conn io.ReadWriteCloser
	buf  []byte
}

// Used for WebSocket-based Bolt connections
type WsConn struct {
	conn io.ReadWriteCloser
	buf  []byte
}

func NewDirectConn(c io.ReadWriteCloser) DirectConn {
	return DirectConn{c, make([]byte, 1024*1024)}
}

func NewWsConn(c io.ReadWriteCloser) WsConn {
	return WsConn{c, make([]byte, 128*1024)}
}

func (c DirectConn) ReadMessage() (*Message, error) {
	var n int
	var err error

	pos := 0
	for {
		n, err = c.conn.Read(c.buf[pos : pos+2])
		if err != nil {
			return nil, err
		}
		msglen := int(binary.BigEndian.Uint16(c.buf[pos : pos+n]))
		pos = pos + n

		if msglen < 1 {
			// 0x00 0x00 would mean we're done
			break
		}

		endOfData := pos + msglen
		// handle short reads of user data
		for pos < endOfData {
			n, err = c.conn.Read(c.buf[pos:endOfData])
			if err != nil {
				return nil, err
			}
			pos = pos + n
		}
	}

	t := IdentifyType(c.buf[:pos])

	// Copy data into Message...
	data := make([]byte, pos)
	copy(data, c.buf[:pos])

	return &Message{
		T:    t,
		Data: data,
	}, nil
}

func (c WsConn) ReadMessage() (*Message, error) {
	header, err := ws.ReadHeader(c.conn)
	if err != nil {
		return nil, err
	}

	// TODO: handle header.Length == 0 situations?
	if header.Length == 0 {
		panic("header.Length = 0!")
	}

	n, err := c.conn.Read(c.buf[:header.Length])
	if err != nil {
		return nil, err
	}

	if header.Masked {
		ws.Cipher(c.buf[:n], header.Mask, 0)
		header.Masked = false
	}

	msgtype := IdentifyType(c.buf[:header.Length])
	if msgtype == UnknownMsg {
		return nil, errors.New("could not determine message type")
	}

	data := make([]byte, header.Length)
	copy(data, c.buf[:header.Length])

	return &Message{
		T:    msgtype,
		Data: data,
	}, nil
}

func (c DirectConn) WriteMessage(m *Message) error {
	// TODO validate message?

	n, err := c.conn.Write(m.Data)
	if err != nil {
		return err
	}
	if n != len(m.Data) {
		// TODO: loop to write all data?
		panic("incomplete message written")
	}

	return nil
}

func (c WsConn) WriteMessage(m *Message) error {
	frame := ws.NewBinaryFrame(m.Data)
	err := ws.WriteFrame(c.conn, frame)

	return err
}

func (c DirectConn) Close() error {
	return c.conn.Close()
}

func (c WsConn) Close() error {
	return c.conn.Close()
}
