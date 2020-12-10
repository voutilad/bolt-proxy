package bolt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/gobwas/ws"
)

// An abstraction of a Bolt-aware io.ReadWriterCloser. Allows for sending and
// receiving Messages, abstracting away the nuances of the transport.
type BoltConn interface {
	R() <-chan *Message
	WriteMessage(*Message) error
	io.Closer
}

// Designed for operating direct (e.g. TCP/IP-only) Bolt connections
type DirectConn struct {
	conn io.ReadWriteCloser
	buf  []byte
	r    <-chan *Message
}

// Used for WebSocket-based Bolt connections
type WsConn struct {
	conn io.ReadWriteCloser
	buf  []byte
	r    <-chan *Message
}

func NewDirectConn(c io.ReadWriteCloser) DirectConn {
	msgchan := make(chan *Message)
	dc := DirectConn{
		conn: c,
		buf:  make([]byte, 1024*1024),
		r:    msgchan,
	}

	go func() {
		for {
			message, err := dc.readMessage()
			if err != nil {
				if err == io.EOF {
					log.Println("direct bolt connection hung-up")
					close(msgchan)
					return
				}
				log.Printf("direct bolt connection error! %s\n", err)
				return
			}
			msgchan <- message
		}
	}()

	return dc
}

func (c DirectConn) R() <-chan *Message {
	return c.r
}

func (c DirectConn) readMessage() (*Message, error) {
	var n int
	var err error

	pos := 0
	for {
		n, err = c.conn.Read(c.buf[pos : pos+2])
		if err != nil {
			return nil, err
		}
		// TODO: deal with this horrible issue!
		if n < 2 {
			panic("under-read?!")
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

func (c DirectConn) Close() error {
	return c.conn.Close()
}

func NewWsConn(c io.ReadWriteCloser) WsConn {
	msgchan := make(chan *Message)
	ws := WsConn{
		conn: c,
		buf:  make([]byte, 1024*1024),
		r:    msgchan,
	}

	go func() {
		for {
			message, err := ws.readMessage()
			if err != nil {
				if err == io.EOF {
					log.Println("bolt ws connection hung-up")
					close(msgchan)
					return
				}
				log.Printf("ws bolt connection error! %s\n", err)
				return
			}
			msgchan <- message
		}
	}()

	return ws
}

func (c WsConn) R() <-chan *Message {
	return c.r
}

func (c WsConn) readMessage() (*Message, error) {
	header, err := ws.ReadHeader(c.conn)
	if err != nil {
		return nil, err
	}

	switch header.OpCode {
	case ws.OpClose:
		return nil, io.EOF
	case ws.OpPing, ws.OpPong, ws.OpContinuation, ws.OpText:
		msg := fmt.Sprintf("unsupported websocket opcode: %v\n", header.OpCode)
		return nil, errors.New(msg)
	}

	// TODO: handle header.Length == 0 situations?
	if header.Length == 0 {
		return nil, errors.New("zero length header?!")
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
func (c WsConn) WriteMessage(m *Message) error {
	frame := ws.NewBinaryFrame(m.Data)
	err := ws.WriteFrame(c.conn, frame)

	return err
}

func (c WsConn) Close() error {
	return c.conn.Close()
}
