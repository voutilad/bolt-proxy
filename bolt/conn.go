package bolt

import (
	"bytes"
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
			messages, err := ws.readMessages()
			if err != nil {
				if err == io.EOF {
					log.Println("bolt ws connection hung-up")
					close(msgchan)
					return
				}
				log.Printf("ws bolt connection error! %s\n", err)
				return
			}
			for _, message := range messages {
				if message == nil {
					panic("ws message = nil!")
				}
				msgchan <- message
			}
		}
	}()

	return ws
}

func (c WsConn) R() <-chan *Message {
	return c.r
}

// Read 0 or many Bolt Messages from a WebSocket frame since, apparently,
// small Bolt Messages sometimes get packed into a single Frame(?!).
//
// For example, I've seen RUN + PULL all in 1 WebSocket frame.
func (c WsConn) readMessages() ([]*Message, error) {
	messages := make([]*Message, 0)

	header, err := ws.ReadHeader(c.conn)
	if err != nil {
		return nil, err
	}

	if !header.Fin {
		panic("unsupported header fin")
	}

	switch header.OpCode {
	case ws.OpClose:
		return nil, io.EOF
	case ws.OpPing, ws.OpPong, ws.OpContinuation, ws.OpText:
		panic(fmt.Sprintf("unsupported websocket opcode: %v\n", header.OpCode))
		// return nil, errors.New(msg)
	}

	// TODO: handle header.Length == 0 situations?
	if header.Length == 0 {
		return nil, errors.New("zero length header?!")
	}

	// TODO: under-reads!!!
	n, err := c.conn.Read(c.buf[:header.Length])
	if err != nil {
		return nil, err
	}

	if header.Masked {
		ws.Cipher(c.buf[:n], header.Mask, 0)
		header.Masked = false
	}

	// WebSocket frames might contain multiple bolt messages...oh, joy
	// XXX: for now we don't look for chunks across frame boundaries
	pos := 0
	chunking := false

	for pos < int(header.Length) {
		msglen := int(binary.BigEndian.Uint16(c.buf[pos : pos+2]))

		// since we've already got the data in our buffer, we can
		// just peek to see if it's complete or chunked
		if !bytes.Equal([]byte{0x0, 0x0}, c.buf[msglen:msglen+2]) {
			chunking = true
		} else {
			chunking = false
		}

		// we'll let the combination of the type and the chunking
		// flag dictate behavior as we're not cleaning our buffer
		// afterwards, so maaaaaybe there was a false positive
		sizeOfMsg := msglen + 4
		msgtype := IdentifyType(c.buf[pos:])
		if msgtype == UnknownMsg && chunking {
			msgtype = ChunkedMsg
			sizeOfMsg = msglen + 2
		}

		data := make([]byte, sizeOfMsg)
		copy(data, c.buf[pos:pos+sizeOfMsg])
		msg := Message{
			T:    msgtype,
			Data: data,
		}
		//fmt.Printf("**** appending msg: %#v\n", msg)
		messages = append(messages, &msg)

		pos = pos + sizeOfMsg
	}

	fmt.Printf("**** parsed %d ws bolt messages\n", len(messages))

	return messages, nil
}
func (c WsConn) WriteMessage(m *Message) error {
	frame := ws.NewBinaryFrame(m.Data)
	err := ws.WriteFrame(c.conn, frame)

	return err
}

func (c WsConn) Close() error {
	return c.conn.Close()
}
