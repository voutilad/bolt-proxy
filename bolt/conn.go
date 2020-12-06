package bolt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type BoltConn interface {
	ReadMessage() (*Message, error)
	WriteMessage(*Message) error
	io.Closer
}

type DirectConn struct {
	conn io.ReadWriteCloser
	buf  []byte
}

type WsConn struct {
	conn io.ReadWriteCloser
	buf  []byte
}

func NewDirectConn(c io.ReadWriteCloser) DirectConn {
	return DirectConn{c, make([]byte, 2048)}
}

func NewWsConn(c io.ReadWriteCloser) WsConn {
	return WsConn{c, make([]byte, 2048)}
}

func (c DirectConn) ReadMessage() (*Message, error) {
	n, err := c.conn.Read(c.buf[:2])
	if err != nil {
		return nil, err
	}

	msglen := int(binary.BigEndian.Uint16(c.buf[:2]))
	if msglen < 2 {
		return nil, errors.New("invalid message length")
	}

	n, err = c.conn.Read(c.buf[2 : msglen+4])
	if err != nil {
		return nil, err
	}
	if n != msglen+2 {
		fmt.Printf("ERROR: buf=%#v\n", c.buf[2:msglen+2])
		panic("bolt message read was too short")
	}

	msgtype := IdentifyType(c.buf[:msglen+4])
	if msgtype == UnknownMsg {
		return nil, errors.New("could not determine message type")
	}

	data := make([]byte, msglen+4)
	copy(data, c.buf[:len(data)])

	return &Message{
		T:    msgtype,
		Data: data,
	}, nil
}

func (c *WsConn) ReadMessage() (*Message, error) {
	return nil, nil
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
