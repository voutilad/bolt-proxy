package frontend

import (
	"container/ring"
)

type Session struct {
	principal   string
	credentials []byte
	pool        *Pool
}

type Pool struct {
	readers, writers *ring.Ring
}
