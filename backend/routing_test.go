package backend

import (
	"testing"
	"time"
)

func TestExpiringRoutingTable(t *testing.T) {
	rt := RoutingTable{Ttl: time.Hour, CreatedAt: time.Unix(0, 0)}

	if !rt.Expired() {
		t.Fatalf("expected routing table to be expired")
	}

	rt.CreatedAt = time.Now()
	if rt.Expired() {
		t.Fatalf("expected routing table to be valid")
	}
}
