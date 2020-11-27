package router

import "testing"

// Does our RoutingTable iterate in the order given?
func TestIteratingReaders(t *testing.T) {
	hosts := []string{"hostA", "hostB"}

	rt := NewRoutingTable()
	rt.ReplaceReaders(hosts)

	for _, expected := range hosts {
		actual, err := rt.NextReader()
		if err != nil {
			t.Fatal(err)
		}
		if actual != expected {
			t.Fatalf("expected %s, got %s\n", expected, actual)
		}
	}
}

// Does our RoutingTable iterate writers in the order given?
func TestIteratingWriters(t *testing.T) {
	hosts := []string{"hostA", "hostB"}

	rt := NewRoutingTable()
	rt.ReplaceWriters(hosts)

	for _, expected := range hosts {
		actual, err := rt.NextWriter()
		if err != nil {
			t.Fatal(err)
		}
		if actual != expected {
			t.Fatalf("expected %s, got %s\n", expected, actual)
		}
	}
}
