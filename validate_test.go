package main

import (
	"testing"
)

func TestParsingReadMode(t *testing.T) {
	// BEGIN { "mode": "r" }
	buf := []byte{0xa1, 0x84, 0x6d, 0x6f, 0x64, 0x65, 0x81, 0x72, 0x0, 0x0}

	result, err := parseTinyMap(buf)
	if err != nil {
		t.Errorf("failed to parse read transaction's tiny map: %v", err)
		return
	}
	val, ok := result["mode"]
	if !ok {
		t.Errorf("failed to find a 'mode' key in tiny map")
		return
	}

	if len(val) != 1 || val[0] != 'r' {
		t.Errorf("mode value isn't a single byte 'r'")
		return
	}
}

func TestParsingEmptyMode(t *testing.T) {
	// BEGIN { }
	buf := []byte{0xa0, 0x0, 0x0}

	result, err := parseTinyMap(buf)
	if err != nil {
		t.Errorf("failed to parse read transaction's tiny map: %v", err)
		return
	}
	if len(result) != 0 {
		t.Errorf("expected empty tiny map")
		return
	}
}
