package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
)

// Mode of a Bolt Transaction for determining cluster routing
type TxMode string

const (
	ReadMode  TxMode = "READ"
	WriteMode        = "WRITE"
)

// Inspect bytes for valid Bolt Magic pattern, returning true if found.
// Otherwise, returns false with an error.
func ValidateMagic(magic []byte) (bool, error) {
	// 0x60, 0x60, 0xb0, 0x17
	if len(magic) < 4 {
		return false, errors.New("magic too short")
	}
	if bytes.Equal(magic, []byte{0x60, 0x60, 0xb0, 0x17}) {
		return true, nil
	}

	return false, errors.New("invalid magic bytes")
}

// Inspect client and server communication for valid Bolt handshake,
// returning the handshake value's bytes.
//
// If the handshake is input bytes or handshake is invalide, returns
// an error and an empty byte array.
func ValidateHandshake(client, server []byte) ([]byte, error) {
	if len(server) != 4 {
		return nil, errors.New("server handshake wrong size")
	}

	if len(client) != 16 {
		return nil, errors.New("client handshake wrong size")
	}

	for i := 0; i < 4; i++ {
		part := client[i*4 : i*4+4]
		if bytes.Equal(server, part) {
			return part, nil
		}
	}
	return []byte{}, errors.New("couldn't find handshake match!")
}

// Try parsing some bytes into a Packstream Tiny Map, returning it as a map
// of strings to their values as byte arrays.
//
// If not found or something horribly wrong, return nil and an error. Also,
// will panic on a nil input.
//
// Note that this is only designed for finding the first and most likely
// useful tiny map in a byte array. As such it does not tell you where that
// map ends in the array!
func parseTinyMap(buf []byte) (map[string][]byte, error) {
	log.Printf("debug: %#v\n", buf)

	if buf == nil {
		panic("cannot parse nil byte array for structs")
	}

	if len(buf) < 1 {
		return nil, errors.New("bytes empty, cannot parse struct")
	}

	pos := 0
	if buf[pos]>>4 != 0xa {
		return nil, errors.New("bytes missing tiny-map prefix of 0xa")
	}

	numMembers := int(buf[pos] & 0xf)
	pos++

	result := make(map[string][]byte)
	for i := 0; i < numMembers; i++ {
		memberType := buf[pos] >> 4

		// tiny maps, being the cutest, have member names as tiny strings
		if memberType != 0x8 {
			return nil, errors.New("found non-tiny-string member name?!")
		}
		memberSize := int(buf[pos] & 0xf)
		pos++
		name := fmt.Sprintf("%s", buf[pos:pos+memberSize])
		pos = pos + memberSize

		// for now, to keep this simple and focused, we bail
		// if we don't find string-based values. There are 2
		// we will handle: tiny strings and regular strings
		valueSize := 0

		if buf[pos]>>4 == 0x8 {
			valueSize = int(buf[pos] & 0xf)
			pos++
		} else if buf[pos]>>4 == 0xd {
			readAhead := 1 << int(buf[pos]&0xf)
			pos++

			valueBytes := append([]byte{buf[pos] & 0xf}, buf[pos+1:pos+readAhead]...)
			valueSize = int(binary.BigEndian.Uint32(valueBytes))
			pos = pos + readAhead
		} else {
			log.Printf("XXX bad value type? %#v\n", buf[pos])
			return nil, errors.New("found non-tiny-string or non-string value in tiny map!")
		}

		result[name] = buf[pos : pos+valueSize]
		pos = pos + valueSize
	}
	return result, nil
}

// Try to find and validate the TxMode for some given bytes, returning
// the TxMode if found or if valid looking Bolt chatter. Otherwise,
// returns nil and an error.
func ValidateMode(buf []byte) (TxMode, error) {
	if ParseBoltMsg(buf) == BeginMsg {
		tinyMap, err := parseTinyMap(buf[4:])
		if err != nil {
			log.Fatal(err)
		}

		for name := range tinyMap {
			if name == "mode" && tinyMap[name][0] == 'r' {
				return ReadMode, nil
			}
		}
	}
	return WriteMode, nil
}
