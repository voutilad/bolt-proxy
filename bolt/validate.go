package bolt

import (
	"bytes"
	"errors"
	"log"
)

// Mode of a Bolt Transaction for determining cluster routing
type Mode string

const (
	ReadMode  Mode = "READ"
	WriteMode      = "WRITE"
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

// Try to find and validate the Mode for some given bytes, returning
// the Mode if found or if valid looking Bolt chatter. Otherwise,
// returns nil and an error.
func ValidateMode(buf []byte) (Mode, error) {
	if IdentifyType(buf) == BeginMsg {
		tinyMap, _, err := ParseTinyMap(buf[4:])
		if err != nil {
			log.Fatal(err)
		}

		value, found := tinyMap["mode"]
		if found {
			mode, ok := value.(string)
			if ok && mode == "r" {
				return ReadMode, nil
			}
		}
	}
	return WriteMode, nil
}
