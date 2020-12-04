package bolt

import (
	"encoding/binary"
	"errors"
	"fmt"
)

type BoltMsg string

type TinyMap struct {
	data map[string]interface{}
	Size uint
}

const (
	ResetMsg    BoltMsg = "RESET"
	RunMsg              = "RUN"
	DiscardMsg          = "DISCARD"
	PullMsg             = "PULL"
	RecordMsg           = "RECORD"
	SuccessMsg          = "SUCCESS"
	IgnoreMsg           = "IGNORE"
	FailureMsg          = "FAILURE"
	HelloMsg            = "HELLO"
	GoodbyeMsg          = "GOODBYE"
	BeginMsg            = "BEGIN"
	CommitMsg           = "COMMIT"
	RollbackMsg         = "ROLLBACK"
	UnknownMsg          = "?UNKNOWN?"
	NopMsg              = "NOP"
)

// Parse a byte into the corresponding BoltMsg
func BoltMsgFromByte(b byte) BoltMsg {
	switch b {
	case 0x0f:
		return ResetMsg
	case 0x10:
		return RunMsg
	case 0x2f:
		return DiscardMsg
	case 0x3f:
		return PullMsg
	case 0x71:
		return RecordMsg
	case 0x70:
		return SuccessMsg
	case 0x7e:
		return IgnoreMsg
	case 0x7f:
		return FailureMsg
	case 0x01:
		return HelloMsg
	case 0x02:
		return GoodbyeMsg
	case 0x11:
		return BeginMsg
	case 0x12:
		return CommitMsg
	case 0x13:
		return RollbackMsg
	default:
		return UnknownMsg
	}
}

// Try to extract the BoltMsg from some given bytes.
func ParseBoltMsg(buf []byte) BoltMsg {

	// If the byte array is too small, it could be an empty message
	if len(buf) < 4 {
		return NopMsg
	}

	// Some larger, usually non-Record messages, start with
	// a zero byte prefix
	if buf[0] == 0x0 {
		return BoltMsgFromByte(buf[3])
	}

	// Otherwise the message byte is after the message size
	return BoltMsgFromByte(buf[2])
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
func ParseTinyMap(buf []byte) (map[string]interface{}, uint, error) {
	//	fmt.Printf("tinymap debug: %#v\n", buf)

	if buf == nil {
		panic("cannot parse nil byte array for structs")
	}

	if len(buf) < 1 {
		return nil, 0, errors.New("bytes empty, cannot parse struct")
	}

	pos := uint(0)
	if buf[pos]>>4 != 0xa {
		return nil, pos, errors.New("bytes missing tiny-map prefix of 0xa")
	}

	numMembers := int(buf[pos] & 0xf)
	pos++

	result := make(map[string]interface{})

	for i := 0; i < numMembers; i++ {
		memberType := buf[pos] >> 4

		// tiny maps, being the cutest, have member names as tiny strings
		if memberType != 0x8 {
			return nil, pos, errors.New("found non-tiny-string member name?!")
		}
		memberSize := uint(buf[pos] & 0xf)
		pos++
		name := fmt.Sprintf("%s", buf[pos:pos+memberSize])
		pos = pos + memberSize

		// now for the value
		switch buf[pos] >> 4 {
		case 0x8: // tiny-string
			valueSize := uint(buf[pos] & 0xf)
			pos++
			result[name] = fmt.Sprintf("%s", buf[pos:pos+valueSize])
			pos = pos + valueSize
		case 0xd: // string
			readAhead := uint(1 << uint(buf[pos]&0xf))
			pos++
			valueBytes := buf[pos : pos+readAhead]
			valueBytes = append(make([]byte, 8), valueBytes...)
			valueSize := uint(binary.BigEndian.Uint64(valueBytes[len(valueBytes)-8:]))
			pos = pos + readAhead
			result[name] = fmt.Sprintf("%s", buf[pos:pos+valueSize])
			pos = pos + valueSize
		case 0xa: // tiny-map
			value, n, err := ParseTinyMap(buf[pos:])
			if err != nil {
				return nil, pos, err
			}
			result[name] = value
			pos = pos + n
		default:
			return nil, pos, errors.New("found non-tiny-string or non-string value in tiny map!")
		}
	}

	return result, pos, nil
}
