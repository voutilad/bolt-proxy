package bolt

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
)

type Message struct {
	T    Type
	Data []byte
}

type Type string

const (
	ResetMsg    Type = "RESET"
	RunMsg           = "RUN"
	DiscardMsg       = "DISCARD"
	PullMsg          = "PULL"
	RecordMsg        = "RECORD"
	SuccessMsg       = "SUCCESS"
	IgnoreMsg        = "IGNORE"
	FailureMsg       = "FAILURE"
	HelloMsg         = "HELLO"
	GoodbyeMsg       = "GOODBYE"
	BeginMsg         = "BEGIN"
	CommitMsg        = "COMMIT"
	RollbackMsg      = "ROLLBACK"
	UnknownMsg       = "?UNKNOWN?"
	NopMsg           = "NOP"
	ChunkedMsg       = "CHUNKED" // not a true bolt message
)

// Parse a byte into the corresponding Bolt message Type
func TypeFromByte(b byte) Type {
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

func LogMessage(who string, msg *Message) {
	end := 64
	suffix := fmt.Sprintf("...+%d bytes", len(msg.Data))
	if len(msg.Data) < 64 {
		end = len(msg.Data)
		suffix = ""
	}
	switch msg.T {
	case BeginMsg, FailureMsg:
		log.Printf("[%s] <%s>: %#v\n%s\n", who, msg.T, msg.Data, msg.Data)
	default:
		log.Printf("[%s] <%s>: %#v%s\n", who, msg.T, msg.Data[:end], suffix)
	}
}

func LogMessages(who string, messages []*Message) {
	for _, msg := range messages {
		LogMessage(who, msg)
	}
}

// Try to extract the BoltMsg from some given bytes.
func IdentifyType(buf []byte) Type {

	// If the byte array is too small, it could be an empty message
	if len(buf) < 4 {
		return NopMsg
	}

	return TypeFromByte(buf[3])
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
func ParseTinyMap(buf []byte) (map[string]interface{}, int, error) {
	// fmt.Printf("tinymap debug: %#v\n", buf)
	if buf == nil {
		panic("cannot parse nil byte array for structs")
	}

	result := make(map[string]interface{})

	if len(buf) < 1 {
		return result, 0, errors.New("bytes empty, cannot parse struct")
	}

	pos := 0
	if buf[pos]>>4 != 0xa {
		fmt.Sprintf("XXX: not a tiny map buf[pos] = %#v\n", buf[pos])
		return result, pos, errors.New("bytes missing tiny-map prefix of 0xa")
	}

	numMembers := int(buf[pos] & 0xf)
	pos++

	//	fmt.Printf("XXX DEBUG numMembers: %d\n", numMembers)
	for i := 0; i < numMembers; i++ {
		//		fmt.Printf("XXX DEBUG i = %d, pos = %d\n", i, pos)
		// map keys are tiny-strings typically
		name, n, err := ParseTinyString(buf[pos:])
		if err != nil {
			panic(err)
		}
		pos = pos + n

		// now for the value
		switch buf[pos] >> 4 {
		case 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7: // tiny-int
			val, err := ParseTinyInt(buf[pos])
			if err != nil {
				panic(err)
				return result, pos, err
			}
			result[name] = val
			pos++
		case 0x8: // tiny-string
			val, n, err := ParseTinyString(buf[pos:])
			if err != nil {
				panic(err)
				return result, pos, err
			}
			result[name] = val
			pos = pos + n
		case 0x9: // tiny-array
			val, n, err := ParseTinyArray(buf[pos:])
			if err != nil {
				panic(err)
				return result, pos, err
			}
			//		log.Printf("DEBUG tiny-array: n=%d, val=%v\n", n, val)
			result[name] = val
			pos = pos + n
		case 0xd: // string
			val, n, err := ParseString(buf[pos:])
			if err != nil {
				panic(err)
				return result, pos, err
			}
			result[name] = val
			pos = pos + n
		case 0xa: // tiny-map
			value, n, err := ParseTinyMap(buf[pos:])
			if err != nil {
				panic(err)
				return nil, pos, err
			}
			result[name] = value
			pos = pos + n
		case 0xc: // floats, nil, and bools
			nib := int(buf[pos] & 0xf)
			switch nib {
			case 0: // packed nil/null
				result[name] = nil
				pos++
			case 1: // packed float
				panic("can't do floats yet")
			case 2:
				result[name] = false
				pos++
			case 3:
				result[name] = true
				pos++
			}
		default:
			errMsg := fmt.Sprintf("found unsupported encoding type: %#v\n", buf[pos])
			return nil, pos, errors.New(errMsg)
		}
	}

	return result, pos, nil
}

// Parse a TinyInt...which is a simply 7-bit number.
func ParseTinyInt(b byte) (int, error) {
	if b > 0x7f {
		return 0, errors.New("expected tiny-int!")
	}
	return int(b), nil
}

// Parse a packed Int
func ParseInt(buf []byte) (int, int, error) {
	if len(buf) < 2 || buf[0]>>4 != 0xc {
		return 0, 0, errors.New("can't parse int, invalid byte buf")
	}

	var i, n int

	switch buf[0] {
	case 0xc8:
		i = int(int8(buf[1]))
		n = 2
	case 0xc9:
		i = int(int16(binary.BigEndian.Uint16(buf[1:3])))
		n = 3
	case 0xca:
		i = int(int32(binary.BigEndian.Uint32(buf[1:5])))
		n = 5
	case 0xcb:
		i = int(int64(binary.BigEndian.Uint64(buf[1:9])))
		n = 9
	}

	return i, n, nil
}

// Parse a TinyString from a byte slice, returning the string (if valid) and
// the number of bytes processed from the slice (including the 0x80 prefix).
//
// Otherwise, return an empty string, 0, and an error.
func ParseTinyString(buf []byte) (string, int, error) {
	//	fmt.Printf("DEBUG: ParseTinyString: %#v\n", buf)
	if len(buf) == 0 || buf[0]>>4 != 0x8 {
		return "", 0, errors.New("expected tiny-string!")
	}

	size := int(buf[0] & 0xf)
	if size == 0 {
		return "", 1, nil
	}

	return fmt.Sprintf("%s", buf[1:size+1]), size + 1, nil
}

func ParseString(buf []byte) (string, int, error) {
	if len(buf) < 1 || buf[0]>>4 != 0xd {
		return "", 0, errors.New("expected string!")
	}
	pos := 0

	// how many bytes is the encoding for the string length?
	readAhead := int(1 << int(buf[pos]&0xf))
	pos++

	// decode the amount of bytes to read to get the string length
	sizeBytes := buf[pos : pos+readAhead]
	sizeBytes = append(make([]byte, 8), sizeBytes...)
	pos = pos + readAhead

	// decode the actual string length
	size := int(binary.BigEndian.Uint64(sizeBytes[len(sizeBytes)-8:]))
	return fmt.Sprintf("%s", buf[pos:pos+size]), pos + size, nil
}

func ParseTinyArray(buf []byte) ([]interface{}, int, error) {
	if buf[0]>>4 != 0x9 {
		return nil, 0, errors.New("expected tiny-array")
	}
	size := int(buf[0] & 0xf)
	array := make([]interface{}, size)
	pos := 1

	for i := 0; i < size; i++ {
		memberType := buf[pos] >> 4
		switch memberType {
		case 0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7: // tiny-int
			val, err := ParseTinyInt(buf[pos])
			if err != nil {
				return array, pos, err
			}
			array[i] = val
			pos++
		case 0x8: // tiny-string
			val, n, err := ParseTinyString(buf[pos:])
			if err != nil {
				return array, pos, err
			}
			array[i] = val
			pos = pos + n
		case 0xc:
			nib := int(buf[pos] & 0xf)
			switch nib {
			case 0: // packed nil/null
				array[i] = nil
				pos++
			case 1: // packed float
				panic("can't do floats yet")
			case 2:
				array[i] = false
				pos++
			case 3:
				array[i] = true
				pos++
			case 0x8, 0x9, 0xa, 0xb:
				val, n, err := ParseInt(buf[pos:])
				if err != nil {
					return array, pos, err
				}
				array[i] = val
				pos = pos + n
			}
		case 0xd: // regular string
			val, n, err := ParseString(buf[pos:])
			if err != nil {
				return array, pos, err
			}
			array[i] = val
			pos = pos + n
		default:
			errMsg := fmt.Sprintf("found unsupported encoding type: %#v", memberType)
			return array, pos, errors.New(errMsg)
		}
	}

	return array, pos, nil
}
