package main

type BoltMsg string

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
