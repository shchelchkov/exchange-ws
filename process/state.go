package process

import "strconv"

type StreamState int32

const (
	StateStopped StreamState = iota
	StateConnecting
	StateConnected
	StateReconnecting
)

func (s StreamState) String() string {
	switch s {
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	default:
		return "stopped"
	}
}

func (s StreamState) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(s.String())), nil
}
