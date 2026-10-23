// Package chat holds the RPC contract shared by the chat server and client.
package chat

import (
	"errors"
	"fmt"
	"time"
	"unicode"
	"unicode/utf8"
)

// ServiceName is the name the server registers its RPC service under.
const ServiceName = "Chat"

const (
	MaxUsernameLen = 20
	MaxMessageLen  = 1000
)

// Error strings travel over RPC as plain text; clients match on them.
const (
	ErrUsernameTaken  = "username already taken"
	ErrInvalidSession = "not joined (invalid or expired session)"
	ErrServerClosing  = "server is shutting down"
	ErrEmptyMessage   = "message is empty"
	ErrMessageTooLong = "message too long"
)

// EventType says what an Event means.
type EventType string

const (
	EventMessage  EventType = "message"
	EventJoin     EventType = "join"
	EventLeave    EventType = "leave"
	EventShutdown EventType = "shutdown"
)

// Event is one item a client receives when it polls.
type Event struct {
	Type EventType
	From string // sender (message) or subject (join/leave)
	Text string // message body, or extra detail for leave/shutdown
	Time time.Time
}

type Empty struct{}

type JoinArgs struct{ Username string }

type JoinReply struct {
	Token string   // session token; required for every later call
	Users []string // everyone online, including the caller
}

type SendArgs struct {
	Token string
	Text  string
}

type PollArgs struct{ Token string }

type PollReply struct{ Events []Event }

type UsersArgs struct{ Token string }

type UsersReply struct{ Users []string }

type LeaveArgs struct{ Token string }

// ValidateUsername checks the shared username rules.
func ValidateUsername(name string) error {
	n := utf8.RuneCountInString(name)
	if n == 0 {
		return errors.New("username must not be empty")
	}
	if n > MaxUsernameLen {
		return fmt.Errorf("username must be at most %d characters", MaxUsernameLen)
	}
	for _, r := range name {
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r) ||
			r == '_' || r == '-' || r == '.') {
			return errors.New("username may contain only letters, digits, '_', '-' and '.'")
		}
	}
	return nil
}
