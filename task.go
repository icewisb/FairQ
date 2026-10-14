// Package fairq defines tasks and the public queue contracts.
package fairq

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Task struct {
	ID      string
	Type    string
	Payload []byte
}

type EnqueueOptions struct {
	GroupKey  string
	MaxRetry  int           // Zero selects the default (25).
	Timeout   time.Duration // Zero means no handler deadline.
	Retention time.Duration // Zero deletes successful tasks.
}

type TaskInfo struct {
	Task
	GroupKey     string
	State        string
	RetryCount   int
	MaxRetry     int
	Timeout      time.Duration
	Retention    time.Duration
	EnqueuedAt   time.Time
	PendingSince time.Time
	StartedAt    time.Time
	FinishedAt   time.Time
	Error        string
}

type Handler func(context.Context, Task) error

type Code string

const (
	Empty            Code = "EMPTY"
	TaskIDConflict   Code = "TASK_ID_CONFLICT"
	CorruptReference Code = "CORRUPT_REFERENCE"
	StateConflict    Code = "STATE_CONFLICT"
	LeaseExpired     Code = "LEASE_EXPIRED"
	DecodeFailed     Code = "DECODE_FAILED"
	Internal         Code = "INTERNAL"
)

type Error struct {
	Code Code
	Op   string
	Err  error
}

func (e *Error) Error() string         { return fmt.Sprintf("fairq %s: %s", e.Op, e.Code) }
func (e *Error) Unwrap() error         { return e.Err }
func IsCode(err error, code Code) bool { var e *Error; return errors.As(err, &e) && e.Code == code }

// ErrTaskNotFound is a read result, not a Lua transition error code.
var ErrTaskNotFound = errors.New("fairq: task not found")
