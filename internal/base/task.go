package base

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"fairq"
)

const MaxPayload = 256 * 1024
const MaxExactInteger = 1<<53 - 1

type Message struct {
	Type    string `json:"type"`
	Payload []byte `json:"payload"`
}

func Group(key string) (string, error) {
	if key == "" || len(key) > 256 || !utf8.ValidString(key) {
		return "", fmt.Errorf("fairq: GroupKey must contain 1..256 valid UTF-8 bytes")
	}
	return base64.RawURLEncoding.EncodeToString([]byte(key)), nil
}

func Validate(task fairq.Task, opts fairq.EnqueueOptions, limit int) (string, []byte, error) {
	group, err := Group(opts.GroupKey)
	if err != nil {
		return "", nil, err
	}
	if task.ID == "" || !utf8.ValidString(task.ID) || task.Type == "" || !utf8.ValidString(task.Type) {
		return "", nil, fmt.Errorf("fairq: ID and Type must be nonempty UTF-8")
	}
	if len(task.Payload) > limit {
		return "", nil, fmt.Errorf("fairq: payload exceeds %d bytes", limit)
	}
	if opts.MaxRetry < 0 || int64(opts.MaxRetry) >= MaxExactInteger {
		return "", nil, fmt.Errorf("fairq: invalid MaxRetry")
	}
	for _, d := range []time.Duration{opts.Timeout, opts.Retention} {
		if d < 0 || d%time.Millisecond != 0 {
			return "", nil, fmt.Errorf("fairq: durations must be nonnegative integer milliseconds")
		}
	}
	msg, err := json.Marshal(Message{task.Type, task.Payload})
	return group, msg, err
}

func Decode(msg []byte) (Message, error) {
	var m Message
	err := json.Unmarshal(msg, &m)
	if err == nil && (m.Type == "" || !utf8.ValidString(m.Type)) {
		err = fmt.Errorf("invalid task type")
	}
	return m, err
}
