//go:build !linux

package gateway

import (
	"errors"
	"os"
)

func lockOutbox(string) (*os.File, error) {
	return nil, errors.New("durable usage outbox requires Linux; use the Linux container stack")
}

func syncOutboxDir(string) error { return errors.New("durable outbox requires Linux") }
