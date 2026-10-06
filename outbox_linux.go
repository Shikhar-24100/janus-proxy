package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

func lockOutbox(path string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(path, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.New("cannot open usage outbox lock")
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("usage outbox is already owned by another process")
	}
	return f, nil
}

func syncOutboxDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
