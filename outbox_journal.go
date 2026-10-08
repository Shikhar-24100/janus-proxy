package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"
)

const outboxBatchSize = 32
const outboxBatchWait = time.Millisecond
const outboxHeaderSize = 12

type outboxRecord struct {
	Put json.RawMessage `json:"put,omitempty"`
	Ack string          `json:"ack,omitempty"`
}

// JNS1 + uint32 payload length + CRC32(payload) + JSON payload.
func encodeOutboxFrame(record outboxRecord) []byte {
	body, _ := json.Marshal(record)
	frame := make([]byte, outboxHeaderSize+len(body))
	copy(frame, "JNS1")
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(body)))
	binary.BigEndian.PutUint32(frame[8:12], crc32.ChecksumIEEE(body))
	copy(frame[outboxHeaderSize:], body)
	return frame
}

func (o *usageOutbox) loadJournal() error {
	r := bufio.NewReader(o.journal)
	for {
		header := make([]byte, outboxHeaderSize)
		_, err := io.ReadFull(r, header)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return o.repairTail()
		}
		if err != nil {
			return errors.New("cannot read usage journal")
		}
		size := binary.BigEndian.Uint32(header[4:8])
		if string(header[:4]) != "JNS1" || size == 0 || size > maxOutboxEventBytes+256 {
			return errors.New("invalid usage journal frame; retained for inspection")
		}
		body := make([]byte, size)
		if _, err := io.ReadFull(r, body); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return o.repairTail()
		} else if err != nil {
			return errors.New("cannot read usage journal payload")
		}
		if crc32.ChecksumIEEE(body) != binary.BigEndian.Uint32(header[8:12]) {
			return errors.New("usage journal checksum mismatch; retained for inspection")
		}
		var record outboxRecord
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&record) != nil || decoder.Decode(new(any)) != io.EOF || (len(record.Put) > 0) == (record.Ack != "") {
			return errors.New("invalid usage journal operation")
		}
		if len(record.Put) > 0 {
			event, err := decodeUsageEvent(string(record.Put))
			if err != nil {
				return errors.New("invalid usage journal event")
			}
			data, _ := json.Marshal(event)
			if old, exists := o.records[event.RequestID]; exists && !bytes.Equal(old, data) {
				return errors.New("conflicting usage journal request ID")
			}
			o.records[event.RequestID] = data
		} else {
			if len(record.Ack) > 128 {
				return errors.New("invalid usage journal acknowledgement")
			}
			delete(o.records, record.Ack)
		}
		o.offset += int64(outboxHeaderSize) + int64(size)
	}
	_, err := o.journal.Seek(o.offset, io.SeekStart)
	return err
}

// Short trailing headers/payloads are repaired; complete corrupt frames fail closed.
func (o *usageOutbox) repairTail() error {
	if err := o.journal.Truncate(o.offset); err != nil {
		return errors.New("cannot truncate incomplete usage journal tail")
	}
	if _, err := o.journal.Seek(o.offset, io.SeekStart); err != nil {
		return err
	}
	if err := o.flush(o.journal); err != nil {
		return err
	}
	o.repaired.Add(1)
	return nil
}

func (o *usageOutbox) flush(file *os.File) error {
	if err := o.syncFile(file); err != nil {
		return err
	}
	o.syncs.Add(1)
	return nil
}

func (o *usageOutbox) runWriter() {
	defer close(o.done)
	for {
		var first outboxCommand
		select {
		case <-o.stop:
			return
		case first = <-o.queue:
		}
		batch := []outboxCommand{first}
		timer := time.NewTimer(outboxBatchWait)
	collect:
		for len(batch) < outboxBatchSize {
			select {
			case cmd := <-o.queue:
				batch = append(batch, cmd)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		o.commitBatch(batch)
	}
}

func (o *usageOutbox) commitBatch(batch []outboxCommand) {
	changes := make(map[string]json.RawMessage)
	results := make([]error, len(batch))
	var frames bytes.Buffer
	putCount, operationCount := uint64(0), uint64(0)
	o.mu.Lock()
	count := len(o.records)
	for index, cmd := range batch {
		previous, changed := changes[cmd.id]
		if !changed {
			previous = o.records[cmd.id]
		}
		if cmd.data != nil {
			if previous != nil {
				if !bytes.Equal(previous, cmd.data) {
					results[index] = errors.New("conflicting usage outbox request ID")
				}
				continue
			}
			if count >= o.limit {
				results[index] = errors.New("usage outbox is full")
				continue
			}
			frames.Write(encodeOutboxFrame(outboxRecord{Put: cmd.data}))
			changes[cmd.id] = cmd.data
			count++
			putCount++
		} else {
			if previous == nil {
				continue
			}
			frames.Write(encodeOutboxFrame(outboxRecord{Ack: cmd.id}))
			changes[cmd.id] = nil
			count--
		}
		operationCount++
	}
	liveBytes := int64(0)
	for _, data := range o.records {
		liveBytes += int64(len(data) + outboxHeaderSize + 16)
	}
	o.mu.Unlock()
	var err error
	if o.rollback {
		err = o.journal.Truncate(o.offset)
		if err == nil {
			_, err = o.journal.Seek(o.offset, io.SeekStart)
		}
		if err == nil {
			err = o.flush(o.journal)
		}
		if err == nil {
			o.rollback = false
			o.journalBytes.Store(o.offset)
		}
	}
	if err == nil && o.directoryPending {
		err = o.syncDir(o.dir)
		if err == nil {
			o.directoryPending = false
			o.compactions.Add(1)
		}
	}
	if err == nil && frames.Len() > 0 && o.offset+int64(frames.Len()) > o.compactAt && o.offset > 2*liveBytes {
		err = o.compact()
	}
	if err == nil && frames.Len() > 0 {
		data := frames.Bytes()
		var n int
		n, err = o.writeFile(o.journal, data)
		o.journalBytes.Store(o.offset + int64(n))
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		if err == nil {
			err = o.flush(o.journal)
		}
		if err != nil {
			o.rollback = true
		} else {
			o.offset += int64(len(data))
			o.batches.Add(1)
			o.operations.Add(operationCount)
			o.writes.Add(putCount)
		}
	}
	o.mu.Lock()
	o.blocked = err != nil
	if err == nil {
		for id, data := range changes {
			if data == nil {
				delete(o.records, id)
			} else {
				o.records[id] = data
			}
		}
	}
	o.mu.Unlock()
	for index, cmd := range batch {
		result := results[index]
		if result == nil {
			result = err
		}
		if result != nil {
			o.errors.Add(1)
		}
		cmd.done <- result
	}
}

// Both sides of a crash during rename describe the same live state.
func (o *usageOutbox) compact() (err error) {
	f, err := os.CreateTemp(o.dir, ".journal-*.tmp")
	if err != nil {
		return err
	}
	installed := false
	defer func() {
		if !installed {
			f.Close()
		}
		os.Remove(f.Name())
	}()
	o.mu.Lock()
	var frames bytes.Buffer
	for _, data := range o.records {
		frames.Write(encodeOutboxFrame(outboxRecord{Put: data}))
	}
	o.mu.Unlock()
	if n, writeErr := f.Write(frames.Bytes()); writeErr != nil {
		return writeErr
	} else if n != frames.Len() {
		return io.ErrShortWrite
	}
	if err = o.flush(f); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(o.dir, outboxJournalName)); err != nil {
		return err
	}
	old := o.journal
	o.journal = f
	o.offset = int64(frames.Len())
	o.journalBytes.Store(o.offset)
	o.directoryPending = true
	installed = true
	old.Close()
	if err = o.syncDir(o.dir); err != nil {
		return err
	}
	o.directoryPending = false
	o.compactions.Add(1)
	return nil
}
