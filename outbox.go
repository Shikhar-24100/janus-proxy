package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const maxOutboxEventBytes = 16 << 10
const outboxJournalName = "journal.v1"

// The writer owns the journal. mu protects the live index and reservations,
// and is never held across disk I/O. Each replica owns its own directory.
type usageOutbox struct {
	mu                                                sync.Mutex
	dir                                               string
	lock, journal                                     *os.File
	records                                           map[string]json.RawMessage
	reserved, limit                                   int
	blocked                                           bool
	recovered                                         []requestEvent
	writes, errors, replayed                          atomic.Uint64
	batches, operations, syncs, compactions, repaired atomic.Uint64
	journalBytes                                      atomic.Int64
	offset, compactAt                                 int64
	rollback, directoryPending                        bool
	queue                                             chan outboxCommand
	stop, done                                        chan struct{}
	lifeMu                                            sync.Mutex
	closed                                            bool
	active                                            sync.WaitGroup
	closeOnce                                         sync.Once
	// Injectable boundaries for failure tests.
	syncFile  func(*os.File) error
	syncDir   func(string) error
	writeFile func(*os.File, []byte) (int, error)
}

type outboxCommand struct {
	id   string
	data json.RawMessage // nil means Redis has confirmed delivery.
	done chan error
}

func openUsageOutbox(path, sizeMB string) (*usageOutbox, error) {
	limitMB := 32
	if sizeMB != "" {
		n, err := strconv.Atoi(sizeMB)
		if err != nil || n < 1 || n > 1024 {
			return nil, errors.New("USAGE_OUTBOX_MAX_MB must be between 1 and 1024")
		}
		limitMB = n
	}
	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		return nil, errors.New("usage outbox parent directory must already exist")
	}
	_, before := os.Stat(path)
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, errors.New("cannot create usage outbox directory")
	}
	if os.IsNotExist(before) {
		if err := syncOutboxDir(filepath.Dir(path)); err != nil {
			return nil, errors.New("cannot flush usage outbox parent; durable outbox requires Linux")
		}
	}
	lock, err := lockOutbox(path)
	if err != nil {
		return nil, err
	}
	o := &usageOutbox{dir: path, lock: lock, records: make(map[string]json.RawMessage), limit: limitMB * (1 << 20) / maxOutboxEventBytes,
		compactAt: int64(min(limitMB, 4)) << 20, syncFile: func(f *os.File) error { return f.Sync() }, syncDir: syncOutboxDir,
		writeFile: func(f *os.File, data []byte) (int, error) { return f.Write(data) }}
	ok := false
	defer func() {
		if !ok {
			o.close()
		}
	}()
	legacy, err := o.readLegacy()
	if err != nil {
		return nil, err
	}
	o.journal, err = os.OpenFile(filepath.Join(path, outboxJournalName), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, errors.New("cannot open usage journal")
	}
	if err := o.loadJournal(); err != nil {
		return nil, err
	}
	// Persist imports before deleting old files. Repeated migration is safe:
	// PostgreSQL deduplicates original IDs even if a delivery is repeated.
	for id, data := range legacy {
		if old, exists := o.records[id]; exists {
			if !bytes.Equal(old, data) {
				return nil, errors.New("conflicting legacy and journal usage records")
			}
			continue
		}
		frame := encodeOutboxFrame(outboxRecord{Put: data})
		if n, err := o.journal.Write(frame); err != nil || n != len(frame) {
			return nil, errors.New("cannot migrate legacy usage event")
		}
		o.offset += int64(len(frame))
		o.records[id] = data
	}
	if err := o.flush(o.journal); err != nil {
		return nil, errors.New("cannot flush usage journal")
	}
	if err := o.syncDir(path); err != nil {
		return nil, errors.New("cannot flush usage journal directory")
	}
	for id := range legacy {
		if err := os.Remove(filepath.Join(path, outboxName(id))); err != nil {
			return nil, errors.New("cannot remove migrated usage file")
		}
	}
	if len(legacy) > 0 {
		if err := o.syncDir(path); err != nil {
			return nil, errors.New("cannot confirm legacy file removal")
		}
	}
	for _, data := range o.records {
		event, _ := decodeUsageEvent(string(data))
		o.recovered = append(o.recovered, event)
	}
	o.journalBytes.Store(o.offset)
	o.queue, o.stop, o.done = make(chan outboxCommand, 64), make(chan struct{}), make(chan struct{})
	go o.runWriter()
	ok = true
	return o, nil
}

func (o *usageOutbox) readLegacy() (map[string]json.RawMessage, error) {
	entries, err := os.ReadDir(o.dir)
	if err != nil {
		return nil, errors.New("cannot read usage outbox")
	}
	legacy := make(map[string]json.RawMessage)
	for _, entry := range entries {
		name := entry.Name()
		if name == ".lock" {
			continue
		}
		if (strings.HasPrefix(name, ".event-") || strings.HasPrefix(name, ".journal-")) && strings.HasSuffix(name, ".tmp") {
			if err := os.Remove(filepath.Join(o.dir, name)); err != nil {
				return nil, errors.New("cannot clean uncommitted usage file")
			}
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			return nil, errors.New("unexpected usage outbox entry")
		}
		if name == outboxJournalName {
			continue
		}
		if info.Size() > maxOutboxEventBytes || !strings.HasSuffix(name, ".json") {
			return nil, errors.New("unexpected or oversized usage outbox file")
		}
		data, err := os.ReadFile(filepath.Join(o.dir, name))
		if err != nil {
			return nil, errors.New("cannot read legacy usage event")
		}
		event, err := decodeUsageEvent(string(data))
		if err != nil || name != outboxName(event.RequestID) {
			return nil, errors.New("corrupt legacy usage event; retained for inspection")
		}
		canonical, _ := json.Marshal(event)
		legacy[event.RequestID] = canonical
	}
	return legacy, nil
}

func outboxName(id string) string {
	hash := sha256.Sum256([]byte(id))
	return hex.EncodeToString(hash[:]) + ".json"
}

func (o *usageOutbox) reserve() (func(), bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.blocked || len(o.records)+o.reserved >= o.limit {
		return nil, false
	}
	o.reserved++
	var once sync.Once
	return func() { once.Do(func() { o.mu.Lock(); o.reserved--; o.mu.Unlock() }) }, true
}

func (o *usageOutbox) put(event requestEvent) error {
	data, err := json.Marshal(event)
	if err != nil || len(data) > maxOutboxEventBytes || validateUsageEvent(event) != nil {
		return errors.New("invalid or oversized outbox event")
	}
	return o.submit(outboxCommand{id: event.RequestID, data: data, done: make(chan error, 1)})
}

func (o *usageOutbox) remove(id string) error {
	return o.submit(outboxCommand{id: id, done: make(chan error, 1)})
}

func (o *usageOutbox) submit(cmd outboxCommand) error {
	o.lifeMu.Lock()
	if o.closed {
		o.lifeMu.Unlock()
		return errors.New("usage outbox is closed")
	}
	o.active.Add(1)
	o.lifeMu.Unlock()
	defer o.active.Done()
	o.queue <- cmd
	return <-cmd.done
}

func (o *usageOutbox) snapshot() (records, bytes, reserved, capacity int64, blocked bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, data := range o.records {
		bytes += int64(len(data))
	}
	return int64(len(o.records)), bytes, int64(o.reserved), int64(o.limit * maxOutboxEventBytes), o.blocked
}

func (o *usageOutbox) close() {
	o.closeOnce.Do(func() {
		o.lifeMu.Lock()
		o.closed = true
		o.lifeMu.Unlock()
		o.active.Wait()
		if o.done != nil {
			close(o.stop)
			<-o.done
		}
		if o.journal != nil {
			o.journal.Close()
		}
		if o.lock != nil {
			o.lock.Close()
		}
	})
}
