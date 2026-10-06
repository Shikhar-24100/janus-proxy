package main

import (
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

// Only accounting metadata lives here: no prompts, responses, or API keys.
// A single process owns a directory; each replica needs its own persistent volume.
type usageOutbox struct {
	mu              sync.Mutex
	stripes         [64]sync.Mutex
	dir             string
	lock            *os.File
	files           map[string]int64
	reserved, limit int
	blocked         bool
	recovered       []requestEvent
	writes          atomic.Uint64
	errors          atomic.Uint64
	replayed        atomic.Uint64
	// Injectable filesystem boundaries let tests exercise disk/flush failures.
	syncDir func(string) error
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
	// Compose pre-creates this path inside a persistent Linux volume. Also flush
	// its parent when opening so a newly created final directory is recorded.
	if os.IsNotExist(before) {
		if err := syncOutboxDir(filepath.Dir(path)); err != nil {
			return nil, errors.New("cannot flush usage outbox parent; durable outbox requires Linux")
		}
	}
	lock, err := lockOutbox(path)
	if err != nil {
		return nil, err
	}
	o := &usageOutbox{dir: path, lock: lock, files: make(map[string]int64), limit: limitMB * (1 << 20) / maxOutboxEventBytes, syncDir: syncOutboxDir}
	ok := false
	defer func() {
		if !ok {
			o.close()
		}
	}()
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, errors.New("cannot read usage outbox")
	}
	for _, entry := range entries {
		name := entry.Name()
		if name == ".lock" {
			continue
		}
		if strings.HasPrefix(name, ".event-") && strings.HasSuffix(name, ".tmp") {
			if err := os.Remove(filepath.Join(path, name)); err != nil {
				return nil, errors.New("cannot clean uncommitted outbox file")
			}
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() || info.Size() > maxOutboxEventBytes || !strings.HasSuffix(name, ".json") {
			return nil, errors.New("unexpected or oversized usage outbox file; inspect before restarting")
		}
		data, err := os.ReadFile(filepath.Join(path, name))
		if err != nil {
			return nil, errors.New("cannot read usage outbox event")
		}
		event, err := decodeUsageEvent(string(data))
		if err != nil || name != outboxName(event.RequestID) {
			return nil, errors.New("corrupt usage outbox event; retained for inspection")
		}
		o.files[name] = info.Size()
		o.recovered = append(o.recovered, event)
	}
	// Existing backlog is replayed even if a reduced configuration is now full.
	if err := o.syncDir(path); err != nil {
		return nil, errors.New("cannot flush usage outbox directory")
	}
	ok = true
	return o, nil
}

func outboxName(id string) string {
	hash := sha256.Sum256([]byte(id))
	return hex.EncodeToString(hash[:]) + ".json"
}

func (o *usageOutbox) reserve() (func(), bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.blocked || len(o.files)+o.reserved >= o.limit {
		return nil, false
	}
	o.reserved++
	var once sync.Once
	return func() { once.Do(func() { o.mu.Lock(); o.reserved--; o.mu.Unlock() }) }, true
}

func (o *usageOutbox) put(event requestEvent) (err error) {
	data, err := json.Marshal(event)
	if err != nil || len(data) > maxOutboxEventBytes || validateUsageEvent(event) != nil {
		return errors.New("invalid or oversized outbox event")
	}
	// Serialize the same request ID, but let independent files flush concurrently.
	// Holding the registry lock across fsync would queue every request behind disk.
	hash := sha256.Sum256([]byte(event.RequestID))
	stripe := &o.stripes[int(hash[0])%len(o.stripes)]
	stripe.Lock()
	defer stripe.Unlock()
	defer func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		if err != nil {
			o.blocked = true
			o.errors.Add(1)
		} else {
			o.blocked = false
		}
	}()
	name := outboxName(event.RequestID)
	o.mu.Lock()
	if _, exists := o.files[name]; exists {
		o.mu.Unlock()
		// A previous directory flush may have failed after rename. Confirm it now.
		return o.syncDir(o.dir)
	}
	if len(o.files) >= o.limit {
		o.mu.Unlock()
		return errors.New("usage outbox is full")
	}
	// Reserve the filename before I/O so concurrent publishers cannot exceed capacity.
	o.files[name] = 0
	o.mu.Unlock()
	renamed := false
	defer func() {
		if !renamed {
			o.mu.Lock()
			delete(o.files, name)
			o.mu.Unlock()
		}
	}()
	f, err := os.CreateTemp(o.dir, ".event-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() { f.Close(); os.Remove(tmp) }()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, filepath.Join(o.dir, name)); err != nil {
		return err
	}
	renamed = true
	o.mu.Lock()
	o.files[name] = int64(len(data))
	o.mu.Unlock()
	if err = o.syncDir(o.dir); err != nil {
		return err
	}
	o.writes.Add(1)
	return nil
}

func (o *usageOutbox) remove(id string) (err error) {
	hash := sha256.Sum256([]byte(id))
	stripe := &o.stripes[int(hash[0])%len(o.stripes)]
	stripe.Lock()
	defer stripe.Unlock()
	defer func() {
		if err != nil {
			o.mu.Lock()
			o.blocked = true
			o.mu.Unlock()
			o.errors.Add(1)
		}
	}()
	name := outboxName(id)
	if err = os.Remove(filepath.Join(o.dir, name)); err != nil && !os.IsNotExist(err) {
		return err
	}
	// Do not mark removal complete until its directory entry is durably deleted.
	if err = o.syncDir(o.dir); err != nil {
		return err
	}
	o.mu.Lock()
	delete(o.files, name)
	o.mu.Unlock()
	return nil
}

func (o *usageOutbox) snapshot() (records, bytes, reserved, capacity int64, blocked bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, size := range o.files {
		bytes += size
	}
	return int64(len(o.files)), bytes, int64(o.reserved), int64(o.limit * maxOutboxEventBytes), o.blocked
}

func (o *usageOutbox) close() {
	if o.lock != nil {
		o.lock.Close()
	}
}
