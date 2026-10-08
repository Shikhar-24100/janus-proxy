package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func outboxFixture(t *testing.T) *usageOutbox {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("durable outbox requires Linux")
	}
	o, err := openUsageOutbox(t.TempDir(), "1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.close)
	return o
}

func TestOutboxCapacityAndLock(t *testing.T) {
	o := outboxFixture(t)
	o.limit = 1
	release, ok := o.reserve()
	if !ok {
		t.Fatal("first reservation rejected")
	}
	if _, ok := o.reserve(); ok {
		t.Fatal("overbooked disk capacity")
	}
	event := sampleUsageEvent()
	if err := o.put(event); err != nil {
		t.Fatal(err)
	}
	release()
	release()
	if _, ok := o.reserve(); ok {
		t.Fatal("backlog capacity was ignored")
	}
	if other, err := openUsageOutbox(o.dir, "1"); err == nil {
		other.close()
		t.Fatal("two processes can own the same outbox")
	}
	if err := o.remove(event.RequestID); err != nil {
		t.Fatal(err)
	}
	release, ok = o.reserve()
	if !ok {
		t.Fatal("removed event did not free capacity")
	}
	release()
}

func TestOutboxFullRejectsBeforeProvider(t *testing.T) {
	o := outboxFixture(t)
	o.limit = 1
	if err := o.put(sampleUsageEvent()); err != nil {
		t.Fatal(err)
	}
	metrics := newTelemetry(nil)
	metrics.usage = &usagePipeline{outbox: o}
	calls := 0
	h := metrics.observe(requireAPIKey("key", metrics.admit(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls++ }))))
	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("Authorization", "Bearer key")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 503 || calls != 0 || metrics.admission.active.Load() != 0 {
		t.Fatal("full outbox admitted a request")
	}
}

func TestOutboxFlushFailureDoesNotEnqueue(t *testing.T) {
	o := outboxFixture(t)
	p := queueFixture(t, &failingUsageStore{})
	p.outbox = o
	o.syncFile = func(*os.File) error { return errors.New("disk flush failed") }
	event := sampleUsageEvent()
	if err := p.handoff(context.Background(), event); err == nil {
		t.Fatal("unflushed record was accepted")
	}
	if p.client.XLen(context.Background(), p.stream).Val() != 0 {
		t.Fatal("Redis received an event before durable local write")
	}
	if _, ok := o.reserve(); ok {
		t.Fatal("disk failure did not pause admission")
	}
	o.syncFile = func(f *os.File) error { return f.Sync() }
	if err := p.handoff(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	n, _, _, _, blocked := o.snapshot()
	if n != 0 || blocked {
		t.Fatal("recovery left outbox blocked")
	}
}

func TestOutboxCorruptionStopsStartup(t *testing.T) {
	o := outboxFixture(t)
	path := o.dir
	o.close()
	if err := os.WriteFile(filepath.Join(path, "corrupt.json"), []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if next, err := openUsageOutbox(path, "1"); err == nil {
		next.close()
		t.Fatal("corrupt accounting was silently skipped")
	}
	if _, err := os.Stat(filepath.Join(path, "corrupt.json")); err != nil {
		t.Fatal("corrupt record discarded")
	}
}

func TestOutboxSlowFlushDoesNotSpendRedisBudget(t *testing.T) {
	o := outboxFixture(t)
	p := queueFixture(t, &failingUsageStore{})
	p.outbox = o
	o.syncFile = func(f *os.File) error {
		time.Sleep(250 * time.Millisecond)
		return f.Sync()
	}
	if err := p.handoff(context.Background(), sampleUsageEvent()); err != nil {
		t.Fatal("local flush spent Redis's separate timeout budget:", err)
	}
	if p.client.XLen(context.Background(), p.stream).Val() != 1 {
		t.Fatal("event was not delivered")
	}
}

func TestOutboxUnconfirmedHandoffSurvivesShutdown(t *testing.T) {
	o := outboxFixture(t)
	store := &failingUsageStore{}
	store.fail.Store(true)
	p := queueFixture(t, store)
	p.capacity = 1
	if err := p.enqueue(context.Background(), sampleUsageEvent()); err != nil {
		t.Fatal(err)
	}
	p.outbox = o
	p.start()
	event := sampleUsageEvent()
	p.publish(event)
	if p.pending.Load() != 1 {
		t.Fatal("full Redis queue did not retain handoff")
	}
	p.cancel()
	<-p.done
	<-p.publisherDone
	o.close()
	reopened, err := openUsageOutbox(o.dir, "1")
	if err != nil || len(reopened.recovered) != 1 || reopened.recovered[0].RequestID != event.RequestID {
		t.Fatal("shutdown lost unconfirmed outbox record")
	}
	t.Cleanup(reopened.close)
	recovery := queueFixture(t, &failingUsageStore{})
	recovery.outbox = reopened
	recovery.start()
	waitFor(t, func() bool { return recovery.persisted.Load() == 1 && recovery.pending.Load() == 0 })
}

func TestOutboxGroupCommitConfirmsAfterSharedFlush(t *testing.T) {
	o := outboxFixture(t)
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(proceed) })
	syncBefore := o.syncs.Load()
	o.syncFile = func(f *os.File) error {
		select {
		case <-entered:
		default:
			close(entered)
			<-proceed
		}
		return f.Sync()
	}
	first := make(chan error, 1)
	go func() { first <- o.put(sampleUsageEvent()) }()
	<-entered
	select {
	case <-first:
		t.Fatal("put confirmed before fsync")
	default:
	}
	commands := make([]outboxCommand, outboxBatchSize)
	for i := range commands {
		event := sampleUsageEvent()
		data, _ := json.Marshal(event)
		commands[i] = outboxCommand{id: event.RequestID, data: data, done: make(chan error, 1)}
		o.queue <- commands[i]
	}
	once.Do(func() { close(proceed) })
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	for _, command := range commands {
		if err := <-command.done; err != nil {
			t.Fatal(err)
		}
	}
	if o.batches.Load() != 2 || o.syncs.Load()-syncBefore != 2 || o.operations.Load() != 33 {
		t.Fatal("32 queued events did not share one durable flush")
	}
}
func TestOutboxConcurrentSameRequestWritesOnce(t *testing.T) {
	o := outboxFixture(t)
	event := sampleUsageEvent()
	results := make(chan error, 16)
	for range 16 {
		go func() { results <- o.put(event) }()
	}
	for range 16 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	n, _, _, _, _ := o.snapshot()
	if n != 1 || o.writes.Load() != 1 {
		t.Fatal("same-ID publishers duplicated local events")
	}
}

func TestOutboxShutdownKeepsOwnershipUntilPublishEnds(t *testing.T) {
	o := outboxFixture(t)
	p := queueFixture(t, &failingUsageStore{})
	p.outbox = o
	p.start()
	entered, proceed := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(proceed) })
	o.syncFile = func(f *os.File) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-proceed
		return f.Sync()
	}
	published, closed := make(chan struct{}), make(chan struct{})
	go func() { p.publish(sampleUsageEvent()); close(published) }()
	<-entered
	go func() { p.close(); close(closed) }()
	waitFor(t, func() bool { p.publisherMu.Lock(); defer p.publisherMu.Unlock(); return p.closing })
	if other, err := openUsageOutbox(o.dir, "1"); err == nil {
		other.close()
		t.Fatal("shutdown unlocked an in-progress publish")
	}
	select {
	case <-closed:
		t.Fatal("shutdown returned before publish ended")
	default:
	}
	once.Do(func() { close(proceed) })
	<-published
	<-closed
	if other, err := openUsageOutbox(o.dir, "1"); err != nil {
		t.Fatal("shutdown did not release ownership")
	} else {
		other.close()
	}
}

// A real subprocess is killed after fsync. This proves recovery does not depend
// on graceful shutdown or an in-memory retry job surviving.
func TestOutboxCrashHelper(t *testing.T) {
	path := os.Getenv("JANUS_TEST_OUTBOX_CHILD")
	if path == "" {
		return
	}
	var event requestEvent
	if err := json.Unmarshal([]byte(os.Getenv("JANUS_TEST_OUTBOX_EVENT")), &event); err != nil {
		t.Fatal(err)
	}
	o, err := openUsageOutbox(path, "1")
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("JANUS_TEST_OUTBOX_PHASE") == "partial" {
		o.writeFile = func(f *os.File, data []byte) (int, error) {
			n, err := f.Write(data[:len(data)/2])
			if err != nil {
				return n, err
			}
			fmt.Println("outbox-partial")
			time.Sleep(time.Hour)
			return n, nil
		}
	}
	if err := o.put(event); err != nil {
		t.Fatal(err)
	}
	fmt.Println("outbox-durable")
	time.Sleep(time.Hour)
}

func TestOutboxHardCrashRecovery(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("durable outbox requires Linux")
	}
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("database_already_committed_%t", committed), func(t *testing.T) {
			store := postgresFixture(t)
			event := sampleUsageEvent()
			cleanupUsageRows(t, store, event.TenantID)
			if committed {
				if err := store.Save(context.Background(), event); err != nil {
					t.Fatal(err)
				}
			}
			path := t.TempDir()
			data, _ := json.Marshal(event)
			cmd := exec.Command(os.Args[0], "-test.run=^TestOutboxCrashHelper$", "-test.timeout=1m")
			cmd.Env = append(os.Environ(), "JANUS_TEST_OUTBOX_CHILD="+path, "JANUS_TEST_OUTBOX_EVENT="+string(data))
			stdout, err := cmd.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { cmd.Process.Kill() })
			scanner := bufio.NewScanner(stdout)
			if !scanner.Scan() || scanner.Text() != "outbox-durable" {
				t.Fatal("child did not durably write event")
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			o, err := openUsageOutbox(path, "1")
			if err != nil || len(o.recovered) != 1 {
				t.Fatal("hard crash lost record or left lock held")
			}
			t.Cleanup(o.close)
			p := queueFixture(t, store)
			p.outbox = o
			p.start()
			waitFor(t, func() bool { return p.pending.Load() == 0 && p.persisted.Load() == 1 })
			var requests, attempts int
			if err := store.pool.QueryRow(context.Background(), "SELECT count(*) FROM janus_usage_requests WHERE request_id=$1", event.RequestID).Scan(&requests); err != nil {
				t.Fatal(err)
			}
			if err := store.pool.QueryRow(context.Background(), "SELECT count(*) FROM janus_usage_attempts WHERE request_id=$1", event.RequestID).Scan(&attempts); err != nil {
				t.Fatal(err)
			}
			if requests != 1 || attempts != 1 {
				t.Fatal("replay lost or duplicated accounting")
			}
			n, _, _, _, _ := o.snapshot()
			if n != 0 || o.replayed.Load() != 1 {
				t.Fatal("replayed record was not removed")
			}
		})
	}
}

func BenchmarkOutboxDurableCycle(b *testing.B) {
	if runtime.GOOS != "linux" {
		b.Skip("durable outbox requires Linux")
	}
	o, err := openUsageOutbox(b.TempDir(), "1")
	if err != nil {
		b.Fatal(err)
	}
	defer o.close()
	event := sampleUsageEvent()
	b.ResetTimer()
	for range b.N {
		if err := o.put(event); err != nil {
			b.Fatal(err)
		}
		if err := o.remove(event.RequestID); err != nil {
			b.Fatal(err)
		}
	}
}
