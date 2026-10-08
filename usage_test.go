package main

import (
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func sampleUsageEvent() requestEvent {
	prompt, completion, total := int64(20), int64(30), int64(50)
	return requestEvent{TenantID: "test-" + rand.Text(), RequestID: rand.Text(), Time: time.Now().UTC(), Event: "chat_request", Route: "primary", Status: 200, Outcome: "success", Cache: "BYPASS", Duration: 2,
		Attempts: []attemptObservation{{Route: "primary", Outcome: "success", Status: 200, Duration: 1, Usage: &tokenUsage{Prompt: &prompt, Completion: &completion, Total: &total}}}}
}

type failingUsageStore struct {
	fail  atomic.Bool
	calls atomic.Int32
	mu    sync.Mutex
	saved map[string]requestEvent
}

func (store *failingUsageStore) Save(ctx context.Context, event requestEvent) error {
	store.calls.Add(1)
	if store.fail.Load() {
		return errors.New("database unavailable")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saved == nil {
		store.saved = make(map[string]requestEvent)
	}
	store.saved[event.RequestID] = event
	return nil
}

func queueFixture(t *testing.T, store usageStore) *usagePipeline {
	t.Helper()
	l := testQuota(t, "60", 60000)
	p := &usagePipeline{client: l.client, store: store, stream: "janus:usage:test:" + rand.Text(), group: usageGroup, consumer: rand.Text(), claimIdle: time.Millisecond, capacity: 100}
	if err := p.initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.cancel()
		if p.done != nil {
			<-p.done
			<-p.publisherDone
		}
		p.client.Del(context.Background(), p.stream)
	})
	return p
}

// Inject a lost reply AFTER Redis commits; retry must not append twice, even
// if the worker has already acknowledged/deleted the first stream entry.
type lostUsageReply struct{ injected atomic.Bool }

func (h *lostUsageReply) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *lostUsageReply) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *lostUsageReply) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		args := cmd.Args()
		if err == nil && (cmd.Name() == "eval" || cmd.Name() == "evalsha") && len(args) > 2 && args[2] == 2 && h.injected.CompareAndSwap(false, true) {
			return context.DeadlineExceeded
		}
		return err
	}
}

func TestRedisUsageLostReplyRetriesWithoutDuplicate(t *testing.T) {
	store := &failingUsageStore{}
	p := queueFixture(t, store)
	hook := &lostUsageReply{}
	p.client.AddHook(hook)
	p.start()
	event := sampleUsageEvent()
	var releases atomic.Int32
	p.publish(event, func() { releases.Add(1) })
	waitFor(t, func() bool { return p.pending.Load() == 0 && p.persisted.Load() == 1 && releases.Load() == 1 })
	if !hook.injected.Load() || p.retriesTotal.Load() != 1 || p.queued.Load() != 1 || p.enqueueErr.Load() != 0 || store.calls.Load() != 1 {
		t.Fatal("lost reply was not recovered once")
	}
	if err := p.enqueue(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	n, err := p.client.XLen(context.Background(), p.stream).Result()
	if err != nil || n != 0 {
		t.Fatal("retry after acknowledgement recreated the event")
	}
}

func TestRedisUsageShutdownReportsUnconfirmedAndReleasesSeat(t *testing.T) {
	store := &failingUsageStore{}
	store.fail.Store(true)
	p := queueFixture(t, store)
	p.capacity = 1
	if err := p.enqueue(context.Background(), sampleUsageEvent()); err != nil {
		t.Fatal(err)
	}
	p.start()
	var releases atomic.Int32
	p.publish(sampleUsageEvent(), func() { releases.Add(1) })
	if p.pending.Load() != 1 {
		t.Fatal("event was not retained")
	}
	p.cancel()
	<-p.publisherDone
	<-p.done
	if p.pending.Load() != 0 || releases.Load() != 1 || p.enqueueErr.Load() != 1 || p.queued.Load() != 0 {
		t.Fatal("shutdown hid an unconfirmed event or leaked a seat")
	}
}

func TestRedisUsageBacklogClosesAdmissionAndRecovers(t *testing.T) {
	store := &failingUsageStore{}
	store.fail.Store(true)
	p := queueFixture(t, store)
	p.capacity = 1
	if err := p.enqueue(context.Background(), sampleUsageEvent()); err != nil {
		t.Fatal(err)
	}
	p.start()
	metrics := newTelemetry(nil)
	metrics.usage = p
	metrics.admission, _ = newAdmissionGate("1")
	metrics.admission.slots <- struct{}{}
	metrics.admission.active.Add(1)
	p.publish(sampleUsageEvent(), func() { metrics.admission.active.Add(-1); <-metrics.admission.slots })
	if p.pending.Load() != 1 || metrics.admission.active.Load() != 1 {
		t.Fatal("unconfirmed usage lost its seat")
	}
	calls := 0
	handler := metrics.observe(metrics.admit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(200) })))
	rejected := httptest.NewRecorder()
	handler.ServeHTTP(rejected, httptest.NewRequest("POST", "/", nil))
	if rejected.Code != 503 || calls != 0 {
		t.Fatal("provider was invoked during usage recovery")
	}
	store.fail.Store(false)
	waitFor(t, func() bool {
		return p.pending.Load() == 0 && p.persisted.Load() == 2 && metrics.admission.active.Load() == 0
	})
	accepted := httptest.NewRecorder()
	handler.ServeHTTP(accepted, httptest.NewRequest("POST", "/", nil))
	if accepted.Code != 200 || calls != 1 || p.enqueueErr.Load() != 0 {
		t.Fatal("admission did not recover")
	}
}

func readUsageMessage(t *testing.T, p *usagePipeline) redis.XMessage {
	t.Helper()
	streams, err := p.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{Group: p.group, Consumer: "crashed-consumer", Streams: []string{p.stream, ">"}, Count: 1, Block: -1}).Result()
	if err != nil || len(streams) != 1 || len(streams[0].Messages) != 1 {
		t.Fatalf("read usage entry: %v", err)
	}
	return streams[0].Messages[0]
}

func TestRedisUsageFailureRecoveryAndCapacity(t *testing.T) {
	store := &failingUsageStore{}
	store.fail.Store(true)
	p := queueFixture(t, store)
	p.capacity = 1
	event := sampleUsageEvent()
	if err := p.enqueue(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := p.enqueue(context.Background(), sampleUsageEvent()); err == nil {
		t.Fatal("full queue discarded backlog to admit another event")
	}
	message := readUsageMessage(t, p)
	if err := p.process(context.Background(), message); err == nil {
		t.Fatal("failed database save was acknowledged")
	}
	pending, err := p.client.XPending(context.Background(), p.stream, p.group).Result()
	if err != nil || pending.Count != 1 {
		t.Fatal("failed entry not retained pending")
	}
	// Simulate restart: new consumer recovers the previous consumer's pending entry.
	store.fail.Store(false)
	p.consumer = rand.Text()
	time.Sleep(5 * time.Millisecond)
	messages, _, err := p.client.XAutoClaim(context.Background(), &redis.XAutoClaimArgs{Stream: p.stream, Group: p.group, Consumer: p.consumer, Start: "0-0", MinIdle: time.Millisecond, Count: 1}).Result()
	if err != nil || len(messages) != 1 || messages[0].ID != message.ID {
		t.Fatal("new worker failed to reclaim pending usage")
	}
	if err := p.process(context.Background(), messages[0]); err != nil {
		t.Fatal(err)
	}
	length, err := p.client.XLen(context.Background(), p.stream).Result()
	if err != nil || length != 0 || p.persisted.Load() != 1 {
		t.Fatal("committed entry not removed from queue")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.saved) != 1 || store.saved[event.RequestID].TenantID != event.TenantID {
		t.Fatal("worker changed usage identity")
	}
}

func TestRedisUsageWorkerRetriesWithoutBlockingPublisher(t *testing.T) {
	store := &failingUsageStore{}
	store.fail.Store(true)
	p := queueFixture(t, store)
	p.start()
	p.publish(sampleUsageEvent())
	deadline := time.Now().Add(3 * time.Second)
	for store.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if store.calls.Load() == 0 {
		t.Fatal("background worker never attempted database write")
	}
	// Publishing succeeds while database writes fail; it does not call the store.
	p.publish(sampleUsageEvent())
	if p.queued.Load() != 2 || p.enqueueErr.Load() != 0 {
		t.Fatal("database outage blocked queue handoff")
	}
	store.fail.Store(false)
	deadline = time.Now().Add(4 * time.Second)
	for p.persisted.Load() != 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.persisted.Load() != 2 || p.workerErr.Load() == 0 {
		t.Fatal("worker did not retry and drain after database recovery")
	}
}

func TestRedisUsageMalformedEntryAndEnqueueMetrics(t *testing.T) {
	p := queueFixture(t, &failingUsageStore{})
	p.client.XAdd(context.Background(), &redis.XAddArgs{Stream: p.stream, Values: map[string]any{"event": "not JSON"}})
	message := readUsageMessage(t, p)
	if !errors.Is(p.process(context.Background(), message), errInvalidQueuedUsage) || p.invalid.Load() != 1 {
		t.Fatal("malformed entry not identified")
	}
	if pending, err := p.client.XPending(context.Background(), p.stream, p.group).Result(); err != nil || pending.Count != 1 {
		t.Fatal("malformed entry silently discarded")
	}
	// Queue capacity errors retain the event; unauthenticated logs never enter storage.
	p.capacity = 1
	p.publish(sampleUsageEvent())
	p.publish(requestEvent{})
	if p.enqueueErr.Load() != 0 || p.retriesTotal.Load() != 1 || p.pending.Load() != 1 || p.queued.Load() != 0 {
		t.Fatal("enqueue failure or unauthenticated event incorrectly accounted")
	}
}

func postgresFixture(t *testing.T) *postgresUsageStore {
	t.Helper()
	url := os.Getenv("POSTGRES_TEST_URL")
	if url == "" {
		t.Skip("set POSTGRES_TEST_URL for PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := newPostgresUsageStore(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.pool.Close)
	return store
}

func cleanupUsageRows(t *testing.T, store *postgresUsageStore, tenantID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := store.pool.Exec(ctx, `DELETE FROM janus_usage_attempts WHERE request_id IN (SELECT request_id FROM janus_usage_requests WHERE tenant_id=$1)`, tenantID)
		if err == nil {
			_, err = store.pool.Exec(ctx, `DELETE FROM janus_usage_requests WHERE tenant_id=$1`, tenantID)
		}
		if err != nil {
			t.Error("cannot clean test-owned usage rows")
		}
	})
}

func TestPostgresUsageDeduplicationAndDailyReport(t *testing.T) {
	store := postgresFixture(t)
	event := sampleUsageEvent()
	event.Route = "fallback"
	event.Attempts = append([]attemptObservation{{Route: "primary", Outcome: "failure", Status: 503, Duration: 1}}, event.Attempts...)
	event.Attempts[1].Route = "fallback"
	cleanupUsageRows(t, store, event.TenantID)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := store.Save(ctx, event); err != nil {
				t.Error("concurrent delivery failed")
			}
		}()
	}
	wg.Wait()
	cacheHit := event
	cacheHit.RequestID, cacheHit.Route, cacheHit.Cache, cacheHit.Attempts = rand.Text(), "cache", "HIT", nil
	if err := store.Save(context.Background(), cacheHit); err != nil {
		t.Fatal(err)
	}
	var unknown int
	if err := store.pool.QueryRow(context.Background(), `SELECT count(*) FROM janus_usage_attempts WHERE request_id=$1 AND total_tokens IS NULL`, event.RequestID).Scan(&unknown); err != nil || unknown != 1 {
		t.Fatal("unknown usage became zero")
	}
	query, err := os.ReadFile("queries/daily_usage.sql")
	if err != nil {
		t.Fatal(err)
	}
	rows, err := store.pool.Query(context.Background(), string(query))
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	found := false
	for rows.Next() {
		var day time.Time
		var tenant string
		var requests, hits, prompt, completion, total, missing int64
		if err := rows.Scan(&day, &tenant, &requests, &hits, &prompt, &completion, &total, &missing); err != nil {
			t.Fatal(err)
		}
		if tenant == event.TenantID {
			found = true
			if requests != 2 || hits != 1 || prompt != 20 || completion != 30 || total != 50 || missing != 1 {
				t.Fatal("daily report doubled fallback requests, duplicate deliveries or cache-hit tokens")
			}
		}
	}
	if rows.Err() != nil || !found {
		t.Fatal("daily report missing test tenant")
	}
}

func TestPostgresUsageCommitBeforeAcknowledgement(t *testing.T) {
	store := postgresFixture(t)
	p := queueFixture(t, store)
	event := sampleUsageEvent()
	cleanupUsageRows(t, store, event.TenantID)
	if err := p.enqueue(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	message := readUsageMessage(t, p)
	// Simulate a database commit followed by worker crash before Redis XACK.
	if err := store.Save(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := p.process(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := store.pool.QueryRow(context.Background(), `SELECT count(*) FROM janus_usage_requests WHERE request_id=$1`, event.RequestID).Scan(&count); err != nil || count != 1 {
		t.Fatal("replayed committed event duplicated storage")
	}
	if pending, err := p.client.XPending(context.Background(), p.stream, p.group).Result(); err != nil || pending.Count != 0 {
		t.Fatal("replayed committed event not acknowledged")
	}
}

func TestUsageEventValidation(t *testing.T) {
	event := sampleUsageEvent()
	if err := validateUsageEvent(event); err != nil {
		t.Fatal(err)
	}
	event.Attempts[0].Usage.Total = nil
	if err := validateUsageEvent(event); err == nil {
		t.Fatal("partial usage accepted")
	}
	if _, err := decodeUsageEvent(strings.Repeat("x", (16<<10)+1)); err == nil {
		t.Fatal("oversized queue payload accepted")
	}
}

type failingBatchStore struct {
	failingUsageStore
	batchCalls atomic.Int32
	maxBatch   atomic.Int32
}

func (store *failingBatchStore) SaveBatch(ctx context.Context, events []requestEvent) error {
	store.batchCalls.Add(1)
	for previous := store.maxBatch.Load(); int32(len(events)) > previous; previous = store.maxBatch.Load() {
		if store.maxBatch.CompareAndSwap(previous, int32(len(events))) {
			break
		}
	}
	if store.fail.Load() {
		return errors.New("database unavailable")
	}
	for _, event := range events {
		if err := store.Save(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func TestRedisUsageWorkerDrainsRecoveryPagesAndNewWork(t *testing.T) {
	store := &failingBatchStore{}
	p := queueFixture(t, store)
	p.capacity = 256
	for range 150 {
		if err := p.enqueue(context.Background(), sampleUsageEvent()); err != nil {
			t.Fatal(err)
		}
	}
	// A dead consumer has more than two recovery pages assigned to it.
	streams, err := p.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{Group: p.group, Consumer: "dead-worker", Streams: []string{p.stream, ">"}, Count: 150, Block: -1}).Result()
	if err != nil || len(streams) != 1 || len(streams[0].Messages) != 150 {
		t.Fatal("cannot prepare pending recovery pages")
	}
	for range 10 {
		if err := p.enqueue(context.Background(), sampleUsageEvent()); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(5 * time.Millisecond)
	p.start()
	waitFor(t, func() bool { return p.persisted.Load() == 160 })
	if store.maxBatch.Load() != 64 || p.client.XLen(context.Background(), p.stream).Val() != 0 {
		t.Fatal("worker did not drain bounded recovery batches and new arrivals")
	}
	pending, err := p.client.XPending(context.Background(), p.stream, p.group).Result()
	if err != nil || pending.Count != 0 || p.workerErr.Load() != 0 {
		t.Fatal("recovered deliveries remain pending")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.saved) != 160 {
		t.Fatal("recovery lost or duplicated usage identities")
	}
}

func TestRedisUsageWorkerRevisitsYoungPendingEntry(t *testing.T) {
	p := queueFixture(t, &failingBatchStore{})
	p.claimIdle = 100 * time.Millisecond
	p.metrics = newTelemetry(nil)
	if err := p.enqueue(context.Background(), sampleUsageEvent()); err != nil {
		t.Fatal(err)
	}
	readUsageMessage(t, p)
	p.start()
	waitFor(t, func() bool { return p.persisted.Load() == 1 })
	if p.client.XLen(context.Background(), p.stream).Val() != 0 {
		t.Fatal("periodic recovery did not drain pending work")
	}
}

func TestRedisUsageBatchFailureAndPoisonNeighbor(t *testing.T) {
	store := &failingBatchStore{}
	p := queueFixture(t, store)
	for range 3 {
		if err := p.enqueue(context.Background(), sampleUsageEvent()); err != nil {
			t.Fatal(err)
		}
	}
	p.client.XAdd(context.Background(), &redis.XAddArgs{Stream: p.stream, Values: map[string]any{"event": "invalid"}})
	streams, err := p.client.XReadGroup(context.Background(), &redis.XReadGroupArgs{Group: p.group, Consumer: p.consumer, Streams: []string{p.stream, ">"}, Count: 16, Block: -1}).Result()
	if err != nil || len(streams) != 1 || len(streams[0].Messages) != 4 {
		t.Fatal("cannot read test batch")
	}
	messages := streams[0].Messages
	store.fail.Store(true)
	if err := p.processBatch(context.Background(), messages); err == nil {
		t.Fatal("failed batch acknowledged")
	}
	pending, err := p.client.XPending(context.Background(), p.stream, p.group).Result()
	if err != nil || pending.Count != 4 || p.persisted.Load() != 0 {
		t.Fatal("failed batch lost pending entries")
	}
	store.fail.Store(false)
	if err := p.processBatch(context.Background(), messages); err != nil {
		t.Fatal(err)
	}
	pending, err = p.client.XPending(context.Background(), p.stream, p.group).Result()
	length := p.client.XLen(context.Background(), p.stream).Val()
	if err != nil || pending.Count != 1 || length != 1 || p.persisted.Load() != 3 || store.batchCalls.Load() != 2 {
		t.Fatal("batch recovery lost valid entries or discarded poison entry")
	}
}

func TestPostgresUsageBatchReplayAndValidation(t *testing.T) {
	store := postgresFixture(t)
	first := sampleUsageEvent()
	cleanupUsageRows(t, store, first.TenantID)
	second := first
	second.RequestID, second.Route, second.Cache, second.Attempts = rand.Text(), "cache", "HIT", nil
	third := first
	third.RequestID = rand.Text()
	third.Attempts = []attemptObservation{{Route: "primary", Outcome: "failure", Status: 503, Duration: 1}, first.Attempts[0]}
	third.Attempts[1].Route = "fallback"
	events := []requestEvent{first, second, third, first}
	for range 2 {
		if err := store.SaveBatch(context.Background(), events); err != nil {
			t.Fatal(err)
		}
	}
	var requests, attempts, unknown int
	if err := store.pool.QueryRow(context.Background(), `SELECT count(*) FROM janus_usage_requests WHERE tenant_id=$1`, first.TenantID).Scan(&requests); err != nil || requests != 3 {
		t.Fatal("batch replay duplicated or lost requests")
	}
	if err := store.pool.QueryRow(context.Background(), `SELECT count(*), count(*) FILTER (WHERE total_tokens IS NULL) FROM janus_usage_attempts WHERE request_id IN ($1,$2,$3)`, first.RequestID, second.RequestID, third.RequestID).Scan(&attempts, &unknown); err != nil || attempts != 3 || unknown != 1 {
		t.Fatal("batch replay changed attempts or unknown usage")
	}
	valid := first
	valid.RequestID = rand.Text()
	invalid := valid
	invalid.Event = "invalid"
	if err := store.SaveBatch(context.Background(), []requestEvent{valid, invalid}); err == nil {
		t.Fatal("invalid batch accepted")
	}
	if err := store.pool.QueryRow(context.Background(), `SELECT count(*) FROM janus_usage_requests WHERE request_id=$1`, valid.RequestID).Scan(&requests); err != nil || requests != 0 {
		t.Fatal("invalid batch partially saved")
	}
	// Go accepts this date, but PostgreSQL timestamps cannot represent it. A SQL
	// failure after the first insert must roll back the entire transaction.
	outsideRange := valid
	outsideRange.RequestID = rand.Text()
	outsideRange.Time = time.Date(-10000, time.January, 1, 0, 0, 0, 0, time.UTC)
	if err := store.SaveBatch(context.Background(), []requestEvent{valid, outsideRange}); err == nil {
		t.Fatal("out-of-range PostgreSQL timestamp unexpectedly saved")
	}
	if err := store.pool.QueryRow(context.Background(), `SELECT count(*) FROM janus_usage_requests WHERE request_id IN ($1,$2)`, valid.RequestID, outsideRange.RequestID).Scan(&requests); err != nil || requests != 0 {
		t.Fatal("database failure did not roll back the whole batch")
	}
}
