package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const usageStream = "janus:usage:v1"
const usageGroup = "postgres-v1"
const maxUsageQueue = 100000

var errInvalidQueuedUsage = errors.New("invalid usage queue entry")

// Do not trim unacknowledged events to make room. Refuse new events at capacity.
var enqueueUsageScript = redis.NewScript(`
local previous = redis.call('GET', KEYS[2])
if previous then return previous end
if redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[1]) then return redis.error_reply('USAGE_QUEUE_FULL') end
local id = redis.call('XADD', KEYS[1], '*', 'event', ARGV[2])
redis.call('SET', KEYS[2], id, 'PX', 30000)
return id
`)

// One consumer group owns this stream. Delete only after successful persistence.
var acknowledgeUsageScript = redis.NewScript(`
local ids = {}
for i=2,#ARGV do ids[#ids+1] = ARGV[i] end
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], unpack(ids))
redis.call('XDEL', KEYS[1], unpack(ids))
return acknowledged
`)

type usagePipeline struct {
	client        *redis.Client
	store         usageStore
	stream        string
	group         string
	consumer      string
	claimIdle     time.Duration
	capacity      int
	queued        atomic.Uint64
	enqueueErr    atomic.Uint64
	persisted     atomic.Uint64
	workerErr     atomic.Uint64
	invalid       atomic.Uint64
	cancel        context.CancelFunc
	done          chan struct{}
	publisherCtx  context.Context
	publisherDone chan struct{}
	publisherMu   sync.Mutex
	publishWG     sync.WaitGroup
	closing       bool
	retries       chan usageRetry
	pending       atomic.Int64
	retriesTotal  atomic.Uint64
	outbox        *usageOutbox
	metrics       *telemetry
}

type usageRetry struct {
	event   requestEvent
	release func()
}

func newUsagePipeline(ctx context.Context, redisURL string, store usageStore) (*usagePipeline, error) {
	return newUsagePipelineWithStream(ctx, redisURL, store, usageStream)
}

// An isolated stream lets local benchmarks exercise the real worker without
// consuming production events. The normal gateway always uses usageStream.
func newUsagePipelineWithStream(ctx context.Context, redisURL string, store usageStore, stream string) (*usagePipeline, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, errors.New("USAGE_REDIS_URL is invalid")
	}
	options.ContextTimeoutEnabled = true
	options.MaxRetries = -1
	options.DialTimeout = 300 * time.Millisecond
	options.ReadTimeout = 2 * time.Second
	options.WriteTimeout = 300 * time.Millisecond
	options.PoolTimeout = 200 * time.Millisecond
	// Bounded concurrency without serializing sixteen publishers behind four sockets.
	options.PoolSize = 16
	options.MaxActiveConns = 16
	p := &usagePipeline{client: redis.NewClient(options), store: store, stream: stream, group: usageGroup, consumer: rand.Text(), claimIdle: 30 * time.Second, capacity: maxUsageQueue}
	if err := p.initialize(ctx); err != nil {
		p.client.Close()
		return nil, errors.New("cannot initialize usage queue")
	}
	return p, nil
}

func (p *usagePipeline) initialize(ctx context.Context) error {
	if p.publisherCtx == nil {
		p.publisherCtx, p.cancel = context.WithCancel(context.Background())
		p.retries = make(chan usageRetry, defaultMaxInflight)
	}
	err := p.client.XGroupCreateMkStream(ctx, p.stream, p.group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// The database is off the request path, but the reliable handoff is a bounded write.
func (p *usagePipeline) publish(event requestEvent, releases ...func()) {
	var release func()
	if len(releases) > 0 {
		release = releases[0]
	}
	complete := func() {
		if release != nil {
			release()
		}
	}
	if event.TenantID == "" {
		complete()
		return // Rejected authentication has no tenant usage to account for.
	}
	if validateUsageEvent(event) != nil {
		p.enqueueErr.Add(1)
		complete()
		return
	}
	p.publisherMu.Lock()
	if p.closing {
		p.publisherMu.Unlock()
		p.unconfirmed(usageRetry{event, release})
		return
	}
	p.publishWG.Add(1)
	p.publisherMu.Unlock()
	defer p.publishWG.Done()
	err := p.handoff(p.publisherCtx, event)
	if err == nil {
		p.queued.Add(1)
		complete()
		return
	}
	p.retriesTotal.Add(1)
	// Keep the event AND its admission seat until Redis confirms the handoff.
	// The channel is sized to the gateway limit, so admitted handlers bound it.
	p.publisherMu.Lock()
	defer p.publisherMu.Unlock()
	if p.publisherCtx.Err() != nil {
		p.unconfirmed(usageRetry{event, release})
		return
	}
	p.pending.Add(1)
	select {
	case p.retries <- usageRetry{event, release}:
	case <-p.publisherCtx.Done():
		p.pending.Add(-1)
		p.unconfirmed(usageRetry{event, release})
	}
}

// A confirmed local write precedes Redis. Local deletion follows Redis confirmation.
func (p *usagePipeline) handoff(ctx context.Context, event requestEvent) error {
	if p.outbox != nil {
		started := time.Now()
		err := p.outbox.put(event)
		p.metrics.observeStage(3, started)
		if err != nil {
			return err
		}
	}
	// File sync cannot be interrupted by a context. Start Redis's budget only
	// after local persistence, rather than spending it waiting on disk locks.
	redisCtx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	err := p.enqueue(redisCtx, event)
	cancel()
	if err != nil {
		return err
	}
	if p.outbox != nil {
		return p.outbox.remove(event.RequestID)
	}
	return nil
}

func (p *usagePipeline) enqueue(ctx context.Context, event requestEvent) error {
	if err := validateUsageEvent(event); err != nil {
		return err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return enqueueUsageScript.Run(ctx, p.client, []string{p.stream, p.stream + ":handoff:" + event.RequestID}, p.capacity, string(data)).Err()
}

func (p *usagePipeline) start() {
	ctx := p.publisherCtx
	if p.outbox != nil {
		p.pending.Add(int64(len(p.outbox.recovered)))
	}
	p.done = make(chan struct{})
	p.publisherDone = make(chan struct{})
	go func() { defer close(p.publisherDone); p.retryHandoffs(ctx) }()
	go func() {
		defer close(p.done)
		p.run(ctx)
	}()
}

func (p *usagePipeline) close() {
	p.publisherMu.Lock()
	p.closing = true
	if p.cancel != nil {
		p.cancel()
	}
	p.publisherMu.Unlock()
	// Keep exclusive ownership until all file writes already started have ended.
	p.publishWG.Wait()
	if p.cancel != nil {
		if p.done != nil {
			<-p.done
		}
		if p.publisherDone != nil {
			<-p.publisherDone
		}
	}
	p.client.Close()
	if p.outbox != nil {
		p.outbox.close()
	}
}

func (p *usagePipeline) unconfirmed(job usageRetry) {
	p.enqueueErr.Add(1)
	log.Printf("Usage handoff unconfirmed at shutdown for request %s; inspect outbox for recoverable events", job.event.RequestID)
	if job.release != nil {
		job.release()
	}
}

func (p *usagePipeline) retryHandoffs(ctx context.Context) {
	defer func() {
		// Serialize draining with submissions so cancellation cannot strand a job.
		p.publisherMu.Lock()
		defer p.publisherMu.Unlock()
		for {
			select {
			case job := <-p.retries:
				p.pending.Add(-1)
				p.unconfirmed(job)
			default:
				return
			}
		}
	}()
	// Recovery runs before arrivals are admitted. It does not acquire new seats.
	if p.outbox != nil {
		for index, event := range p.outbox.recovered {
			p.outbox.replayed.Add(1)
			if !p.retryHandoff(ctx, usageRetry{event: event}) {
				// Remaining records stay on disk; no per-event memory queue to drain.
				p.pending.Add(-int64(len(p.outbox.recovered) - index - 1))
				return
			}
		}
		p.outbox.recovered = nil
	}
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-p.retries:
			if !p.retryHandoff(ctx, job) {
				return
			}
		}
	}
}

func (p *usagePipeline) retryHandoff(ctx context.Context, job usageRetry) bool {
	delay := 50 * time.Millisecond
	for {
		err := p.handoff(ctx, job.event)
		if err == nil {
			p.queued.Add(1)
			p.pending.Add(-1)
			if job.release != nil {
				job.release()
			}
			return true
		}
		if ctx.Err() != nil {
			p.pending.Add(-1)
			p.unconfirmed(job)
			return false
		}
		p.retriesTotal.Add(1)
		if !usagePause(ctx, delay) {
			p.pending.Add(-1)
			p.unconfirmed(job)
			return false
		}
		delay = min(delay*2, time.Second)
	}
}

func (p *usagePipeline) run(ctx context.Context) {
	cursor := "0-0"
	backoff := time.Second
	for ctx.Err() == nil {
		messages, next, err := p.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{Stream: p.stream, Group: p.group, Consumer: p.consumer, MinIdle: p.claimIdle, Start: cursor, Count: 16}).Result()
		if err == nil {
			cursor = next
			if len(messages) == 0 {
				var streams []redis.XStream
				streams, err = p.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: p.group, Consumer: p.consumer, Streams: []string{p.stream, ">"}, Count: 16, Block: 250 * time.Millisecond}).Result()
				for _, stream := range streams {
					messages = append(messages, stream.Messages...)
				}
			}
		}
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			p.workerErr.Add(1)
			if !usagePause(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, 10*time.Second)
			// Recreate a missing group only after an explicit Redis state reset.
			if strings.HasPrefix(err.Error(), "NOGROUP") {
				_ = p.initialize(ctx)
				cursor = "0-0"
			}
			continue
		}
		backoff = time.Second
		batches := [][]redis.XMessage{messages}
		if _, ok := p.store.(batchUsageStore); !ok {
			batches = nil
			for _, message := range messages {
				batches = append(batches, []redis.XMessage{message})
			}
		}
		for _, batch := range batches {
			if ctx.Err() != nil {
				return
			}
			delay := time.Second
			for {
				err := p.processBatch(ctx, batch)
				if ctx.Err() != nil {
					return
				}
				if err == nil {
					break
				}
				p.workerErr.Add(1)
				if errors.Is(err, errInvalidQueuedUsage) {
					break // Retain poison entry for inspection, continue other work.
				}
				// Retry the same pending entry; no acknowledgement before commit.
				if !usagePause(ctx, delay) {
					return
				}
				delay = min(delay*2, 10*time.Second)
			}
		}
	}
}

func (p *usagePipeline) process(ctx context.Context, message redis.XMessage) error {
	return p.processBatch(ctx, []redis.XMessage{message})
}

func (p *usagePipeline) processBatch(ctx context.Context, messages []redis.XMessage) error {
	events := make([]requestEvent, 0, len(messages))
	args := []any{p.group}
	for _, message := range messages {
		data, ok := message.Values["event"].(string)
		event, err := decodeUsageEvent(data)
		if !ok || err != nil {
			// Retain poison entries; valid neighbors can still commit.
			p.invalid.Add(1)
			if len(messages) > 1 {
				p.workerErr.Add(1)
			}
			continue
		}
		events = append(events, event)
		args = append(args, message.ID)
	}
	if len(events) == 0 {
		return errInvalidQueuedUsage
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	var err error
	if store, ok := p.store.(batchUsageStore); ok {
		err = store.SaveBatch(writeCtx, events)
	} else {
		for _, event := range events {
			if err = p.store.Save(writeCtx, event); err != nil {
				break
			}
		}
	}
	cancel()
	if err != nil {
		return err
	}
	ackCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := acknowledgeUsageScript.Run(ackCtx, p.client, []string{p.stream}, args...).Err(); err != nil {
		return err
	}
	p.persisted.Add(uint64(len(events)))
	return nil
}

func usagePause(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
