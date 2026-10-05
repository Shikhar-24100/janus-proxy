package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"log"
	"strings"
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
if redis.call('XLEN', KEYS[1]) >= tonumber(ARGV[1]) then return redis.error_reply('USAGE_QUEUE_FULL') end
return redis.call('XADD', KEYS[1], '*', 'event', ARGV[2])
`)

// One consumer group owns this stream. Delete only after successful persistence.
var acknowledgeUsageScript = redis.NewScript(`
local acknowledged = redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
redis.call('XDEL', KEYS[1], ARGV[2])
return acknowledged
`)

type usagePipeline struct {
	client     *redis.Client
	store      usageStore
	stream     string
	group      string
	consumer   string
	claimIdle  time.Duration
	capacity   int
	queued     atomic.Uint64
	enqueueErr atomic.Uint64
	persisted  atomic.Uint64
	workerErr  atomic.Uint64
	invalid    atomic.Uint64
	cancel     context.CancelFunc
	done       chan struct{}
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
	options.PoolSize = 4
	options.MaxActiveConns = 4
	p := &usagePipeline{client: redis.NewClient(options), store: store, stream: stream, group: usageGroup, consumer: rand.Text(), claimIdle: 30 * time.Second, capacity: maxUsageQueue}
	if err := p.initialize(ctx); err != nil {
		p.client.Close()
		return nil, errors.New("cannot initialize usage queue")
	}
	return p, nil
}

func (p *usagePipeline) initialize(ctx context.Context) error {
	err := p.client.XGroupCreateMkStream(ctx, p.stream, p.group, "0").Err()
	if err != nil && !strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

// The database is off the request path, but the reliable handoff is a bounded write.
func (p *usagePipeline) publish(event requestEvent) {
	if event.TenantID == "" {
		return // Rejected authentication has no tenant usage to account for.
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := p.enqueue(ctx, event); err != nil {
		p.enqueueErr.Add(1)
		log.Printf("Usage enqueue failed for request %s; event not confirmed in queue", event.RequestID)
		return
	}
	p.queued.Add(1)
}

func (p *usagePipeline) enqueue(ctx context.Context, event requestEvent) error {
	if err := validateUsageEvent(event); err != nil {
		return err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return enqueueUsageScript.Run(ctx, p.client, []string{p.stream}, p.capacity, string(data)).Err()
}

func (p *usagePipeline) start() {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel, p.done = cancel, make(chan struct{})
	go func() {
		defer close(p.done)
		p.run(ctx)
	}()
}

func (p *usagePipeline) close() {
	if p.cancel != nil {
		p.cancel()
		<-p.done
	}
	p.client.Close()
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
		for _, message := range messages {
			if ctx.Err() != nil {
				return
			}
			delay := time.Second
			for {
				err := p.process(ctx, message)
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
	data, ok := message.Values["event"].(string)
	event, err := decodeUsageEvent(data)
	if !ok || err != nil {
		// Leave malformed entries pending for inspection; never silently discard them.
		p.invalid.Add(1)
		return errInvalidQueuedUsage
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = p.store.Save(writeCtx, event)
	cancel()
	if err != nil {
		return err
	}
	ackCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := acknowledgeUsageScript.Run(ackCtx, p.client, []string{p.stream}, p.group, message.ID).Err(); err != nil {
		return err
	}
	p.persisted.Add(1)
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
