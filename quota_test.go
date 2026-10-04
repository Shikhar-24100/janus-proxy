package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testQuota(t *testing.T, rpm string, tpm int) *RateLimiter {
	t.Helper()
	url := os.Getenv("REDIS_TEST_URL")
	if url == "" {
		t.Skip("set REDIS_TEST_URL for quota integration tests")
	}
	l, err := newRateLimiter(url, rpm, fmt.Sprintf("quota-test-%d", time.Now().UnixNano()))
	if err != nil {
		t.Fatal(err)
	}
	l.tpm = tpm
	t.Cleanup(func() { l.client.Del(context.Background(), l.quotaKeys()...); l.client.Close() })
	return l
}

func TestRedisReservationMath(t *testing.T) {
	l := testQuota(t, "10", 1000)
	ctx := context.Background()
	first, err := l.Reserve(ctx, 600)
	if err != nil || !first.allowed || first.tokenRemaining != 400 {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	denied, err := l.Reserve(ctx, 500)
	if err != nil || denied.allowed || !denied.tokenDenied || denied.remaining != 9 {
		t.Fatalf("denied=%+v err=%v", denied, err)
	}
	if err := l.Settle(ctx, first.reservation, 250); err != nil {
		t.Fatal(err)
	}
	// Repeated settlement must not give a second refund, even with different usage.
	if err := l.Settle(ctx, first.reservation, 0); err != nil {
		t.Fatal(err)
	}
	second, err := l.Reserve(ctx, 500)
	if err != nil || !second.allowed || second.tokenRemaining != 250 {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	// Actual usage can exceed the estimate. Record the debt, don't cut an answer.
	if err := l.Settle(ctx, second.reservation, 900); err != nil {
		t.Fatal(err)
	}
	denied, err = l.Reserve(ctx, 1)
	if err != nil || denied.allowed || denied.tokenRemaining != 0 {
		t.Fatalf("debt=%+v err=%v", denied, err)
	}
}

func TestRedisLateSettlement(t *testing.T) {
	l := testQuota(t, "10", 1000)
	ctx := context.Background()
	first, err := l.Reserve(ctx, 600)
	if err != nil {
		t.Fatal(err)
	}
	now, err := l.client.Time(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	l.client.ZAdd(ctx, l.quotaKeys()[1], redis.Z{Score: float64(now.Add(-61 * time.Second).UnixMilli()), Member: first.reservation})
	if err := l.Settle(ctx, first.reservation, 0); err != nil {
		t.Fatal(err)
	}
	total, err := l.client.Get(ctx, l.quotaKeys()[3]).Int64()
	if err != nil || total != 600 {
		t.Fatal("late settlement altered a newer window")
	}
	second, err := l.Reserve(ctx, 1000)
	if err != nil || !second.allowed || second.tokenRemaining != 0 {
		t.Fatalf("expiry=%+v err=%v", second, err)
	}
	if err := l.Settle(ctx, first.reservation, 0); err != nil {
		t.Fatal(err)
	}
	total, _ = l.client.Get(ctx, l.quotaKeys()[3]).Int64()
	if total != 1000 {
		t.Fatal("expired refund granted new credits")
	}
}

func TestRedisConcurrentTPMReservations(t *testing.T) {
	l := testQuota(t, "100", 1000)
	var admitted, failures atomic.Int32
	var wait sync.WaitGroup
	for i := 0; i < 40; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			d, err := l.Reserve(context.Background(), 100)
			if err != nil {
				failures.Add(1)
			} else if d.allowed {
				admitted.Add(1)
			}
		}()
	}
	wait.Wait()
	if admitted.Load() != 10 || failures.Load() != 0 {
		t.Fatalf("admitted=%d failures=%d", admitted.Load(), failures.Load())
	}
}

func TestRedisRPMRejectDoesNotChargeTPM(t *testing.T) {
	l := testQuota(t, "1", 1000)
	first, err := l.Reserve(context.Background(), 100)
	if err != nil || !first.allowed {
		t.Fatal(err)
	}
	second, err := l.Reserve(context.Background(), 100)
	if err != nil || second.allowed || second.tokenRemaining != 900 {
		t.Fatalf("RPM rejection charged TPM: %+v %v", second, err)
	}
}

func TestRedisFallbackTokensWithoutRPM(t *testing.T) {
	l := testQuota(t, "1", 1000)
	ctx := context.Background()
	first, err := l.Reserve(ctx, 600)
	if err != nil || !first.allowed {
		t.Fatal("primary reservation failed", err)
	}
	fallback, err := l.ReserveTokens(ctx, 400)
	if err != nil || !fallback.allowed || fallback.remaining != 0 || fallback.tokenRemaining != 0 {
		t.Fatalf("fallback=%+v err=%v", fallback, err)
	}
	denied, err := l.ReserveTokens(ctx, 1)
	if err != nil || denied.allowed || !denied.tokenDenied {
		t.Fatal("fallback bypassed TPM")
	}
	l.Settle(ctx, fallback.reservation, 100)
	total, err := l.client.Get(ctx, l.quotaKeys()[3]).Int64()
	if err != nil || total != 700 {
		t.Fatalf("unknown primary charge lost: %d %v", total, err)
	}
}
