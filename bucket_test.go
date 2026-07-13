package tokenbucket

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newRedis(t *testing.T) redis.Cmdable {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skip("Redis unavailable:", err)
	}
	return c
}

func TestTokenBucketAllowsUpToCapacity(t *testing.T) {
	tb := NewTokenBucket(newRedis(t))
	cfg := BucketConfig{Capacity: 5, RefillPerSecond: 0}
	const tenant = "rl-acme"
	t.Cleanup(func() {
		newRedis(t).Del(context.Background(), bucketKey(tenant, "test"))
	})

	for i := 0; i < 5; i++ {
		dec, err := tb.TryConsume(context.Background(), tenant, "test", cfg, 1)
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if !dec.Allowed {
			t.Fatalf("call %d should be allowed (capacity=5), got %+v", i, dec)
		}
	}

	dec, _ := tb.TryConsume(context.Background(), tenant, "test", cfg, 1)
	if dec.Allowed {
		t.Fatalf("6th call should be rejected (no refill), got %+v", dec)
	}
}

func TestTokenBucketRefillsOverTime(t *testing.T) {
	tb := NewTokenBucket(newRedis(t))
	cfg := BucketConfig{Capacity: 2, RefillPerSecond: 10} // refill 10/s
	const tenant = "rl-refill"
	t.Cleanup(func() {
		newRedis(t).Del(context.Background(), bucketKey(tenant, "test"))
	})

	tb.TryConsume(context.Background(), tenant, "test", cfg, 1)
	tb.TryConsume(context.Background(), tenant, "test", cfg, 1)
	dec, _ := tb.TryConsume(context.Background(), tenant, "test", cfg, 1)
	if dec.Allowed {
		t.Fatalf("expected reject after burst, got %+v", dec)
	}

	time.Sleep(150 * time.Millisecond) // 0.15s * 10/s = 1.5 token
	dec, _ = tb.TryConsume(context.Background(), tenant, "test", cfg, 1)
	if !dec.Allowed {
		t.Fatalf("expected allow after refill, got %+v", dec)
	}
}

func TestTokenBucketTenantIsolation(t *testing.T) {
	tb := NewTokenBucket(newRedis(t))
	cfg := BucketConfig{Capacity: 1, RefillPerSecond: 0}
	t.Cleanup(func() {
		newRedis(t).Del(context.Background(),
			bucketKey("rl-tA", "test"), bucketKey("rl-tB", "test"))
	})

	decA, _ := tb.TryConsume(context.Background(), "rl-tA", "test", cfg, 1)
	if !decA.Allowed {
		t.Fatal("tenant A first call should pass")
	}
	decAagain, _ := tb.TryConsume(context.Background(), "rl-tA", "test", cfg, 1)
	if decAagain.Allowed {
		t.Fatal("tenant A second call should fail (capacity=1)")
	}
	decB, _ := tb.TryConsume(context.Background(), "rl-tB", "test", cfg, 1)
	if !decB.Allowed {
		t.Fatal("tenant B must not be affected by tenant A's bucket")
	}
}

func TestTokenBucketRejectsZeroCapacity(t *testing.T) {
	tb := NewTokenBucket(newRedis(t))
	_, err := tb.TryConsume(context.Background(), "x", "y",
		BucketConfig{Capacity: 0, RefillPerSecond: 1}, 1)
	if err == nil {
		t.Fatal("expected error on capacity=0")
	}
}

func TestTokenBucketRetryAfterIsPositiveWhenRejected(t *testing.T) {
	tb := NewTokenBucket(newRedis(t))
	cfg := BucketConfig{Capacity: 1, RefillPerSecond: 5}
	const tenant = "rl-retry"
	t.Cleanup(func() {
		newRedis(t).Del(context.Background(), bucketKey(tenant, "test"))
	})

	tb.TryConsume(context.Background(), tenant, "test", cfg, 1) // exhaust
	dec, _ := tb.TryConsume(context.Background(), tenant, "test", cfg, 1)
	if dec.Allowed {
		t.Fatal("second call should be rejected")
	}
	if dec.RetryAfter <= 0 || dec.RetryAfter > time.Second {
		t.Errorf("retry_after expected within (0, 1s], got %v", dec.RetryAfter)
	}
}

func TestBucketKeyNamespace(t *testing.T) {
	cases := map[string]string{
		"rate:t:acme:turns":    bucketKey("acme", "turns"),
		"rate:t:_global:turns": bucketKey("", "turns"),
		"rate:t:acme:_default": bucketKey("acme", ""),
	}
	for want, got := range cases {
		if got != want {
			t.Errorf("expected %q, got %q", want, got)
		}
	}
}

func TestTokenBucketZeroTokensDefaultsToOne(t *testing.T) {
	tb := NewTokenBucket(newRedis(t))
	cfg := BucketConfig{Capacity: 10, RefillPerSecond: 0}
	// Use unique tenant per test run to avoid Redis state pollution.
	tenant := fmt.Sprintf("rl-zero-tokens-%d", time.Now().UnixNano())

	ctx := context.Background()
	d, err := tb.TryConsume(ctx, tenant, "api", cfg, 0)
	if err != nil {
		t.Fatalf("TryConsume(0 tokens): %v", err)
	}
	if !d.Allowed {
		t.Error("should be allowed when bucket has capacity and tokens defaults to 1")
	}
	if d.Remaining < 8 || d.Remaining > 9.5 {
		t.Errorf("remaining = %v, want ~9", d.Remaining)
	}
}

func TestTokenBucketNegativeTokensDefaultsToOne(t *testing.T) {
	tb := NewTokenBucket(newRedis(t))
	cfg := BucketConfig{Capacity: 10, RefillPerSecond: 0}
	tenant := fmt.Sprintf("rl-neg-tokens-%d", time.Now().UnixNano())

	ctx := context.Background()
	d, err := tb.TryConsume(ctx, tenant, "api", cfg, -5)
	if err != nil {
		t.Fatalf("TryConsume(-5 tokens): %v", err)
	}
	if !d.Allowed {
		t.Error("should be allowed — negative tokens treated as 1")
	}
}

func TestBucketKeyGlobalFallback(t *testing.T) {
	// empty tenantID → "_global" namespace
	key := bucketKey("", "myop")
	if key == "" {
		t.Error("bucketKey with empty tenant must not be empty")
	}
	globalKey := bucketKey("", "myop")
	tenantKey := bucketKey("tenant-a", "myop")
	if globalKey == tenantKey {
		t.Error("global and tenant keys must differ")
	}
}
