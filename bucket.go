// Package tokenbucket implements a Redis-backed lazy-refill token bucket.
//
// Extracted from the Archon agent platform (https://github.com/diogoX451).
package tokenbucket

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// BucketConfig describes one quota class. Capacity is the burst size;
// RefillPerSecond is the steady-state rate. WaitTokens is unused at
// this stage (TryConsume is non-blocking) but reserved so the same
// struct can later support BlockingConsume(timeout).
type BucketConfig struct {
	Name            string  `mapstructure:"name"            json:"name"`
	Capacity        float64 `mapstructure:"capacity"        json:"capacity"`
	RefillPerSecond float64 `mapstructure:"refill_per_sec"  json:"refill_per_sec"`
}

// Decision is what TryConsume returns. Allowed is the only required
// bit; Remaining and RetryAfter are advisory and surface as response
// headers (X-RateLimit-Remaining, Retry-After).
type Decision struct {
	Allowed    bool
	Remaining  float64
	RetryAfter time.Duration
}

// TokenBucket runs the limiter against a Redis client. Concurrency-
// safe: every call to TryConsume is one atomic Lua eval; nothing
// in this struct is mutated after construction.
type TokenBucket struct {
	client redis.Cmdable
	ttl    time.Duration
}

// NewTokenBucket binds the limiter to a Redis client. TTL is the
// inactivity window after which an idle bucket key is removed (so
// abandoned tenants don't keep state forever); 1h is a sensible
// default for HTTP traffic.
func NewTokenBucket(client redis.Cmdable) *TokenBucket {
	return &TokenBucket{client: client, ttl: time.Hour}
}

// Lua script: lazy refill + conditional consume + write-through.
// Returns {allowed_int, remaining_str, retry_after_ms_int}.
//
//   - KEYS[1] is the bucket key (already namespaced by tenant).
//   - ARGV[1] capacity, ARGV[2] refill/sec, ARGV[3] now_ms,
//     ARGV[4] tokens_to_consume, ARGV[5] ttl_seconds.
const tokenBucketLua = `
local key = KEYS[1]
local cap = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local req = tonumber(ARGV[4])
local ttl = tonumber(ARGV[5])

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then tokens = cap end
if ts == nil then ts = now_ms end

local elapsed_sec = math.max(0, now_ms - ts) / 1000.0
tokens = math.min(cap, tokens + elapsed_sec * refill)

local allowed = 0
local retry_ms = 0
if tokens >= req then
  tokens = tokens - req
  allowed = 1
else
  if refill > 0 then
    retry_ms = math.ceil(((req - tokens) / refill) * 1000)
  else
    retry_ms = 1000
  end
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now_ms)
redis.call('EXPIRE', key, ttl)

return {allowed, tostring(tokens), retry_ms}
`

// TryConsume attempts to deduct `tokens` from the (tenantID, bucket)
// pair. Returns Allowed=false with a Retry-After hint when the bucket
// is empty.
//
// tenantID may be empty — global / unauthenticated traffic shares a
// single bucket under the namespace "_global". This keeps anonymous
// callers boxed without dropping them.
func (b *TokenBucket) TryConsume(ctx context.Context, tenantID, bucket string, cfg BucketConfig, tokens float64) (Decision, error) {
	if cfg.Capacity <= 0 {
		return Decision{}, errors.New("bucket capacity must be > 0")
	}
	if tokens <= 0 {
		tokens = 1
	}

	key := bucketKey(tenantID, bucket)
	now := time.Now().UnixMilli()
	res, err := b.client.Eval(ctx, tokenBucketLua, []string{key},
		cfg.Capacity, cfg.RefillPerSecond, now, tokens, int64(b.ttl.Seconds()),
	).Result()
	if err != nil {
		return Decision{}, err
	}
	arr, ok := res.([]interface{})
	if !ok || len(arr) != 3 {
		return Decision{}, fmt.Errorf("unexpected redis result: %T %v", res, res)
	}

	allowedNum, _ := arr[0].(int64)
	remainingStr, _ := arr[1].(string)
	retryMsNum, _ := arr[2].(int64)

	remaining, _ := strconv.ParseFloat(remainingStr, 64)
	return Decision{
		Allowed:    allowedNum == 1,
		Remaining:  remaining,
		RetryAfter: time.Duration(retryMsNum) * time.Millisecond,
	}, nil
}

// bucketKey is the canonical Redis key. Format mirrors the
// workflow keys: tenant prefix first, then logical name.
func bucketKey(tenantID, bucket string) string {
	t := strings.TrimSpace(tenantID)
	if t == "" {
		t = "_global"
	}
	b := strings.TrimSpace(bucket)
	if b == "" {
		b = "_default"
	}
	return fmt.Sprintf("rate:t:%s:%s", t, b)
}
