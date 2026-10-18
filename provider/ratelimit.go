package provider

import (
	"context"
	"crypto/sha256"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// WeTrakr rate limits for a free account, from its Limits page: 200 GETs and
// 60 POST/PUT/DELETE per minute, and at most 2 writes and 10 reads in flight
// per user. The host already runs one RPC at a time per connection, so the
// plugin only has to pace: one write per second keeps inside the write window,
// and reads go at full speed for a burst and then at a pace that stays inside
// the read window. VIP accounts have higher limits; the free ones are safe for
// everyone.
const (
	writeInterval = time.Second
	writeBurst    = 1
	readInterval  = 330 * time.Millisecond
	readBurst     = 20

	// A 429 whose Retry-After is this short, which is what the per-minute
	// window and CONCURRENCY_LIMIT send, is retried in place. Longer waits
	// defer the connection instead of holding an RPC open.
	maxInPlaceRetryWait = 10 * time.Second
	maxRetryAttempts    = 2

	// Without a Retry-After hint, wait out one full per-minute window.
	defaultRetryAfter = time.Minute
)

// parseRetryAfter reads an RFC 9110 Retry-After value: delay-seconds or an
// HTTP-date. ok is false when the value is absent or malformed. An HTTP-date
// that has already passed parses as zero.
func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		if seconds > int64(math.MaxInt64/time.Second) {
			return time.Duration(math.MaxInt64), true
		}
		return time.Duration(seconds) * time.Second, true
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(at.Sub(now), 0), true
	}
	return 0, false
}

// sleepContext waits for d, returning early with ctx.Err() when ctx ends.
func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

const credentialLimiterIdleTTL = 10 * time.Minute

// credentialLimiter paces requests per credential, so one account's burst
// never delays another's. It lives in the plugin process and is keyed by a
// SHA-256 digest of the credential: the map never holds a raw token.
type credentialLimiter struct {
	every time.Duration
	burst int
	now   func() time.Time

	mu        sync.Mutex
	limiters  map[[sha256.Size]byte]*credentialLimiterEntry
	lastSweep time.Time
}

type credentialLimiterEntry struct {
	limiter  *rate.Limiter
	lastUsed time.Time
}

func newCredentialLimiter(every time.Duration, burst int) *credentialLimiter {
	return &credentialLimiter{
		every:    every,
		burst:    max(burst, 1),
		now:      time.Now,
		limiters: make(map[[sha256.Size]byte]*credentialLimiterEntry),
	}
}

// Wait blocks until credential may send its next request. It returns an error
// without waiting when ctx is already done or its deadline would pass first.
func (l *credentialLimiter) Wait(ctx context.Context, credential string) error {
	return l.limiter(credential).Wait(ctx)
}

func (l *credentialLimiter) limiter(credential string) *rate.Limiter {
	key := sha256.Sum256([]byte(credential))
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.lastSweep) >= credentialLimiterIdleTTL {
		l.lastSweep = now
		// An idle limiter with a full bucket behaves exactly like a new one, so
		// dropping it cannot let a credential exceed its rate.
		for key, entry := range l.limiters {
			if now.Sub(entry.lastUsed) >= credentialLimiterIdleTTL &&
				entry.limiter.TokensAt(now) >= float64(l.burst) {
				delete(l.limiters, key)
			}
		}
	}
	entry, ok := l.limiters[key]
	if !ok {
		entry = &credentialLimiterEntry{limiter: rate.NewLimiter(rate.Every(l.every), l.burst)}
		l.limiters[key] = entry
	}
	entry.lastUsed = now
	return entry.limiter
}
