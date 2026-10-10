// Copyright (c) 2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package admission

import (
	"sync"
	"time"
)

// tokenBucket is a simple thread-safe token bucket with an injectable
// clock. Fractional tokens are kept so that a cluster rate R split over N
// nodes (R/N each) sums back to exactly R.
type tokenBucket struct {
	mu sync.Mutex

	now   func() time.Time
	rate  float64 // effective refill rate (tokens/sec); baseline + bonus
	burst float64

	tokens float64
	last   time.Time

	// consumed is the number of tokens granted since the last window reset.
	consumed float64
	// wanted counts requests that had to queue (or were rejected) since
	// the last window reset; it signals demand for redistributed refill.
	wanted int64
}

func newTokenBucket(rate float64, burst float64, now func() time.Time) *tokenBucket {
	if now == nil {
		now = time.Now
	}
	return &tokenBucket{
		now:    now,
		rate:   rate,
		burst:  burst,
		tokens: burst, // a fresh bucket starts full
		last:   now(),
	}
}

// refill must be called with mu held.
func (b *tokenBucket) refill(now time.Time) {
	if now.After(b.last) {
		b.tokens += b.rate * now.Sub(b.last).Seconds()
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
}

// reconfigure changes rate/burst while keeping accumulated tokens. Tokens
// above the new burst are capped. A rate change never grants the burst
// again, so in-flight requests admitted before the change are unaffected.
func (b *tokenBucket) reconfigure(rate, burst float64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	b.refill(now)
	b.rate = rate
	b.burst = burst
	if b.tokens > burst {
		b.tokens = burst
	}
}

// effectiveRate returns the current refill rate.
func (b *tokenBucket) effectiveRate() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rate
}

// reconfigureRate updates only the refill rate (used by the cluster
// aggregator every exchange window); burst is untouched.
func (b *tokenBucket) reconfigureRate(rate float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.refill(now)
	b.rate = rate
}

// tryTake attempts to consume one token without waiting.
func (b *tokenBucket) tryTake() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refill(b.now())
	if b.tokens >= 1 {
		b.tokens--
		b.consumed++
		return true
	}
	b.wanted++
	return false
}

// refund gives back one previously taken token (used when acquiring from
// several buckets atomically and a later bucket in the set is empty).
func (b *tokenBucket) refund() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consumed--
	if b.tokens < b.burst {
		b.tokens++
	}
}

// waitFor is the duration until one token becomes available. A zero or
// negative rate means tokens will never refill.
func (b *tokenBucket) waitFor() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refill(b.now())
	if b.tokens >= 1 {
		return 0
	}
	if b.rate <= 0 {
		return time.Duration(1<<63 - 1) // never
	}
	secs := (1 - b.tokens) / b.rate
	d := time.Duration(secs * float64(time.Second))
	if d < 0 {
		return 0
	}
	return d
}

// available returns the current whole number of available tokens.
func (b *tokenBucket) available() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill(b.now())
	return int(b.tokens)
}

// retryAfter suggests when a token will be available, minimum 1 second.
// It is rounded UP to whole seconds so clients never retry too early.
func (b *tokenBucket) retryAfter() time.Duration {
	d := b.waitFor()
	if d <= 0 || d == time.Duration(1<<63-1) {
		return time.Second
	}
	d = (d + time.Second - 1) / time.Second * time.Second
	if d < time.Second {
		d = time.Second
	}
	return d
}

// resetWindow returns and clears the counters accumulated since the last
// reset, together with the current number of available tokens.
func (b *tokenBucket) resetWindow() (consumed float64, wanted int64, available int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	consumed = b.consumed
	wanted = b.wanted
	b.consumed = 0
	b.wanted = 0
	b.refill(b.now())
	return consumed, wanted, int(b.tokens)
}
