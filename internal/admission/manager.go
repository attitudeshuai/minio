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
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// QuotaSlot identifies one of the per-rule token buckets.
type QuotaSlot string

const (
	// SlotTotal is the rule-wide total quota.
	SlotTotal QuotaSlot = "total"
	// SlotRead is the read-class quota.
	SlotRead QuotaSlot = "read"
	// SlotWrite is the write-class quota.
	SlotWrite QuotaSlot = "write"
	// SlotList is the list-class quota.
	SlotList QuotaSlot = "list"
)

const numShards = 16

// Stats holds admission counters for one rule slot.
type Stats struct {
	Accepted int64 `json:"accepted"`
	Queued   int64 `json:"queued"`
	Rejected int64 `json:"rejected"`
	Waiting  int64 `json:"waiting"`
}

// SlotUsage is the point-in-time state of one rule slot.
type SlotUsage struct {
	Slot  QuotaSlot  `json:"slot"`
	Quota ClassQuota `json:"quota"`
	Stats Stats      `json:"stats"`
	Avail int        `json:"available"`
}

// RuleUsage is the point-in-time state of one rule.
type RuleUsage struct {
	Scope Scope       `json:"scope"`
	Name  string      `json:"name"`
	Slots []SlotUsage `json:"slots"`
}

// WindowUsage is one slot's activity during the last exchange window.
type WindowUsage struct {
	Scope     Scope
	Name      string
	Slot      QuotaSlot
	Rate      float64 // configured cluster rate
	Burst     int     // configured cluster burst
	EffRate   float64 // current local effective refill rate
	Consumed  float64 // tokens granted during the window
	Wanted    int64   // queued/rejected attempts during the window
	Available int     // tokens available at window end
}

// Decision is the result of an admission check.
type Decision struct {
	Allowed    bool
	Canceled   bool
	Scope      Scope
	Name       string
	Class      ActionClass
	RetryAfter time.Duration
	Limit      float64
	Remaining  int
}

// Option configures a Manager.
type Option func(*Manager)

// WithClock injects a clock (mainly for tests).
func WithClock(now func() time.Time) Option {
	return func(m *Manager) { m.now = now }
}

// WithNodeCount injects the cluster node count provider.
func WithNodeCount(n func() int) Option {
	return func(m *Manager) { m.nodeCount = n }
}

// WithWaiter injects the wait function used while queued. The function must
// return true when the duration elapses and false when ctx is canceled.
// Defaults to a real timer; tests with a virtual clock inject a waiter that
// advances the clock itself.
func WithWaiter(wait func(ctx context.Context, d time.Duration) bool) Option {
	return func(m *Manager) { m.wait = wait }
}

func defaultWaiter(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

type slotState struct {
	quota  ClassQuota
	bucket *tokenBucket

	accepted atomic.Int64
	queued   atomic.Int64
	rejected atomic.Int64
	waiting  atomic.Int64
}

type ruleState struct {
	mu    sync.RWMutex
	rule  Rule
	slots [4]*slotState // index order: total, read, write, list
}

func slotIndex(slot QuotaSlot) int {
	switch slot {
	case SlotTotal:
		return 0
	case SlotRead:
		return 1
	case SlotWrite:
		return 2
	case SlotList:
		return 3
	}
	return -1
}

func classIndex(class ActionClass) int {
	switch class {
	case ClassRead:
		return 1
	case ClassWrite:
		return 2
	case ClassList:
		return 3
	}
	return -1
}

func (rs *ruleState) get(i int) *slotState {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.slots[i]
}

type managerShard struct {
	mu    sync.RWMutex
	rules map[Scope]map[string]*ruleState
}

// Manager holds all local admission state for one node.
type Manager struct {
	shards [numShards]*managerShard

	enabled     atomic.Bool
	defaultWait time.Duration
	now         func() time.Time
	nodeCount   func() int
	wait        func(ctx context.Context, d time.Duration) bool
}

// NewManager creates a Manager. The manager starts disabled; it is enabled
// by ApplyRules when at least one rule exists.
func NewManager(defaultWait time.Duration, opts ...Option) *Manager {
	m := &Manager{
		defaultWait: defaultWait,
		now:         time.Now,
		nodeCount:   func() int { return 1 },
		wait:        defaultWaiter,
	}
	for i := range m.shards {
		m.shards[i] = &managerShard{rules: make(map[Scope]map[string]*ruleState)}
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

// Enabled reports whether any rule is active.
func (m *Manager) Enabled() bool {
	return m.enabled.Load()
}

// shardIndexOf is a zero-allocation FNV-1a 32-bit shard selector by name.
func shardIndexOf(name string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(name); i++ {
		h ^= uint32(name[i])
		h *= prime32
	}
	return h % numShards
}

func (m *Manager) shardForName(name string) *managerShard {
	return m.shards[shardIndexOf(name)]
}

func (m *Manager) localShare(rate float64, burst int) (float64, float64) {
	n := m.nodeCount()
	if n < 1 {
		n = 1
	}
	return rate / float64(n), float64(burst) / float64(n)
}

func (m *Manager) newSlot(q *ClassQuota) *slotState {
	rate, burst := m.localShare(q.Rate, q.Burst)
	return &slotState{
		quota:  *q,
		bucket: newTokenBucket(rate, burst, m.now),
	}
}

// ApplyRules replaces the full rule set. Existing token buckets are kept
// (their accumulated tokens preserved) when the same slot is present in the
// new rule; removed slots/rules are reclaimed immediately.
func (m *Manager) ApplyRules(rules Rules) error {
	if err := rules.Validate(); err != nil {
		return err
	}
	if len(rules) == 0 {
		for _, sh := range m.shards {
			sh.mu.Lock()
			sh.rules = make(map[Scope]map[string]*ruleState)
			sh.mu.Unlock()
		}
		m.enabled.Store(false)
		return nil
	}

	// Index the incoming rules by shard, then by scope/name.
	incoming := make(map[uint32]map[Scope]map[string]Rule, numShards)
	for _, r := range rules {
		si := shardIndexOf(r.Name)
		if incoming[si] == nil {
			incoming[si] = make(map[Scope]map[string]Rule)
		}
		if incoming[si][r.Scope] == nil {
			incoming[si][r.Scope] = make(map[string]Rule)
		}
		incoming[si][r.Scope][r.Name] = r
	}

	for si, sh := range m.shards {
		want := incoming[uint32(si)]
		sh.mu.Lock()
		next := make(map[Scope]map[string]*ruleState, len(want))
		for scope, byName := range want {
			scopeMap := make(map[string]*ruleState, len(byName))
			for name, r := range byName {
				if old := sh.rules[scope][name]; old != nil {
					m.reconfigureRule(old, r)
					scopeMap[name] = old
				} else {
					scopeMap[name] = m.newRule(r)
				}
			}
			next[scope] = scopeMap
		}
		sh.rules = next
		sh.mu.Unlock()
	}
	m.enabled.Store(true)
	return nil
}

func (m *Manager) newRule(r Rule) *ruleState {
	rs := &ruleState{rule: r}
	m.assignSlots(rs, r)
	return rs
}

// reconfigureRule updates a rule in place, reusing slot buckets.
func (m *Manager) reconfigureRule(rs *ruleState, r Rule) {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	var next [4]*slotState
	quotaBySlot := [4]*ClassQuota{r.Total, r.Reads, r.Writes, r.Lists}
	for i, q := range quotaBySlot {
		old := rs.slots[i]
		switch {
		case q == nil:
			// slot removed; bucket reclaimed.
		case old != nil:
			rate, burst := m.localShare(q.Rate, q.Burst)
			old.bucket.reconfigure(rate, burst)
			old.quota = *q
			next[i] = old
		default:
			next[i] = m.newSlot(q)
		}
	}
	rs.slots = next
	rs.rule = r
}

func (m *Manager) assignSlots(rs *ruleState, r Rule) {
	rs.slots[0] = slotOrNil(m, r.Total)
	rs.slots[1] = slotOrNil(m, r.Reads)
	rs.slots[2] = slotOrNil(m, r.Writes)
	rs.slots[3] = slotOrNil(m, r.Lists)
}

func slotOrNil(m *Manager, q *ClassQuota) *slotState {
	if q == nil {
		return nil
	}
	return m.newSlot(q)
}

func (m *Manager) lookup(scope Scope, name string) *ruleState {
	sh := m.shardForName(name)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return sh.rules[scope][name]
}

// Remove deletes one rule and reclaims its counters.
func (m *Manager) Remove(scope Scope, name string) {
	sh := m.shardForName(name)
	sh.mu.Lock()
	delete(sh.rules[scope], name)
	sh.mu.Unlock()
}

// RemoveNameIn deletes a rule in a scope if pred(name) is true. Returns the
// removed names.
func (m *Manager) RemoveNameIn(scope Scope, pred func(name string) bool) []string {
	var removed []string
	for _, sh := range m.shards {
		sh.mu.Lock()
		for name, rs := range sh.rules[scope] {
			if pred(name) {
				delete(sh.rules[scope], name)
				removed = append(removed, rs.rule.Name)
			}
		}
		sh.mu.Unlock()
	}
	return removed
}

// RuleCount returns the number of active rules (mainly for tests/telemetry).
func (m *Manager) RuleCount() int {
	n := 0
	for _, sh := range m.shards {
		sh.mu.RLock()
		for _, byName := range sh.rules {
			n += len(byName)
		}
		sh.mu.RUnlock()
	}
	return n
}

// participants fills out (capacity 2: total first, class second) with the
// slots a request must acquire and returns how many were written.
func participants(rs *ruleState, class ActionClass, out *[2]*slotState) int {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	n := 0
	if rs.slots[0] != nil {
		out[0] = rs.slots[0]
		n++
	}
	if i := classIndex(class); i > 0 && rs.slots[i] != nil {
		out[n] = rs.slots[i]
		n++
	}
	return n
}

// tryAll attempts to take a token from every participating slot, rolling
// back earlier takes on failure.
func tryAll(slots []*slotState) (*slotState, bool) {
	for i, s := range slots {
		if !s.bucket.tryTake() {
			for _, taken := range slots[:i] {
				taken.bucket.refund()
			}
			return s, false
		}
	}
	return nil, true
}

// Admit checks one dimension (bucket or caller) for one request.
//
// If the request is admitted, one token is consumed from the total slot and
// the class slot that exist. If tokens are momentarily unavailable, the call
// blocks up to the smallest configured wait (or the manager default) before
// rejecting. A canceled context is reported as Decision.Canceled and is not
// counted as rejected.
func (m *Manager) Admit(ctx context.Context, scope Scope, name string, class ActionClass) Decision {
	d := Decision{Allowed: true, Scope: scope, Name: name, Class: class}
	if !m.enabled.Load() {
		return d
	}
	rs := m.lookup(scope, name)
	if rs == nil {
		return d
	}
	var slotArr [2]*slotState
	n := participants(rs, class, &slotArr)
	slots := slotArr[:n]
	if n == 0 {
		return d
	}

	if _, ok := tryAll(slots); ok {
		for _, s := range slots {
			s.accepted.Add(1)
		}
		d.Remaining = minAvailable(slots)
		return d
	}

	// Queue with a bounded wait.
	maxWait := time.Duration(1<<63 - 1)
	for _, s := range slots {
		if w := s.quota.WaitOr(m.defaultWait); w < maxWait {
			maxWait = w
		}
	}
	for _, s := range slots {
		s.queued.Add(1)
		s.waiting.Add(1)
	}
	defer func() {
		for _, s := range slots {
			s.waiting.Add(-1)
		}
	}()

	deadline := m.now().Add(maxWait)
	for {
		if fail, ok := tryAll(slots); ok {
			for _, s := range slots {
				s.accepted.Add(1)
			}
			d.Remaining = minAvailable(slots)
			return d
		} else if maxWait <= 0 {
			return m.reject(slots, fail)
		}

		// Wait until every slot is expected to be ready, capped by deadline.
		ready := time.Duration(0)
		for _, s := range slots {
			if w := s.bucket.waitFor(); w > ready {
				ready = w
			}
		}
		sleep := ready
		if left := deadline.Sub(m.now()); sleep > left {
			sleep = left
		}
		if sleep <= 0 {
			fail, _ := tryAll(slots)
			return m.reject(slots, fail)
		}

		if !m.wait(ctx, sleep) {
			d.Canceled = true
			d.Allowed = false
			return d
		}
	}
}

func (m *Manager) reject(slots []*slotState, fail *slotState) Decision {
	for _, s := range slots {
		s.rejected.Add(1)
	}
	retryAfter := time.Second
	limit := 0.0
	for _, s := range slots {
		if d := s.bucket.retryAfter(); d > retryAfter {
			retryAfter = d
			limit = s.quota.Rate
		}
	}
	if fail != nil && limit == 0 {
		limit = fail.quota.Rate
	}
	return Decision{
		Allowed:    false,
		RetryAfter: retryAfter,
		Limit:      limit,
		Remaining:  0,
	}
}

func minAvailable(slots []*slotState) int {
	min := -1
	for _, s := range slots {
		a := s.bucket.available()
		if min < 0 || a < min {
			min = a
		}
	}
	if min < 0 {
		return 0
	}
	return min
}

// Refund returns one token from the same slots a successful Admit took
// (total plus the class-denominated slot). It also reverses the accepted
// counter, so a request rejected by a later dimension is not counted.
func (m *Manager) Refund(scope Scope, name string, class ActionClass) {
	rs := m.lookup(scope, name)
	if rs == nil {
		return
	}
	participants := [2]*slotState{nil, nil}
	rs.mu.RLock()
	participants[0] = rs.slots[0]
	if ci := classIndex(class); ci > 0 && rs.slots[ci] != nil {
		participants[1] = rs.slots[ci]
	}
	rs.mu.RUnlock()

	for _, s := range participants {
		if s == nil {
			continue
		}
		s.bucket.refund()
		s.accepted.Add(-1)
	}
}

// SnapshotStats returns current rules and counters for inspection.
func (m *Manager) SnapshotStats() []RuleUsage {
	var out []RuleUsage
	for _, sh := range m.shards {
		sh.mu.RLock()
		for _, byName := range sh.rules {
			for _, rs := range byName {
				rs.mu.RLock()
				usage := RuleUsage{Scope: rs.rule.Scope, Name: rs.rule.Name}
				rs.mu.RUnlock()

				// Re-acquire only slot locks below; rule identity is immutable
				// for the life of a ruleState (only replaced wholesale under
				// shard write lock), so the copied usage header is safe.
				rs.mu.RLock()
				for i, s := range rs.slots {
					if s == nil {
						continue
					}
					usage.Slots = append(usage.Slots, SlotUsage{
						Slot:  QuotaSlot([4]QuotaSlot{SlotTotal, SlotRead, SlotWrite, SlotList}[i]),
						Quota: s.quota,
						Stats: Stats{
							Accepted: s.accepted.Load(),
							Queued:   s.queued.Load(),
							Rejected: s.rejected.Load(),
							Waiting:  s.waiting.Load(),
						},
						Avail: s.bucket.available(),
					})
				}
				rs.mu.RUnlock()
				out = append(out, usage)
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// DrainWindowUsages returns and resets per-slot window counters. Only slots
// that saw traffic (consumed tokens or queued/rejected demand) are reported.
func (m *Manager) DrainWindowUsages() []WindowUsage {
	var out []WindowUsage
	for _, sh := range m.shards {
		sh.mu.RLock()
		for _, byName := range sh.rules {
			for _, rs := range byName {
				rs.mu.RLock()
				for i, s := range rs.slots {
					if s == nil {
						continue
					}
					consumed, wanted, avail := s.bucket.resetWindow()
					if consumed == 0 && wanted == 0 {
						continue
					}
					out = append(out, WindowUsage{
						Scope:     rs.rule.Scope,
						Name:      rs.rule.Name,
						Slot:      QuotaSlot([4]QuotaSlot{SlotTotal, SlotRead, SlotWrite, SlotList}[i]),
						Rate:      s.quota.Rate,
						Burst:     s.quota.Burst,
						EffRate:   s.bucket.effectiveRate(),
						Consumed:  consumed,
						Wanted:    wanted,
						Available: avail,
					})
				}
				rs.mu.RUnlock()
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// SetSlotRate overrides the effective refill rate of one slot. It is used
// by the cluster aggregator to redistribute unused peer refill. The burst
// is intentionally never changed here, preserving the Σburst = B invariant.
func (m *Manager) SetSlotRate(scope Scope, name string, slot QuotaSlot, rate float64) bool {
	rs := m.lookup(scope, name)
	if rs == nil {
		return false
	}
	i := slotIndex(slot)
	if i < 0 {
		return false
	}
	s := rs.get(i)
	if s == nil {
		return false
	}
	if rate < 0 {
		rate = 0
	}
	s.bucket.reconfigureRate(rate)
	return true
}

// GetSlotEffectiveRate returns the current effective refill rate (baseline
// plus any redistributed share) of a slot.
func (m *Manager) GetSlotEffectiveRate(scope Scope, name string, slot QuotaSlot) (float64, bool) {
	rs := m.lookup(scope, name)
	if rs == nil {
		return 0, false
	}
	s := rs.get(slotIndex(slot))
	if s == nil {
		return 0, false
	}
	return s.bucket.effectiveRate(), true
}

// GetSlotBaseline returns the local baseline rate/burst share for a slot.
func (m *Manager) GetSlotBaseline(scope Scope, name string, slot QuotaSlot) (rate, burst float64, ok bool) {
	rs := m.lookup(scope, name)
	if rs == nil {
		return 0, 0, false
	}
	s := rs.get(slotIndex(slot))
	if s == nil {
		return 0, 0, false
	}
	r, b := m.localShare(s.quota.Rate, s.quota.Burst)
	return r, b, true
}
