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
	"encoding/json"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock. Its waiter advances the clock by
// the requested duration so queueing behavior is fully deterministic.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func (c *fakeClock) waiter(ctx context.Context, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	c.advance(d)
	return ctx.Err() == nil
}

func waitPtr(d time.Duration) *time.Duration { return &d }

func newTestManager(t *testing.T, defaultWait time.Duration, nodes int) (*Manager, *fakeClock) {
	t.Helper()
	c := newFakeClock()
	m := NewManager(defaultWait,
		WithClock(c.now),
		WithNodeCount(func() int { return nodes }),
		WithWaiter(c.waiter),
	)
	return m, c
}

func quotaPtr(q ClassQuota) *ClassQuota { return &q }

func TestRulesValidation(t *testing.T) {
	good := Rules{
		{Scope: ScopeBucket, Name: "b", Total: quotaPtr(ClassQuota{Rate: 10, Burst: 20})},
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("expected valid: %v", err)
	}

	for i, r := range []Rule{
		{Scope: "weird", Name: "b", Total: quotaPtr(ClassQuota{Rate: 1, Burst: 1})},
		{Scope: ScopeBucket, Name: "", Total: quotaPtr(ClassQuota{Rate: 1, Burst: 1})},
		{Scope: ScopeBucket, Name: "b", Total: quotaPtr(ClassQuota{Rate: -1, Burst: 1})},
		{Scope: ScopeBucket, Name: "b", Total: quotaPtr(ClassQuota{Rate: 1, Burst: 0})},
		{Scope: ScopeBucket, Name: "b"},
	} {
		var rs Rules = []Rule{r}
		if err := rs.Validate(); err == nil {
			t.Fatalf("case %d: expected invalid rule", i)
		}
	}

	dup := Rules{
		{Scope: ScopeCaller, Name: "u", Total: quotaPtr(ClassQuota{Rate: 1, Burst: 1})},
		{Scope: ScopeCaller, Name: "u", Reads: quotaPtr(ClassQuota{Rate: 1, Burst: 1})},
	}
	if err := dup.Validate(); err == nil {
		t.Fatal("expected duplicate rule error")
	}
}

func TestClassQuotaJSONWaitMS(t *testing.T) {
	q := ClassQuota{Rate: 5.5, Burst: 10, Wait: waitPtr(250 * time.Millisecond)}
	data, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	var got ClassQuota
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.Rate != 5.5 || got.Burst != 10 || got.Wait == nil || *got.Wait != 250*time.Millisecond {
		t.Fatalf("round trip mismatch: %+v", got)
	}

	// wait_ms omitted -> nil wait (use manager default).
	if err := json.Unmarshal([]byte(`{"rate":1,"burst":1}`), &got); err != nil {
		t.Fatal(err)
	}
	if got.Wait != nil {
		t.Fatalf("expected nil wait, got %v", *got.Wait)
	}
}

func TestDisabledManagerAdmitsAll(t *testing.T) {
	m, _ := newTestManager(t, time.Second, 1)
	if m.Enabled() {
		t.Fatal("manager should start disabled")
	}
	for i := 0; i < 1000; i++ {
		if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); !d.Allowed {
			t.Fatalf("request %d rejected while disabled", i)
		}
	}
	if m.RuleCount() != 0 {
		t.Fatal("expected no rules")
	}
}

func TestTotalOnlyBurstAndImmediateReject(t *testing.T) {
	m, _ := newTestManager(t, 250*time.Millisecond, 1)
	rules := Rules{{
		Scope: ScopeBucket,
		Name:  "b",
		Total: quotaPtr(ClassQuota{Rate: 10, Burst: 2, Wait: waitPtr(0)}),
	}}
	if err := m.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}
	if !m.Enabled() {
		t.Fatal("manager should be enabled")
	}

	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); !d.Allowed {
		t.Fatal("1st should be admitted")
	}
	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassWrite); !d.Allowed {
		t.Fatal("2nd should be admitted (burst=2)")
	}
	d := m.Admit(context.Background(), ScopeBucket, "b", ClassList)
	if d.Allowed || d.Canceled {
		t.Fatalf("3rd should be rejected immediately, got %+v", d)
	}
	if d.RetryAfter < time.Second {
		t.Fatalf("Retry-After must be >=1s, got %v", d.RetryAfter)
	}
	if d.Remaining != 0 {
		t.Fatalf("remaining must be 0 on reject, got %d", d.Remaining)
	}

	// Unknown rule / other bucket is unaffected.
	if d := m.Admit(context.Background(), ScopeBucket, "other", ClassRead); !d.Allowed {
		t.Fatal("unconfigured bucket must be admitted")
	}
}

func TestClassAndTotalCombination(t *testing.T) {
	m, _ := newTestManager(t, 0, 1)
	rules := Rules{{
		Scope: ScopeBucket,
		Name:  "b",
		Total: quotaPtr(ClassQuota{Rate: 100, Burst: 100, Wait: waitPtr(0)}),
		Reads: quotaPtr(ClassQuota{Rate: 100, Burst: 1, Wait: waitPtr(0)}),
	}}
	if err := m.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}

	// First read consumes the only read burst; total still has plenty.
	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); !d.Allowed {
		t.Fatal("first read admitted")
	}
	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); d.Allowed {
		t.Fatal("second read must be rejected by read quota, total has capacity")
	}
	// Writes only touch total -> admitted.
	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassWrite); !d.Allowed {
		t.Fatal("write must be admitted via total")
	}
	// List without list quota only touches total.
	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassList); !d.Allowed {
		t.Fatal("list must be admitted via total")
	}
}

func TestQueueWaitAdmitThenReject(t *testing.T) {
	m, c := newTestManager(t, 250*time.Millisecond, 1)
	rules := Rules{{
		Scope: ScopeCaller,
		Name:  "u",
		Total: quotaPtr(ClassQuota{Rate: 1, Burst: 1, Wait: waitPtr(time.Second)}),
	}}
	if err := m.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if d := m.Admit(ctx, ScopeCaller, "u", ClassRead); !d.Allowed {
		t.Fatal("burst token")
	}
	// 2nd queues, the fake clock advances 1s inside the waiter, token refills.
	before := c.now()
	if d := m.Admit(ctx, ScopeCaller, "u", ClassRead); !d.Allowed || d.Canceled {
		t.Fatalf("queued request should be admitted after refill, got %+v", d)
	}
	if c.now().Sub(before) < time.Second {
		t.Fatalf("expected to wait ~1s, waited %v", c.now().Sub(before))
	}

	// 3rd with a shorter wait than the refill delay is rejected.
	rules[0].Total.Wait = waitPtr(500 * time.Millisecond)
	if err := m.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}
	d := m.Admit(ctx, ScopeCaller, "u", ClassWrite)
	if d.Allowed {
		t.Fatal("request beyond short wait must be rejected")
	}
	if d.RetryAfter < time.Second {
		t.Fatalf("retry-after >= 1s, got %v", d.RetryAfter)
	}
}

func TestQueueCancelNotRejected(t *testing.T) {
	m, _ := newTestManager(t, 250*time.Millisecond, 1)
	rules := Rules{{
		Scope: ScopeBucket,
		Name:  "b",
		Total: quotaPtr(ClassQuota{Rate: 1, Burst: 1, Wait: waitPtr(10 * time.Second)}),
	}}
	if err := m.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}
	// Consume the burst token.
	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); !d.Allowed {
		t.Fatal("burst token")
	}

	// Cancel the context immediately. The waiter observes the canceled ctx.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	// Install a waiter that does not advance the clock when canceled.
	origWait := m.wait
	m.wait = func(ctx context.Context, d time.Duration) bool {
		if ctx.Err() != nil {
			return false
		}
		<-ctx.Done()
		return false
	}
	defer func() { m.wait = origWait }()

	d := m.Admit(ctx, ScopeBucket, "b", ClassRead)
	if d.Allowed || !d.Canceled {
		t.Fatalf("expected canceled decision, got %+v", d)
	}
	us := m.SnapshotStats()
	if len(us) != 1 || us[0].Slots[0].Stats.Rejected != 0 {
		t.Fatalf("canceled request must not count as rejected: %+v", us)
	}
	if us[0].Slots[0].Stats.Queued == 0 {
		t.Fatal("canceled request should still be counted as having queued")
	}
}

func TestApplyRulesPreserveAndReclaim(t *testing.T) {
	m, c := newTestManager(t, 0, 1)
	rules := Rules{{
		Scope: ScopeBucket,
		Name:  "b",
		Total: quotaPtr(ClassQuota{Rate: 1, Burst: 5, Wait: waitPtr(0)}),
	}}
	if err := m.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}
	// Consume 3 of the 5 burst tokens.
	for i := 0; i < 3; i++ {
		if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); !d.Allowed {
			t.Fatal("admit")
		}
	}
	// Reconfigure with a larger rate/burst; accumulated tokens are kept.
	c.advance(time.Second) // 1 token refills at old rate
	rules[0].Total = quotaPtr(ClassQuota{Rate: 100, Burst: 50, Wait: waitPtr(0)})
	if err := m.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}
	// Before reconfigure: tokens = 5-3+1 = 3; new rate does not reset to 50.
	// Immediately admit 3; the 4th must fail (no queue, no refill time).
	for i := 0; i < 3; i++ {
		if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); !d.Allowed {
			t.Fatalf("expected %d-th admit from preserved tokens", i+1)
		}
	}
	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); d.Allowed {
		t.Fatal("tokens must not be reset to the new burst")
	}

	// Delete the rule -> counters reclaimed, manager may disable.
	if err := m.ApplyRules(nil); err != nil {
		t.Fatal(err)
	}
	if m.Enabled() || m.RuleCount() != 0 {
		t.Fatal("empty rule set must disable and clear the manager")
	}
	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); !d.Allowed {
		t.Fatal("after deletion everything is admitted")
	}
}

func TestNodeShareSplit(t *testing.T) {
	m, _ := newTestManager(t, 0, 4)
	rules := Rules{{
		Scope: ScopeBucket,
		Name:  "b",
		Total: quotaPtr(ClassQuota{Rate: 10, Burst: 8, Wait: waitPtr(0)}),
	}}
	if err := m.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}
	// Local burst share is 8/4 = 2 tokens.
	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); !d.Allowed {
		t.Fatal("1")
	}
	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); !d.Allowed {
		t.Fatal("2")
	}
	if d := m.Admit(context.Background(), ScopeBucket, "b", ClassRead); d.Allowed {
		t.Fatal("3rd must be rejected: local share is 2 of cluster burst 8 over N=4")
	}
	rate, burst, ok := m.GetSlotBaseline(ScopeBucket, "b", SlotTotal)
	if !ok || rate != 2.5 || burst != 2 {
		t.Fatalf("unexpected baseline %v %v %v", rate, burst, ok)
	}
}

func TestRefundRestoresTokensAndCounter(t *testing.T) {
	mgr := NewManager(0)
	rules := Rules{{
		Scope: ScopeBucket, Name: "b",
		Total: &ClassQuota{Rate: 1, Burst: 1},
		Reads: &ClassQuota{Rate: 1, Burst: 1},
	}}
	if err := mgr.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if d := mgr.Admit(ctx, ScopeBucket, "b", ClassRead); !d.Allowed {
		t.Fatal("first admit")
	}
	if d := mgr.Admit(ctx, ScopeBucket, "b", ClassRead); d.Allowed {
		t.Fatal("second admit must fail")
	}
	mgr.Refund(ScopeBucket, "b", ClassRead)
	if d := mgr.Admit(ctx, ScopeBucket, "b", ClassRead); !d.Allowed {
		t.Fatal("after refund admission must succeed")
	}
	var accepted int64
	for _, u := range mgr.SnapshotStats() {
		for _, s := range u.Slots {
			accepted += s.Stats.Accepted
		}
	}
	if accepted != 2 {
		t.Fatalf("net accepted across 2 slots want 2, got %d", accepted)
	}
}

func TestRetryAfterMinOneSecond(t *testing.T) {
	c := newFakeClock()
	b := newTokenBucket(10, 1, c.now)
	b.rate = 10 // 100ms until the next token
	b.burst = 1
	b.tokens = 0
	if d := b.retryAfter(); d != time.Second {
		t.Fatalf("sub-second wait must round up to 1s, got %v", d)
	}
}

func TestStatsCounters(t *testing.T) {
	m, _ := newTestManager(t, 250*time.Millisecond, 1)
	rules := Rules{{
		Scope: ScopeBucket,
		Name:  "b",
		Total: quotaPtr(ClassQuota{Rate: 1, Burst: 1, Wait: waitPtr(time.Second)}),
	}}
	if err := m.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	m.Admit(ctx, ScopeBucket, "b", ClassRead) // accepted immediately
	m.Admit(ctx, ScopeBucket, "b", ClassRead) // queued then accepted (1s refill)
	// Force a rejection via immediate-wait rule (token consumed above).
	rules[0].Total.Wait = waitPtr(0)
	_ = m.ApplyRules(rules)
	m.Admit(ctx, ScopeBucket, "b", ClassRead) // likely rejected: no token left

	var st Stats
	for _, u := range m.SnapshotStats() {
		if u.Scope == ScopeBucket && u.Name == "b" {
			st = u.Slots[0].Stats
		}
	}
	if st.Accepted < 2 {
		t.Fatalf("expected >=2 accepted, got %+v", st)
	}
	if st.Queued == 0 {
		t.Fatalf("expected queued count, got %+v", st)
	}
	if st.Rejected == 0 {
		t.Fatalf("expected at least one rejection, got %+v", st)
	}
	if st.Waiting != 0 {
		t.Fatalf("waiting must return to zero, got %d", st.Waiting)
	}
}

func BenchmarkManagerAdmitDisabled(b *testing.B) {
	m, _ := newTestManager(&testing.T{}, 0, 1)
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Admit(ctx, ScopeBucket, "b", ClassRead)
		}
	})
}

func BenchmarkManagerAdmitEnabled(b *testing.B) {
	m, _ := newTestManager(&testing.T{}, 0, 1)
	if err := m.ApplyRules(Rules{{
		Scope: ScopeBucket,
		Name:  "b",
		Total: quotaPtr(ClassQuota{Rate: 1000000, Burst: 1000000, Wait: waitPtr(0)}),
	}}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Admit(ctx, ScopeBucket, "b", ClassRead)
		}
	})
}
