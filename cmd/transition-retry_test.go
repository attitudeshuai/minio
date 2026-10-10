// Copyright (c) 2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type schedulerHarness struct {
	ctx        context.Context
	journal    *transitionFailureJournal
	health     *transitionTierHealthMgr
	scheduler  *transitionFailureScheduler
	dispatched []transitionFailureRecord
	probeErr   map[string]error
	probeCalls []string
	cfg        transitionFailureConfig
	now        time.Time
}

func newSchedulerHarness(t *testing.T, threshold int) *schedulerHarness {
	t.Helper()
	ctx := context.Background()
	store := newFakeTransitionStore()
	j := newTransitionFailureJournal(ctx)
	if err := j.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	h := newTransitionTierHealthMgr(ctx)
	if err := h.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	cfg := testFailureCfg()
	cfg.PauseThreshold = threshold
	cfg.ProbeInterval = time.Minute
	// Inject the same config into the health manager (production reads the
	// global ILM config).
	h.cfgFn = func() transitionFailureConfig { return cfg }
	sh := &schedulerHarness{
		ctx:      ctx,
		journal:  j,
		health:   h,
		probeErr: make(map[string]error),
		now:      now,
	}
	sh.scheduler = &transitionFailureScheduler{
		journal: j,
		health:  h,
		enqueue: func(rec transitionFailureRecord) { sh.dispatched = append(sh.dispatched, rec) },
		probe: func(ctx context.Context, tier string) error {
			sh.probeCalls = append(sh.probeCalls, tier)
			return sh.probeErr[tier]
		},
		cfgFn:    func() transitionFailureConfig { return cfg },
		nowFn:    func() time.Time { return sh.now },
		baseline: make(map[string]failureBaseline),
		probeAt:  make(map[string]time.Time),
		claimed:  make(map[string]dispatchClaim),
	}
	return sh
}

func newDueRecord(tier, object string, failedAt time.Time) transitionFailureRecord {
	oi := testObjectInfo("b", object, "v-"+object)
	rec := recordTransitionFailure(nil, oi, tier, false,
		transitionFailureBackendDown, "backend down", testFailureCfg(), failedAt)
	rec.NextRetryAt = failedAt.Add(time.Second)
	return rec
}

func advanceRecord(rec transitionFailureRecord, failedAt time.Time) transitionFailureRecord {
	oi := ObjectInfo{
		Bucket:    rec.Bucket,
		Name:      rec.Object,
		VersionID: rec.VersionID,
		Size:      rec.Size,
		ETag:      rec.ETag,
		ModTime:   rec.ModTime,
	}
	out := recordTransitionFailure(&rec, oi, rec.Tier, rec.Noncurrent,
		transitionFailureBackendDown, "backend still down", testFailureCfg(), failedAt)
	out.NextRetryAt = failedAt.Add(time.Second)
	return out
}

func TestSchedulerAutoPauseProbeResume(t *testing.T) {
	sh := newSchedulerHarness(t, 3)

	t0 := sh.now.Add(-time.Minute)
	r1 := newDueRecord("WARM-S", "o1", t0)
	r2 := newDueRecord("WARM-S", "o2", t0)
	sh.journal.upsert(sh.ctx, r1)
	sh.journal.upsert(sh.ctx, r2)

	// Tick 1 (priming): existing records are dispatched but NOT counted as
	// new failures (leader takeover must not false-pause).
	sh.scheduler.tick(sh.ctx)
	if len(sh.dispatched) != 2 {
		t.Fatalf("tick1: expected 2 dispatched, got %d", len(sh.dispatched))
	}
	if h, _ := sh.health.Get("WARM-S"); h.ConsecutiveFailures != 0 || h.Paused {
		t.Fatalf("tick1: priming must not count failures, got %+v", h)
	}

	// Tick 2: first real diff - two advances, streak 2; clock moved beyond
	// the recomputed attempt-2 backoff (~16-24s).
	sh.now = sh.now.Add(60 * time.Second)
	sh.dispatched = nil
	r1 = advanceRecord(r1, sh.now.Add(-50*time.Second))
	r2 = advanceRecord(r2, sh.now.Add(-40*time.Second))
	sh.journal.upsert(sh.ctx, r1)
	sh.journal.upsert(sh.ctx, r2)
	sh.scheduler.tick(sh.ctx)
	if h, _ := sh.health.Get("WARM-S"); h.ConsecutiveFailures != 2 || h.Paused {
		t.Fatalf("tick2: expected streak=2 not paused, got %+v", h)
	}
	if len(sh.dispatched) != 2 {
		t.Fatalf("tick2: advanced records should be re-dispatched, got %d", len(sh.dispatched))
	}

	// Tick 3: two more failed attempts cross the threshold of 3 -> paused.
	sh.now = sh.now.Add(60 * time.Second)
	sh.dispatched = nil
	r1 = advanceRecord(r1, sh.now.Add(-50*time.Second))
	r2 = advanceRecord(r2, sh.now.Add(-40*time.Second))
	sh.journal.upsert(sh.ctx, r1)
	sh.journal.upsert(sh.ctx, r2)
	sh.scheduler.tick(sh.ctx)
	h, _ := sh.health.Get("WARM-S")
	if !h.Paused || h.Manual || h.ConsecutiveFailures != 4 {
		t.Fatalf("tick3: expected auto-paused streak=4, got %+v", h)
	}
	if len(sh.dispatched) != 0 {
		t.Fatalf("tick3: paused tier must not dispatch, got %d", len(sh.dispatched))
	}

	// Before the probe interval elapses, no probe is attempted.
	sh.now = sh.now.Add(30 * time.Second)
	sh.probeErr["WARM-S"] = errors.New("must not be called")
	sh.scheduler.tick(sh.ctx)
	if h, _ := sh.health.Get("WARM-S"); !h.Paused {
		t.Fatalf("tick4: expected to remain paused")
	}
	if len(sh.probeCalls) != 0 {
		t.Fatalf("tick4: no probe expected within interval, got %v", sh.probeCalls)
	}

	// At the interval, the failing probe keeps the tier paused.
	sh.now = sh.now.Add(40 * time.Second)
	sh.probeErr["WARM-S"] = errors.New("still down")
	sh.scheduler.tick(sh.ctx)
	if h, _ := sh.health.Get("WARM-S"); !h.Paused {
		t.Fatalf("tick5: expected to remain paused after failed probe")
	}

	// Another interval later, a healthy probe resumes and dispatches.
	sh.now = sh.now.Add(time.Minute)
	sh.probeErr = map[string]error{}
	sh.dispatched = nil
	sh.scheduler.tick(sh.ctx)
	if h, _ := sh.health.Get("WARM-S"); h.Paused || h.Manual || h.ConsecutiveFailures != 0 {
		t.Fatalf("tick6: expected resumed healthy tier, got %+v", h)
	}
	if len(sh.dispatched) != 2 {
		t.Fatalf("tick6: expected list resumed (2 dispatched), got %d", len(sh.dispatched))
	}

	// Successful retries remove records: success evidence keeps streak 0.
	sh.now = sh.now.Add(10 * time.Second)
	sh.journal.succeed(sh.ctx, r1.ID)
	sh.journal.succeed(sh.ctx, r2.ID)
	sh.dispatched = nil
	sh.scheduler.tick(sh.ctx)
	if len(sh.dispatched) != 0 {
		t.Fatalf("tick7: no due records expected, got %d", len(sh.dispatched))
	}
	if h, _ := sh.health.Get("WARM-S"); h.Paused || h.ConsecutiveFailures != 0 {
		t.Fatalf("tick7: unexpected health %+v", h)
	}
}

func TestSchedulerManualPauseNeverAutoResumes(t *testing.T) {
	sh := newSchedulerHarness(t, 3)
	r := newDueRecord("WARM-M", "o1", sh.now.Add(-time.Minute))
	sh.journal.upsert(sh.ctx, r)

	if err := sh.health.Pause(sh.ctx, "WARM-M", true, "operator"); err != nil {
		t.Fatal(err)
	}
	// Probe reports healthy for many consecutive ticks.
	for i := 0; i < 5; i++ {
		sh.now = sh.now.Add(time.Minute)
		sh.scheduler.tick(sh.ctx)
	}
	h, _ := sh.health.Get("WARM-M")
	if !h.Paused || !h.Manual {
		t.Fatalf("manual pause must not be auto-resumed: %+v", h)
	}
	if len(sh.dispatched) != 0 {
		t.Fatalf("no dispatch while manually paused, got %d", len(sh.dispatched))
	}

	// Explicit resume unblocks the list.
	if err := sh.health.Resume(sh.ctx, "WARM-M"); err != nil {
		t.Fatal(err)
	}
	sh.now = sh.now.Add(time.Minute)
	sh.scheduler.tick(sh.ctx)
	if len(sh.dispatched) != 1 {
		t.Fatalf("expected dispatch after manual resume, got %d", len(sh.dispatched))
	}
}

func TestSchedulerDispatchFilters(t *testing.T) {
	sh := newSchedulerHarness(t, 0) // auto-pause disabled
	now := sh.now

	past := newDueRecord("WARM-A", "past", now.Add(-time.Minute))
	sh.journal.upsert(sh.ctx, past)

	future := newDueRecord("WARM-A", "future", now.Add(time.Hour))
	future.NextRetryAt = now.Add(time.Hour)
	sh.journal.upsert(sh.ctx, future)

	exh := newDueRecord("WARM-A", "exh", now.Add(-time.Minute))
	exh.Status = transitionFailureExhausted
	exh.NextRetryAt = time.Time{}
	sh.journal.upsert(sh.ctx, exh)

	paused := newDueRecord("WARM-P", "paused", now.Add(-time.Minute))
	sh.journal.upsert(sh.ctx, paused)
	if err := sh.health.Pause(sh.ctx, "WARM-P", false, "auto"); err != nil {
		t.Fatal(err)
	}
	// Probe keeps failing, so the tier stays paused through the tick.
	sh.probeErr["WARM-P"] = errors.New("still down")

	sh.scheduler.tick(sh.ctx)
	if len(sh.dispatched) != 1 || sh.dispatched[0].Object != "past" {
		t.Fatalf("expected exactly the due healthy record dispatched, got %+v", sh.dispatched)
	}
}

func TestSchedulerPerTierAndTotalLimits(t *testing.T) {
	sh := newSchedulerHarness(t, 0)
	now := sh.now

	mk := func(tier string, n int) {
		for i := 0; i < n; i++ {
			rec := newDueRecord(tier, fmt.Sprintf("%s-%03d", tier, i), now.Add(-time.Minute))
			sh.journal.upsert(sh.ctx, rec)
		}
	}

	// Two tiers with 120 due records each: capped at 100 per tier.
	mk("WARM-A", 120)
	mk("WARM-B", 120)
	sh.scheduler.tick(sh.ctx)
	if len(sh.dispatched) != 200 {
		t.Fatalf("expected 200 (per-tier cap), got %d", len(sh.dispatched))
	}
	perTier := map[string]int{}
	for _, rec := range sh.dispatched {
		perTier[rec.Tier]++
	}
	if perTier["WARM-A"] != 100 || perTier["WARM-B"] != 100 {
		t.Fatalf("unexpected per-tier dispatch: %+v", perTier)
	}

	// Six tiers with 120 each would allow 600, but total cap is 500.
	sh.dispatched = nil
	sh2 := newSchedulerHarness(t, 0)
	for _, tier := range []string{"T1", "T2", "T3", "T4", "T5", "T6"} {
		for i := 0; i < 120; i++ {
			rec := newDueRecord(tier, fmt.Sprintf("%s-%03d", tier, i), now.Add(-time.Minute))
			sh2.journal.upsert(sh2.ctx, rec)
		}
	}
	sh2.scheduler.tick(sh2.ctx)
	if len(sh2.dispatched) != transitionRetryTotalLimit {
		t.Fatalf("expected total cap %d, got %d", transitionRetryTotalLimit, len(sh2.dispatched))
	}
}

func TestSchedulerDispatchLease(t *testing.T) {
	sh := newSchedulerHarness(t, 0)
	rec := newDueRecord("WARM-L", "o1", sh.now.Add(-time.Minute))
	sh.journal.upsert(sh.ctx, rec)

	// Priming tick dispatches once and claims the record.
	sh.scheduler.tick(sh.ctx)
	if len(sh.dispatched) != 1 {
		t.Fatalf("expected one dispatch, got %d", len(sh.dispatched))
	}

	// Unchanged record within the lease is not dispatched again.
	sh.now = sh.now.Add(10 * time.Second)
	sh.dispatched = nil
	sh.scheduler.tick(sh.ctx)
	if len(sh.dispatched) != 0 {
		t.Fatalf("claimed record must not be re-dispatched, got %d", len(sh.dispatched))
	}

	// An observed failure advancement releases the claim; the recomputed
	// backoff is at most ~24s, so move the clock past it.
	sh.now = sh.now.Add(30 * time.Second)
	sh.dispatched = nil
	rec = advanceRecord(rec, sh.now.Add(-25*time.Second))
	sh.journal.upsert(sh.ctx, rec)
	sh.scheduler.tick(sh.ctx)
	if len(sh.dispatched) != 1 {
		t.Fatalf("advanced record should be dispatched again, got %d", len(sh.dispatched))
	}

	// Unchanged again: re-claimed.
	sh.now = sh.now.Add(10 * time.Second)
	sh.dispatched = nil
	sh.scheduler.tick(sh.ctx)
	if len(sh.dispatched) != 0 {
		t.Fatalf("re-claimed record must not be dispatched, got %d", len(sh.dispatched))
	}

	// After the lease expires, the unchanged record is retried.
	sh.now = sh.now.Add(transitionDispatchLease + time.Second)
	sh.dispatched = nil
	sh.scheduler.tick(sh.ctx)
	if len(sh.dispatched) != 1 {
		t.Fatalf("lease expiry should allow re-dispatch, got %d", len(sh.dispatched))
	}
}

func TestSchedulerPrimingSeedsNoFailureAndProbe(t *testing.T) {
	sh := newSchedulerHarness(t, 3)
	// 50 pre-existing pending records for one tier.
	for i := 0; i < 50; i++ {
		rec := newDueRecord("WARM-B", fmt.Sprintf("o%03d", i), sh.now.Add(-time.Minute))
		sh.journal.upsert(sh.ctx, rec)
	}
	// And an already auto-paused tier on the shared document.
	if err := sh.health.Pause(sh.ctx, "WARM-P", false, "auto"); err != nil {
		t.Fatal(err)
	}
	sh.probeErr["WARM-P"] = errors.New("must not be called")

	sh.scheduler.tick(sh.ctx)

	// No false auto-pause from backlog and no streak counting.
	if h, _ := sh.health.Get("WARM-B"); h.Paused || h.ConsecutiveFailures != 0 {
		t.Fatalf("priming must not count existing failures: %+v", h)
	}
	// Existing auto-paused tier is not probed immediately (interval seeded).
	if len(sh.probeCalls) != 0 {
		t.Fatalf("priming must seed probe interval, probes: %v", sh.probeCalls)
	}

	// Second tick with unchanged records: still no new failure evidence.
	sh.now = sh.now.Add(30 * time.Second)
	sh.scheduler.tick(sh.ctx)
	if h, _ := sh.health.Get("WARM-B"); h.Paused || h.ConsecutiveFailures != 0 {
		t.Fatalf("unchanged backlog must not count: %+v", h)
	}

	// An increment now crosses the threshold (delta for one record).
	rec := sh.journal.list("WARM-B", "", transitionFailurePending)[0]
	advanced := advanceRecord(rec, sh.now)
	sh.journal.upsert(sh.ctx, advanced)
	sh.now = sh.now.Add(30 * time.Second)
	sh.scheduler.tick(sh.ctx)
	h, _ := sh.health.Get("WARM-B")
	// Single delta = streak 1; under threshold 3, still healthy.
	if h.Paused || h.ConsecutiveFailures != 1 {
		t.Fatalf("expected streak 1 healthy, got %+v", h)
	}
}
