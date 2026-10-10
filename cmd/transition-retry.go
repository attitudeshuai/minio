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
	"sort"
	"sync"
	"time"

	"github.com/minio/minio/internal/logger"
)

const (
	// transitionFailureTick is the leader reconciliation interval.
	transitionFailureTick = 5 * time.Second
	// transitionRetryPerTierLimit bounds records dispatched to one tier per
	// tick, so an unhealthy tier cannot starve healthy tiers.
	transitionRetryPerTierLimit = 100
	// transitionRetryTotalLimit bounds total dispatches per tick.
	transitionRetryTotalLimit = 500
	// transitionDispatchLease is how long a dispatched record is considered
	// in-flight before it may be dispatched again. It must comfortably exceed
	// the expected duration of a large object transition.
	transitionDispatchLease = time.Minute
)

var (
	transitionFailureInitMu  sync.Mutex
	transitionFailureStarted bool

	// transitionTierProbeFn checks tier availability; overridable in tests.
	transitionTierProbeFn = func(ctx context.Context, tier string) error {
		return globalTierConfigMgr.Verify(ctx, tier)
	}
)

// initTransitionFailureHandlerOnce starts the cluster-wide failure handling
// components when the feature is enabled and the object layer is ready. It is
// safe to call repeatedly: while the feature is off (default) it does
// nothing, so deployments without new configuration keep the legacy behavior.
func initTransitionFailureHandlerOnce() {
	cfg := globalILMConfig.getTransitionFailureConfig()
	if !cfg.Enabled {
		return
	}
	objAPI := newObjectLayerFn()
	if objAPI == nil || globalTransitionState == nil {
		return
	}
	transitionFailureInitMu.Lock()
	defer transitionFailureInitMu.Unlock()
	if transitionFailureStarted {
		return
	}

	ctx := GlobalContext

	journal := newTransitionFailureJournal(ctx)
	if err := journal.Init(ctx, objAPI); err != nil {
		transitionLogIf(ctx, err)
		return
	}
	health := newTransitionTierHealthMgr(ctx)
	if err := health.Init(ctx, objAPI); err != nil {
		transitionLogIf(ctx, err)
		return
	}
	globalTransitionFailureJournal = journal
	globalTransitionTierHealth = health

	// Replay tasks left queued/in-flight on this node by a previous process.
	replayTransitionPendingTasks(ctx, objAPI)

	go health.refreshLoop(ctx)
	go runTransitionPendingSnapshotLoop(ctx, objAPI)
	go runTransitionFailureLeader(ctx, objAPI, journal, health)

	transitionFailureStarted = true
	logger.Info("ILM transition failure handling enabled (max attempts: %d, backoff: %v..%v, pause threshold: %d, probe interval: %v)",
		cfg.MaxAttempts, cfg.RetryDelay, cfg.RetryMaxDelay, cfg.PauseThreshold, cfg.ProbeInterval)
}

type failureBaseline struct {
	tier         string
	attempts     int
	lastFailedAt time.Time
}

// dispatchClaim marks a dispatched record as in-flight until an outcome is
// observed (record deleted/advanced) or the lease expires.
type dispatchClaim struct {
	until        time.Time
	attempts     int
	lastFailedAt time.Time
	status       transitionFailureStatus
}

// transitionFailureScheduler is the per-tick reconciliation state machine.
// It is split from the goroutine/leader plumbing so it can be unit tested
// with virtual time and fakes.
type transitionFailureScheduler struct {
	journal *transitionFailureJournal
	health  *transitionTierHealthMgr
	enqueue func(rec transitionFailureRecord)
	probe   func(ctx context.Context, tier string) error
	cfgFn   func() transitionFailureConfig
	nowFn   func() time.Time

	// baseline is the diff watermark for failure evidence. It is seeded on
	// the first tick after leader acquisition WITHOUT counting, so a leader
	// takeover never mistakes pre-existing records for new failures.
	baseline map[string]failureBaseline
	probeAt  map[string]time.Time
	claimed  map[string]dispatchClaim
	primed   bool
}

func newTransitionFailureScheduler(journal *transitionFailureJournal, health *transitionTierHealthMgr) *transitionFailureScheduler {
	return &transitionFailureScheduler{
		journal:  journal,
		health:   health,
		enqueue:  func(rec transitionFailureRecord) { globalTransitionState.queueTransitionRetry(rec) },
		probe:    transitionTierProbeFn,
		cfgFn:    func() transitionFailureConfig { return globalILMConfig.getTransitionFailureConfig() },
		nowFn:    UTCNow,
		baseline: make(map[string]failureBaseline),
		probeAt:  make(map[string]time.Time),
		claimed:  make(map[string]dispatchClaim),
	}
}

// reconcileClaims releases dispatch claims whose record disappeared,
// advanced (an outcome was observed) or whose lease expired.
func (s *transitionFailureScheduler) reconcileClaims(byID map[string]transitionFailureRecord, now time.Time) {
	for id, claim := range s.claimed {
		rec, ok := byID[id]
		if !ok {
			delete(s.claimed, id)
			continue
		}
		if rec.Attempts != claim.attempts || !rec.LastFailedAt.Equal(claim.lastFailedAt) ||
			rec.Status != claim.status || now.After(claim.until) {
			delete(s.claimed, id)
		}
	}
}

// tick performs one reconciliation round: probe paused tiers, aggregate
// cluster-wide failure evidence, auto-pause, and dispatch due records.
func (s *transitionFailureScheduler) tick(ctx context.Context) {
	cfg := s.cfgFn()
	now := s.nowFn()

	if err := s.journal.refresh(ctx); err != nil {
		transitionLogIf(ctx, err)
	}
	if err := s.health.Reload(ctx); err != nil {
		transitionLogIf(ctx, err)
	}
	records := s.journal.list("", "", "")
	byID := make(map[string]transitionFailureRecord, len(records))
	for _, rec := range records {
		byID[rec.ID] = rec
	}

	// Release in-flight claims that completed, advanced or expired.
	s.reconcileClaims(byID, now)

	// On the very first tick after acquiring leadership, seed the probes of
	// tiers that were already auto-paused so the probe interval is honored
	// across leader changes as well.
	if !s.primed {
		for tier, h := range s.health.All() {
			if h.Paused && !h.Manual {
				if _, ok := s.probeAt[tier]; !ok {
					s.probeAt[tier] = now.Add(cfg.ProbeInterval)
				}
			}
		}
	}

	// 1. Probe auto-paused tiers and resume them on recovery.
	for tier, h := range s.health.All() {
		if !h.Paused || h.Manual {
			continue
		}
		if next := s.probeAt[tier]; !next.IsZero() && now.Before(next) {
			continue
		}
		if err := s.probe(ctx, tier); err != nil {
			transitionLogIf(ctx, err)
			s.probeAt[tier] = now.Add(cfg.ProbeInterval)
			continue
		}
		if err := s.health.Resume(ctx, tier); err != nil {
			transitionLogIf(ctx, err)
			s.probeAt[tier] = now.Add(cfg.ProbeInterval)
			continue
		}
		delete(s.probeAt, tier)
		logger.Info("Remote tier %s recovered; resuming transitions from failure list", tier)
	}

	// 2. Diff failure/success evidence against the previous tick. The first
	// tick only seeds the baseline, so pre-existing failures never count as
	// new failures when leadership is acquired.
	if s.primed {
		seen := make(map[string]struct{}, len(records))
		failures := make(map[string]int)
		failureErr := make(map[string]string)
		failureErrAt := make(map[string]time.Time)
		for _, rec := range records {
			seen[rec.ID] = struct{}{}
			prev, ok := s.baseline[rec.ID]
			switch {
			case !ok:
				if rec.Status == transitionFailurePending {
					failures[rec.Tier]++
				}
			case rec.Attempts > prev.attempts || rec.LastFailedAt.After(prev.lastFailedAt):
				delta := rec.Attempts - prev.attempts
				if delta < 1 {
					delta = 1
				}
				failures[rec.Tier] += delta
			}
			if rec.LastFailedAt.After(failureErrAt[rec.Tier]) {
				failureErrAt[rec.Tier] = rec.LastFailedAt
				failureErr[rec.Tier] = rec.LastErr
			}
		}
		successes := make(map[string]int)
		for id, b := range s.baseline {
			if _, ok := seen[id]; !ok {
				successes[b.tier]++
			}
		}
		for tier, n := range failures {
			if successes[tier] > 0 {
				// Mixed evidence: a successful attempt proves reachability,
				// reset instead of pausing.
				if err := s.health.ResetStreak(ctx, tier); err != nil {
					transitionLogIf(ctx, err)
				}
				continue
			}
			if s.health.IsPaused(tier) {
				continue
			}
			if err := s.health.RecordFailures(ctx, tier, n, failureErr[tier]); err != nil {
				transitionLogIf(ctx, err)
				continue
			}
			h, _ := s.health.Get(tier)
			if h.Paused && !h.Manual {
				// Honor the probe interval for the freshly paused tier.
				s.probeAt[tier] = now.Add(cfg.ProbeInterval)
				logger.Info("Remote tier %s auto-paused after %d consecutive transition failures: %s",
					tier, h.ConsecutiveFailures, failureErr[tier])
			}
		}
		for tier := range successes {
			if failures[tier] > 0 {
				continue
			}
			if err := s.health.ResetStreak(ctx, tier); err != nil {
				transitionLogIf(ctx, err)
			}
		}
	}

	// 3. Dispatch due records, per-tier capped so one tier cannot starve the
	// others; paused tiers and records with an active in-flight claim are
	// skipped.
	byTier := make(map[string][]transitionFailureRecord)
	for _, rec := range records {
		if rec.Status != transitionFailurePending || rec.NextRetryAt.IsZero() || rec.NextRetryAt.After(now) {
			continue
		}
		if s.health.IsPaused(rec.Tier) {
			continue
		}
		if _, claimed := s.claimed[rec.ID]; claimed {
			continue
		}
		byTier[rec.Tier] = append(byTier[rec.Tier], rec)
	}
	dispatched := 0
	for tier := range byTier {
		due := byTier[tier]
		sort.Slice(due, func(i, k int) bool {
			if due[i].NextRetryAt.Equal(due[k].NextRetryAt) {
				return due[i].ID < due[k].ID
			}
			return due[i].NextRetryAt.Before(due[k].NextRetryAt)
		})
		if len(due) > transitionRetryPerTierLimit {
			due = due[:transitionRetryPerTierLimit]
		}
		for _, rec := range due {
			if dispatched >= transitionRetryTotalLimit {
				break
			}
			s.enqueue(rec)
			s.claimed[rec.ID] = dispatchClaim{
				until:        now.Add(transitionDispatchLease),
				attempts:     rec.Attempts,
				lastFailedAt: rec.LastFailedAt,
				status:       rec.Status,
			}
			dispatched++
		}
		if dispatched >= transitionRetryTotalLimit {
			break
		}
	}

	// 4. Rebuild the baseline and mark the scheduler primed.
	baseline := make(map[string]failureBaseline, len(records))
	for _, rec := range records {
		baseline[rec.ID] = failureBaseline{
			tier:         rec.Tier,
			attempts:     rec.Attempts,
			lastFailedAt: rec.LastFailedAt,
		}
	}
	s.baseline = baseline
	s.primed = true
}

// runTransitionFailureLeader executes the reconciliation loop only on the
// node holding the cluster leader lock; another node takes over transparently
// if the leader disappears, because all state is in the shared store.
func runTransitionFailureLeader(ctx context.Context, objAPI ObjectLayer, journal *transitionFailureJournal, health *transitionTierHealthMgr) {
	if globalLeaderLock == nil {
		// Non erasure mode: leader election is unavailable; failure
		// journaling and pause gating still work node-locally.
		return
	}
	ctx, cancel := globalLeaderLock.GetLock(ctx)
	defer cancel()

	scheduler := newTransitionFailureScheduler(journal, health)
	ticker := time.NewTicker(transitionFailureTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scheduler.tick(ctx)
		}
	}
}
