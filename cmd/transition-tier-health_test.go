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
	"sync"
	"testing"

	"github.com/minio/minio/internal/config"
	"github.com/minio/minio/internal/config/ilm"
)

func withTestFailureThreshold(t *testing.T, threshold int) {
	t.Helper()
	kvs := config.KVS{
		{Key: "transition_failure_handling", Value: "on"},
		{Key: "transition_tier_pause_threshold", Value: "0"},
	}
	// Build through LookupConfig for unrelated defaults, then override threshold.
	cfg, err := ilm.LookupConfig(kvs)
	if err != nil {
		t.Fatal(err)
	}
	cfg.TransitionTierPauseThreshold = threshold
	globalILMConfig.update(cfg)
	t.Cleanup(func() {
		globalILMConfig.update(ilm.Config{ExpirationWorkers: 100, TransitionWorkers: 100})
	})
}

func TestTierHealthAutoPauseAndReset(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	m := newTransitionTierHealthMgr(ctx)
	if err := m.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	withTestFailureThreshold(t, 3)

	if err := m.RecordFailures(ctx, "WARM-H", 2, "err1"); err != nil {
		t.Fatal(err)
	}
	if m.IsPaused("WARM-H") {
		t.Fatalf("tier must not pause before threshold")
	}
	if err := m.RecordFailures(ctx, "WARM-H", 1, "err2"); err != nil {
		t.Fatal(err)
	}
	h, _ := m.Get("WARM-H")
	if !h.Paused || h.Manual {
		t.Fatalf("expected auto-paused tier, got %+v", h)
	}
	if h.ConsecutiveFailures != 3 || h.LastErr != "err2" || h.PausedAt.IsZero() {
		t.Fatalf("unexpected auto-pause state: %+v", h)
	}

	// Further failure accounting does not touch a paused tier's streak twice.
	if err := m.RecordFailures(ctx, "WARM-H", 5, "err3"); err != nil {
		t.Fatal(err)
	}
	h, _ = m.Get("WARM-H")
	if h.ConsecutiveFailures != 3 {
		t.Fatalf("record failures must be ignored while auto-paused, got %d", h.ConsecutiveFailures)
	}

	// Explicit resume clears everything.
	if err := m.Resume(ctx, "WARM-H"); err != nil {
		t.Fatal(err)
	}
	h, _ = m.Get("WARM-H")
	if h.Paused || h.Manual || h.ConsecutiveFailures != 0 || h.ResumedAt.IsZero() {
		t.Fatalf("unexpected resumed state: %+v", h)
	}
	if h.Version != 4 {
		t.Fatalf("expected monotonic version 4 (2 records + pause-batch + resume), got %d", h.Version)
	}
}

func TestTierHealthManualPauseNotAutoCleared(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	m := newTransitionTierHealthMgr(ctx)
	if err := m.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	withTestFailureThreshold(t, 1)

	if err := m.Pause(ctx, "WARM-M", true, "operator"); err != nil {
		t.Fatal(err)
	}
	// Failure accounting and streak resets must not clear a manual pause.
	if err := m.RecordFailures(ctx, "WARM-M", 10, "err"); err != nil {
		t.Fatal(err)
	}
	if err := m.ResetStreak(ctx, "WARM-M"); err != nil {
		t.Fatal(err)
	}
	h, _ := m.Get("WARM-M")
	if !h.Paused || !h.Manual {
		t.Fatalf("manual pause must survive, got %+v", h)
	}
	if err := m.Resume(ctx, "WARM-M"); err != nil {
		t.Fatal(err)
	}
	h, _ = m.Get("WARM-M")
	if h.Paused || h.Manual {
		t.Fatalf("explicit resume must clear manual pause, got %+v", h)
	}
}

func TestTierHealthConcurrentMutations(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	m := newTransitionTierHealthMgr(ctx)
	if err := m.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	withTestFailureThreshold(t, 0)

	const n = 32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			var err error
			switch i % 4 {
			case 0:
				err = m.Pause(ctx, "WARM-Z", true, "manual")
			case 1:
				err = m.Resume(ctx, "WARM-Z")
			case 2:
				err = m.RecordFailures(ctx, "WARM-Z", 1, "err")
			case 3:
				err = m.ResetStreak(ctx, "WARM-Z")
			}
			if err != nil {
				t.Errorf("mutation %d failed: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	// Every successful mutation increments the per-tier version exactly
	// once, so the final version must equal the number of mutations.
	h, _ := m.Get("WARM-Z")
	if h.Version != n {
		t.Fatalf("expected version=%d after %d serialized mutations, got %d", n, n, h.Version)
	}

	// Reload a fresh manager from the same shared store - same conclusion.
	m2 := newTransitionTierHealthMgr(ctx)
	if err := m2.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	h2, _ := m2.Get("WARM-Z")
	if h2.Version != h.Version || h2.Paused != h.Paused || h2.Manual != h.Manual {
		t.Fatalf("reloaded manager diverged: %+v vs %+v", h, h2)
	}
}

func TestTierHealthReloadMissing(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	m := newTransitionTierHealthMgr(ctx)
	if err := m.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	if m.IsPaused("NOPE") {
		t.Fatalf("empty document must report not paused")
	}
	if got := m.All(); len(got) != 0 {
		t.Fatalf("expected no tiers, got %v", got)
	}
	// Reload while the backing object vanished must not error.
	if err := m.Reload(ctx); err != nil {
		t.Fatalf("reload on missing document: %v", err)
	}
}
