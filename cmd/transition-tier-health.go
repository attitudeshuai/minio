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
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// transitionTierHealthPath is the cluster-shared object holding the pause
// state of all remote tiers.
const transitionTierHealthPath = "transition-tier-health.json"

// transitionTierHealthRefreshInterval is how often non-leader nodes reload
// the tier pause state from the cluster shared store.
const transitionTierHealthRefreshInterval = 3 * time.Second

// transitionTierHealth is the state of one remote tier.
type transitionTierHealth struct {
	Tier                string    `json:"tier"`
	Paused              bool      `json:"paused"`
	Manual              bool      `json:"manual"`
	ConsecutiveFailures int       `json:"consecutiveFailures"`
	PausedAt            time.Time `json:"pausedAt,omitempty"`
	LastFailureAt       time.Time `json:"lastFailureAt,omitempty"`
	LastErr             string    `json:"lastErr,omitempty"`
	ResumedAt           time.Time `json:"resumedAt,omitempty"`
	// Version is the per-tier, monotonically increasing change counter.
	Version uint64 `json:"version"`
}

type transitionTierHealthDoc struct {
	Tiers     map[string]transitionTierHealth `json:"tiers"`
	UpdatedAt time.Time                       `json:"updatedAt"`
}

func newTransitionTierHealthDoc() transitionTierHealthDoc {
	return transitionTierHealthDoc{Tiers: make(map[string]transitionTierHealth)}
}

// transitionTierHealthMgr keeps the authoritative tier pause state in the
// cluster shared store and mirrors it locally. Writes are serialized
// cluster-wide with a distributed lock; all nodes converge their mirrors by
// periodic reload, so every node observes the same conclusion.
type transitionTierHealthMgr struct {
	ctx   context.Context
	store transitionFailureStore

	cfgFn func() transitionFailureConfig

	mu  sync.RWMutex
	doc transitionTierHealthDoc
}

func newTransitionTierHealthMgr(ctx context.Context) *transitionTierHealthMgr {
	return &transitionTierHealthMgr{
		ctx: ctx,
		doc: newTransitionTierHealthDoc(),
		cfgFn: func() transitionFailureConfig {
			return globalILMConfig.getTransitionFailureConfig()
		},
	}
}

func (m *transitionTierHealthMgr) Init(ctx context.Context, store transitionFailureStore) error {
	m.store = store
	return m.Reload(ctx)
}

// Reload replaces the local mirror with the shared document.
func (m *transitionTierHealthMgr) Reload(ctx context.Context) error {
	if m == nil || m.store == nil {
		return nil
	}
	data, err := readConfig(ctx, m.store, transitionTierHealthPath)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			m.mu.Lock()
			m.doc = newTransitionTierHealthDoc()
			m.mu.Unlock()
			return nil
		}
		return err
	}
	var doc transitionTierHealthDoc
	if err = json.Unmarshal(data, &doc); err != nil {
		return err
	}
	if doc.Tiers == nil {
		doc.Tiers = make(map[string]transitionTierHealth)
	}
	m.mu.Lock()
	m.doc = doc
	m.mu.Unlock()
	return nil
}

func (m *transitionTierHealthMgr) refreshLoop(ctx context.Context) {
	ticker := time.NewTicker(transitionTierHealthRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.Reload(ctx); err != nil {
				transitionLogIf(ctx, err)
			}
		}
	}
}

// IsPaused reports whether new transitions must be dispatched to the tier.
func (m *transitionTierHealthMgr) IsPaused(tier string) bool {
	if m == nil {
		return false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.doc.Tiers[tier].Paused
}

// Get returns a copy of the tier state.
func (m *transitionTierHealthMgr) Get(tier string) (transitionTierHealth, bool) {
	if m == nil {
		return transitionTierHealth{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.doc.Tiers[tier]
	return h, ok
}

// All returns copies of all tier states.
func (m *transitionTierHealthMgr) All() map[string]transitionTierHealth {
	out := make(map[string]transitionTierHealth)
	if m == nil {
		return out
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.doc.Tiers {
		out[k] = v
	}
	return out
}

// mutate serializes a read-modify-write of the health document cluster-wide.
func (m *transitionTierHealthMgr) mutate(ctx context.Context, tier string, fn func(h *transitionTierHealth)) error {
	if m == nil || m.store == nil {
		return errServerNotInitialized
	}
	lk := m.store.NewNSLock(minioMetaBucket, transitionTierHealthPath)
	lkctx, err := lk.GetLock(ctx, sharedLockTimeout)
	if err != nil {
		return err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx)

	// Always read the freshest document before mutating.
	data, err := readConfig(ctx, m.store, transitionTierHealthPath)
	doc := newTransitionTierHealthDoc()
	switch {
	case err == nil:
		if uerr := json.Unmarshal(data, &doc); uerr != nil {
			return uerr
		}
		if doc.Tiers == nil {
			doc.Tiers = make(map[string]transitionTierHealth)
		}
	case errors.Is(err, errConfigNotFound):
		// first write
	default:
		return err
	}

	h := doc.Tiers[tier]
	h.Tier = tier
	fn(&h)
	h.Version++
	doc.Tiers[tier] = h
	doc.UpdatedAt = UTCNow()

	payload, err := json.Marshal(&doc)
	if err != nil {
		return err
	}
	if err = saveConfig(ctx, m.store, transitionTierHealthPath, payload); err != nil {
		return err
	}
	m.mu.Lock()
	m.doc = doc
	m.mu.Unlock()
	return nil
}

// Pause puts the tier into paused state. Manual pauses are only cleared by an
// explicit Resume.
func (m *transitionTierHealthMgr) Pause(ctx context.Context, tier string, manual bool, reason string) error {
	return m.mutate(ctx, tier, func(h *transitionTierHealth) {
		if h.Paused && h.Manual == manual {
			// Keep the original pause time; only refresh reason.
			h.LastErr = reason
			return
		}
		h.Paused = true
		h.Manual = manual
		h.PausedAt = UTCNow()
		h.LastErr = reason
		if manual {
			// Manual pause is an operator decision; start streak from zero.
			h.ConsecutiveFailures = 0
		}
	})
}

// Resume clears the paused state. It is used both by administrators (manual
// resume) and by the leader after a successful recovery probe.
func (m *transitionTierHealthMgr) Resume(ctx context.Context, tier string) error {
	return m.mutate(ctx, tier, func(h *transitionTierHealth) {
		h.Paused = false
		h.Manual = false
		h.ConsecutiveFailures = 0
		h.ResumedAt = UTCNow()
		h.LastErr = ""
	})
}

// RecordFailures advances the consecutive failure streak and, when the
// configured threshold is reached, auto-pauses the tier. A manually paused
// tier is never auto-resumed nor re-counted here.
func (m *transitionTierHealthMgr) RecordFailures(ctx context.Context, tier string, n int, lastErr string) error {
	if n <= 0 {
		return nil
	}
	cfg := m.cfgFn()
	threshold := cfg.PauseThreshold
	return m.mutate(ctx, tier, func(h *transitionTierHealth) {
		// While paused (manual or auto) no new tasks are dispatched to the
		// tier, so failure accounting is frozen until recovery is probed.
		if h.Manual || h.Paused {
			return
		}
		h.ConsecutiveFailures += n
		h.LastFailureAt = UTCNow()
		if lastErr != "" {
			h.LastErr = lastErr
		}
		if threshold > 0 && !h.Paused && h.ConsecutiveFailures >= threshold {
			h.Paused = true
			h.PausedAt = UTCNow()
		}
	})
}

// ResetStreak clears the consecutive failure streak for a healthy (not
// manually paused) tier.
func (m *transitionTierHealthMgr) ResetStreak(ctx context.Context, tier string) error {
	return m.mutate(ctx, tier, func(h *transitionTierHealth) {
		if h.Manual {
			return
		}
		h.ConsecutiveFailures = 0
	})
}
