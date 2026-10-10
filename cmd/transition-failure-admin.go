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
	"time"
)

// adminStateReloadTimeout bounds how long an admin read waits to converge
// with the cluster shared store before answering.
const adminStateReloadTimeout = 10 * time.Second

// listTierFailures converges the failure journal with the shared store and
// returns the matching failure records.
func listTierFailures(ctx context.Context, journal *transitionFailureJournal, tier, bucket string, status transitionFailureStatus) ([]tierFailureInfo, error) {
	reloadCtx, cancel := context.WithTimeout(ctx, adminStateReloadTimeout)
	defer cancel()
	if err := journal.refresh(reloadCtx); err != nil {
		return nil, err
	}
	records := journal.list(tier, bucket, status)
	infos := make([]tierFailureInfo, 0, len(records))
	for _, rec := range records {
		infos = append(infos, newTierFailureInfo(rec))
	}
	return infos, nil
}

// clearTierFailures deletes matching failure records. When all is false only
// exhausted records are eligible.
func clearTierFailures(ctx context.Context, journal *transitionFailureJournal, tier string, all bool) (int, error) {
	return journal.clear(ctx, tier, all)
}

// listTierStatus converges both shared documents and returns the status of
// every relevant tier.
func listTierStatus(ctx context.Context, health *transitionTierHealthMgr, journal *transitionFailureJournal, tierFilter string) ([]tierStatusInfo, error) {
	reloadCtx, cancel := context.WithTimeout(ctx, adminStateReloadTimeout)
	defer cancel()
	if err := health.Reload(reloadCtx); err != nil {
		return nil, err
	}
	var counts map[string]transitionFailureCounts
	if journal != nil {
		if err := journal.refresh(reloadCtx); err != nil {
			return nil, err
		}
		counts = journal.countsByTier()
	}

	healths := health.All()

	// Include every configured tier plus any tier with state or records.
	tiers := make(map[string]struct{})
	for _, t := range globalTierConfigMgr.ListTiers() {
		tiers[t.Name] = struct{}{}
	}
	for name := range healths {
		tiers[name] = struct{}{}
	}
	for name := range counts {
		tiers[name] = struct{}{}
	}

	names := make([]string, 0, len(tiers))
	for name := range tiers {
		if tierFilter != "" && name != tierFilter {
			continue
		}
		names = append(names, name)
	}
	sort.Strings(names)

	infos := make([]tierStatusInfo, 0, len(names))
	for _, name := range names {
		info := tierStatusInfo{Tier: name}
		if h, ok := healths[name]; ok {
			info.Paused = h.Paused
			info.Manual = h.Manual
			info.ConsecutiveFailures = h.ConsecutiveFailures
			info.PausedAt = h.PausedAt
			info.LastFailureAt = h.LastFailureAt
			info.LastErr = h.LastErr
			info.ResumedAt = h.ResumedAt
			info.Version = h.Version
		}
		if counts != nil {
			info.PendingFailures = counts[name].Pending
			info.ExhaustedFailures = counts[name].Exhausted
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// validateAdminTierName reloads tier configuration and verifies the tier exists.
func validateAdminTierName(ctx context.Context, objAPI ObjectLayer, tier string) error {
	if err := globalTierConfigMgr.Reload(ctx, objAPI); err != nil {
		return err
	}
	if !globalTierConfigMgr.IsTierValid(tier) {
		return errTierNotFound
	}
	return nil
}

// pauseTransitionTier validates the tier and applies a manual pause.
func pauseTransitionTier(ctx context.Context, objAPI ObjectLayer, health *transitionTierHealthMgr, tier, reason string) error {
	if err := validateAdminTierName(ctx, objAPI, tier); err != nil {
		return err
	}
	return health.Pause(ctx, tier, true, reason)
}

// resumeTransitionTier validates the tier and clears any pause state.
func resumeTransitionTier(ctx context.Context, objAPI ObjectLayer, health *transitionTierHealthMgr, tier string) error {
	if err := validateAdminTierName(ctx, objAPI, tier); err != nil {
		return err
	}
	return health.Resume(ctx, tier)
}
