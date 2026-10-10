// Copyright (c) 2024 MinIO, Inc.
//
// # This file is part of MinIO Object Storage stack
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

package cmd

import (
	"context"
)

const (
	expiryPendingTasks             = "expiry_pending_tasks"
	transitionActiveTasks          = "transition_active_tasks"
	transitionPendingTasks         = "transition_pending_tasks"
	transitionMissedImmediateTasks = "transition_missed_immediate_tasks"
	transitionFailedTasks          = "transition_failed_tasks"
	transitionRetriedTasks         = "transition_retried_tasks"
	transitionRetrySucceededTasks  = "transition_retry_succeeded_tasks"
	transitionSkippedPausedTasks   = "transition_skipped_paused_tasks"
	transitionPausedTiers          = "transition_paused_tiers"
	versionsScanned                = "versions_scanned"
)

var (
	ilmExpiryPendingTasksMD             = NewGaugeMD(expiryPendingTasks, "Number of pending ILM expiry tasks in the queue")
	ilmTransitionActiveTasksMD          = NewGaugeMD(transitionActiveTasks, "Number of active ILM transition tasks")
	ilmTransitionPendingTasksMD         = NewGaugeMD(transitionPendingTasks, "Number of pending ILM transition tasks in the queue")
	ilmTransitionMissedImmediateTasksMD = NewCounterMD(transitionMissedImmediateTasks, "Number of missed immediate ILM transition tasks")
	ilmTransitionFailedTasksMD          = NewCounterMD(transitionFailedTasks, "Total number of failed ILM transition attempts archived since server start")
	ilmTransitionRetriedTasksMD         = NewCounterMD(transitionRetriedTasks, "Total number of retried ILM transition attempts since server start")
	ilmTransitionRetrySucceededTasksMD  = NewCounterMD(transitionRetrySucceededTasks, "Total number of failed ILM transitions that eventually succeeded on retry since server start")
	ilmTransitionSkippedPausedTasksMD   = NewCounterMD(transitionSkippedPausedTasks, "Total number of ILM transition tasks skipped because their target tier was paused since server start")
	ilmTransitionPausedTiersMD          = NewGaugeMD(transitionPausedTiers, "Number of remote tiers currently paused for ILM transitions")
	ilmVersionsScannedMD                = NewCounterMD(versionsScanned, "Total number of object versions checked for ILM actions since server start")
)

// loadILMMetrics - `MetricsLoaderFn` for ILM metrics.
func loadILMMetrics(_ context.Context, m MetricValues, _ *metricsCache) error {
	if globalExpiryState != nil {
		m.Set(expiryPendingTasks, float64(globalExpiryState.PendingTasks()))
	}
	if globalTransitionState != nil {
		m.Set(transitionActiveTasks, float64(globalTransitionState.ActiveTasks()))
		m.Set(transitionPendingTasks, float64(globalTransitionState.PendingTasks()))
		m.Set(transitionMissedImmediateTasks, float64(globalTransitionState.MissedImmediateTasks()))
		m.Set(transitionFailedTasks, float64(globalTransitionState.FailedTasks()))
		m.Set(transitionRetriedTasks, float64(globalTransitionState.RetriedTasks()))
		m.Set(transitionRetrySucceededTasks, float64(globalTransitionState.RetrySucceededTasks()))
		m.Set(transitionSkippedPausedTasks, float64(globalTransitionState.SkippedPausedTasks()))
	}
	pausedTiers := 0
	if globalTransitionTierHealth != nil {
		for _, h := range globalTransitionTierHealth.All() {
			if h.Paused {
				pausedTiers++
			}
		}
	}
	m.Set(transitionPausedTiers, float64(pausedTiers))
	m.Set(versionsScanned, float64(globalScannerMetrics.lifetime(scannerMetricILM)))

	return nil
}
