// Copyright (c) 2015-2024 MinIO, Inc.
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

package ilm

import "github.com/minio/minio/internal/config"

const (
	transitionWorkers            = "transition_workers"
	expirationWorkers            = "expiration_workers"
	transitionFailureHandling    = "transition_failure_handling"
	transitionRetryMaxAttempts   = "transition_retry_max_attempts"
	transitionRetryDelay         = "transition_retry_delay"
	transitionRetryMaxDelay      = "transition_retry_max_delay"
	transitionTierPauseThreshold = "transition_tier_pause_threshold"
	transitionTierProbeInterval  = "transition_tier_probe_interval"

	// EnvILMTransitionWorkers env variable to configure number of transition workers
	EnvILMTransitionWorkers = "MINIO_ILM_TRANSITION_WORKERS"
	// EnvILMExpirationWorkers env variable to configure number of expiration workers
	EnvILMExpirationWorkers = "MINIO_ILM_EXPIRATION_WORKERS"
	// EnvILMTransitionFailureHandling env variable to enable/disable transition failure handling
	EnvILMTransitionFailureHandling = "MINIO_ILM_TRANSITION_FAILURE_HANDLING"
	// EnvILMTransitionRetryMaxAttempts env variable to configure max transition retry attempts
	EnvILMTransitionRetryMaxAttempts = "MINIO_ILM_TRANSITION_RETRY_MAX_ATTEMPTS"
	// EnvILMTransitionRetryDelay env variable to configure the base transition retry delay
	EnvILMTransitionRetryDelay = "MINIO_ILM_TRANSITION_RETRY_DELAY"
	// EnvILMTransitionRetryMaxDelay env variable to configure the max transition retry delay
	EnvILMTransitionRetryMaxDelay = "MINIO_ILM_TRANSITION_RETRY_MAX_DELAY"
	// EnvILMTransitionTierPauseThreshold env variable to configure consecutive failures before auto-pausing a tier
	EnvILMTransitionTierPauseThreshold = "MINIO_ILM_TRANSITION_TIER_PAUSE_THRESHOLD"
	// EnvILMTransitionTierProbeInterval env variable to configure the probe interval for auto-paused tiers
	EnvILMTransitionTierProbeInterval = "MINIO_ILM_TRANSITION_TIER_PROBE_INTERVAL"
)

var (
	defaultHelpPostfix = func(key string) string {
		return config.DefaultHelpPostfix(DefaultKVS, key)
	}

	// Help holds configuration keys and their default values for the ILM
	// subsystem
	Help = config.HelpKVS{
		config.HelpKV{
			Key:         transitionWorkers,
			Type:        "number",
			Description: `set the number of transition workers` + defaultHelpPostfix(transitionWorkers),
			Optional:    true,
		},
		config.HelpKV{
			Key:         expirationWorkers,
			Type:        "number",
			Description: `set the number of expiration workers` + defaultHelpPostfix(expirationWorkers),
			Optional:    true,
		},
		config.HelpKV{
			Key:         transitionFailureHandling,
			Type:        "on|off",
			Description: `enable failed transition task journaling, exponential backoff retries and per-tier pause/resume` + defaultHelpPostfix(transitionFailureHandling),
			Optional:    true,
		},
		config.HelpKV{
			Key:         transitionRetryMaxAttempts,
			Type:        "number",
			Description: `maximum number of attempts for a failed transition (including the first attempt)` + defaultHelpPostfix(transitionRetryMaxAttempts),
			Optional:    true,
		},
		config.HelpKV{
			Key:         transitionRetryDelay,
			Type:        "duration",
			Description: `base delay for exponential backoff retries of failed transitions` + defaultHelpPostfix(transitionRetryDelay),
			Optional:    true,
		},
		config.HelpKV{
			Key:         transitionRetryMaxDelay,
			Type:        "duration",
			Description: `maximum delay for exponential backoff retries of failed transitions` + defaultHelpPostfix(transitionRetryMaxDelay),
			Optional:    true,
		},
		config.HelpKV{
			Key:         transitionTierPauseThreshold,
			Type:        "number",
			Description: `number of consecutive failures to a remote tier after which it is auto-paused; set to 0 to disable auto-pausing` + defaultHelpPostfix(transitionTierPauseThreshold),
			Optional:    true,
		},
		config.HelpKV{
			Key:         transitionTierProbeInterval,
			Type:        "duration",
			Description: `interval at which an auto-paused tier is probed for recovery` + defaultHelpPostfix(transitionTierProbeInterval),
			Optional:    true,
		},
	}
)
