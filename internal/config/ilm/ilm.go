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

import (
	"fmt"
	"strconv"
	"time"

	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/env"
)

// DefaultKVS default configuration values for ILM subsystem
var DefaultKVS = config.KVS{
	config.KV{
		Key:   transitionWorkers,
		Value: "100",
	},
	config.KV{
		Key:   expirationWorkers,
		Value: "100",
	},
	config.KV{
		Key:   transitionFailureHandling,
		Value: config.EnableOff,
	},
	config.KV{
		Key:   transitionRetryMaxAttempts,
		Value: "10",
	},
	config.KV{
		Key:   transitionRetryDelay,
		Value: "10s",
	},
	config.KV{
		Key:   transitionRetryMaxDelay,
		Value: "30m",
	},
	config.KV{
		Key:   transitionTierPauseThreshold,
		Value: "10",
	},
	config.KV{
		Key:   transitionTierProbeInterval,
		Value: "1m",
	},
}

// Config represents the different configuration values for ILM subsystem
type Config struct {
	TransitionWorkers int
	ExpirationWorkers int

	// TransitionFailureHandling is the master switch for the transition
	// failure handling (failure journal, exponential backoff retries,
	// per-tier pause/resume and restart persistence). It is off by
	// default; when off, behavior is identical to before the feature
	// existed.
	TransitionFailureHandling bool

	// TransitionRetryMaxAttempts is the maximum number of attempts for a
	// failed transition, including the first attempt.
	TransitionRetryMaxAttempts int

	// TransitionRetryDelay is the base delay used by exponential backoff.
	TransitionRetryDelay time.Duration

	// TransitionRetryMaxDelay is the cap on the backoff delay.
	TransitionRetryMaxDelay time.Duration

	// TransitionTierPauseThreshold is the number of consecutive failures
	// observed against a tier after which the tier is auto-paused. A value
	// of zero disables auto-pausing (manual pause remains available).
	TransitionTierPauseThreshold int

	// TransitionTierProbeInterval is the interval at which an auto-paused
	// tier is probed for availability.
	TransitionTierProbeInterval time.Duration
}

// LookupConfig - lookup ilm config and override with valid environment settings if any.
func LookupConfig(kvs config.KVS) (cfg Config, err error) {
	cfg = Config{
		TransitionWorkers:            100,
		ExpirationWorkers:            100,
		TransitionRetryMaxAttempts:   10,
		TransitionRetryDelay:         10 * time.Second,
		TransitionRetryMaxDelay:      30 * time.Minute,
		TransitionTierPauseThreshold: 10,
		TransitionTierProbeInterval:  time.Minute,
	}

	if err = config.CheckValidKeys(config.ILMSubSys, kvs, DefaultKVS); err != nil {
		return cfg, err
	}

	tw, err := strconv.Atoi(env.Get(EnvILMTransitionWorkers, kvs.GetWithDefault(transitionWorkers, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	ew, err := strconv.Atoi(env.Get(EnvILMExpirationWorkers, kvs.GetWithDefault(expirationWorkers, DefaultKVS)))
	if err != nil {
		return cfg, err
	}

	cfg.TransitionWorkers = tw
	cfg.ExpirationWorkers = ew

	enabled, err := config.ParseBool(env.Get(EnvILMTransitionFailureHandling, kvs.GetWithDefault(transitionFailureHandling, DefaultKVS)))
	if err != nil {
		return cfg, fmt.Errorf("'ilm:%s' value must be '%s' or '%s': %w", transitionFailureHandling, config.EnableOn, config.EnableOff, err)
	}
	cfg.TransitionFailureHandling = enabled

	maxAttempts, err := strconv.Atoi(env.Get(EnvILMTransitionRetryMaxAttempts, kvs.GetWithDefault(transitionRetryMaxAttempts, DefaultKVS)))
	if err != nil {
		return cfg, fmt.Errorf("'ilm:%s' value invalid: %w", transitionRetryMaxAttempts, err)
	}
	if maxAttempts < 1 {
		return cfg, fmt.Errorf("'ilm:%s' value must be a positive integer", transitionRetryMaxAttempts)
	}
	cfg.TransitionRetryMaxAttempts = maxAttempts

	retryDelay, err := time.ParseDuration(env.Get(EnvILMTransitionRetryDelay, kvs.GetWithDefault(transitionRetryDelay, DefaultKVS)))
	if err != nil {
		return cfg, fmt.Errorf("'ilm:%s' value invalid: %w", transitionRetryDelay, err)
	}
	if retryDelay <= 0 {
		return cfg, fmt.Errorf("'ilm:%s' value must be a positive duration", transitionRetryDelay)
	}
	cfg.TransitionRetryDelay = retryDelay

	retryMaxDelay, err := time.ParseDuration(env.Get(EnvILMTransitionRetryMaxDelay, kvs.GetWithDefault(transitionRetryMaxDelay, DefaultKVS)))
	if err != nil {
		return cfg, fmt.Errorf("'ilm:%s' value invalid: %w", transitionRetryMaxDelay, err)
	}
	if retryMaxDelay < retryDelay {
		return cfg, fmt.Errorf("'ilm:%s' value must be >= '%s'(%s)", transitionRetryMaxDelay, transitionRetryDelay, retryDelay)
	}
	cfg.TransitionRetryMaxDelay = retryMaxDelay

	pauseThreshold, err := strconv.Atoi(env.Get(EnvILMTransitionTierPauseThreshold, kvs.GetWithDefault(transitionTierPauseThreshold, DefaultKVS)))
	if err != nil {
		return cfg, fmt.Errorf("'ilm:%s' value invalid: %w", transitionTierPauseThreshold, err)
	}
	if pauseThreshold < 0 {
		return cfg, fmt.Errorf("'ilm:%s' value must be zero or a positive integer", transitionTierPauseThreshold)
	}
	cfg.TransitionTierPauseThreshold = pauseThreshold

	probeInterval, err := time.ParseDuration(env.Get(EnvILMTransitionTierProbeInterval, kvs.GetWithDefault(transitionTierProbeInterval, DefaultKVS)))
	if err != nil {
		return cfg, fmt.Errorf("'ilm:%s' value invalid: %w", transitionTierProbeInterval, err)
	}
	if probeInterval <= 0 {
		return cfg, fmt.Errorf("'ilm:%s' value must be a positive duration", transitionTierProbeInterval)
	}
	cfg.TransitionTierProbeInterval = probeInterval

	return cfg, nil
}
