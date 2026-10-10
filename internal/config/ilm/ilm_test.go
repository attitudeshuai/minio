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

package ilm

import (
	"testing"
	"time"

	"github.com/minio/minio/internal/config"
)

func TestLookupConfigDefaults(t *testing.T) {
	cfg, err := LookupConfig(nil)
	if err != nil {
		t.Fatalf("expected no error with empty KVS, got %v", err)
	}
	if cfg.TransitionFailureHandling {
		t.Fatalf("transition failure handling must be off by default")
	}
	if cfg.TransitionWorkers != 100 || cfg.ExpirationWorkers != 100 {
		t.Fatalf("unexpected worker defaults: %+v", cfg)
	}
	if cfg.TransitionRetryMaxAttempts != 10 {
		t.Fatalf("unexpected default max attempts: %d", cfg.TransitionRetryMaxAttempts)
	}
	if cfg.TransitionRetryDelay != 10*time.Second {
		t.Fatalf("unexpected default retry delay: %v", cfg.TransitionRetryDelay)
	}
	if cfg.TransitionRetryMaxDelay != 30*time.Minute {
		t.Fatalf("unexpected default retry max delay: %v", cfg.TransitionRetryMaxDelay)
	}
	if cfg.TransitionTierPauseThreshold != 10 {
		t.Fatalf("unexpected default pause threshold: %d", cfg.TransitionTierPauseThreshold)
	}
	if cfg.TransitionTierProbeInterval != time.Minute {
		t.Fatalf("unexpected default probe interval: %v", cfg.TransitionTierProbeInterval)
	}
}

func TestLookupConfigEnabled(t *testing.T) {
	kvs := config.KVS{
		config.KV{Key: transitionFailureHandling, Value: config.EnableOn},
		config.KV{Key: transitionRetryMaxAttempts, Value: "5"},
		config.KV{Key: transitionRetryDelay, Value: "2s"},
		config.KV{Key: transitionRetryMaxDelay, Value: "1m"},
		config.KV{Key: transitionTierPauseThreshold, Value: "0"},
		config.KV{Key: transitionTierProbeInterval, Value: "30s"},
	}
	cfg, err := LookupConfig(kvs)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !cfg.TransitionFailureHandling {
		t.Fatalf("expected feature enabled")
	}
	if cfg.TransitionRetryMaxAttempts != 5 || cfg.TransitionRetryDelay != 2*time.Second ||
		cfg.TransitionRetryMaxDelay != time.Minute || cfg.TransitionTierPauseThreshold != 0 ||
		cfg.TransitionTierProbeInterval != 30*time.Second {
		t.Fatalf("unexpected cfg: %+v", cfg)
	}
}

func TestLookupConfigInvalid(t *testing.T) {
	cases := []config.KVS{
		{{Key: transitionFailureHandling, Value: "yes"}},
		{{Key: transitionRetryMaxAttempts, Value: "0"}},
		{{Key: transitionRetryMaxAttempts, Value: "abc"}},
		{{Key: transitionRetryDelay, Value: "0s"}},
		{{Key: transitionRetryDelay, Value: "notduration"}},
		{{Key: transitionRetryDelay, Value: "10m"}, {Key: transitionRetryMaxDelay, Value: "1s"}},
		{{Key: transitionTierPauseThreshold, Value: "-1"}},
		{{Key: transitionTierProbeInterval, Value: "0s"}},
		{{Key: "unknown_key", Value: "x"}},
	}
	for i, kvs := range cases {
		if _, err := LookupConfig(kvs); err == nil {
			t.Fatalf("case %d: expected error for %v", i, kvs)
		}
	}
}
