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
	"testing"
	"time"

	"github.com/minio/minio/internal/config/ilm"
)

func TestInitTransitionFailureHandlerGate(t *testing.T) {
	// Feature disabled (default): nothing starts regardless of object layer.
	globalILMConfig.update(ilm.Config{
		ExpirationWorkers:          100,
		TransitionWorkers:          100,
		TransitionRetryMaxAttempts: 10,
		TransitionRetryDelay:       time.Second,
		TransitionRetryMaxDelay:    time.Minute,
	})
	initTransitionFailureHandlerOnce()
	if transitionFailureStarted || globalTransitionFailureJournal != nil || globalTransitionTierHealth != nil {
		t.Fatalf("handler must not start while disabled")
	}

	// Enabled but object layer unavailable in unit tests: still no start and
	// no panic; it will be retried on the next dynamic config application.
	cfg, err := ilm.LookupConfig(nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg.TransitionFailureHandling = true
	globalILMConfig.update(cfg)
	initTransitionFailureHandlerOnce()
	if transitionFailureStarted {
		t.Fatalf("handler must not start without an object layer")
	}

	// Restore defaults.
	globalILMConfig.update(ilm.Config{ExpirationWorkers: 100, TransitionWorkers: 100})
}
