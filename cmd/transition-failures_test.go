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
	"testing"
	"time"
)

func TestClassifyTransitionError(t *testing.T) {
	cases := []struct {
		name           string
		err            error
		wantCat        transitionFailureCategory
		wantRecordable bool
		wantRetryable  bool
	}{
		{"nil", nil, "", false, false},
		{"backend-down", BackendDown{Err: "host down"}, transitionFailureBackendDown, true, true},
		{"closed-connection", errors.New("read tcp: use of closed network connection"), transitionFailureBackendDown, true, true},
		{"permission", tierPermErr{Op: tierPut, Err: errors.New("access denied")}, transitionFailurePermission, true, true},
		{"tier-not-found", errTierNotFound, transitionFailureConfiguration, true, false},
		{"tier-invalid-config", errTierInvalidConfig, transitionFailureConfiguration, true, false},
		{"object-not-found", errFileNotFound, "", false, false},
		{"version-not-found", errFileVersionNotFound, "", false, false},
		{"canceled", context.Canceled, "", false, false},
		{"internal", errors.New("some unexpected error"), transitionFailureInternal, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cat, recordable, retryable := classifyTransitionError(tc.err)
			if cat != tc.wantCat || recordable != tc.wantRecordable || retryable != tc.wantRetryable {
				t.Fatalf("got (%s,%v,%v), want (%s,%v,%v)", cat, recordable, retryable, tc.wantCat, tc.wantRecordable, tc.wantRetryable)
			}
		})
	}
}

func TestBackoffDelay(t *testing.T) {
	base := 10 * time.Second
	max := 30 * time.Minute
	cases := []struct {
		attempts int
		jitter   float64
		want     time.Duration
	}{
		{1, 0, 10 * time.Second},
		{2, 0, 20 * time.Second},
		{3, 0, 40 * time.Second},
		{4, 0, 80 * time.Second},
		{8, 0, 1280 * time.Second},
		{20, 0, max},
		{1, 1, 12 * time.Second},
		{2, -1, 16 * time.Second},
	}
	for _, tc := range cases {
		got := backoffDelay(tc.attempts, base, max, tc.jitter)
		if got != tc.want {
			t.Fatalf("attempts=%d jitter=%.1f: got %v want %v", tc.attempts, tc.jitter, got, tc.want)
		}
	}
	// Randomized wrapper stays within the jitter band and cap.
	for attempts := 1; attempts <= 25; attempts++ {
		got := transitionBackoff(attempts, base, max)
		if got <= 0 || got > max {
			t.Fatalf("attempts=%d: out of range delay %v", attempts, got)
		}
	}
}

func testFailureCfg() transitionFailureConfig {
	return transitionFailureConfig{
		Enabled:       true,
		MaxAttempts:   3,
		RetryDelay:    10 * time.Second,
		RetryMaxDelay: 30 * time.Minute,
	}
}

func TestRecordTransitionFailureMerge(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	oi := ObjectInfo{
		Bucket:    "bucket",
		Name:      "object",
		VersionID: "v1",
		Size:      42,
		ETag:      "etag",
		ModTime:   now.Add(-time.Hour),
	}
	cfg := testFailureCfg()

	first := recordTransitionFailure(nil, oi, "WARM-TIER", false,
		transitionFailureBackendDown, "down", cfg, now)
	if first.ID == "" || first.Attempts != 1 || first.Status != transitionFailurePending {
		t.Fatalf("unexpected first record: %+v", first)
	}
	if first.FirstFailedAt != now || first.LastFailedAt != now {
		t.Fatalf("timestamps not set on first record")
	}
	// NextRetryAt must be in the jittered band.
	minAt := now.Add(backoffDelay(1, cfg.RetryDelay, cfg.RetryMaxDelay, -1))
	maxAt := now.Add(backoffDelay(1, cfg.RetryDelay, cfg.RetryMaxDelay, 1))
	if first.NextRetryAt.Before(minAt) || first.NextRetryAt.After(maxAt) {
		t.Fatalf("next retry %v outside [%v,%v]", first.NextRetryAt, minAt, maxAt)
	}

	second := recordTransitionFailure(&first, oi, "WARM-TIER", false,
		transitionFailureInternal, "boom", cfg, now.Add(time.Minute))
	if second.ID != first.ID {
		t.Fatalf("record ID must remain stable across attempts")
	}
	if second.Attempts != 2 || second.FirstFailedAt != now || second.LastFailedAt != now.Add(time.Minute) {
		t.Fatalf("merge did not update attempts/timestamps correctly: %+v", second)
	}
	if second.LastErr != "boom" || second.Category != transitionFailureInternal {
		t.Fatalf("latest error/category must be reflected")
	}

	third := recordTransitionFailure(&second, oi, "WARM-TIER", false,
		transitionFailureBackendDown, "still down", cfg, now.Add(2*time.Minute))
	if third.Attempts != 3 || third.Status != transitionFailureExhausted {
		t.Fatalf("expected exhausted on third attempt, got %+v", third)
	}
	if !third.NextRetryAt.IsZero() {
		t.Fatalf("exhausted record must not carry a next retry time")
	}
}

func TestRecordTransitionFailureIdentity(t *testing.T) {
	id1 := transitionFailureRecordID("TIER", "b", "o", "v")
	id2 := transitionFailureRecordID("TIER", "b", "o", "v")
	id3 := transitionFailureRecordID("TIER", "b", "o", "different")
	if id1 != id2 || id1 == "" || id1 == id3 {
		t.Fatalf("record ID must be deterministic and identity-sensitive")
	}
	if got := transitionFailureRecordPath("TIER", "abc"); got != "transition-failures/TIER/abc.json" {
		t.Fatalf("unexpected record path %q", got)
	}
}
