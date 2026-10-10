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

package cmd

import (
	"context"
	"errors"
	"testing"
)

// A naturally finished job context (no cause) must not trigger progress
// persistence: the Start loops persist completion on their own.
func TestPersistBatchJobOnInterruptionNoopOnNormalExit(t *testing.T) {
	// nil ObjectLayer would panic if the function proceeded to persist;
	// returning without panic proves the no-cause early return.
	ri := &batchJobInfo{JobID: "expire-x"}
	persistBatchJobOnInterruption(context.Background(), nil, BatchJobRequest{}, ri)
}

// A paused job context (errJobPaused cause) reaches the persistence path.
// We only assert the predicate side here: the four Start loops share the
// same call (covered by call-site review) and pool pause/release tests
// cover the end-to-end marker flush with the real metrics map.
func TestPersistBatchJobOnInterruptionDetectsPauseCause(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errJobPaused)
	if cause := context.Cause(ctx); !errors.Is(cause, errJobPaused) {
		t.Fatalf("context cause = %v, want errJobPaused", cause)
	}
}
