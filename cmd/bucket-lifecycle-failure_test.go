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
	"testing"
	"time"

	"github.com/minio/minio/internal/bucket/lifecycle"
)

func newTestTransitionState(t *testing.T, enabled bool) *transitionState {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	st := &transitionState{
		transitionCh: make(chan transitionTask, 10),
		ctx:          ctx,
		killCh:       make(chan struct{}),
		lastDayStats: make(map[string]*lastDayTierStats),
		pendingTasks: make(map[uint64]transitionTask),
	}
	if enabled {
		st.failureCfg = testFailureCfg()
	}
	t.Cleanup(cancel)
	return st
}

func withGlobalJournal(t *testing.T, st *transitionState) (*transitionFailureJournal, *fakeTransitionStore) {
	t.Helper()
	store := newFakeTransitionStore()
	j := newTransitionFailureJournal(st.ctx)
	if err := j.Init(st.ctx, store); err != nil {
		t.Fatal(err)
	}
	old := globalTransitionFailureJournal
	globalTransitionFailureJournal = j
	t.Cleanup(func() { globalTransitionFailureJournal = old })
	return j, store
}

func transitionEventFor(tier string, noncurrent bool) lifecycle.Event {
	action := lifecycle.TransitionAction
	if noncurrent {
		action = lifecycle.TransitionVersionAction
	}
	return lifecycle.Event{Action: action, StorageClass: tier}
}

func waitFor(timeout time.Duration, fn func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fn()
}

func TestWorkerFailureArchivedWhenEnabled(t *testing.T) {
	st := newTestTransitionState(t, true)
	journal, _ := withGlobalJournal(t, st)

	called := make(chan struct{}, 1)
	st.execTransitionObject = func(ctx context.Context, objAPI ObjectLayer, oi ObjectInfo, lae lcAuditEvent) error {
		called <- struct{}{}
		return BackendDown{Err: "host down"}
	}
	go st.worker(nil)

	oi := testObjectInfo("bucket-a", "object-1", "v1")
	task := transitionTask{objInfo: oi, event: transitionEventFor("WARM-A", false), src: lcEventSrc_Scanner}
	st.enqueue(task, false)

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("transition executor was not called")
	}

	if !waitFor(5*time.Second, func() bool { return len(journal.list("WARM-A", "", "")) == 1 }) {
		t.Fatalf("failure record not archived: %+v", journal.list("", "", ""))
	}
	if st.FailedTasks() != 1 {
		t.Fatalf("expected failedTasks=1, got %d", st.FailedTasks())
	}
	recs := journal.list("WARM-A", "", transitionFailurePending)
	if len(recs) != 1 || recs[0].Category != transitionFailureBackendDown || recs[0].Attempts != 1 {
		t.Fatalf("unexpected record: %+v", recs)
	}

	// Pending tracking must have been cleaned up after completion.
	if !waitFor(time.Second, func() bool {
		st.pendingMu.Lock()
		defer st.pendingMu.Unlock()
		return len(st.pendingTasks) == 0
	}) {
		t.Fatalf("pending task was not removed after processing")
	}
}

func TestWorkerNoJournalWhenDisabled(t *testing.T) {
	st := newTestTransitionState(t, false)
	if globalTransitionFailureJournal != nil {
		old := globalTransitionFailureJournal
		globalTransitionFailureJournal = nil
		t.Cleanup(func() { globalTransitionFailureJournal = old })
	}

	called := make(chan struct{}, 1)
	st.execTransitionObject = func(ctx context.Context, objAPI ObjectLayer, oi ObjectInfo, lae lcAuditEvent) error {
		called <- struct{}{}
		return BackendDown{Err: "host down"}
	}
	go st.worker(nil)

	oi := testObjectInfo("bucket-a", "object-1", "v1")
	st.enqueue(transitionTask{objInfo: oi, event: transitionEventFor("WARM-A", false), src: lcEventSrc_Scanner}, false)
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("transition executor was not called")
	}
	if st.FailedTasks() != 0 || st.SkippedPausedTasks() != 0 {
		t.Fatalf("counters must stay zero when feature disabled: failed=%d skipped=%d",
			st.FailedTasks(), st.SkippedPausedTasks())
	}
	// Pending tracking is disabled too.
	if len(st.pendingTasks) != 0 {
		t.Fatalf("no pending tracking when feature is disabled")
	}
}

func TestWorkerRetrySuccessRemovesRecord(t *testing.T) {
	st := newTestTransitionState(t, true)
	journal, _ := withGlobalJournal(t, st)

	now := UTCNow()
	oi := testObjectInfo("bucket-a", "object-ok", "v1")
	rec := recordTransitionFailure(nil, oi, "WARM-A", false,
		transitionFailureBackendDown, "down", testFailureCfg(), now.Add(-2*time.Minute))
	rec.NextRetryAt = now.Add(-time.Minute) // due
	journal.upsert(st.ctx, rec)

	called := make(chan struct{}, 1)
	st.execTransitionObject = func(ctx context.Context, objAPI ObjectLayer, oi ObjectInfo, lae lcAuditEvent) error {
		called <- struct{}{}
		return nil
	}
	go st.worker(nil)
	st.enqueue(transitionTask{
		objInfo:  oi,
		event:    transitionEventFor("WARM-A", false),
		src:      lcEventSrc_Scanner,
		attempts: 1,
	}, false)

	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("transition executor was not called")
	}
	if !waitFor(5*time.Second, func() bool { return len(journal.list("", "", "")) == 0 }) {
		t.Fatalf("record should be removed after successful retry: %+v", journal.list("", "", ""))
	}
	if st.RetrySucceededTasks() != 1 {
		t.Fatalf("expected retrySucceeded=1, got %d", st.RetrySucceededTasks())
	}
}

func TestWorkerSkipsPausedTierBeforeExecution(t *testing.T) {
	ctx := context.Background()
	st := newTestTransitionState(t, true)
	store := newFakeTransitionStore()
	health := newTransitionTierHealthMgr(st.ctx)
	if err := health.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	if err := health.Pause(ctx, "WARM-P", false, "auto"); err != nil {
		t.Fatal(err)
	}
	old := globalTransitionTierHealth
	globalTransitionTierHealth = health
	t.Cleanup(func() { globalTransitionTierHealth = old })

	execCalled := make(chan struct{}, 1)
	st.execTransitionObject = func(ctx context.Context, objAPI ObjectLayer, oi ObjectInfo, lae lcAuditEvent) error {
		execCalled <- struct{}{}
		return nil
	}
	go st.worker(nil)

	oi := testObjectInfo("bucket-a", "object-1", "v1")
	st.enqueue(transitionTask{objInfo: oi, event: transitionEventFor("WARM-P", false), src: lcEventSrc_Scanner}, false)

	select {
	case <-execCalled:
		t.Fatal("worker must not execute transitions for a paused tier")
	case <-time.After(300 * time.Millisecond):
	}
	if st.SkippedPausedTasks() < 1 {
		t.Fatalf("expected skipped counter, got %d", st.SkippedPausedTasks())
	}
}

func TestQueueGateForPausedTier(t *testing.T) {
	ctx := context.Background()
	st := newTestTransitionState(t, true)
	store := newFakeTransitionStore()
	health := newTransitionTierHealthMgr(st.ctx)
	if err := health.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	if err := health.Pause(ctx, "WARM-P", false, "auto"); err != nil {
		t.Fatal(err)
	}
	old := globalTransitionTierHealth
	globalTransitionTierHealth = health
	t.Cleanup(func() { globalTransitionTierHealth = old })

	oi := testObjectInfo("b", "o", "v")
	st.queueTransitionTask(oi, transitionEventFor("WARM-P", false), lcEventSrc_Scanner)
	if len(st.transitionCh) != 0 {
		t.Fatalf("paused tier task must not enter the channel")
	}
	// Other tiers are unaffected.
	st.queueTransitionTask(oi, transitionEventFor("WARM-OK", false), lcEventSrc_Scanner)
	if len(st.transitionCh) != 1 {
		t.Fatalf("healthy tier task must be queued, got %d", len(st.transitionCh))
	}
	if st.SkippedPausedTasks() != 1 {
		t.Fatalf("expected one skipped task, got %d", st.SkippedPausedTasks())
	}
}
