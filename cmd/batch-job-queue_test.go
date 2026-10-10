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
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
)

type fakeSchedClock struct{ t time.Time }

func (f *fakeSchedClock) Now() time.Time { return f.t }
func (f *fakeSchedClock) add(d time.Duration) {
	f.t = f.t.Add(d)
}

func testEntry(id string, typ madmin.BatchJobType, p batchPriority) *batchJobEntry {
	return &batchJobEntry{
		id:       id,
		user:     "alice",
		typ:      typ,
		priority: p,
		job:      &BatchJobRequest{ID: id, Priority: string(p)},
	}
}

// testWorker simulates one persistent pool worker with one parked ticket.
type testWorker struct {
	s      *batchScheduler
	ticket chan *batchJobEntry
}

func newTestWorker(s *batchScheduler) *testWorker {
	w := &testWorker{s: s, ticket: make(chan *batchJobEntry, 1)}
	s.addIdle(w.ticket)
	return w
}

func (w *testWorker) recv() *batchJobEntry {
	select {
	case e := <-w.ticket:
		return e
	default:
		return nil
	}
}

// ready models a worker looping back after its job ended.
func (w *testWorker) ready() { w.s.addIdle(w.ticket) }

// dispatchAndRecv triggers one scheduler pass and returns the entry this
// worker received (nil if nothing was admissible).
func (w *testWorker) dispatchAndRecv() *batchJobEntry {
	w.s.dispatch()
	return w.recv()
}

func newTestScheduler(t *testing.T, maxJobs int, caps map[madmin.BatchJobType]int, gated func() bool) (*batchScheduler, *fakeSchedClock) {
	t.Helper()
	clk := &fakeSchedClock{t: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)}
	s := newBatchScheduler(maxJobs, caps, gated)
	s.clock = clk
	return s, clk
}

func TestBatchSchedulerPriorityAndFIFO(t *testing.T) {
	s, _ := newTestScheduler(t, 1, nil, nil)
	w := newTestWorker(s)

	s.enqueue(testEntry("X", madmin.BatchJobReplicate, batchPriorityNormal))
	occupy := w.dispatchAndRecv()
	if occupy == nil {
		t.Fatal("first job should have been admitted")
	}

	s.enqueue(testEntry("A", madmin.BatchJobReplicate, batchPriorityNormal))
	s.enqueue(testEntry("B", madmin.BatchJobReplicate, batchPriorityNormal))
	s.enqueue(testEntry("C", madmin.BatchJobReplicate, batchPriorityHigh))
	s.enqueue(testEntry("D", madmin.BatchJobReplicate, batchPriorityNormal))

	want := []string{"C", "A", "B", "D"}
	s.release(occupy, false)
	w.ready()
	for i, id := range want {
		e := w.dispatchAndRecv()
		if e == nil {
			t.Fatalf("step %d: expected admission of %s", i, id)
		}
		if e.id != id {
			t.Fatalf("step %d: admitted %s, want %s", i, e.id, id)
		}
		s.release(e, false)
		w.ready()
	}
	s.dispatch()
	if e := w.recv(); e != nil {
		t.Fatalf("queue should be empty, got %s", e.id)
	}
}

func TestBatchSchedulerAgingAntiStarvation(t *testing.T) {
	s, clk := newTestScheduler(t, 1, nil, nil)
	w := newTestWorker(s)

	s.enqueue(testEntry("hold", madmin.BatchJobExpire, batchPriorityNormal))
	occupy := w.dispatchAndRecv()
	s.enqueue(testEntry("low", madmin.BatchJobExpire, batchPriorityLow))

	// 19 slices: low weight = 19, a fresh high still overtakes.
	clk.add(19 * batchAgingSlice)
	s.enqueue(testEntry("high1", madmin.BatchJobExpire, batchPriorityHigh))
	s.release(occupy, false)
	w.ready()
	if e := w.dispatchAndRecv(); e == nil || e.id != "high1" {
		t.Fatalf("fresh high must overtake a 19-slice old low, got %v", e)
	}

	// After 20 slices the low ties a fresh high and wins on submit order.
	clk.add(batchAgingSlice)
	s.enqueue(testEntry("high2", madmin.BatchJobExpire, batchPriorityHigh))
	s.release(s.getEntryOrFail(t, "high1"), false)
	w.ready()
	e := w.dispatchAndRecv()
	if e == nil || e.id != "low" {
		t.Fatalf("aged low must be admitted before a fresh high at 20 slices, got %v", e)
	}
	s.release(e, false)
	w.ready()
	if e := w.dispatchAndRecv(); e == nil || e.id != "high2" {
		t.Fatalf("high2 must follow, got %v", e)
	}
}

// getEntryOrFail fetches a live entry for release in tests.
func (s *batchScheduler) getEntryOrFail(t *testing.T, id string) *batchJobEntry {
	t.Helper()
	e, ok := s.get(id)
	if !ok {
		t.Fatalf("entry %s missing", id)
	}
	return e
}

func TestBatchSchedulerTotalAndPerTypeCaps(t *testing.T) {
	caps := map[madmin.BatchJobType]int{madmin.BatchJobReplicate: 1}
	s, _ := newTestScheduler(t, 3, caps, nil)
	workers := []*testWorker{newTestWorker(s), newTestWorker(s), newTestWorker(s)}

	jobs := []*batchJobEntry{
		testEntry("r1", madmin.BatchJobReplicate, batchPriorityNormal),
		testEntry("r2", madmin.BatchJobReplicate, batchPriorityNormal),
		testEntry("e1", madmin.BatchJobExpire, batchPriorityNormal),
		testEntry("e2", madmin.BatchJobExpire, batchPriorityNormal),
		testEntry("e3", madmin.BatchJobExpire, batchPriorityNormal),
	}
	for _, j := range jobs {
		s.enqueue(j)
	}

	s.dispatch()
	owner := map[string]*testWorker{}
	admitted := map[string]bool{}
	for _, w := range workers {
		if e := w.recv(); e != nil {
			admitted[e.id] = true
			owner[e.id] = w
		}
	}
	if !admitted["r1"] || !admitted["e1"] || !admitted["e2"] {
		t.Fatalf("unexpected first admissions: %v", admitted)
	}

	snap := s.snapshot(s.now(), "")
	if snap.Running != 3 || snap.Waiting != 2 {
		t.Fatalf("running=%d waiting=%d", snap.Running, snap.Waiting)
	}
	if occ := snap.ByType[string(madmin.BatchJobReplicate)]; occ.Running != 1 || occ.Waiting != 1 || occ.Max != 1 {
		t.Fatalf("replicate occupancy wrong: %+v", occ)
	}

	// An expire completion must be backfilled by the waiting expire job,
	// while the second replicate stays blocked by the class cap.
	s.release(s.getEntryOrFail(t, "e1"), false)
	owner["e1"].ready()
	s.dispatch()
	if e := owner["e1"].recv(); e == nil || e.id != "e3" {
		t.Fatalf("waiting expire must backfill, got %v", e)
	} else {
		owner["e3"] = owner["e1"]
	}
	// No other worker can be holding r2 while the cap is saturated.
	for _, w := range workers {
		if e := w.recv(); e != nil {
			t.Fatalf("replicate cap must keep r2 blocked, got %s", e.id)
		}
	}

	// Once a replicate slot frees, r2 is admitted.
	s.release(s.getEntryOrFail(t, "r1"), false)
	owner["r1"].ready()
	s.dispatch()
	if e := owner["r1"].recv(); e == nil || e.id != "r2" {
		t.Fatalf("r2 must be admitted after r1 finished, got %v", e)
	}
}

func TestBatchSchedulerWaitingPauseResume(t *testing.T) {
	s, _ := newTestScheduler(t, 1, nil, nil)
	w := newTestWorker(s)

	s.enqueue(testEntry("hold", madmin.BatchJobKeyRotate, batchPriorityNormal))
	occupy := w.dispatchAndRecv()

	a := testEntry("A", madmin.BatchJobKeyRotate, batchPriorityHigh)
	b := testEntry("B", madmin.BatchJobKeyRotate, batchPriorityNormal)
	s.enqueue(a)
	s.enqueue(b)

	if !s.pauseWaiting("A") {
		t.Fatal("pauseWaiting should succeed for a queued job")
	}
	if s.pauseWaiting("A") {
		t.Fatal("double pause should report false (not queued anymore)")
	}
	s.release(occupy, false)
	w.ready()
	if e := w.dispatchAndRecv(); e == nil || e.id != "B" {
		t.Fatalf("normal B must run while high A is frozen, got %v", e)
	}
	if e := w.dispatchAndRecv(); e != nil {
		t.Fatalf("frozen A must not be admitted, got %s", e.id)
	}
	if !s.resumeWaiting("A") {
		t.Fatal("resumeWaiting should succeed for a frozen job")
	}
	if s.resumeWaiting("A") {
		t.Fatal("double resume should report false")
	}
	s.release(s.getEntryOrFail(t, "B"), false)
	w.ready()
	if e := w.dispatchAndRecv(); e == nil || e.id != "A" {
		t.Fatalf("resumed A must be admitted, got %v", e)
	}
}

func TestBatchSchedulerWatermarkGating(t *testing.T) {
	gated := true
	s, _ := newTestScheduler(t, 2, nil, func() bool { return gated })
	w := newTestWorker(s)
	s.enqueue(testEntry("j1", madmin.BatchJobExpire, batchPriorityHigh))
	if e := w.dispatchAndRecv(); e != nil {
		t.Fatalf("no admissions while watermark gated, got %s", e.id)
	}
	gated = false
	if e := w.dispatchAndRecv(); e == nil || e.id != "j1" {
		t.Fatalf("admissions resume after gate clears, got %v", e)
	}
}

func TestBatchSchedulerRemoveWaiting(t *testing.T) {
	s, _ := newTestScheduler(t, 1, nil, nil)
	w := newTestWorker(s)
	s.enqueue(testEntry("hold", madmin.BatchJobExpire, batchPriorityNormal))
	occupy := w.dispatchAndRecv()
	s.enqueue(testEntry("w1", madmin.BatchJobExpire, batchPriorityNormal))
	if !s.removeWaiting("w1") {
		t.Fatal("removeWaiting should find the queued job")
	}
	if s.removeWaiting("w1") {
		t.Fatal("double remove should return false")
	}
	if s.removeWaiting(occupy.id) {
		t.Fatal("removeWaiting must not remove running jobs")
	}
	s.release(occupy, false)
	w.ready()
	s.dispatch()
	if e := w.recv(); e != nil {
		t.Fatalf("nothing should be admitted, got %s", e.id)
	}
}

func TestBatchSchedulerRequeueAfterPause(t *testing.T) {
	s, clk := newTestScheduler(t, 1, nil, nil)
	w := newTestWorker(s)
	s.enqueue(testEntry("A", madmin.BatchJobReplicate, batchPriorityNormal))
	e := w.dispatchAndRecv()
	if e == nil {
		t.Fatal("expected A to be admitted")
	}
	// It ran for a while before being paused; its original enqueue time is old.
	clk.add(time.Minute)
	s.release(e, true)
	if got := s.snapshot(s.now(), ""); got.Running != 0 {
		t.Fatalf("paused release must free running slot, running=%d", got.Running)
	}
	if _, ok := s.get(e.id); !ok {
		t.Fatal("paused entry must remain tracked for resume")
	}
	w.ready()
	// New normal job submitted while A is paused goes first since A is not
	// queued yet.
	s.enqueue(testEntry("n1", madmin.BatchJobReplicate, batchPriorityNormal))
	other := w.dispatchAndRecv()
	if other == nil || other.id != "n1" {
		t.Fatalf("new job should run while A is paused, got %v", other)
	}
	// Resume A: it re-enters with a fresh timestamp and eventually runs.
	s.requeue(e)
	s.release(other, false)
	w.ready()
	again := w.dispatchAndRecv()
	if again == nil || again.id != e.id {
		t.Fatalf("requeued paused job should run, got %v", again)
	}
	if !again.enqueuedAt.Equal(clk.t) {
		t.Fatalf("resumed job must get a fresh enqueue time, got %v want %v", again.enqueuedAt, clk.t)
	}
}

func TestBatchSchedulerSnapshotPositions(t *testing.T) {
	s, _ := newTestScheduler(t, 1, nil, nil)
	w := newTestWorker(s)
	s.enqueue(testEntry("run", madmin.BatchJobReplicate, batchPriorityNormal))
	running := w.dispatchAndRecv()
	if running == nil {
		t.Fatal("expected running job")
	}
	t.Cleanup(func() { s.release(running, false) })

	s.enqueue(testEntry("q1", madmin.BatchJobReplicate, batchPriorityNormal))
	s.enqueue(testEntry("q2", madmin.BatchJobReplicate, batchPriorityHigh))
	s.enqueue(testEntry("q3", madmin.BatchJobExpire, batchPriorityLow))

	snap := s.snapshot(s.now(), "")
	pos := map[string]int{}
	state := map[string]string{}
	var foundRunning *BatchQueueJobInfo
	for i := range snap.Jobs {
		j := &snap.Jobs[i]
		pos[j.ID] = j.QueuePosition
		state[j.ID] = j.State
		if j.ID == running.id {
			foundRunning = j
		}
	}
	if pos["q2"] != 1 || pos["q1"] != 2 || pos["q3"] != 3 {
		t.Fatalf("positions = q1:%d q2:%d q3:%d; want 2/1/3", pos["q1"], pos["q2"], pos["q3"])
	}
	if foundRunning == nil || foundRunning.State != "running" || foundRunning.QueuePosition != 0 {
		t.Fatalf("running entry view wrong: %+v", foundRunning)
	}
	if state["q2"] != "queued" {
		t.Fatalf("q2 state = %s", state["q2"])
	}

	// Type filter.
	replOnly := s.snapshot(s.now(), string(madmin.BatchJobReplicate))
	for _, j := range replOnly.Jobs {
		if j.Type != string(madmin.BatchJobReplicate) {
			t.Fatalf("filter leaked %s entry: %s", j.Type, j.ID)
		}
	}

	// Frozen waiter is reported as paused without a queue position.
	s.pauseWaiting("q2")
	frozen := s.snapshot(s.now(), "")
	for i := range frozen.Jobs {
		if frozen.Jobs[i].ID == "q2" {
			if frozen.Jobs[i].State != "paused" || frozen.Jobs[i].QueuePosition != 0 {
				t.Fatalf("frozen q2 view wrong: %+v", frozen.Jobs[i])
			}
			return
		}
	}
	t.Fatal("q2 missing from snapshot")
}
