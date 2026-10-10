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
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	batchcfg "github.com/minio/minio/internal/config/batch"
)

func testLimits(maxJobs, queueCapacity int) batchcfg.Limits {
	return batchcfg.Limits{
		MaxJobs:       maxJobs,
		QueueCapacity: queueCapacity,
	}
}

// stubRunner replaces real batch job execution in pool tests. A run blocks
// until its entry context is canceled (pause/cancel/pool shutdown) or until
// release(id) is called.
type stubRunner struct {
	mu      sync.Mutex
	starts  map[string]int
	release map[string]chan struct{}
	started chan string
}

func newStubRunner() *stubRunner {
	return &stubRunner{
		starts:  make(map[string]int),
		release: make(map[string]chan struct{}),
		started: make(chan string, 64),
	}
}

func (s *stubRunner) fn(e *batchJobEntry) {
	s.mu.Lock()
	s.starts[e.id]++
	ch := make(chan struct{})
	s.release[e.id] = ch
	s.mu.Unlock()

	s.started <- e.id

	select {
	case <-e.ctx.Done():
	case <-ch:
	}
}

func (s *stubRunner) releaseOnce(id string) {
	s.mu.Lock()
	ch := s.release[id]
	s.mu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
}

func (s *stubRunner) startCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.starts[id]
}

func (s *stubRunner) waitStarted(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-s.started:
		if got != want {
			t.Fatalf("start order: got %s, want %s", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s to start", want)
	}
}

func (s *stubRunner) assertNoStart(t *testing.T, ids ...string) {
	t.Helper()
	for {
		select {
		case got := <-s.started:
			for _, id := range ids {
				if got == id {
					t.Fatalf("%s unexpectedly started", id)
				}
			}
		default:
			return
		}
	}
}

// newTestBatchJobPool builds a pool with real worker goroutines but a stub
// executor and no disk/background-resume dependencies.
func newTestBatchJobPool(t *testing.T, maxJobs, queueCapacity int, stub *stubRunner) *BatchJobPool {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	j := &BatchJobPool{
		ctx:          ctx,
		workerKillCh: make(chan struct{}, maxJobs+4),
		runFn:        stub.fn,
	}
	j.queueCap.Store(int64(queueCapacity))
	j.scheduler = newBatchScheduler(maxJobs, nil, j.resourcesGated)
	j.ResizeWorkers(maxJobs)
	return j
}

func poolJobReq(id string) *BatchJobRequest {
	return &BatchJobRequest{ID: id, Expire: &BatchJobExpire{}}
}

func TestBatchPoolQueueCapacityAndReject(t *testing.T) {
	stub := newStubRunner()
	j := newTestBatchJobPool(t, 1, 2, stub)

	if err := j.queueJob(poolJobReq("j1")); err != nil {
		t.Fatal(err)
	}
	stub.waitStarted(t, "j1")

	for _, id := range []string{"j2", "j3"} {
		if err := j.queueJob(poolJobReq(id)); err != nil {
			t.Fatalf("%s should queue: %v", id, err)
		}
	}
	err := j.queueJob(poolJobReq("j4"))
	if err == nil || !strings.Contains(err.Error(), "queue is currently full") {
		t.Fatalf("4th submit must be rejected with the legacy full error, got %v", err)
	}

	snap := j.queueSnapshot("")
	if snap.Running != 1 || snap.Waiting != 2 || snap.MaxJobs != 1 {
		t.Fatalf("snapshot = running %d waiting %d max %d", snap.Running, snap.Waiting, snap.MaxJobs)
	}

	// Completing j1 pulls j2, then j3 - nothing is lost or rejected.
	stub.releaseOnce("j1")
	stub.waitStarted(t, "j2")
	stub.releaseOnce("j2")
	stub.waitStarted(t, "j3")
}

func TestBatchPoolCancelWaiting(t *testing.T) {
	stub := newStubRunner()
	j := newTestBatchJobPool(t, 1, 10, stub)

	if err := j.queueJob(poolJobReq("j1")); err != nil {
		t.Fatal(err)
	}
	stub.waitStarted(t, "j1")
	for _, id := range []string{"j2", "j3"} {
		if err := j.queueJob(poolJobReq(id)); err != nil {
			t.Fatal(err)
		}
	}

	if err := j.canceler("j2", true); err != nil {
		t.Fatalf("cancel waiting job: %v", err)
	}
	if err := j.canceler("j2", true); err != errNoSuchJob {
		t.Fatalf("double cancel should return errNoSuchJob, got %v", err)
	}

	stub.releaseOnce("j1")
	stub.waitStarted(t, "j3")
	stub.assertNoStart(t, "j2")
}

func TestBatchPoolPauseRunningAndResume(t *testing.T) {
	stub := newStubRunner()
	j := newTestBatchJobPool(t, 1, 10, stub)

	if err := j.queueJob(poolJobReq("j1")); err != nil {
		t.Fatal(err)
	}
	stub.waitStarted(t, "j1")

	// Pause: run unwinds with errJobPaused, the entry parks and frees the slot.
	if err := j.pauseJob("j1"); err != nil {
		t.Fatal(err)
	}
	if err := j.pauseJob("j1"); err != nil {
		t.Fatalf("pause should be idempotent: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		snap := j.queueSnapshot("")
		state := ""
		for _, job := range snap.Jobs {
			if job.ID == "j1" {
				state = job.State
			}
		}
		if snap.Running == 0 && state == "paused" {
			break
		}
		time.Sleep(10 * time.Millisecond)
		if time.Now().After(deadline) {
			t.Fatalf("j1 did not park as paused: %+v", j.queueSnapshot(""))
		}
	}

	// A fresh job takes the freed slot while j1 is paused.
	if err := j.queueJob(poolJobReq("j2")); err != nil {
		t.Fatal(err)
	}
	stub.waitStarted(t, "j2")

	// Resume j1: it re-enters the queue and runs once j2 finishes.
	if err := j.resumeJob("j1"); err != nil {
		t.Fatal(err)
	}
	// A second resume is rejected because j1 is queued again, not paused.
	if err := j.resumeJob("j1"); err == nil {
		t.Fatal("resume on a non-paused queued job must return an error")
	}
	stub.releaseOnce("j2")
	stub.waitStarted(t, "j1")
	if stub.startCount("j1") != 2 {
		t.Fatalf("j1 should have run twice, ran %d times", stub.startCount("j1"))
	}

	// Second run completes naturally: the entry disappears from the pool.
	stub.releaseOnce("j1")
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if j.scheduler.totalCount() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if j.scheduler.totalCount() != 0 {
		t.Fatalf("all entries should be terminal after completion, got %d", j.scheduler.totalCount())
	}
}

func TestBatchPoolPauseWaitingAndResume(t *testing.T) {
	stub := newStubRunner()
	j := newTestBatchJobPool(t, 1, 10, stub)

	if err := j.queueJob(poolJobReq("j1")); err != nil {
		t.Fatal(err)
	}
	stub.waitStarted(t, "j1")

	jr := poolJobReq("j2")
	jr.Priority = "high"
	if err := j.queueJob(jr); err != nil {
		t.Fatal(err)
	}
	if err := j.pauseJob("j2"); err != nil {
		t.Fatal(err)
	}
	stub.assertNoStart(t, "j2")

	// Resume the frozen job; it runs as soon as the slot frees.
	if err := j.resumeJob("j2"); err != nil {
		t.Fatal(err)
	}
	stub.releaseOnce("j1")
	stub.waitStarted(t, "j2")
}

func TestBatchPoolPauseUnknownJob(t *testing.T) {
	stub := newStubRunner()
	j := newTestBatchJobPool(t, 1, 10, stub)
	if err := j.pauseJob("ghost"); err != errNoSuchJob {
		t.Fatalf("pause unknown = %v, want errNoSuchJob", err)
	}
	if err := j.resumeJob("ghost"); err != errNoSuchJob {
		t.Fatalf("resume unknown = %v, want errNoSuchJob", err)
	}
}

func TestBatchPoolUpdateLimitsGrowsWorkers(t *testing.T) {
	stub := newStubRunner()
	j := newTestBatchJobPool(t, 1, 10, stub)

	if err := j.queueJob(poolJobReq("j1")); err != nil {
		t.Fatal(err)
	}
	stub.waitStarted(t, "j1")

	// Second job waits with a single worker busy.
	if err := j.queueJob(poolJobReq("j2")); err != nil {
		t.Fatal(err)
	}
	stub.assertNoStart(t, "j2")

	j.updateLimits(testLimits(2, 10))
	stub.waitStarted(t, "j2")

	j.workerMu.Lock()
	size := j.workerSize
	j.workerMu.Unlock()
	if size != 2 {
		t.Fatalf("worker size = %d, want 2", size)
	}

	// The type cap is honored immediately too.
	if err := j.queueJob(poolJobReq("j3")); err != nil {
		t.Fatal(err)
	}
}

func TestBatchPoolQueueSnapshotJSON(t *testing.T) {
	stub := newStubRunner()
	j := newTestBatchJobPool(t, 2, 10, stub)

	if err := j.queueJob(poolJobReq("j1")); err != nil {
		t.Fatal(err)
	}
	stub.waitStarted(t, "j1")
	if err := j.queueJob(poolJobReq("j2")); err != nil {
		t.Fatal(err)
	}
	stub.waitStarted(t, "j2")
	wait := poolJobReq("j3")
	wait.Priority = "high"
	if err := j.queueJob(wait); err != nil {
		t.Fatal(err)
	}

	snap := j.queueSnapshot("")
	if snap.MaxJobs != 2 || snap.Running != 2 || snap.Waiting != 1 {
		t.Fatalf("snapshot totals wrong: %+v", snap.BatchSchedulerSnapshot)
	}
	var found bool
	for _, job := range snap.Jobs {
		if job.ID == "j3" {
			found = true
			if job.State != "queued" || job.QueuePosition != 1 || job.Priority != "high" || job.User != "" {
				t.Fatalf("j3 view wrong: %+v", job)
			}
			if job.WaitDuration < 0 {
				t.Fatalf("wait duration must be non-negative")
			}
		}
	}
	if !found {
		t.Fatal("waiting j3 missing from snapshot")
	}
	if snap.Watermarks.CPU.Threshold != 0 || snap.Watermarks.CPU.Gated {
		t.Fatalf("watermarks should be disabled without a monitor: %+v", snap.Watermarks)
	}

	// JSON serialization of the exact API response shape.
	raw, err := json.Marshal(&snap)
	if err != nil {
		t.Fatalf("snapshot must marshal to JSON: %v", err)
	}
	body := string(raw)
	for _, key := range []string{`"node"`, `"maxJobs"`, `"running"`, `"waiting"`, `"byType"`, `"watermarks"`, `"queuePosition"`, `"throttleWait"`, `"j3"`} {
		if !strings.Contains(body, key) {
			t.Fatalf("snapshot JSON missing %s: %s", key, body)
		}
	}

	// Type filter drops unrelated jobs.
	replOnly := j.queueSnapshot(string(madmin.BatchJobReplicate))
	for _, job := range replOnly.Jobs {
		if job.Type != string(madmin.BatchJobReplicate) {
			t.Fatalf("filter leaked %s: %s", job.Type, job.ID)
		}
	}
}

func TestBatchPoolUpdateLimitsShrinkAndGrow(t *testing.T) {
	stub := newStubRunner()
	j := newTestBatchJobPool(t, 2, 10, stub)

	for _, id := range []string{"j1", "j2"} {
		if err := j.queueJob(poolJobReq(id)); err != nil {
			t.Fatal(err)
		}
	}
	stub.waitStarted(t, "j1")
	stub.waitStarted(t, "j2")

	// Shrink while both jobs run: they are not interrupted.
	j.updateLimits(testLimits(1, 10))
	j.workerMu.Lock()
	size := j.workerSize
	j.workerMu.Unlock()
	if size != 1 {
		t.Fatalf("worker size after shrink = %d, want 1", size)
	}
	if stub.startCount("j1") != 1 || stub.startCount("j2") != 1 {
		t.Fatal("shrink must not interrupt running jobs")
	}

	// Completing both jobs settles the pool with one effective worker.
	stub.releaseOnce("j1")
	stub.releaseOnce("j2")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if j.scheduler.snapshot(time.Now(), "").Running == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := j.scheduler.snapshot(time.Now(), ""); got.Running != 0 {
		t.Fatalf("jobs should have completed after shrink, running=%d", got.Running)
	}

	// Growing back spawns new workers and admissions resume.
	j.updateLimits(testLimits(4, 10))
	if err := j.queueJob(poolJobReq("j3")); err != nil {
		t.Fatal(err)
	}
	stub.waitStarted(t, "j3")
	stub.releaseOnce("j3")
}

func TestBatchPoolPriorityResolution(t *testing.T) {
	stub := newStubRunner()
	j := newTestBatchJobPool(t, 1, 10, stub)

	// Unspecified priority resolves against globalBatchConfig (defaults to
	// normal); explicit high is stored as declared.
	if err := j.queueJob(poolJobReq("j1")); err != nil {
		t.Fatal(err)
	}
	stub.waitStarted(t, "j1")
	hi := poolJobReq("j2")
	hi.Priority = "high"
	lo := poolJobReq("j3")
	lo.Priority = "low"
	if err := j.queueJob(hi); err != nil {
		t.Fatal(err)
	}
	if err := j.queueJob(lo); err != nil {
		t.Fatal(err)
	}
	e2, _ := j.scheduler.get("j2")
	e3, _ := j.scheduler.get("j3")
	if e2.priority != batchPriorityHigh || e3.priority != batchPriorityLow {
		t.Fatalf("priority resolution = j2:%s j3:%s", e2.priority, e3.priority)
	}
}
