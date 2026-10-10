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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
)

// stressRunner completes jobs when their release channel is closed or when
// the job context ends, and tracks starts for completeness checks.
type stressRunner struct {
	mu      sync.Mutex
	release map[string]chan struct{}
	starts  map[string]int
}

func newStressRunner() *stressRunner {
	return &stressRunner{
		release: make(map[string]chan struct{}),
		starts:  make(map[string]int),
	}
}

func (s *stressRunner) fn(e *batchJobEntry) {
	s.mu.Lock()
	ch := make(chan struct{})
	s.release[e.id] = ch
	s.starts[e.id]++
	s.mu.Unlock()

	select {
	case <-e.ctx.Done():
	case <-ch:
	}
}

func (s *stressRunner) completeRandom(r *rand.Rand) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, ch := range s.release {
		select {
		case <-ch:
			continue
		default:
		}
		if r.Intn(2) == 0 {
			close(ch)
			delete(s.release, id)
			return true
		}
	}
	return false
}

func (s *stressRunner) startCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.starts[id]
}

// TestBatchPoolStressInterleaving hammers the pool with concurrent
// submissions, completions, pauses, resumes and cancels while continuously
// asserting the admission invariants. Run with -race for concurrency
// verification.
func TestBatchPoolStressInterleaving(t *testing.T) {
	const (
		maxJobs       = 4
		queueCapacity = 64
		duration      = 2 * time.Second
		replCap       = 2
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stub := newStressRunner()
	j := &BatchJobPool{
		ctx:          ctx,
		workerKillCh: make(chan struct{}, maxJobs+16),
		runFn:        stub.fn,
	}
	j.queueCap.Store(queueCapacity)
	j.scheduler = newBatchScheduler(maxJobs, map[madmin.BatchJobType]int{
		madmin.BatchJobReplicate: replCap,
	}, j.resourcesGated)
	j.ResizeWorkers(maxJobs)

	rng := rand.New(rand.NewSource(1))
	var seq atomic.Int64
	var submitted, rejected atomic.Int64

	jobTypes := []madmin.BatchJobType{madmin.BatchJobReplicate, madmin.BatchJobKeyRotate, madmin.BatchJobExpire}
	priorities := []string{"", "low", "normal", "high"}

	mkJob := func() *BatchJobRequest {
		n := seq.Add(1)
		req := &BatchJobRequest{ID: fmt.Sprintf("job-%d", n)}
		switch jobTypes[rng.Intn(len(jobTypes))] {
		case madmin.BatchJobReplicate:
			req.Replicate = &BatchJobReplicateV1{}
		case madmin.BatchJobKeyRotate:
			req.KeyRotate = &BatchJobKeyRotateV1{}
		default:
			req.Expire = &BatchJobExpire{}
		}
		req.Priority = priorities[rng.Intn(len(priorities))]
		return req
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Submitters.
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := j.queueJob(mkJob()); err != nil {
					rejected.Add(1)
				} else {
					submitted.Add(1)
				}
				time.Sleep(time.Microsecond)
			}
		}()
	}

	// Completers.
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				stub.completeRandom(rng)
				time.Sleep(2 * time.Microsecond)
			}
		}()
	}

	// pausedOutside tracks jobs paused while running and parked outside
	// the waiting heap; their explicit resume may transiently add a
	// waiter beyond the new-submission capacity.
	var pausedOutside atomic.Int64

	// Operators: pause/resume/cancel random live jobs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			snap := j.queueSnapshot("")
			if len(snap.Jobs) == 0 {
				time.Sleep(time.Millisecond)
				continue
			}
			target := snap.Jobs[rng.Intn(len(snap.Jobs))]
			switch rng.Intn(3) {
			case 0:
				if j.pauseJob(target.ID) == nil && target.State == "running" {
					pausedOutside.Add(1)
				}
			case 1:
				if j.resumeJob(target.ID) == nil && target.State == "paused" && !target.Frozen {
					pausedOutside.Add(-1)
				}
			case 2:
				if j.canceler(target.ID, true) == nil && target.State == "paused" && !target.Frozen {
					pausedOutside.Add(-1)
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()

	// Invariant watcher: running counts must never exceed limits, and the
	// waiting heap may exceed the new-submission capacity only by entries
	// parked outside the heap (paused-while-running) awaiting requeue.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			snap := j.queueSnapshot("")
			if snap.Running > maxJobs {
				t.Errorf("invariant: running %d > maxJobs %d", snap.Running, maxJobs)
				return
			}
			if occ := snap.ByType[string(madmin.BatchJobReplicate)]; occ.Running > replCap {
				t.Errorf("invariant: replicate running %d > cap %d", occ.Running, replCap)
				return
			}
			if limit := int64(queueCapacity) + pausedOutside.Load(); int64(snap.Waiting) > limit {
				t.Errorf("invariant: waiting %d > capacity %d + pausedOutside %d", snap.Waiting, queueCapacity, pausedOutside.Load())
				return
			}
			time.Sleep(500 * time.Microsecond)
		}
	}()

	time.Sleep(duration)
	close(stop)
	wg.Wait()

	// Shut the pool down; canceled running jobs unwind and release their slots.
	cancel()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		snap := j.queueSnapshot("")
		if snap.Running == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	snap := j.queueSnapshot("")
	if snap.Running != 0 {
		t.Fatalf("running jobs should drain on shutdown, got %d", snap.Running)
	}

	// Counter consistency: by-type running must sum to total.
	var sum int
	for _, occ := range snap.ByType {
		sum += occ.Running
		if occ.Running < 0 {
			t.Fatalf("negative per-type running counter: %+v", occ)
		}
	}
	if sum != snap.Running {
		t.Fatalf("by-type running %d != total %d", sum, snap.Running)
	}

	t.Logf("stress done: submitted=%d rejected=%d live=%d", submitted.Load(), rejected.Load(), snap.Waiting)
	if submitted.Load() == 0 {
		t.Fatal("expected some jobs to be accepted")
	}
}
