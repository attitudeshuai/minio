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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/madmin-go/v3"
	batchcfg "github.com/minio/minio/internal/config/batch"
)

// errJobPaused is the context cause used when a running batch job is
// paused by the operator. Start implementations detect it to persist the
// progress marker without marking the job complete/failed.
var errJobPaused = errors.New("batch job paused")

//msgp:ignore BatchJobPool

// BatchJobPool owns the batch job admission scheduler and the pool of
// workers that run admitted jobs on this node.
type BatchJobPool struct {
	ctx      context.Context
	objLayer ObjectLayer

	workerMu     sync.Mutex
	workerKillCh chan struct{}
	workerSize   int

	scheduler *batchScheduler
	// monitor reports resource watermarks; nil means gating is disabled.
	monitor  batchResourceGate
	queueCap atomic.Int64

	// runFn replaces real job execution when set (unit tests).
	runFn func(*batchJobEntry)
}

// batchResourceGate is implemented by the resource watermark monitor.
type batchResourceGate interface {
	gated() bool
	view() batchWatermarkView
	updateThresholds(cpu, mem, drive int)
}

var globalBatchJobPool *BatchJobPool

// batchQueueSnapshotResp is the node-local queue status returned by the
// /queue-jobs admin API.
type batchQueueSnapshotResp struct {
	Node         string             `json:"node"`
	ThrottleWait time.Duration      `json:"throttleWait"`
	Watermarks   batchWatermarkView `json:"watermarks"`
	BatchSchedulerSnapshot
}

// newBatchJobPool creates a pool of batch job workers. The number of
// workers equals maxJobs (one worker per concurrently running job).
func newBatchJobPool(ctx context.Context, o ObjectLayer, workers int) *BatchJobPool {
	j := &BatchJobPool{
		ctx:          ctx,
		objLayer:     o,
		workerKillCh: make(chan struct{}, workers),
	}

	limits := globalBatchConfig.Limits()
	if workers > 0 {
		limits.MaxJobs = workers
	}
	if limits.MaxJobs <= 0 {
		// Defensive: LookupConfig guarantees > 0 but do not let an unset
		// zero config create a pool with no workers.
		limits.MaxJobs = 100
	}
	if limits.QueueCapacity <= 0 {
		limits.QueueCapacity = 10000
	}
	j.queueCap.Store(int64(limits.QueueCapacity))
	j.scheduler = newBatchScheduler(limits.MaxJobs, typeCapsFromLimits(limits), j.resourcesGated)
	j.ResizeWorkers(limits.MaxJobs)

	go j.dispatchTick()

	randomWait := func() time.Duration {
		// randomWait depends on the number of nodes to avoid triggering resume and cleanups at the same time.
		return time.Duration(rand.Float64() * float64(time.Duration(globalEndpoints.NEndpoints())*time.Hour))
	}

	go func() {
		j.resume(randomWait)
		j.cleanupReports(randomWait)
	}()

	return j
}

func typeCapsFromLimits(l batchcfg.Limits) map[madmin.BatchJobType]int {
	return map[madmin.BatchJobType]int{
		madmin.BatchJobReplicate: l.ReplicationMaxJobs,
		madmin.BatchJobKeyRotate: l.KeyRotationMaxJobs,
		madmin.BatchJobExpire:    l.ExpirationMaxJobs,
	}
}

// dispatchTick periodically re-runs admission so that aging waiters get
// re-ordered and watermark/limit changes are picked up without an explicit
// event.
func (j *BatchJobPool) dispatchTick() {
	t := time.NewTicker(batchAgingSlice)
	defer t.Stop()
	for {
		select {
		case <-j.ctx.Done():
			return
		case <-t.C:
			j.scheduler.dispatch()
		}
	}
}

// resourcesGated implements the watermark gate injected into the scheduler.
func (j *BatchJobPool) resourcesGated() bool {
	if j == nil || j.monitor == nil {
		return false
	}
	return j.monitor.gated()
}

// throttleWait returns the extra per-object delay running batch workers must
// apply while resource watermarks are breached; zero otherwise.
func (j *BatchJobPool) throttleWait() time.Duration {
	if j == nil || !j.resourcesGated() {
		return 0
	}
	return globalBatchConfig.Watermarks().ResourceThrottleWait
}

// batchResourceThrottleWait is the nil-safe helper used by the job Start
// loops.
func batchResourceThrottleWait() time.Duration {
	if globalBatchJobPool == nil {
		return 0
	}
	return globalBatchJobPool.throttleWait()
}

func defaultPriorityFor(typ madmin.BatchJobType) batchPriority {
	p, err := normalizeBatchPriority(globalBatchConfig.TypePriority(string(typ)))
	if err != nil {
		return batchPriorityNormal
	}
	return p
}

// queueJob admits or enqueues a submitted job. Jobs past the waiting queue
// capacity are rejected with the same error the pre-admission buffered
// channel returned.
func (j *BatchJobPool) queueJob(req *BatchJobRequest) error {
	if j == nil {
		return errInvalidArgument
	}
	priority := batchPriority(req.Priority)
	if priority == batchPriorityUnspecified || !priority.valid() {
		priority = defaultPriorityFor(req.Type())
	}

	jctx, jcancel := context.WithCancelCause(j.ctx)
	e := &batchJobEntry{
		id:       req.ID,
		user:     req.User,
		typ:      req.Type(),
		priority: priority,
		job:      req,
		ctx:      jctx,
		cancel:   jcancel,
	}

	if !j.scheduler.tryEnqueue(e, int(j.queueCap.Load())) {
		return fmt.Errorf("batch job queue is currently full please try again later %#v", req)
	}
	j.scheduler.dispatch()
	return nil
}

// AddWorker runs one long-lived job worker.
func (j *BatchJobPool) AddWorker() {
	if j == nil {
		return
	}
	for {
		if j.ctx.Err() != nil {
			return
		}
		e := j.acquire()
		if e == nil {
			return
		}
		j.runJob(e)
	}
}

// acquire blocks until the scheduler admits a job to this worker or the
// worker is asked to exit.
func (j *BatchJobPool) acquire() *batchJobEntry {
	ticket := make(chan *batchJobEntry, 1)
	j.scheduler.addIdle(ticket)
	j.scheduler.dispatch()

	select {
	case e := <-ticket:
		return e
	case <-j.workerKillCh:
	case <-j.ctx.Done():
	}
	// The scheduler may have dispatched into this ticket at the same time
	// the worker was woken to exit; honor the admission instead of losing
	// the job.
	select {
	case e := <-ticket:
		return e
	default:
	}
	j.scheduler.reclaimIdle(ticket)
	return nil
}

// runJob executes an admitted job and accounts for its terminal state.
func (j *BatchJobPool) runJob(e *batchJobEntry) {
	e.job.ctx = e.ctx

	if j.runFn != nil {
		j.runFn(e)
		j.finish(e)
		return
	}

	bucketNotFound := false
	job := e.job
	switch {
	case job.Replicate != nil:
		var err error
		if job.Replicate.RemoteToLocal() {
			err = job.Replicate.StartFromSource(job.ctx, j.objLayer, *job)
		} else {
			err = job.Replicate.Start(job.ctx, j.objLayer, *job)
		}
		switch {
		case err == nil:
		case isErrBucketNotFound(err):
			bucketNotFound = true
		default:
			batchLogIf(j.ctx, err)
		}
	case job.KeyRotate != nil:
		if err := job.KeyRotate.Start(job.ctx, j.objLayer, *job); err != nil {
			if isErrBucketNotFound(err) {
				bucketNotFound = true
			} else {
				batchLogIf(j.ctx, err)
			}
		}
	case job.Expire != nil:
		if err := job.Expire.Start(job.ctx, j.objLayer, *job); err != nil {
			if isErrBucketNotFound(err) {
				bucketNotFound = true
			} else {
				batchLogIf(j.ctx, err)
			}
		}
	}

	if bucketNotFound {
		// Bucket disappeared permanently; drop the job definition like the
		// previous implementation did.
		job.delete(GlobalContext, j.objLayer)
	}
	j.finish(e)
}

// finish transitions the entry out of running: paused jobs park with their
// progress flushed, everything else is terminal. It then kicks admission
// so the freed slot is immediately reused.
func (j *BatchJobPool) finish(e *batchJobEntry) {
	paused := errors.Is(context.Cause(e.ctx), errJobPaused)
	if paused {
		// Persist the last progress marker with an independent context:
		// the job context is already canceled.
		ctx, cancel := context.WithTimeout(GlobalContext, 30*time.Second)
		globalBatchJobsMetrics.flushJobProgress(ctx, j.objLayer, *e.job)
		cancel()
	}
	j.scheduler.release(e, paused)
	j.scheduler.dispatch()
}

// ResizeWorkers sets the job worker pool to new size. A shrink does not
// interrupt running jobs; workers only observe the kill signal while parked
// waiting for an admission.
func (j *BatchJobPool) ResizeWorkers(n int) {
	if j == nil {
		return
	}

	j.workerMu.Lock()
	defer j.workerMu.Unlock()

	for j.workerSize < n {
		j.workerSize++
		go j.AddWorker()
	}
	for j.workerSize > n {
		j.workerSize--
		go func() { j.workerKillCh <- struct{}{} }()
	}
}

// updateLimits applies hot-reloaded concurrency settings.
func (j *BatchJobPool) updateLimits(l batchcfg.Limits) {
	if j == nil {
		return
	}
	j.queueCap.Store(int64(l.QueueCapacity))
	j.scheduler.setLimits(l.MaxJobs, typeCapsFromLimits(l))
	j.ResizeWorkers(l.MaxJobs)
	j.scheduler.dispatch()
}

// canceler cancels a queued/running/paused job and drops its scheduler
// entry. Running jobs are removed once their Start unwinds; waiting and
// paused jobs are removed immediately.
func (j *BatchJobPool) canceler(jobID string, cancel bool) error {
	if j == nil || !cancel {
		return errInvalidArgument
	}
	e, ok := j.scheduler.get(jobID)
	if !ok {
		return errNoSuchJob
	}
	switch e.state {
	case batchJobStateQueued:
		if !j.scheduler.removeWaiting(jobID) {
			return errNoSuchJob
		}
		e.cancel(context.Canceled)
		j.scheduler.dispatch()
	case batchJobStateRunning:
		e.cancel(context.Canceled)
		// finish() removes the entry after Start unwinds.
	case batchJobStatePaused:
		e.cancel(context.Canceled)
		j.scheduler.removeEntry(jobID)
		j.scheduler.dispatch()
	default:
		return errNoSuchJob
	}
	return nil
}

// pauseJob freezes a waiting job or pauses a running job. Idempotent for
// already paused jobs.
func (j *BatchJobPool) pauseJob(jobID string) error {
	if j == nil {
		return errInvalidArgument
	}
	e, ok := j.scheduler.get(jobID)
	if !ok {
		return errNoSuchJob
	}
	switch e.state {
	case batchJobStateQueued:
		if !j.scheduler.pauseWaiting(jobID) {
			return errNoSuchJob
		}
		j.scheduler.dispatch()
	case batchJobStateRunning:
		e.cancel(errJobPaused)
	case batchJobStatePaused:
		// already paused
	default:
		return errNoSuchJob
	}
	return nil
}

// resumeJob thaws a frozen waiting job or re-queues a paused (ran) job with
// a fresh context and queue timestamp.
func (j *BatchJobPool) resumeJob(jobID string) error {
	if j == nil {
		return errInvalidArgument
	}
	e, ok := j.scheduler.get(jobID)
	if !ok {
		return errNoSuchJob

	}
	if e.state == batchJobStatePaused && e.waitingPaused {
		if !j.scheduler.resumeWaiting(jobID) {
			return errNoSuchJob
		}
		j.scheduler.dispatch()
		return nil
	}
	if e.state != batchJobStatePaused {
		return fmt.Errorf("batch job %s is not paused: %s", jobID, e.state)
	}
	jctx, jcancel := context.WithCancelCause(j.ctx)
	e.ctx = jctx
	e.cancel = jcancel
	j.scheduler.requeue(e)
	j.scheduler.dispatch()
	return nil
}

// queueSnapshot returns the node-local queue status.
func (j *BatchJobPool) queueSnapshot(jobType string) batchQueueSnapshotResp {
	resp := batchQueueSnapshotResp{
		Node:         globalLocalNodeName,
		ThrottleWait: j.throttleWait(),
	}
	resp.Watermarks = j.watermarkView()
	resp.BatchSchedulerSnapshot = j.scheduler.snapshot(time.Now(), jobType)
	return resp
}

func (j *BatchJobPool) watermarkView() batchWatermarkView {
	if j != nil && j.monitor != nil {
		return j.monitor.view()
	}
	w := globalBatchConfig.Watermarks()
	return batchWatermarkView{
		CPU:   batchWatermarkStat{Threshold: w.CPU},
		Mem:   batchWatermarkStat{Threshold: w.Mem},
		Drive: batchWatermarkStat{Threshold: w.Drive},
	}
}
