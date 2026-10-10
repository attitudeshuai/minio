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
	"container/heap"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/minio/madmin-go/v3"
)

// batchAgingSlice is the time a job has to wait in the queue to gain one
// extra point of effective priority. A low priority job catches up with a
// freshly queued high priority job after at most 20 slices.
const batchAgingSlice = 10 * time.Second

type batchJobState uint8

const (
	batchJobStateQueued batchJobState = iota
	batchJobStateRunning
	// batchJobStatePaused covers a job that was paused while running and
	// waits for a resume, as well as a waiting job that was frozen.
	batchJobStatePaused
)

func (s batchJobState) String() string {
	switch s {
	case batchJobStateRunning:
		return "running"
	case batchJobStatePaused:
		return "paused"
	default:
		return "queued"
	}
}

// batchJobEntry is the scheduler side record of a submitted batch job.
type batchJobEntry struct {
	job  *BatchJobRequest
	id   string
	user string
	typ  madmin.BatchJobType

	// priority is the resolved base scheduling priority (explicit job
	// priority wins over the configured per-type default).
	priority batchPriority

	seq        uint64
	enqueuedAt time.Time
	startedAt  time.Time

	state         batchJobState
	waitingPaused bool

	// ctx/cancel are (re)assigned when the job is admitted.
	ctx    context.Context
	cancel context.CancelCauseFunc
}

// effectiveWeight implements aging: the longer a job has been waiting, the
// higher its effective priority, capped at high+agingMax so the oldest low
// priority job ties a fresh high priority job and wins on submit order.
func (e *batchJobEntry) effectiveWeight(now time.Time) int {
	boost := int(now.Sub(e.enqueuedAt) / batchAgingSlice)
	if boost > batchPriorityAgingMax {
		boost = batchPriorityAgingMax
	}
	return e.priority.weight() + boost
}

// schedClock allows injecting time in tests.
type schedClock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type batchAdmission struct {
	entry  *batchJobEntry
	ticket chan *batchJobEntry
}

// batchScheduler is the single source of truth for waiting/running job
// accounting and admission ordering on a node. All state is guarded by mu.
type batchScheduler struct {
	mu sync.Mutex

	clock schedClock

	all     map[string]*batchJobEntry
	waiting batchWaitHeap
	seq     uint64

	maxJobs   int
	maxByType map[madmin.BatchJobType]int

	runningTotal  int
	runningByType map[madmin.BatchJobType]int

	// idleTickets are parked pool workers waiting for an admission.
	idleTickets []chan *batchJobEntry

	// gatedFn reports whether resource watermarks currently block new
	// admissions; nil means never gated.
	gatedFn func() bool
}

func newBatchScheduler(maxJobs int, maxByType map[madmin.BatchJobType]int, gatedFn func() bool) *batchScheduler {
	s := &batchScheduler{
		clock:         systemClock{},
		all:           make(map[string]*batchJobEntry),
		maxJobs:       maxJobs,
		maxByType:     maxByType,
		runningByType: make(map[madmin.BatchJobType]int),
		gatedFn:       gatedFn,
	}
	heap.Init(&s.waiting)
	return s
}

func (s *batchScheduler) now() time.Time {
	if s.clock != nil {
		return s.clock.Now()
	}
	return time.Now()
}

// nextSeq must be called with mu held.
func (s *batchScheduler) nextSeq() uint64 {
	s.seq++
	return s.seq
}

// enqueue adds a job to the waiting heap. Capacity enforcement is the
// caller's responsibility.
func (s *batchScheduler) enqueue(e *batchJobEntry) {
	s.mu.Lock()
	s.enqueueLocked(e)
	s.mu.Unlock()
}

// tryEnqueue adds a job to the waiting heap unless the waiting capacity is
// already full.
func (s *batchScheduler) tryEnqueue(e *batchJobEntry, capacity int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.waiting) >= capacity {
		return false
	}
	s.enqueueLocked(e)
	return true
}

func (s *batchScheduler) enqueueLocked(e *batchJobEntry) {
	e.seq = s.nextSeq()
	if e.enqueuedAt.IsZero() {
		e.enqueuedAt = s.now()
	}
	e.startedAt = time.Time{}
	e.state = batchJobStateQueued
	e.waitingPaused = false
	s.all[e.id] = e
	heap.Push(&s.waiting, e)
}

// removeWaiting deletes a waiting/frozen job. It returns false if the job is
// not currently held by the scheduler in a waiting-like state.
func (s *batchScheduler) removeWaiting(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.all[id]
	if !ok || e.state != batchJobStateQueued {
		return false
	}
	for i, we := range s.waiting {
		if we == e {
			heap.Remove(&s.waiting, i)
			break
		}
	}
	delete(s.all, id)
	return true
}

// pauseWaiting freezes a waiting job so it is skipped by admission; it
// returns false when the job is running or unknown (the pool then handles
// running jobs by canceling their context).
func (s *batchScheduler) pauseWaiting(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.all[id]
	if !ok || e.state != batchJobStateQueued {
		return false
	}
	e.waitingPaused = true
	e.state = batchJobStatePaused
	return true
}

// resumeWaiting thaws a job frozen while waiting; it returns false when no
// such frozen job exists (the pool then resumes paused running jobs).
func (s *batchScheduler) resumeWaiting(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.all[id]
	if !ok || e.state != batchJobStatePaused || !e.waitingPaused {
		return false
	}
	e.waitingPaused = false
	e.state = batchJobStateQueued
	return true
}

// requeue puts a previously paused (ran) job back into the waiting heap.
// It joins the tail of its priority class with a fresh submit timestamp and
// sequence number, so resumed jobs do not keep the aging boost they earned
// before their pause.
func (s *batchScheduler) requeue(e *batchJobEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.all[e.id]; !ok {
		// Entry was removed (e.g. canceled while paused); nothing to do.
		return
	}
	e.enqueuedAt = time.Time{}
	e.startedAt = time.Time{}
	s.enqueueLocked(e)
}

// get returns a scheduler entry by id.
func (s *batchScheduler) get(id string) (*batchJobEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.all[id]
	return e, ok
}

// addIdle registers a parked worker waiting for an admission.
func (s *batchScheduler) addIdle(ticket chan *batchJobEntry) {
	s.mu.Lock()
	s.idleTickets = append(s.idleTickets, ticket)
	s.mu.Unlock()
}

// reclaimIdle removes a parked worker (it was signaled to exit while
// waiting). It returns true if the ticket was still parked.
func (s *batchScheduler) reclaimIdle(ticket chan *batchJobEntry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, t := range s.idleTickets {
		if t == ticket {
			s.idleTickets = append(s.idleTickets[:i], s.idleTickets[i+1:]...)
			return true
		}
	}
	return false
}

// setLimits updates total/per-type running caps; zero per-type cap means
// unlimited. queueCapacity is enforced by the pool.
func (s *batchScheduler) setLimits(maxJobs int, maxByType map[madmin.BatchJobType]int) {
	s.mu.Lock()
	s.maxJobs = maxJobs
	if maxByType == nil {
		maxByType = map[madmin.BatchJobType]int{}
	}
	s.maxByType = maxByType
	s.mu.Unlock()
}

func (s *batchScheduler) typeCapLocked(typ madmin.BatchJobType) int {
	if s.maxByType == nil {
		return 0
	}
	return s.maxByType[typ]
}

// admissibleLocked reports whether e may start right now.
func (s *batchScheduler) admissibleLocked(e *batchJobEntry) bool {
	if e.state != batchJobStateQueued || e.waitingPaused {
		return false
	}
	if s.runningTotal >= s.maxJobs {
		return false
	}
	if cap := s.typeCapLocked(e.typ); cap > 0 && s.runningByType[e.typ] >= cap {
		return false
	}
	if s.gatedFn != nil && s.gatedFn() {
		return false
	}
	return true
}

// pickLocked returns the index of the highest effective weight (oldest seq
// on tie) admissible waiter, or -1. The waiting slice is re-heapified with
// current ages by the caller before invoking.
func (s *batchScheduler) pickLocked(now time.Time) int {
	best := -1
	for i, e := range s.waiting {
		if !s.admissibleLocked(e) {
			continue
		}
		if best == -1 || s.earlierLocked(e, s.waiting[best], now) {
			best = i
		}
	}
	return best
}

// earlierLocked reports whether a must be admitted before b: higher
// effective weight first, lower seq on tie.
func (s *batchScheduler) earlierLocked(a, b *batchJobEntry, now time.Time) bool {
	wa, wb := a.effectiveWeight(now), b.effectiveWeight(now)
	if wa != wb {
		return wa > wb
	}
	return a.seq < b.seq
}

// dispatch pairs idle workers with currently admissible jobs. It must be
// called whenever waiters, idle workers, limits or watermarks may change.
// Tickets are buffered(1), so sending never blocks.
func (s *batchScheduler) dispatch() []batchAdmission {
	now := s.now()

	s.mu.Lock()
	// Re-heapify because effective weights age with time.
	heap.Init(&s.waiting)

	var admitted []batchAdmission
	for len(s.idleTickets) > 0 {
		if s.gatedFn != nil && s.gatedFn() {
			break
		}
		if s.runningTotal >= s.maxJobs {
			break
		}
		idx := s.pickLocked(now)
		if idx < 0 {
			break
		}
		e := heap.Remove(&s.waiting, idx).(*batchJobEntry)
		e.state = batchJobStateRunning
		e.startedAt = now
		s.runningTotal++
		s.runningByType[e.typ]++

		ticket := s.idleTickets[0]
		s.idleTickets = s.idleTickets[1:]
		admitted = append(admitted, batchAdmission{entry: e, ticket: ticket})
	}
	s.mu.Unlock()

	for _, a := range admitted {
		a.ticket <- a.entry
	}
	return admitted
}

// release accounts for a running job ending. The entry state (paused vs
// terminal) is decided by the caller; paused entries stay in s.all for
// resume, terminal entries are removed.
func (s *batchScheduler) release(e *batchJobEntry, paused bool) {
	s.mu.Lock()
	if e.state == batchJobStateRunning {
		s.runningTotal--
		if s.runningTotal < 0 {
			s.runningTotal = 0
		}
		if s.runningByType[e.typ] > 0 {
			s.runningByType[e.typ]--
		}
	}
	if paused {
		e.state = batchJobStatePaused
	} else {
		delete(s.all, e.id)
	}
	s.mu.Unlock()
}

// removeEntry drops a non-waiting (paused) entry from tracking.
func (s *batchScheduler) removeEntry(id string) {
	s.mu.Lock()
	delete(s.all, id)
	s.mu.Unlock()
}

// waitingCount returns the number of jobs held waiting (frozen jobs count
// as waiting capacity occupants too).
func (s *batchScheduler) waitingCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.waiting)
}

// totalCount returns all live entries (waiting/running/paused).
func (s *batchScheduler) totalCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.all)
}

// BatchQueueJobInfo is the per-job view returned by the queue snapshot API.
type BatchQueueJobInfo struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	User     string `json:"user"`
	Priority string `json:"priority"`
	State    string `json:"state"`
	// Frozen is true for a job paused while waiting for admission; it
	// remains a queue occupant but is skipped by the scheduler. A job
	// paused while running is reported State=paused with Frozen=false and
	// sits outside the waiting heap until resumed.
	Frozen        bool          `json:"frozen"`
	QueuePosition int           `json:"queuePosition"`
	QueuedAt      time.Time     `json:"queuedAt"`
	WaitDuration  time.Duration `json:"waitDuration"`
	StartedAt     time.Time     `json:"startedAt,omitempty"`
}

// BatchTypeOccupancy is the per-type occupancy view.
type BatchTypeOccupancy struct {
	Running int `json:"running"`
	Waiting int `json:"waiting"`
	Max     int `json:"max"` // 0 means unlimited
}

// BatchSchedulerSnapshot is the node-local queue snapshot.
type BatchSchedulerSnapshot struct {
	MaxJobs int                           `json:"maxJobs"`
	Running int                           `json:"running"`
	Waiting int                           `json:"waiting"`
	ByType  map[string]BatchTypeOccupancy `json:"byType"`
	Jobs    []BatchQueueJobInfo           `json:"jobs"`
}

// batchWatermarkStat is the latest observation of one resource dimension.
type batchWatermarkStat struct {
	Threshold int       `json:"threshold"` // configured percentage; 0 disables
	Observed  int       `json:"observed"`  // latest sampled percentage
	Gated     bool      `json:"gated"`     // currently above threshold (with hysteresis)
	Available bool      `json:"available"` // whether sampling works on this platform
	SampledAt time.Time `json:"sampledAt,omitempty"`
}

// batchWatermarkView covers the three gated resource dimensions.
type batchWatermarkView struct {
	CPU   batchWatermarkStat `json:"cpu"`
	Mem   batchWatermarkStat `json:"mem"`
	Drive batchWatermarkStat `json:"drive"`
}

// snapshot builds a consistent view for the queue API.
func (s *batchScheduler) snapshot(now time.Time, jobTypeFilter string) BatchSchedulerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := BatchSchedulerSnapshot{
		MaxJobs: s.maxJobs,
		Running: s.runningTotal,
		Waiting: len(s.waiting),
		ByType:  make(map[string]BatchTypeOccupancy),
	}

	// Init per-type rows for all known types.
	for _, typ := range []madmin.BatchJobType{madmin.BatchJobReplicate, madmin.BatchJobKeyRotate, madmin.BatchJobExpire} {
		occ := BatchTypeOccupancy{
			Running: s.runningByType[typ],
			Max:     s.typeCapLocked(typ),
		}
		snap.ByType[string(typ)] = occ
	}
	// Waiting capacity occupants (including frozen waiters) are counted
	// straight from the heap.
	for _, e := range s.waiting {
		occ := snap.ByType[string(e.typ)]
		occ.Waiting++
		snap.ByType[string(e.typ)] = occ
	}

	// Waiting order for positions: copy heap and sort by admission order.
	waiters := make([]*batchJobEntry, len(s.waiting))
	copy(waiters, s.waiting)
	sort.Slice(waiters, func(i, j int) bool {
		return s.earlierLocked(waiters[i], waiters[j], now)
	})
	position := make(map[string]int, len(waiters))
	for i, e := range waiters {
		position[e.id] = i + 1
	}

	for _, e := range s.all {
		if jobTypeFilter != "" && string(e.typ) != jobTypeFilter {
			continue
		}
		info := BatchQueueJobInfo{
			ID:       e.id,
			Type:     string(e.typ),
			User:     e.user,
			Priority: string(e.priority),
			State:    e.state.String(),
			Frozen:   e.waitingPaused,
			QueuedAt: e.enqueuedAt,
		}
		switch {
		case e.state == batchJobStateQueued && !e.waitingPaused:
			info.QueuePosition = position[e.id]
			info.WaitDuration = now.Sub(e.enqueuedAt)
		case e.state == batchJobStateRunning:
			info.StartedAt = e.startedAt
			info.WaitDuration = e.startedAt.Sub(e.enqueuedAt)
		case e.state == batchJobStatePaused && e.waitingPaused:
			info.WaitDuration = now.Sub(e.enqueuedAt)
		default: // paused after running
			info.StartedAt = e.startedAt
			info.WaitDuration = e.startedAt.Sub(e.enqueuedAt)
		}
		snap.Jobs = append(snap.Jobs, info)
	}

	sort.Slice(snap.Jobs, func(i, j int) bool {
		return snap.Jobs[i].QueuedAt.Before(snap.Jobs[j].QueuedAt)
	})
	return snap
}

// ---- container/heap implementation ----

type batchWaitHeap []*batchJobEntry

func (h batchWaitHeap) Len() int { return len(h) }

func (h batchWaitHeap) Less(i, j int) bool {
	// heap.Pop returns the "smallest"; invert so the first-to-admit pops.
	// Weight ordering without a clock uses submit order as the stable tie
	// breaker; dispatch re-heapifies with aging weights before popping.
	a, b := h[i], h[j]
	wa, wb := a.priority.weight(), b.priority.weight()
	if wa != wb {
		return wa > wb
	}
	return a.seq < b.seq
}

func (h batchWaitHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *batchWaitHeap) Push(x any) { *h = append(*h, x.(*batchJobEntry)) }

func (h *batchWaitHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}
