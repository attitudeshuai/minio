// Copyright (c) 2015-2024 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
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
	"sync"
	"sync/atomic"
	"time"

	xioutil "github.com/minio/minio/internal/ioutil"
)

// Scanner cycle phases.
const (
	scannerPhaseIdle        = "idle"        // No cycle is running, waiting for the next cadence tick.
	scannerPhaseRunning     = "running"     // Cycle is scanning.
	scannerPhasePausing     = "pausing"     // Pause was requested, cycle is reaching the next directory boundary.
	scannerPhasePaused      = "paused"      // Cycle is parked at a directory boundary.
	scannerPhaseStopping    = "stopping"    // Termination was requested, cycle is unwinding.
	scannerPhaseInterrupted = "interrupted" // Cycle ended without completing, checkpoint kept for the same number.
)

// Persisted lifecycle states of a cycle, stored together with the cycle header.
const (
	scannerCycleStateNone        = ""
	scannerCycleStatePaused      = "paused"
	scannerCycleStateInterrupted = "interrupted"
)

// errScannerCycleStopped is returned from scan boundaries when the running
// cycle has been terminated externally. It is not a scan failure: the cycle
// number is not advanced and the checkpoint is kept so the cycle can be
// resumed from where it stopped.
var errScannerCycleStopped = errors.New("scanner cycle stopped")

// gate values for scannerLifecycle.gate, accessed atomically so that the hot
// per-directory path is lock-free when no pause/termination is in effect.
const (
	scannerGateProceed int32 = iota
	scannerGatePause
	scannerGateStop
)

// ScannerCycleStatus is a point-in-time snapshot of the scanner cycle
// lifecycle, safe to consume outside the scanner.
type ScannerCycleStatus struct {
	// Phase is one of the scannerPhase* constants.
	Phase string `json:"phase"`
	// Active reports whether a cycle is currently in progress.
	Active bool `json:"active"`
	// Cycle is the number of the in-progress (or last interrupted) cycle.
	Cycle uint64 `json:"cycle"`
	// Resuming is true while the running cycle continues an interrupted
	// attempt of the same cycle number instead of starting a new one.
	Resuming bool `json:"resuming"`
	// Started is the time the current attempt started.
	Started time.Time `json:"started,omitempty"`
	// ObjectsProcessed is the number of objects processed since the current
	// attempt started.
	ObjectsProcessed uint64 `json:"objectsProcessed"`
	// TotalBatches is the total number of bucket batches planned for this
	// cycle across all sets.
	TotalBatches int `json:"totalBatches"`
	// CompletedBatches is the number of bucket batches already finished
	// (or skipped because a previous attempt finished them).
	CompletedBatches int `json:"completedBatches"`
	// RemainingBatches is the number of bucket batches not finished yet.
	RemainingBatches int `json:"remainingBatches"`
	// ActivePaths are the paths currently being scanned on local drives.
	ActivePaths []string `json:"activePaths,omitempty"`
	// Position records the last reached boundary.
	Position string `json:"position,omitempty"`
	// Reason is the pause/termination reason, if any.
	Reason string `json:"reason,omitempty"`
	// InterruptedAt is when the cycle was last interrupted.
	InterruptedAt time.Time `json:"interruptedAt,omitempty"`
	// LastCompleted is the completion time of the most recent full cycle.
	LastCompleted time.Time `json:"lastCompleted,omitempty"`
	// CyclesCompletedAt are the completion times of the recent cycles.
	CyclesCompletedAt []time.Time `json:"cyclesCompletedAt,omitempty"`
}

// scannerLifecycle turns the data scanner loop into a process with an
// explicit lifecycle: cycles can be paused at directory boundaries, resumed
// and terminated, and their progress can be queried externally.
//
// All directory/bucket boundaries in the scan path call waitAt*Boundary. The
// fast path is a single atomic load, so without an explicit pause request
// the scan cadence and behavior is unchanged.
type scannerLifecycle struct {
	// gate is read on the hot path, keep it first for alignment.
	gate atomic.Int32

	mu       sync.Mutex
	phase    string
	cycle    uint64
	resuming bool
	started  time.Time

	// Batch (bucket) progress, summed across all sets.
	totalBatches int
	doneBatches  int
	lastBatch    string

	// opsAtStart is the lifetime scanned-object counter captured when an
	// attempt begins, used to report per-cycle object progress.
	opsAtStart uint64

	// Lifecycle bookkeeping.
	pauseReason   string
	stopReason    string
	position      string
	interruptedAt time.Time

	// Completed cycles, kept in sync with the persisted cycle header.
	cyclesCompleted []time.Time

	// wantPause records a pause request that arrived while no cycle was
	// active; the next cycle parks at its first boundary.
	wantPause bool

	// wake is closed (and replaced) to release every goroutine parked in a
	// boundary wait, either to resume or to terminate.
	wake chan struct{}
	// pausedCh is closed once the cycle reaches the paused state. It is
	// recreated for every attempt.
	pausedCh chan struct{}

	// onPaused is invoked once, without holding the lifecycle lock, when
	// the running cycle actually reaches the paused state at a boundary. It
	// is used to durably persist the checkpoint.
	onPaused func(position string)
}

// globalScannerLifecycle is the cluster-singleton cycle controller. Only the
// leader-lock holder drives scans, but every scan goroutine (including remote
// work dispatched locally) consults the same controller.
var globalScannerLifecycle = newScannerLifecycle()

func newScannerLifecycle() *scannerLifecycle {
	l := &scannerLifecycle{
		phase: scannerPhaseIdle,
		wake:  make(chan struct{}),
	}
	return l
}

// bootstrap seeds the history of completed cycles from the persisted cycle
// header. It is called once when the scanner loop starts.
func (l *scannerLifecycle) bootstrap(completed []time.Time) {
	l.mu.Lock()
	l.cyclesCompleted = append(make([]time.Time, 0, len(completed)), completed...)
	l.mu.Unlock()
}

// beginCycle marks the start of an attempt of the given cycle number.
// resuming indicates that an interrupted attempt is continued. If a pause was
// requested while idle, the attempt starts in the pausing state.
func (l *scannerLifecycle) beginCycle(cycle uint64, started time.Time, resuming bool) {
	l.mu.Lock()
	l.cycle = cycle
	l.started = started
	l.resuming = resuming
	l.totalBatches = 0
	l.doneBatches = 0
	l.lastBatch = ""
	l.stopReason = ""
	l.interruptedAt = time.Time{}
	l.position = ""
	l.opsAtStart = globalScannerMetrics.lifetime(scannerMetricScanObject)
	l.wake = make(chan struct{})
	l.pausedCh = make(chan struct{})
	if l.wantPause {
		l.phase = scannerPhasePausing
		l.gate.Store(scannerGatePause)
	} else {
		l.phase = scannerPhaseRunning
		l.gate.Store(scannerGateProceed)
		l.pauseReason = ""
	}
	l.mu.Unlock()
}

// addTotalBatches registers bucket batches planned by one set.
func (l *scannerLifecycle) addTotalBatches(n int) {
	l.mu.Lock()
	l.totalBatches += n
	l.mu.Unlock()
}

// completeBatch marks a single bucket batch finished and remembers it as the
// last reached boundary.
func (l *scannerLifecycle) completeBatch(bucket string) {
	l.mu.Lock()
	l.doneBatches++
	l.lastBatch = bucket
	l.position = bucket
	l.mu.Unlock()
}

// isResumingCycle reports whether a scan of wantCycle should skip buckets that
// already carry a completion marker for this cycle.
func (l *scannerLifecycle) isResumingCycle(wantCycle uint32) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.resuming && l.cycle == uint64(wantCycle) &&
		(l.phase == scannerPhaseRunning || l.phase == scannerPhasePausing || l.phase == scannerPhasePaused)
}

// isPaused reports whether partial state must not be made visible while the
// cycle is parked between boundaries.
func (l *scannerLifecycle) isPaused() bool {
	return l.gate.Load() == scannerGatePause
}

// Pause requests the active cycle to park at the next directory boundary and
// waits until it does. A pause requested while no cycle is active is
// remembered and applied when the next cycle starts.
func (l *scannerLifecycle) Pause(ctx context.Context, reason string) error {
	l.mu.Lock()
	switch l.phase {
	case scannerPhaseIdle, scannerPhaseInterrupted:
		// No active cycle: remember the intent and let the next cycle park
		// at its very first boundary.
		l.wantPause = true
		l.pauseReason = reason
		l.mu.Unlock()
		return nil
	case scannerPhaseRunning:
		l.phase = scannerPhasePausing
		l.pauseReason = reason
		// Re-arm the notification for repeated pause/resume within a cycle.
		l.pausedCh = make(chan struct{})
		l.gate.Store(scannerGatePause)
	case scannerPhasePausing, scannerPhasePaused:
		// Already requested, wait for the boundary below.
	default:
		l.mu.Unlock()
		return errScannerCycleStopped
	}
	ch := l.pausedCh
	wake := l.wake
	l.mu.Unlock()

	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-wake:
		// The attempt was terminated or finished before reaching the
		// boundary.
		if globalScannerLifecycle.phaseSnapshot() == scannerPhaseStopping {
			return errScannerCycleStopped
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// phaseSnapshot returns the current phase without blocking.
func (l *scannerLifecycle) phaseSnapshot() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.phase
}

// Resume releases a paused cycle so it continues from the boundary.
func (l *scannerLifecycle) Resume() error {
	l.mu.Lock()
	switch l.phase {
	case scannerPhaseIdle, scannerPhaseInterrupted:
		// Clear a pending pause requested before the cycle started.
		l.wantPause = false
		l.pauseReason = ""
		l.mu.Unlock()
		return nil
	case scannerPhasePausing, scannerPhasePaused:
	default:
		l.mu.Unlock()
		return errors.New("scanner cycle is not paused")
	}
	l.phase = scannerPhaseRunning
	l.pauseReason = ""
	l.wantPause = false
	old := l.wake
	l.wake = make(chan struct{})
	l.gate.Store(scannerGateProceed)
	l.mu.Unlock()
	xioutil.SafeClose(old)
	return nil
}

// Stop terminates the in-progress cycle. Parked goroutines are released and
// scan boundaries return errScannerCycleStopped. The cycle number is not
// advanced; the checkpoint allows a later attempt to continue the same cycle.
func (l *scannerLifecycle) Stop(reason string) error {
	l.mu.Lock()
	l.wantPause = false
	l.pauseReason = ""
	switch l.phase {
	case scannerPhaseIdle, scannerPhaseInterrupted:
		l.mu.Unlock()
		return nil
	case scannerPhaseRunning, scannerPhasePausing, scannerPhasePaused, scannerPhaseStopping:
		l.phase = scannerPhaseStopping
		l.stopReason = reason
		old := l.wake
		l.wake = make(chan struct{})
		l.gate.Store(scannerGateStop)
		l.mu.Unlock()
		xioutil.SafeClose(old)
		return nil
	default:
		l.mu.Unlock()
		return nil
	}
}

// finishCycle records the outcome of an attempt.
func (l *scannerLifecycle) finishCycle(completed bool, at time.Time, reason, position string) {
	l.mu.Lock()
	old := l.wake
	l.wake = make(chan struct{})
	if l.pausedCh != nil {
		ch := l.pausedCh
		l.pausedCh = nil
		xioutil.SafeClose(ch)
	}
	l.gate.Store(scannerGateProceed)
	l.wantPause = false
	if completed {
		l.phase = scannerPhaseIdle
		l.resuming = false
		l.stopReason = ""
		l.pauseReason = ""
		l.position = ""
		l.interruptedAt = time.Time{}
		l.cyclesCompleted = append(l.cyclesCompleted, at)
		if len(l.cyclesCompleted) > dataUsageUpdateDirCycles {
			l.cyclesCompleted = l.cyclesCompleted[len(l.cyclesCompleted)-dataUsageUpdateDirCycles:]
		}
	} else {
		l.phase = scannerPhaseInterrupted
		l.resuming = true
		if reason != "" {
			l.stopReason = reason
		}
		if position != "" {
			l.position = position
		} else if l.lastBatch != "" {
			l.position = l.lastBatch
		}
		l.interruptedAt = at
	}
	l.mu.Unlock()
	xioutil.SafeClose(old)
}

// interruptionReason returns the reason the current attempt was interrupted.
func (l *scannerLifecycle) interruptionReason() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopReason != "" {
		return l.stopReason
	}
	return l.pauseReason
}

// waitAtBoundary blocks while the cycle is paused. A terminated cycle makes it
// return errScannerCycleStopped; leader loss / shutdown surfaces through ctx.
// The caller invokes this only at directory or bucket boundaries, so parking
// here never cuts through an in-flight directory.
func (l *scannerLifecycle) waitAtBoundary(ctx context.Context, position string) error {
	if l.gate.Load() == scannerGateProceed {
		return nil
	}
	l.mu.Lock()
	for {
		switch l.gate.Load() {
		case scannerGateProceed:
			l.mu.Unlock()
			return nil
		case scannerGateStop:
			l.mu.Unlock()
			return errScannerCycleStopped
		}
		if position != "" {
			l.position = position
		}
		var cb func(string)
		var cbPos string
		if l.phase == scannerPhasePausing {
			l.phase = scannerPhasePaused
			if l.pausedCh != nil {
				ch := l.pausedCh
				l.pausedCh = nil
				xioutil.SafeClose(ch)
			}
			cb = l.onPaused
			cbPos = l.position
		}
		wake := l.wake
		l.mu.Unlock()
		if cb != nil {
			// Persist the checkpoint of the parked cycle.
			cb(cbPos)
		}
		select {
		case <-wake:
		case <-ctx.Done():
			return ctx.Err()
		}
		l.mu.Lock()
	}
}

// waitAtFolderBoundary is called between directories.
func (l *scannerLifecycle) waitAtFolderBoundary(ctx context.Context, folder string) error {
	if l.gate.Load() == scannerGateProceed {
		return nil
	}
	return l.waitAtBoundary(ctx, folder)
}

// waitAtBucketBoundary is called between bucket batches.
func (l *scannerLifecycle) waitAtBucketBoundary(ctx context.Context, bucket string) error {
	if l.gate.Load() == scannerGateProceed {
		return nil
	}
	return l.waitAtBoundary(ctx, bucket)
}

// setOnPaused registers the checkpoint persistence callback for the cycle.
func (l *scannerLifecycle) setOnPaused(fn func(position string)) {
	l.mu.Lock()
	l.onPaused = fn
	l.mu.Unlock()
}

// Status returns a snapshot of the cycle lifecycle.
func (l *scannerLifecycle) Status() ScannerCycleStatus {
	l.mu.Lock()
	st := ScannerCycleStatus{
		Phase:             l.phase,
		Active:            l.phase == scannerPhaseRunning || l.phase == scannerPhasePausing || l.phase == scannerPhasePaused || l.phase == scannerPhaseStopping,
		Cycle:             l.cycle,
		Resuming:          l.resuming,
		Started:           l.started,
		TotalBatches:      l.totalBatches,
		CompletedBatches:  l.doneBatches,
		RemainingBatches:  l.totalBatches - l.doneBatches,
		Position:          l.position,
		InterruptedAt:     l.interruptedAt,
		CyclesCompletedAt: append(make([]time.Time, 0, len(l.cyclesCompleted)), l.cyclesCompleted...),
	}
	switch {
	case l.stopReason != "":
		st.Reason = l.stopReason
	case l.pauseReason != "":
		st.Reason = l.pauseReason
	}
	if n := len(l.cyclesCompleted); n > 0 {
		st.LastCompleted = l.cyclesCompleted[n-1]
	}
	if st.RemainingBatches < 0 {
		st.RemainingBatches = 0
	}
	opsBase := l.opsAtStart
	l.mu.Unlock()

	if st.Active {
		if objs := globalScannerMetrics.lifetime(scannerMetricScanObject); objs >= opsBase {
			st.ObjectsProcessed = objs - opsBase
		}
		st.ActivePaths = globalScannerMetrics.getCurrentPaths()
	}
	return st
}

// PauseScannerCycle requests the in-progress cycle to pause at the next
// directory boundary and waits until it is parked. A pause requested while no
// cycle is active is remembered and applied to the next cycle.
func PauseScannerCycle(ctx context.Context, reason string) error {
	return globalScannerLifecycle.Pause(ctx, reason)
}

// ResumeScannerCycle resumes a paused cycle from its boundary.
func ResumeScannerCycle() error {
	return globalScannerLifecycle.Resume()
}

// StopScannerCycle terminates the in-progress cycle. The cycle number is not
// advanced; its checkpoint is kept and a later attempt resumes the same cycle.
func StopScannerCycle(reason string) error {
	return globalScannerLifecycle.Stop(reason)
}

// ScannerCycleStatus returns the current lifecycle status: phase, processed
// objects, remaining batches and the last completion time, among others.
func GetScannerCycleStatus() ScannerCycleStatus {
	return globalScannerLifecycle.Status()
}
