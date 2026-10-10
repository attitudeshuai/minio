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
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/load"
	"github.com/shirou/gopsutil/v3/mem"
)

const (
	// batchResourceSampleInterval is how often resource usage is sampled.
	batchResourceSampleInterval = 10 * time.Second
	// batchWatermarkHysteresis is the percentage points below the breach
	// threshold usage must drop before admissions resume.
	batchWatermarkHysteresis = 5
)

// batchResourceSample is one observation of the three gated dimensions.
// A false okN flag means the dimension could not be sampled on this
// platform/host; its gate state is then left untouched.
type batchResourceSample struct {
	cpu, mem, drive       int
	okCPU, okMem, okDrive bool
}

// batchResourceMonitor keeps the latest resource observations and computes
// the batch admission gate with hysteresis.
type batchResourceMonitor struct {
	ctx     context.Context
	sampler func(context.Context) (batchResourceSample, error)
	wake    func()

	mu       sync.Mutex
	thrCPU   int
	thrMem   int
	thrDrive int
	state    batchWatermarkView
}

func newBatchResourceMonitor(ctx context.Context, o ObjectLayer, cpuThr, memThr, driveThr int, wake func()) *batchResourceMonitor {
	m := &batchResourceMonitor{
		ctx: ctx,
		sampler: func(ctx context.Context) (batchResourceSample, error) {
			return sampleHostResources(ctx, o)
		},
		wake: wake,
	}
	m.setThresholds(cpuThr, memThr, driveThr)
	return m
}

func (m *batchResourceMonitor) setThresholds(cpu, mem, drive int) {
	m.mu.Lock()
	wasGated := m.gatedLocked()
	m.thrCPU, m.thrMem, m.thrDrive = cpu, mem, drive
	// Disabling a dimension immediately releases its gate.
	if cpu == 0 {
		m.state.CPU = batchWatermarkStat{Threshold: 0}
	} else {
		m.state.CPU.Threshold = cpu
	}
	if mem == 0 {
		m.state.Mem = batchWatermarkStat{Threshold: 0}
	} else {
		m.state.Mem.Threshold = mem
	}
	if drive == 0 {
		m.state.Drive = batchWatermarkStat{Threshold: 0}
	} else {
		m.state.Drive.Threshold = drive
	}
	nowGated := m.gatedLocked()
	m.mu.Unlock()

	if wasGated != nowGated && m.wake != nil {
		m.wake()
	}
}

// UpdateThresholds applies hot-reloaded watermark settings.
func (m *batchResourceMonitor) UpdateThresholds(cpu, mem, drive int) {
	m.setThresholds(cpu, mem, drive)
}

// updateThresholds implements batchResourceGate.
func (m *batchResourceMonitor) updateThresholds(cpu, mem, drive int) {
	m.setThresholds(cpu, mem, drive)
}

func (m *batchResourceMonitor) gated() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gatedLocked()
}

func (m *batchResourceMonitor) gatedLocked() bool {
	return m.state.CPU.Gated || m.state.Mem.Gated || m.state.Drive.Gated
}

func (m *batchResourceMonitor) view() batchWatermarkView {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// apply folds one sample into the gate state. It returns whether the
// overall gate state changed (used to wake the scheduler).
func (m *batchResourceMonitor) apply(s batchResourceSample, now time.Time) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	before := m.gatedLocked()
	m.state.CPU = applyWatermark(m.state.CPU, m.thrCPU, s.cpu, s.okCPU, now)
	m.state.Mem = applyWatermark(m.state.Mem, m.thrMem, s.mem, s.okMem, now)
	m.state.Drive = applyWatermark(m.state.Drive, m.thrDrive, s.drive, s.okDrive, now)
	after := m.gatedLocked()
	return before != after
}

// applyWatermark implements the per-dimension transition:
// gated when observed > threshold, released only when observed <= threshold
// minus hysteresis. Disabled dimensions never gate; failed samples leave
// the previous state untouched.
func applyWatermark(prev batchWatermarkStat, threshold, observed int, ok bool, now time.Time) batchWatermarkStat {
	prev.Threshold = threshold
	if threshold == 0 {
		return batchWatermarkStat{Threshold: 0}
	}
	if !ok {
		// Keep prior gated state; mark the sample as unavailable.
		prev.Available = false
		return prev
	}
	prev.Available = true
	prev.Observed = observed
	prev.SampledAt = now
	if prev.Gated {
		prev.Gated = observed > threshold-batchWatermarkHysteresis
	} else {
		prev.Gated = observed > threshold
	}
	return prev
}

// run samples on a ticker until the monitor context ends. Gate flips wake
// the scheduler so admissions react promptly.
func (m *batchResourceMonitor) run() {
	// Sample once at startup so the initial state is current.
	m.sampleOnce()
	ticker := time.NewTicker(batchResourceSampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.sampleOnce()
		}
	}
}

func (m *batchResourceMonitor) sampleOnce() {
	s, err := m.sampler(m.ctx)
	if err != nil {
		// Sampling sources may not exist on some platforms; this is
		// expected and must not spam logs.
		return
	}
	if m.apply(s, time.Now()) && m.wake != nil {
		m.wake()
	}
}

// sampleHostResources is the production sampler: load1 per CPU core,
// virtual memory used percentage and aggregate online drive usage.
func sampleHostResources(ctx context.Context, api ObjectLayer) (batchResourceSample, error) {
	var s batchResourceSample

	if l, err := load.Avg(); err == nil && runtime.NumCPU() > 0 {
		pct := int(l.Load1*100/float64(runtime.NumCPU()) + 0.5)
		if pct > 100 {
			pct = 100
		}
		s.cpu, s.okCPU = pct, true
	}
	if vm, err := mem.VirtualMemory(); err == nil {
		s.mem, s.okMem = int(vm.UsedPercent+0.5), true
	}
	if api != nil {
		info := api.LocalStorageInfo(ctx, false)
		var total, used uint64
		for _, d := range info.Disks {
			if d.TotalSpace <= 0 {
				continue
			}
			total += d.TotalSpace
			used += d.UsedSpace
		}
		if total > 0 {
			s.drive, s.okDrive = int(used*100/total), true
		}
	}
	return s, nil
}
