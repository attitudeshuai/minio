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
	"testing"
	"time"
)

func TestApplyWatermarkTransition(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Disabled dimension never gates regardless of observed value.
	if got := applyWatermark(batchWatermarkStat{}, 0, 99, true, now); got.Gated {
		t.Fatalf("disabled dimension must not gate: %+v", got)
	}

	// Below threshold stays open.
	st := applyWatermark(batchWatermarkStat{}, 80, 70, true, now)
	if st.Gated || !st.Available || st.Observed != 70 {
		t.Fatalf("70/80 should be open and available: %+v", st)
	}

	// Crossing threshold gates.
	st = applyWatermark(st, 80, 85, true, now.Add(time.Second))
	if !st.Gated {
		t.Fatalf("85/80 must gate: %+v", st)
	}

	// Inside the hysteresis band it stays gated (76-79 for threshold 80).
	st = applyWatermark(st, 80, 76, true, now.Add(2*time.Second))
	if !st.Gated {
		t.Fatalf("76 should stay gated within hysteresis band: %+v", st)
	}
	st = applyWatermark(st, 80, 79, true, now.Add(3*time.Second))
	if !st.Gated {
		t.Fatalf("79 should stay gated within hysteresis band: %+v", st)
	}

	// Dropping to the recovery line releases the gate.
	st = applyWatermark(st, 80, 75, true, now.Add(4*time.Second))
	if st.Gated {
		t.Fatalf("75 must release the gate: %+v", st)
	}

	// A failed sample preserves the previous state but marks unavailability.
	st = applyWatermark(st, 80, 0, false, now.Add(5*time.Second))
	if st.Gated || st.Available {
		t.Fatalf("failed sample must preserve open state and mark unavailable: %+v", st)
	}
	gatedPrev := applyWatermark(batchWatermarkStat{Gated: true}, 80, 85, true, now)
	failed := applyWatermark(gatedPrev, 80, 0, false, now)
	if !failed.Gated || failed.Available {
		t.Fatalf("failed sample while gated must keep gate closed: %+v", failed)
	}
}

func TestBatchResourceMonitorGateAndWake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wakes := 0
	m := &batchResourceMonitor{
		ctx:  ctx,
		wake: func() { wakes++ },
	}
	m.setThresholds(80, 0, 0)
	now := time.Now()

	// 70 -> 85 gates and triggers one wake.
	if m.apply(batchResourceSample{cpu: 70, okCPU: true}, now) {
		t.Fatal("70 should not flip the gate from open")
	}
	if !m.apply(batchResourceSample{cpu: 85, okCPU: true}, now) || !m.gated() {
		t.Fatal("85 must gate and report a state flip")
	}

	// 76 stays gated, no wake.
	if m.apply(batchResourceSample{cpu: 76, okCPU: true}, now) {
		t.Fatal("hysteresis band should not flip state")
	}
	// 75 opens the gate and wakes the scheduler.
	if !m.apply(batchResourceSample{cpu: 75, okCPU: true}, now) || m.gated() {
		t.Fatal("75 must open the gate and report a flip")
	}

	// Other dimensions disabled do not influence the aggregate gate.
	m.apply(batchResourceSample{mem: 99, drive: 99, okMem: true, okDrive: true}, now)
	if m.gated() {
		t.Fatal("disabled dimensions must be ignored")
	}

	// Unavailable CPU (e.g. unsupported platform) keeps gate open and
	// must not panic or flip.
	if m.apply(batchResourceSample{okCPU: false}, now) {
		t.Fatal("unavailable sample must not flip gate")
	}

	// Disabling the active threshold releases the gate immediately.
	m.apply(batchResourceSample{cpu: 90, okCPU: true}, now)
	if !m.gated() {
		t.Fatal("expected gated state before disabling")
	}
	m.setThresholds(0, 0, 0)
	if m.gated() {
		t.Fatal("disabling thresholds must release the gate")
	}
	if wakes == 0 {
		t.Fatal("gate transitions must wake the scheduler")
	}

	v := m.view()
	if v.CPU.Threshold != 0 || v.Mem.Threshold != 0 || v.Drive.Threshold != 0 {
		t.Fatalf("disabled view must carry zero thresholds: %+v", v)
	}
}
