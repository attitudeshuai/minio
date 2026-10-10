// Copyright (c) 2024 MinIO, Inc.
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

package admission

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestConcurrentAccess hammers all manager entry points concurrently to catch
// lock-ordering deadlocks and panics (the -race detector additionally covers
// data races where a C toolchain is available).
func TestConcurrentAccess(t *testing.T) {
	mgr := NewManager(0, WithNodeCount(func() int { return 4 }))
	rules := Rules{{Scope: ScopeBucket, Name: "b", Total: &ClassQuota{Rate: 10, Burst: 10}}}
	if err := mgr.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				mgr.Admit(ctx, ScopeBucket, "b", ClassWrite)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			_ = mgr.ApplyRules(rules)
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			mgr.SnapshotStats()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			mgr.DrainWindowUsages()
		}
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			mgr.SetSlotRate(ScopeBucket, "b", SlotTotal, 2.5)
		}
	}()
	wg.Wait()
}
