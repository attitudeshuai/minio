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

package admission

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func totalRule(rate, burst string) Rules {
	return Rules{{
		Scope: ScopeBucket,
		Name:  "b",
		Total: &ClassQuota{Rate: float64(mustAtoi(rate)), Burst: mustAtoi(burst)},
	}}
}

func mustAtoi(s string) int {
	n := 0
	for _, c := range s {
		n = n*10 + int(c-'0')
	}
	return n
}

// simNode is one node in the deterministic cluster simulator.
type simNode struct {
	id  string
	mgr *Manager
	agg *Aggregator
}

// simCluster models the two-phase barrier exchange: usage snaps are merged
// into every node, the lexicographically smallest node acts as leader,
// computes an allocation epoch and pushes it to all followers, which apply
// the same vector in the same round.
type simCluster struct {
	t        *testing.T
	nodes    []*simNode
	clock    *fakeClock
	interval time.Duration
}

func newSimCluster(t *testing.T, n int, rules Rules) *simCluster {
	t.Helper()
	c := newFakeClock()
	cl := &simCluster{
		t:        t,
		clock:    c,
		interval: time.Second,
	}
	for i := 0; i < n; i++ {
		mgr := NewManager(0,
			WithClock(c.now),
			WithNodeCount(func() int { return n }),
		)
		if err := mgr.ApplyRules(rules); err != nil {
			t.Fatal(err)
		}
		id := fmt.Sprintf("node-%d", i)
		cl.nodes = append(cl.nodes, &simNode{
			id:  id,
			mgr: mgr,
			agg: NewAggregator(mgr, id, cl.interval, c.now),
		})
	}
	return cl
}

// exchange runs one barrier round. staleSnaps lists nodes whose usage snaps
// are excluded (they are unreachable this round); noAlloc nodes do not receive
// the leader vector (allocation push lost) and fall back to R/N baseline.
func (cl *simCluster) exchange(staleSnaps, noAlloc map[string]bool) {
	snaps := make([]PeerSnap, len(cl.nodes))
	for i, nd := range cl.nodes {
		snaps[i] = nd.agg.BeginWindow()
	}
	for i, nd := range cl.nodes {
		for j, s := range snaps {
			if i == j || staleSnaps[s.NodeID] {
				continue
			}
			if err := nd.agg.Merge(s); err != nil {
				cl.t.Fatal(err)
			}
		}
	}

	// node-0 is always the leader in these tests.
	leader := cl.nodes[0]
	alloc := leader.agg.BuildAllocation()
	leader.agg.ApplyEpoch(alloc.Epoch)

	for j := 1; j < len(cl.nodes); j++ {
		nd := cl.nodes[j]
		if staleSnaps[nd.id] || noAlloc[nd.id] {
			// Unreachable / lost push: follower runs with whatever it has.
			continue
		}
		if err := nd.agg.MergeAllocation(alloc); err != nil {
			cl.t.Fatal(err)
		}
	}
	for j := 1; j < len(cl.nodes); j++ {
		if !staleSnaps[cl.nodes[j].id] {
			cl.nodes[j].agg.FollowerReallocate()
		}
	}
}

func (cl *simCluster) exchangeSimple() {
	cl.exchange(map[string]bool{}, map[string]bool{})
}

func (cl *simCluster) slotEff(nd *simNode, slot QuotaSlot) float64 {
	r, ok := nd.mgr.GetSlotEffectiveRate(ScopeBucket, "b", slot)
	if !ok {
		cl.t.Fatalf("slot %v missing", slot)
	}
	return r
}

func (cl *simCluster) sumEff(slot QuotaSlot) float64 {
	sum := 0.0
	for _, nd := range cl.nodes {
		sum += cl.slotEff(nd, slot)
	}
	return sum
}

// fireSecond simulates 1 second in 100ms substeps and fires exactly
// arrivals[i] requests at node i, spread evenly; it returns admissions.
func (cl *simCluster) fireSecond(arrivals map[int]int) int {
	const substeps = 10
	stepDur := cl.interval / substeps
	admitted := 0
	ctx := context.Background()
	fired := make(map[int]int, len(arrivals))
	for step := 0; step < substeps; step++ {
		cl.clock.advance(stepDur)
		for idx, total := range arrivals {
			target := total * (step + 1) / substeps
			thisStep := target - fired[idx]
			fired[idx] = target
			for k := 0; k < thisStep; k++ {
				d := cl.nodes[idx].mgr.Admit(ctx, ScopeBucket, "b", ClassWrite)
				if d.Allowed {
					admitted++
				}
			}
		}
	}
	return admitted
}

func TestSingleNodeLeaderAllocation(t *testing.T) {
	cl := newSimCluster(t, 1, totalRule("10", "5"))
	// First round, no traffic observed yet: a single idle node conserves to 0
	// until demand is reported; the next round grants the full R.
	cl.exchangeSimple()
	if r := cl.slotEff(cl.nodes[0], SlotTotal); r != 0 {
		t.Fatalf("idle single node: eff %v, want 0", r)
	}

	// Generate demand: exhaust the burst (5) and record a rejection.
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if d := cl.nodes[0].mgr.Admit(ctx, ScopeBucket, "b", ClassWrite); !d.Allowed {
			t.Fatal("burst token")
		}
	}
	if d := cl.nodes[0].mgr.Admit(ctx, ScopeBucket, "b", ClassWrite); d.Allowed {
		t.Fatal("expected rejection to mark demand")
	}
	cl.exchangeSimple()
	if r := cl.slotEff(cl.nodes[0], SlotTotal); r != 10 {
		t.Fatalf("demanding single node eff=%v, want R=10", r)
	}
}

func TestUniformDistributionLeaderVector(t *testing.T) {
	n := 4
	cl := newSimCluster(t, n, totalRule("100", "40"))
	cl.exchangeSimple() // bootstrap: epoch 1, followers on baseline

	arrivals := map[int]int{}
	for i := 0; i < n; i++ {
		arrivals[i] = 25
	}

	// Window 1: traffic, burst covers initial spread, demand is recorded.
	cl.fireSecond(arrivals)
	cl.exchangeSimple()
	// Epoch 2: everyone demanded and nobody has idle share -> r0 each.
	for i, nd := range cl.nodes {
		if got := cl.slotEff(nd, SlotTotal); got != 25 {
			t.Fatalf("node %d eff=%v want 25", i, got)
		}
	}
	if sum := cl.sumEff(SlotTotal); sum > 100+1e-9 {
		t.Fatalf("cluster sum eff=%v > R", sum)
	}

	// Steady state for four windows: ~R per second, never above R+B.
	cumulative := float64(40) // initial cluster burst
	for s := 0; s < 4; s++ {
		got := cl.fireSecond(arrivals)
		cl.exchangeSimple()
		cumulative += float64(got)
		bound := 100*float64(s+2) + 40
		if cumulative > bound+1 {
			t.Fatalf("sec %d cumulative %v exceeds %v", s, cumulative, bound)
		}
	}
}

func TestSkewedTrafficConvergesUnderLeader(t *testing.T) {
	const N = 4
	cl := newSimCluster(t, N, totalRule("100", "40"))
	cl.exchangeSimple()

	// Window 1: all traffic at node0; bucket burst (10) serves a few, the
	// rejection records demand.
	got := cl.fireSecond(map[int]int{0: 100})
	if got > 10+2 {
		t.Fatalf("window 1 admitted %v, want <= burst share 10", got)
	}

	// Epoch 2: node0 is the sole demander and others are synced-idle: the
	// leader vector gives node0 the full R and everyone else zero.
	cl.exchangeSimple()
	if r := cl.slotEff(cl.nodes[0], SlotTotal); r != 100 {
		t.Fatalf("node0 eff %v want 100", r)
	}
	for i := 1; i < N; i++ {
		if r := cl.slotEff(cl.nodes[i], SlotTotal); r != 0 {
			t.Fatalf("idle node %d eff %v want 0", i, r)
		}
	}
	if sum := cl.sumEff(SlotTotal); sum > 100+1e-9 {
		t.Fatalf("sum eff %v > R", sum)
	}

	// Window 2-4: node0 sustains ~R per second, bounded by R*t+B.
	cumulative := float64(got)
	for s := 0; s < 3; s++ {
		g := cl.fireSecond(map[int]int{0: 100})
		cl.exchangeSimple()
		cumulative += float64(g)
		bound := 100*float64(s+2) + 40
		if cumulative > bound+1 {
			t.Fatalf("post-convergence sec %d cumulative %v > %v", s, cumulative, bound)
		}
		if g < 95 {
			t.Fatalf("post-convergence sec %d admitted %v < 95", s, g)
		}
	}
}

func TestStaleNodeReservedUnderLeader(t *testing.T) {
	cl := newSimCluster(t, 4, totalRule("100", "40"))
	cl.exchangeSimple()
	cl.fireSecond(map[int]int{0: 100})

	// node-2 is unreachable this round: its acks are old and the leader
	// reserves its full baseline (its share is neither borrowed nor lent).
	cl.exchange(map[string]bool{"node-2": true}, nil)
	if r := cl.slotEff(cl.nodes[0], SlotTotal); r > 75+1e-9 {
		t.Fatalf("hot node with one stale peer eff=%v want <= 75", r)
	}
	if sum := cl.sumEff(SlotTotal); sum > 100+1e-9 {
		t.Fatalf("sum eff=%v > R (unreachable node excluded entirely)", sum)
	}
}

// TestMissedAllocationIsConservative proves a dropped leader vector degrades
// the affected follower to baseline; the cluster overshoot for that one
// window is bounded by that node's baseline (R + R/N), never N-fold.
func TestMissedAllocationIsConservative(t *testing.T) {
	cl := newSimCluster(t, 4, totalRule("100", "40"))
	cl.exchangeSimple()
	cl.fireSecond(map[int]int{0: 100})

	// Leader lends all idle shares to node0, but the vector is lost on
	// node-3, which applies its baseline instead.
	cl.exchange(map[string]bool{}, map[string]bool{"node-3": true})
	if r := cl.slotEff(cl.nodes[0], SlotTotal); r != 100 {
		t.Fatalf("node0 eff %v want 100", r)
	}
	if r := cl.slotEff(cl.nodes[3], SlotTotal); r != 25 {
		t.Fatalf("node3 baseline eff %v want 25", r)
	}
	sum := cl.sumEff(SlotTotal)
	if sum > 100+25+1e-9 {
		t.Fatalf("one dropped vector: sum %v > R+R/N=125", sum)
	}

	// Delivery resumes: back to exactly R.
	cl.exchangeSimple()
	if sum := cl.sumEff(SlotTotal); sum > 100+1e-9 {
		t.Fatalf("recovered sum eff %v > R", sum)
	}
}

// TestSnapshotOlderAllocationDoesNotDowngrade covers cross-stream reordering:
// the phase-0 usage snapshot (embedding the previous epoch table) may arrive
// after the phase-1 allocation push. The fresher allocation must survive.
func TestSnapshotOlderAllocationDoesNotDowngrade(t *testing.T) {
	mgr := NewManager(0, WithNodeCount(func() int { return 2 }))
	if err := mgr.ApplyRules(totalRule("10", "4")); err != nil {
		t.Fatal(err)
	}
	a := NewAggregator(mgr, "self", time.Second, nil)

	// Fresh leader peer.
	if err := a.Merge(PeerSnap{NodeID: "lead", Seq: 1, LeaderID: "lead"}); err != nil {
		t.Fatal(err)
	}
	// Epoch 3 push assigns self a rate of 7 (above the 5 baseline).
	if err := a.MergeAllocation(Allocation{
		LeaderID: "lead",
		Epoch:    3,
		Items: []ItemRates{{
			Scope: ScopeBucket, Name: "b", Slot: SlotTotal,
			Rates: map[string]float64{"self": 7, "lead": 3},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	// A newer-sequence snapshot carrying the older epoch-2 table arrives late.
	if err := a.Merge(PeerSnap{
		NodeID: "lead", Seq: 2, LeaderID: "lead", AllocEpoch: 2,
		Allocations: []ItemRates{{
			Scope: ScopeBucket, Name: "b", Slot: SlotTotal,
			Rates: map[string]float64{"self": 9, "lead": 1},
		}},
	}); err != nil {
		t.Fatal(err)
	}

	a.FollowerReallocate()
	if r, _ := mgr.GetSlotEffectiveRate(ScopeBucket, "b", SlotTotal); r != 7 {
		t.Fatalf("stale embedded table downgraded applied vector: rate=%v want 7", r)
	}
}

func TestSnapshotSeqOrdering(t *testing.T) {
	cl := newSimCluster(t, 2, totalRule("10", "4"))
	s1 := PeerSnap{NodeID: "other", Seq: 1}
	if err := cl.nodes[0].agg.Merge(s1); err != nil {
		t.Fatal(err)
	}
	s0 := PeerSnap{NodeID: "other", Seq: 0}
	if err := cl.nodes[0].agg.Merge(s0); err != ErrStaleSnapshot {
		t.Fatalf("older seq dropped, got %v", err)
	}
	s2 := PeerSnap{NodeID: "other", Seq: 2}
	if err := cl.nodes[0].agg.Merge(s2); err != nil {
		t.Fatal(err)
	}
	if cl.nodes[0].agg.AppliedEpoch() != 0 {
		t.Fatal("follower without vector stays at epoch 0")
	}
}
