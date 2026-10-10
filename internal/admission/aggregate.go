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
	"errors"
	"sort"
	"sync"
	"time"
)

// Correctness model — leader-authoritative allocation
//
// Every rule slot with cluster quota (Rate R, Burst B) is represented on
// each of the N nodes by a local token bucket with baseline refill R/N and
// burst B/N. Nodes exchange per-window usage snapshots once per exchange
// interval.
//
// Exactly one node per cluster view — the LEADER, the lexicographically
// smallest node ID among the local node and peers fresh within two exchange
// intervals — computes the allocation vector for the next window and
// publishes it in its own snapshot. Every other node applies only the rate
// the current leader assigned to it in the leader's freshest table, falling
// back to its R/N baseline when the leader table is missing or stale.
//
// Because every applied rate vector originates from a SINGLE computation,
// the sum of refill rates is bounded by R on every window regardless of tick
// skew or delayed/partial snapshot delivery, and the per-node burst shares
// sum to B. Cluster-wide admissions over any interval t are therefore
// bounded by R*t + B; spreading requests across nodes can never relax the
// quota to N*R or N*B.
//
// During a network partition each side independently elects its own leader
// and conservatively applies up to R per connected side; on heal, the unique
// smallest-ID leader is immediately re-established.
//
// In the leader computation: a peer whose latest snapshot is older than two
// intervals, or that has never been seen, is conservatively reserved its
// full R/N baseline (its share is not redistributed). A missing item entry
// in a fresh snapshot means the peer was fully idle for that item.

// ItemKey identifies one rule slot.
type ItemKey struct {
	Scope Scope     `json:"scope"`
	Name  string    `json:"name"`
	Slot  QuotaSlot `json:"slot"`
}

// ItemSnap is one node's activity for one rule slot during the last window.
type ItemSnap struct {
	Scope Scope     `json:"scope"`
	Name  string    `json:"name"`
	Slot  QuotaSlot `json:"slot"`

	// Consumed is the number of tokens admitted during the window.
	Consumed float64 `json:"consumed"`
	// Wanted is true when the node queued or was rejected for this slot,
	// i.e. it has demand beyond its current refill.
	Wanted bool `json:"wanted"`
}

// ItemRates is one slot's per-node refill rate vector computed by the leader.
type ItemRates struct {
	Scope Scope              `json:"scope"`
	Name  string             `json:"name"`
	Slot  QuotaSlot          `json:"slot"`
	Rates map[string]float64 `json:"rates"`
}

// PeerSnap is exchanged between nodes once per exchange interval.
type PeerSnap struct {
	NodeID string `json:"node_id"`
	Seq    uint64 `json:"seq"`
	// Epoch is the allocation epoch the sender is running during the
	// current window (0 = unsynchronized baseline). It also acts as an ack
	// of the leader table the sender applied.
	Epoch uint64 `json:"epoch"`
	// AllocEpoch is the epoch of Allocations (leader snaps only).
	AllocEpoch uint64     `json:"alloc_epoch,omitempty"`
	Items      []ItemSnap `json:"items,omitempty"`
	LeaderID   string     `json:"leader_id,omitempty"`
	// Allocations is populated by the leader with the authoritative,
	// cluster-wide per-node rate vector.
	Allocations []ItemRates `json:"allocations,omitempty"`
}

type peerState struct {
	seq         uint64
	ackEpoch    uint64
	receivedAt  time.Time
	items       map[ItemKey]ItemSnap
	leaderID    string
	allocEpoch  uint64
	allocations map[ItemKey]map[string]float64
}

// Aggregator performs cluster-wide usage aggregation and rate allocation.
type Aggregator struct {
	mgr      *Manager
	nodeID   string
	nodes    func() int
	now      func() time.Time
	interval time.Duration

	mu         sync.Mutex
	seq        uint64
	epoch      uint64 // leader epoch of the vector applied locally
	peers      map[string]peerState
	localItems map[ItemKey]ItemSnap // last window produced by BeginWindow

	// lastEpoch/lastTable describe the vector this node published/applied
	// while leader.
	lastEpoch uint64
	lastTable map[ItemKey]map[string]float64
}

// NewAggregator creates an Aggregator for the local manager.
func NewAggregator(mgr *Manager, nodeID string, interval time.Duration, now func() time.Time) *Aggregator {
	if now == nil {
		now = time.Now
	}
	return &Aggregator{
		mgr:      mgr,
		nodeID:   nodeID,
		nodes:    mgr.nodeCount,
		now:      now,
		interval: interval,
		peers:    make(map[string]peerState),
	}
}

// ErrStaleSnapshot is returned when a snapshot carries an already-seen
// (older or equal) sequence number and is therefore ignored.
var ErrStaleSnapshot = errors.New("admission: stale peer snapshot")

// Merge stores a peer snapshot. Out-of-order/old snapshots are dropped.
func (a *Aggregator) Merge(snap PeerSnap) error {
	if snap.NodeID == "" || snap.NodeID == a.nodeID {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	old, ok := a.peers[snap.NodeID]
	if ok && snap.Seq <= old.seq {
		return ErrStaleSnapshot
	}
	items := make(map[ItemKey]ItemSnap, len(snap.Items))
	for _, it := range snap.Items {
		items[ItemKey{Scope: it.Scope, Name: it.Name, Slot: it.Slot}] = it
	}
	var allocs map[ItemKey]map[string]float64
	if len(snap.Allocations) > 0 {
		allocs = make(map[ItemKey]map[string]float64, len(snap.Allocations))
		for _, ar := range snap.Allocations {
			cp := make(map[string]float64, len(ar.Rates))
			for k, v := range ar.Rates {
				cp[k] = v
			}
			allocs[ItemKey{Scope: ar.Scope, Name: ar.Name, Slot: ar.Slot}] = cp
		}
	}
	next := peerState{
		seq:         snap.Seq,
		ackEpoch:    snap.Epoch,
		receivedAt:  a.now(),
		items:       items,
		leaderID:    snap.LeaderID,
		allocEpoch:  snap.AllocEpoch,
		allocations: allocs,
	}
	// Usage snapshots and out-of-band allocation pushes travel over separate
	// grid streams: a phase-0 snapshot embedding the previous epoch's table
	// can arrive after the phase-1 push of the new epoch. Never let an older
	// allocation replace a newer one already stored for this peer.
	if ok && snap.AllocEpoch < old.allocEpoch {
		next.allocEpoch = old.allocEpoch
		next.allocations = old.allocations
	}
	a.peers[snap.NodeID] = next
	return nil
}

// AppliedEpoch returns the epoch of the allocation vector currently applied.
func (a *Aggregator) AppliedEpoch() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.epoch
}

// BeginWindow drains local window counters and produces the local snapshot
// to push to peers. When no rule is active it still returns an (empty)
// heartbeat snapshot so peers count this node as fresh. The leader's last
// allocation vector is attached.
func (a *Aggregator) BeginWindow() PeerSnap {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.seq++
	seq := a.seq

	now := a.now()
	leader := a.leaderLocked(now)
	isLeader := leader == a.nodeID

	out := PeerSnap{NodeID: a.nodeID, Seq: seq, LeaderID: leader, Epoch: a.epoch}
	if !a.mgr.Enabled() {
		a.localItems = nil
		if isLeader && a.lastTable != nil {
			out.LeaderID = a.nodeID
			out.AllocEpoch = a.lastEpoch
			out.Allocations = exportTable(a.lastTable)
		}
		return out
	}

	usages := a.mgr.DrainWindowUsages()
	items := make([]ItemSnap, 0, len(usages))
	local := make(map[ItemKey]ItemSnap, len(usages))
	windowSecs := a.interval.Seconds()
	if windowSecs <= 0 {
		windowSecs = 1
	}
	for _, u := range usages {
		// Demand is explicit (queue/reject) or saturation at >=90% of the
		// currently applied refill.
		saturated := u.Consumed/windowSecs >= 0.9*u.EffRate
		it := ItemSnap{
			Scope:    u.Scope,
			Name:     u.Name,
			Slot:     u.Slot,
			Consumed: u.Consumed,
			Wanted:   u.Wanted > 0 || saturated,
		}
		items = append(items, it)
		local[ItemKey{Scope: u.Scope, Name: u.Name, Slot: u.Slot}] = it
	}
	a.localItems = local
	out.Items = items
	if isLeader && a.lastTable != nil {
		out.LeaderID = a.nodeID
		out.AllocEpoch = a.lastEpoch
		out.Allocations = exportTable(a.lastTable)
	}
	return out
}

func exportTable(t map[ItemKey]map[string]float64) []ItemRates {
	if len(t) == 0 {
		return nil
	}
	out := make([]ItemRates, 0, len(t))
	for k, rates := range t {
		out = append(out, ItemRates{
			Scope: k.Scope, Name: k.Name, Slot: k.Slot, Rates: rates,
		})
	}
	return out
}

// leaderLocked returns the leader for the caller's current view: the smallest
// node ID among self and peers fresh within two intervals.
func (a *Aggregator) leaderLocked(now time.Time) string {
	leader := a.nodeID
	for id, p := range a.peers {
		if now.Sub(p.receivedAt) <= 2*a.interval && id < leader {
			leader = id
		}
	}
	return leader
}

// LeaderID returns the current leader as seen by this node.
func (a *Aggregator) LeaderID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.leaderLocked(a.now())
}

// IsLeader reports whether this node is currently the allocation leader.
func (a *Aggregator) IsLeader() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.leaderLocked(a.now()) == a.nodeID
}

// Enabled reports whether the underlying manager has active rules.
func (a *Aggregator) Enabled() bool { return a.mgr.Enabled() }

// Allocation is the leader-to-followers push of the authoritative rate
// vector for one epoch, delivered out-of-band right after the usage barrier.
type Allocation struct {
	LeaderID string      `json:"leader_id"`
	Epoch    uint64      `json:"epoch"`
	Items    []ItemRates `json:"items"`
}

// MutationOp identifies an admin rule mutation.
type MutationOp string

const (
	MutationUpsert MutationOp = "upsert"
	MutationDelete MutationOp = "delete"
)

// Mutation is a follower->leader request to mutate one rule. Only the leader
// writes the shared rules file, which serializes all rule updates.
type Mutation struct {
	Op   MutationOp `json:"op"`
	Rule Rule       `json:"rule"`
}

// BuildAllocation computes the next authoritative vector from the usage
// already merged into this aggregator, but does NOT apply it yet: it is
// pushed to followers first and applied cluster-wide at the barrier via
// ApplyEpoch. Returns the vector for the out-of-band push.
func (a *Aggregator) BuildAllocation() Allocation {
	if !a.mgr.Enabled() {
		return Allocation{LeaderID: a.nodeID}
	}
	now := a.now()
	a.mu.Lock()
	epoch := a.lastEpoch + 1
	table := a.computeVectorLocked(now, epoch)
	a.lastEpoch = epoch
	a.lastTable = table
	items := exportTable(table)
	a.mu.Unlock()

	return Allocation{LeaderID: a.nodeID, Epoch: epoch, Items: items}
}

// ApplyEpoch applies the leader vector of the given epoch (the one returned
// by BuildAllocation) to local slots.
func (a *Aggregator) ApplyEpoch(epoch uint64) {
	if !a.mgr.Enabled() {
		a.ResetBaselines()
		return
	}
	a.mu.Lock()
	table := a.lastTable
	if epoch != a.lastEpoch {
		table = nil
	}
	a.epoch = a.lastEpoch
	a.mu.Unlock()
	a.applyVector(table)
}

// LeaderReallocate is the single-node/no-barrier variant: compute and apply
// immediately.
func (a *Aggregator) LeaderReallocate() Allocation {
	al := a.BuildAllocation()
	a.ApplyEpoch(al.Epoch)
	return al
}

// FollowerReallocate applies the freshest allocation received from the
// current leader, or the R/N baseline when it is missing or stale.
func (a *Aggregator) FollowerReallocate() {
	if !a.mgr.Enabled() {
		a.ResetBaselines()
		return
	}
	now := a.now()
	a.mu.Lock()
	leader := a.leaderLocked(now)
	table, allocEpoch := a.leaderTableLocked(leader, now)
	a.lastTable = nil
	a.mu.Unlock()

	if table != nil {
		a.mu.Lock()
		a.epoch = allocEpoch
		a.mu.Unlock()
	}
	a.applyVector(table)
}

// ResetBaselines drops any leader state and applies the R/N baseline shares.
func (a *Aggregator) ResetBaselines() {
	a.mu.Lock()
	a.lastTable = nil
	a.lastEpoch = 0
	a.epoch = 0
	a.mu.Unlock()
	a.applyVector(nil)
}

// MergeAllocation stores an out-of-band allocation push. Only pushes from the
// currently elected leader (or a higher epoch) are accepted; stale/lower
// epochs are ignored.
func (a *Aggregator) MergeAllocation(al Allocation) error {
	if al.LeaderID == "" || al.LeaderID == a.nodeID {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	now := a.now()
	leader := a.leaderLocked(now)
	if al.LeaderID != leader {
		// Reject pushes from a node that is not the leader in our view.
		return ErrStaleSnapshot
	}
	p := a.peers[al.LeaderID]
	if al.Epoch < p.allocEpoch {
		return ErrStaleSnapshot
	}
	a.peers[al.LeaderID] = peerState{
		seq:         p.seq,
		ackEpoch:    p.ackEpoch,
		receivedAt:  now,
		items:       p.items,
		leaderID:    al.LeaderID,
		allocEpoch:  al.Epoch,
		allocations: allocationMap(al.Items),
	}
	return nil
}

func allocationMap(items []ItemRates) map[ItemKey]map[string]float64 {
	if len(items) == 0 {
		return nil
	}
	m := make(map[ItemKey]map[string]float64, len(items))
	for _, it := range items {
		cp := make(map[string]float64, len(it.Rates))
		for k, v := range it.Rates {
			cp[k] = v
		}
		m[ItemKey{Scope: it.Scope, Name: it.Name, Slot: it.Slot}] = cp
	}
	return m
}

// applyVector applies the self row of an authoritative table; a nil table or
// a missing row gives the baseline share.
func (a *Aggregator) applyVector(table map[ItemKey]map[string]float64) {
	for _, ru := range a.mgr.SnapshotStats() {
		for _, su := range ru.Slots {
			r0, _, ok := a.mgr.GetSlotBaseline(ru.Scope, ru.Name, su.Slot)
			if !ok || r0 == 0 {
				continue
			}
			eff := r0
			if vec, ok := table[ItemKey{Scope: ru.Scope, Name: ru.Name, Slot: su.Slot}]; ok {
				if v, ok := vec[a.nodeID]; ok && v >= 0 {
					eff = v
				}
			}
			a.mgr.SetSlotRate(ru.Scope, ru.Name, su.Slot, eff)
		}
	}
}

// leaderTableLocked returns the freshest allocation vector and its epoch
// published by the given leader, or nil when it is missing or stale.
func (a *Aggregator) leaderTableLocked(leader string, now time.Time) (map[ItemKey]map[string]float64, uint64) {
	p, ok := a.peers[leader]
	if !ok || now.Sub(p.receivedAt) > 2*a.interval {
		return nil, 0
	}
	return p.allocations, p.allocEpoch
}

// computeVectorLocked builds the authoritative per-node rate vector for every
// local rule slot at the given epoch. A peer's unused share is redistributed
// only when it is fresh AND has acked the previous epoch (ackEpoch >= epoch-1)
// — i.e. it is known to be synchronized with this leader and will run the
// vector it is assigned. All other nodes (stale, unknown, unsynchronized)
// reserve their full R/N baseline and run the baseline fallback, so the
// assigned vector plus reserves sums to exactly R.
func (a *Aggregator) computeVectorLocked(now time.Time, epoch uint64) map[ItemKey]map[string]float64 {
	ids := make(map[string]struct{}, len(a.peers)+1)
	ids[a.nodeID] = struct{}{}
	for id := range a.peers {
		ids[id] = struct{}{}
	}
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)

	type view struct {
		usedRate float64
		wanted   bool
		synced   bool
	}

	table := make(map[ItemKey]map[string]float64)
	for _, ru := range a.mgr.SnapshotStats() {
		for _, su := range ru.Slots {
			key := ItemKey{Scope: ru.Scope, Name: ru.Name, Slot: su.Slot}
			r0, _, _ := a.mgr.GetSlotBaseline(ru.Scope, ru.Name, su.Slot)
			if r0 == 0 {
				continue
			}

			views := make(map[string]view, len(ordered))
			for _, id := range ordered {
				v := view{synced: true}
				if id == a.nodeID {
					// The leader applies the new vector to itself immediately.
					if it, ok := a.localItems[key]; ok {
						v.usedRate = it.Consumed / secondsOf(a.interval)
						v.wanted = it.Wanted
					}
				} else {
					p, seen := a.peers[id]
					switch {
					case !seen || now.Sub(p.receivedAt) > 2*a.interval:
						// Stale/never seen: reserve baseline, do not lend.
						v.synced = false
						v.usedRate = r0
					case p.ackEpoch == 0 || p.ackEpoch < epoch-1:
						// Not synchronized with the previous vector: reserve
						// baseline; the node itself runs the same baseline.
						v.synced = false
						v.usedRate = r0
					default:
						if it, has := p.items[key]; has {
							v.usedRate = it.Consumed / secondsOf(a.interval)
							v.wanted = it.Wanted
						}
					}
				}
				views[id] = v
			}

			pool := 0.0
			demanders := make([]string, 0, len(ordered))
			for _, id := range ordered {
				v := views[id]
				if !v.synced {
					continue
				}
				if v.wanted {
					demanders = append(demanders, id)
					continue
				}
				if r0-minFloat(v.usedRate, r0) > 0 {
					pool += r0 - minFloat(v.usedRate, r0)
				}
			}

			rates := make(map[string]float64, len(ordered))
			d := len(demanders)
			for _, id := range ordered {
				v := views[id]
				if !v.synced {
					rates[id] = r0
				} else if d > 0 && containsStr(demanders, id) {
					rates[id] = minFloat(r0+pool/float64(d), su.Quota.Rate)
				} else {
					rates[id] = minFloat(v.usedRate, r0)
				}
			}

			// Defensive clamp (rounding safety).
			sum := 0.0
			for _, v := range rates {
				sum += v
			}
			if sum > su.Quota.Rate+1e-9 {
				scale := su.Quota.Rate / sum
				for id := range rates {
					rates[id] *= scale
				}
			}
			table[key] = rates
		}
	}
	return table
}

func secondsOf(d time.Duration) float64 {
	s := d.Seconds()
	if s <= 0 {
		return 1
	}
	return s
}

func containsStr(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
