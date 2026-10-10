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

package cmd

import (
	"context"
	"testing"
	"time"

	"github.com/minio/minio/internal/admission"
)

func TestAdmissionExchangeIntervalEnv(t *testing.T) {
	t.Setenv(admissionExchangeIntervalEnv, "")
	if d := admissionExchangeInterval(); d != admissionDefaultExchange {
		t.Fatalf("default: %v", d)
	}
	t.Setenv(admissionExchangeIntervalEnv, "250ms")
	if d := admissionExchangeInterval(); d != 250*time.Millisecond {
		t.Fatalf("override: %v", d)
	}
	t.Setenv(admissionExchangeIntervalEnv, "garbage")
	if d := admissionExchangeInterval(); d != admissionDefaultExchange {
		t.Fatalf("invalid must fall back: %v", d)
	}
	t.Setenv(admissionExchangeIntervalEnv, "-1s")
	if d := admissionExchangeInterval(); d != admissionDefaultExchange {
		t.Fatalf("negative must fall back: %v", d)
	}
}

// With no notification system and N==1 the exchange tick runs without
// blocking sleeps or RPCs and computes the local allocation immediately.
func TestAdmissionExchangeTickStandalone(t *testing.T) {
	mgr := admission.NewManager(0, admission.WithNodeCount(func() int { return 1 }))
	rules := admission.Rules{{
		Scope: admission.ScopeBucket, Name: "b",
		Total: &admission.ClassQuota{Rate: 10, Burst: 10},
	}}
	if err := mgr.ApplyRules(rules); err != nil {
		t.Fatal(err)
	}
	agg := admission.NewAggregator(mgr, "node-local", time.Second, nil)

	saved := globalNotificationSys
	globalNotificationSys = nil
	defer func() { globalNotificationSys = saved }()

	// Disabled manager: tick is a pure no-op (no RPCs, no timers).
	disabled := admission.NewManager(0)
	aggOff := admission.NewAggregator(disabled, "node-off", time.Second, nil)
	admissionExchangeTick(context.Background(), aggOff)

	// Record demand, then a tick: standalone leader applies full R.
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		mgr.Admit(ctx, admission.ScopeBucket, "b", admission.ClassWrite)
	}
	if d := mgr.Admit(ctx, admission.ScopeBucket, "b", admission.ClassWrite); d.Allowed {
		t.Fatal("expected rejection to mark demand")
	}
	admissionExchangeTick(context.Background(), agg)
	if r, ok := mgr.GetSlotEffectiveRate(admission.ScopeBucket, "b", admission.SlotTotal); !ok || r != 10 {
		t.Fatalf("eff rate N=1: %v ok=%v", r, ok)
	}
}

func TestAdmissionReloadHookFiresOnMutation(t *testing.T) {
	store := newMemAdmissionStore()
	ctx := context.Background()
	sys := &admissionSys{mgr: admission.NewManager(0)}

	calls := 0
	prevHook := admissionPeersReloadHook
	admissionPeersReloadHook = func() { calls++ }
	defer func() { admissionPeersReloadHook = prevHook }()

	rule := admission.Rule{
		Scope: admission.ScopeCaller, Name: "alice",
		Total: &admission.ClassQuota{Rate: 1, Burst: 1},
	}
	if err := sys.upsertRule(ctx, store, rule); err != nil {
		t.Fatal(err)
	}
	if err := sys.deleteRule(ctx, store, admission.ScopeCaller, "alice"); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("reload hook must fire after set and delete, got %d", calls)
	}
}

func TestAdmissionReloadIfChanged(t *testing.T) {
	store := newMemAdmissionStore()
	ctx := context.Background()
	sys := &admissionSys{mgr: admission.NewManager(0)}

	// Nothing on disk, nothing local: no-op, no error.
	if err := sys.reloadIfChanged(ctx, store); err != nil {
		t.Fatal(err)
	}

	rule := admission.Rule{
		Scope: admission.ScopeBucket, Name: "my-bucket",
		Total: &admission.ClassQuota{Rate: 1, Burst: 1},
	}
	if err := sys.upsertRule(ctx, store, rule); err != nil {
		t.Fatal(err)
	}

	// A second system converges to the same content via the poller.
	sys2 := &admissionSys{mgr: admission.NewManager(0)}
	if err := sys2.reloadIfChanged(ctx, store); err != nil {
		t.Fatal(err)
	}
	if !sys2.mgr.Enabled() || len(sys2.currentRules()) != 1 {
		t.Fatal("poller must have applied the rules")
	}

	// No change: second poll is a no-op (still enabled, same rules).
	if err := sys2.reloadIfChanged(ctx, store); err != nil {
		t.Fatal(err)
	}
	if len(sys2.currentRules()) != 1 {
		t.Fatal("rules must be stable")
	}

	// External change (simulating missed broadcast): rate bump in store.
	rule.Total = &admission.ClassQuota{Rate: 42, Burst: 7}
	if err := saveAdmissionRulesToStore(ctx, store, store, admission.Rules{rule}); err != nil {
		t.Fatal(err)
	}
	if err := sys2.reloadIfChanged(ctx, store); err != nil {
		t.Fatal(err)
	}
	got := sys2.currentRules()
	if len(got) != 1 || got[0].Total.Rate != 42 || got[0].Total.Burst != 7 {
		t.Fatalf("poller must converge to changed rules: %+v", got)
	}
}

func TestAdmissionExchangeHandler(t *testing.T) {
	sys := &admissionSys{mgr: admission.NewManager(0)}
	if err := sys.replaceRules(admission.Rules{{
		Scope: admission.ScopeBucket, Name: "b",
		Total: &admission.ClassQuota{Rate: 10, Burst: 10},
	}}); err != nil {
		t.Fatal(err)
	}
	agg := admission.NewAggregator(sys.mgr, "self-node", time.Second, nil)
	sys.setAggregator(agg)

	// Temporarily install as global (handler is a method on peerRESTServer
	// operating on globalAdmissionSys).
	prev := globalAdmissionSys
	globalAdmissionSys = sys
	defer func() { globalAdmissionSys = prev }()

	server := &peerRESTServer{}
	snap := admission.PeerSnap{
		NodeID: "peer-1",
		Seq:    1,
		Items: []admission.ItemSnap{{
			Scope: admission.ScopeBucket, Name: "b", Slot: admission.SlotTotal,
			Consumed: 3, Wanted: true,
		}},
	}
	if _, rerr := server.AdmissionExchangeHandler(admissionPeerSnapPool.NewJSONWith(&snap)); rerr != nil {
		t.Fatalf("merge: %v", rerr)
	}

	// Same sequence again: stale -> remote error.
	if _, rerr := server.AdmissionExchangeHandler(admissionPeerSnapPool.NewJSONWith(&snap)); rerr == nil {
		t.Fatal("duplicate snapshot must be rejected")
	}

	// New sequence accepted.
	snap2 := snap
	snap2.Seq = 2
	if _, rerr := server.AdmissionExchangeHandler(admissionPeerSnapPool.NewJSONWith(&snap2)); rerr != nil {
		t.Fatalf("seq 2: %v", rerr)
	}
}

func TestAdmissionAllocateHandler(t *testing.T) {
	sys := &admissionSys{mgr: admission.NewManager(0, admission.WithNodeCount(func() int { return 4 }))}
	if err := sys.replaceRules(admission.Rules{{
		Scope: admission.ScopeBucket, Name: "b",
		Total: &admission.ClassQuota{Rate: 100, Burst: 40},
	}}); err != nil {
		t.Fatal(err)
	}
	agg := admission.NewAggregator(sys.mgr, "self-node", time.Second, nil)
	sys.setAggregator(agg)

	prev := globalAdmissionSys
	globalAdmissionSys = sys
	defer func() { globalAdmissionSys = prev }()
	server := &peerRESTServer{}

	// First make peer-1 known/fresh; it becomes the elected leader.
	if _, rerr := server.AdmissionExchangeHandler(admissionPeerSnapPool.NewJSONWith(
		&admission.PeerSnap{NodeID: "peer-1", Seq: 1},
	)); rerr != nil {
		t.Fatal(rerr)
	}
	if sys.leaderHost() != "peer-1" {
		t.Fatalf("leader=%s want peer-1", sys.leaderHost())
	}

	// An allocation from the elected leader is accepted and applied.
	al := admission.Allocation{
		LeaderID: "peer-1",
		Epoch:    2,
		Items: []admission.ItemRates{{
			Scope: admission.ScopeBucket, Name: "b", Slot: admission.SlotTotal,
			Rates: map[string]float64{"self-node": 25, "peer-1": 75},
		}},
	}
	if _, rerr := server.AdmissionAllocateHandler(admissionAllocPool.NewJSONWith(&al)); rerr != nil {
		t.Fatalf("alloc: %v", rerr)
	}
	if r, _ := sys.mgr.GetSlotEffectiveRate(admission.ScopeBucket, "b", admission.SlotTotal); r != 25 {
		t.Fatalf("applied rate %v want 25", r)
	}

	// A push from a non-leader is rejected.
	bogus := al
	bogus.LeaderID = "peer-9"
	if _, rerr := server.AdmissionAllocateHandler(admissionAllocPool.NewJSONWith(&bogus)); rerr == nil {
		t.Fatal("non-leader allocation must be rejected")
	}
}
