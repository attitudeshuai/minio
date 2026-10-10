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
	"errors"
	"testing"
	"time"

	"github.com/minio/minio/internal/admission"
)

func ruleFor(scope admission.Scope, name string) admission.Rule {
	return admission.Rule{
		Scope: scope, Name: name,
		Total: &admission.ClassQuota{Rate: 1, Burst: 1},
	}
}

func statsHas(sys *admissionSys, scope admission.Scope, name string) bool {
	for _, u := range sys.mgr.SnapshotStats() {
		if u.Scope == scope && u.Name == name {
			return true
		}
	}
	return false
}

func TestNotifyBucketDeletedReclaims(t *testing.T) {
	store := newMemAdmissionStore()
	ctx := context.Background()
	sys := &admissionSys{mgr: admission.NewManager(0)}
	sys.setStore(store)

	if err := sys.upsertRule(ctx, store, ruleFor(admission.ScopeBucket, "my-bucket")); err != nil {
		t.Fatal(err)
	}
	// Simulate the bucket being deleted.
	delete(store.knownBucket, "my-bucket")
	sys.notifyBucketDeleted(ctx, "my-bucket")

	if sys.mgr.Enabled() {
		t.Fatal("only rule gone -> manager must be disabled")
	}
	if statsHas(sys, admission.ScopeBucket, "my-bucket") {
		t.Fatal("limiter state must be reclaimed")
	}
	if store.hasFile() {
		t.Fatal("persisted rule must be pruned")
	}

	// The config poller must not resurrect the deleted rule.
	if err := sys.reloadIfChanged(ctx, store); err != nil {
		t.Fatal(err)
	}
	if sys.mgr.Enabled() {
		t.Fatal("poller must not resurrect a pruned rule")
	}

	// Unknown rule name is a no-op.
	sys.notifyBucketDeleted(ctx, "never-existed")
}

func TestNotifyCallerDeletedReclaims(t *testing.T) {
	store := newMemAdmissionStore()
	ctx := context.Background()
	sys := &admissionSys{mgr: admission.NewManager(0)}
	sys.setStore(store)

	if err := sys.upsertRule(ctx, store, ruleFor(admission.ScopeCaller, "alice")); err != nil {
		t.Fatal(err)
	}
	sys.notifyCallerDeleted(ctx, "alice")
	if statsHas(sys, admission.ScopeCaller, "alice") || store.hasFile() {
		t.Fatal("caller rule must be reclaimed in memory and store")
	}
}

func TestReapDangling(t *testing.T) {
	store := newMemAdmissionStore()
	ctx := context.Background()
	sys := &admissionSys{mgr: admission.NewManager(0)}
	sys.setStore(store)

	// Existing bucket rule (allowed by validation).
	if err := sys.upsertRule(ctx, store, ruleFor(admission.ScopeBucket, "my-bucket")); err != nil {
		t.Fatal(err)
	}
	// Inject a rule for a bucket that no longer exists by writing the file
	// directly and reloading (upsert would reject a missing bucket).
	if err := saveAdmissionRulesToStore(ctx, store, store, admission.Rules{
		ruleFor(admission.ScopeBucket, "my-bucket"),
		ruleFor(admission.ScopeBucket, "gone-bucket"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := sys.reloadFromStore(ctx, store); err != nil {
		t.Fatal(err)
	}

	// A caller rule, even one with no local IAM record, is never auto-pruned
	// (caller rules are removed only by explicit delete hooks).
	if err := sys.upsertRule(ctx, store, ruleFor(admission.ScopeCaller, "oidc-principal")); err != nil {
		t.Fatal(err)
	}

	// Transient-error bucket: rule must be retained.
	store.errBuckets["flaky-bucket"] = true
	sys.upsert.Lock()
	if err := saveAdmissionRulesToStore(ctx, store, store, append(sys.currentRules(),
		ruleFor(admission.ScopeBucket, "flaky-bucket"))); err != nil {
		t.Fatal(err)
	}
	if err := sys.replaceRules(admission.Rules{
		ruleFor(admission.ScopeBucket, "my-bucket"),
		ruleFor(admission.ScopeBucket, "gone-bucket"),
		ruleFor(admission.ScopeBucket, "flaky-bucket"),
		ruleFor(admission.ScopeCaller, "oidc-principal"),
	}); err != nil {
		t.Fatal(err)
	}
	sys.upsert.Unlock()

	sys.reapDangling(ctx, store)

	got := sys.currentRules()
	names := map[string]bool{}
	for _, r := range got {
		names[string(r.Scope)+"/"+r.Name] = true
	}
	if !names["bucket/my-bucket"] {
		t.Fatal("existing bucket rule must survive")
	}
	if !names["caller/oidc-principal"] {
		t.Fatal("caller rules must never be auto-pruned")
	}
	if !names["bucket/flaky-bucket"] {
		t.Fatal("rule with transient backend error must survive")
	}
	if names["bucket/gone-bucket"] {
		t.Fatal("dangling bucket rule must be pruned")
	}
}

func TestApplyLeaderMutationRouting(t *testing.T) {
	store := newMemAdmissionStore()
	ctx := context.Background()

	// Standalone (no aggregator): mutation is applied locally.
	sys := &admissionSys{mgr: admission.NewManager(0)}
	sys.setStore(store)
	wait := time.Millisecond
	rule := admission.Rule{
		Scope: admission.ScopeCaller, Name: "alice",
		Total: &admission.ClassQuota{Rate: 1, Burst: 1, Wait: &wait},
	}
	if err := sys.applyLeaderMutation(ctx, admission.Mutation{Op: admission.MutationUpsert, Rule: rule}); err != nil {
		t.Fatalf("standalone mutate: %v", err)
	}
	if len(sys.currentRules()) != 1 || !store.hasFile() {
		t.Fatal("standalone mutation must persist")
	}

	// With an aggregator whose leader is another node, local apply is refused.
	sys2 := &admissionSys{mgr: admission.NewManager(0, admission.WithNodeCount(func() int { return 2 }))}
	sys2.setStore(store)
	agg := admission.NewAggregator(sys2.mgr, "self-node", time.Second, nil)
	sys2.setAggregator(agg)
	// Make peer-0 the leader by merging its snapshot (id sorts before self-node).
	if err := agg.Merge(admission.PeerSnap{NodeID: "peer-0", Seq: 1}); err != nil {
		t.Fatal(err)
	}
	err := sys2.applyLeaderMutation(ctx, admission.Mutation{Op: admission.MutationDelete, Rule: rule})
	if !errors.Is(err, errAdmissionNotLeader) {
		t.Fatalf("follower mutate want errAdmissionNotLeader, got %v", err)
	}
}
