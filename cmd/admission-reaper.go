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
	"time"

	"github.com/minio/minio/internal/admission"
)

// admissionReapInterval is how often dangling bucket rules are pruned from
// the persisted rule file. Caller rules are NOT auto-pruned: external
// identity providers (OIDC/LDAP) can have principals with no local IAM
// record, so caller rules are only removed via the explicit delete-user /
// delete-service-account hooks.
const admissionReapInterval = 5 * time.Minute

// notifyBucketDeleted immediately reclaims bucket-rule counters and prunes
// the persisted rule. Called on every node when a bucket disappears.
func (sys *admissionSys) notifyBucketDeleted(ctx context.Context, name string) {
	sys.dropAndPrune(ctx, admission.ScopeBucket, name)
}

// notifyCallerDeleted immediately reclaims caller-rule counters and prunes
// the persisted rule.
func (sys *admissionSys) notifyCallerDeleted(ctx context.Context, name string) {
	sys.dropAndPrune(ctx, admission.ScopeCaller, name)
}

// dropAndPrune removes the rule from local limiter state on every node; the
// shared rules file is rewritten only by the leader (single writer). Other
// nodes converge through the leader's reload broadcast / fallback poller.
func (sys *admissionSys) dropAndPrune(ctx context.Context, scope admission.Scope, name string) {
	sys.mgr.Remove(scope, name)
	if !sys.isLeaderOrStandalone() {
		return
	}
	store := sys.backingStore()
	if store == nil {
		return
	}
	sys.upsert.Lock()
	defer sys.upsert.Unlock()
	if _, ok := sys.currentRules().Find(scope, name); !ok {
		return
	}
	if err := sys.pruneLocked(ctx, store, scope, name); err != nil {
		internalLogIf(ctx, err)
	}
}

// pruneLocked removes one rule from the durable set and rewrites the store.
// The merge base is re-read from the store (like leaderDelete) so a prune
// cannot resurrect a concurrently committed rule. The caller must hold
// sys.upsert.
func (sys *admissionSys) pruneLocked(ctx context.Context, store admissionStore, scope admission.Scope, name string) error {
	cur, err := loadAdmissionRulesFromStore(ctx, store)
	if err != nil {
		return err
	}
	next := make(admission.Rules, 0, len(cur))
	found := false
	for _, x := range cur {
		if x.Scope == scope && x.Name == name {
			found = true
			continue
		}
		next = append(next, x)
	}
	if !found {
		return nil
	}
	if err := saveAdmissionRulesToStore(ctx, store, store, next); err != nil {
		return err
	}
	return sys.replaceRules(next)
}

// reapDangling prunes bucket rules whose target bucket no longer exists.
// Only the leader rewrites the shared rules file; followers converge via the
// reload broadcast. Transient backend errors keep the rule. Caller rules are
// never auto-pruned.
func (sys *admissionSys) reapDangling(ctx context.Context, store admissionStore) {
	if !sys.isLeaderOrStandalone() {
		return
	}
	for _, r := range sys.currentRules() {
		if r.Scope != admission.ScopeBucket {
			continue
		}
		_, err := store.GetBucketInfo(ctx, r.Name, BucketOptions{})
		if err == nil {
			continue // bucket still exists
		}
		if !isErrBucketNotFound(err) {
			// Uncertain: never prune.
			continue
		}
		sys.upsert.Lock()
		if err := sys.pruneLocked(ctx, store, r.Scope, r.Name); err != nil {
			internalLogIf(ctx, err)
		}
		sys.upsert.Unlock()
	}
}

func runAdmissionReaper(ctx context.Context, store admissionStore) {
	ticker := time.NewTicker(admissionReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			globalAdmissionSys.reapDangling(ctx, store)
		}
	}
}
