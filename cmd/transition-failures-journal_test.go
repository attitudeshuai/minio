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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func testObjectInfo(bucket, object, vid string) ObjectInfo {
	return ObjectInfo{
		Bucket:    bucket,
		Name:      object,
		VersionID: vid,
		Size:      100,
		ETag:      "etag-" + vid,
		ModTime:   time.Now().Add(-time.Hour).UTC(),
	}
}

func TestJournalUpsertListSucceed(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	j := newTransitionFailureJournal(ctx)
	if err := j.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	cfg := testFailureCfg()
	now := time.Now().UTC()
	oi := testObjectInfo("bucket-a", "object-1", "v1")

	rec := recordTransitionFailure(nil, oi, "WARM-A", false, transitionFailureBackendDown, "down", cfg, now)
	j.upsert(ctx, rec)
	p := transitionFailureRecordPath(rec.Tier, rec.ID)
	if !store.hasKey(p) {
		t.Fatalf("expected persisted record at %s", p)
	}
	if got := j.list("WARM-A", "", ""); len(got) != 1 {
		t.Fatalf("expected 1 record, got %d", len(got))
	}
	if got := j.list("WARM-B", "", ""); len(got) != 0 {
		t.Fatalf("tier filter leaked records: %d", len(got))
	}

	// Second failure advances the same record.
	rec2 := recordTransitionFailure(&rec, oi, "WARM-A", false, transitionFailureBackendDown, "down", cfg, now.Add(time.Minute))
	j.upsert(ctx, rec2)
	if store.keyCount() != 1 {
		t.Fatalf("expected exactly one stored object, got %d", store.keyCount())
	}
	got := j.list("WARM-A", "", transitionFailurePending)
	if len(got) != 1 || got[0].Attempts != 2 || got[0].FirstFailedAt != now {
		t.Fatalf("unexpected merged record: %+v", got)
	}

	// Successful attempt removes the record.
	j.succeed(ctx, rec.ID)
	if store.hasKey(p) {
		t.Fatalf("expected record removed from shared store")
	}
	if len(j.list("", "", "")) != 0 {
		t.Fatalf("expected empty list after success")
	}
}

func TestJournalBootstrapAndPagination(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	cfg := testFailureCfg()
	now := time.Now().UTC()

	const total = maxObjectList*2 + 5
	for i := 0; i < total; i++ {
		oi := testObjectInfo("bucket", fmt.Sprintf("object-%05d", i), "v")
		rec := recordTransitionFailure(nil, oi, "WARM-P", false, transitionFailureInternal, "err", cfg, now)
		data, err := json.Marshal(&rec)
		if err != nil {
			t.Fatal(err)
		}
		if err := saveConfig(ctx, store, transitionFailureRecordPath(rec.Tier, rec.ID), data); err != nil {
			t.Fatal(err)
		}
	}

	j := newTransitionFailureJournal(ctx)
	if err := j.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	if got := len(j.list("WARM-P", "", "")); got != total {
		t.Fatalf("bootstrap expected %d records, got %d", total, got)
	}
}

func TestJournalDue(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	j := newTransitionFailureJournal(ctx)
	if err := j.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	cfg := testFailureCfg()
	now := time.Now().UTC()

	mkRec := func(tier, object string, nextRetry time.Time, status transitionFailureStatus) transitionFailureRecord {
		oi := testObjectInfo("b", object, "v")
		rec := recordTransitionFailure(nil, oi, tier, false, transitionFailureBackendDown, "err", cfg, now)
		rec.NextRetryAt = nextRetry
		rec.Status = status
		return rec
	}

	past1 := mkRec("WARM-X", "o1", now.Add(-time.Minute), transitionFailurePending)
	past2 := mkRec("WARM-X", "o2", now.Add(-2*time.Minute), transitionFailurePending)
	future := mkRec("WARM-X", "o3", now.Add(time.Hour), transitionFailurePending)
	exh := mkRec("WARM-X", "o4", now.Add(-time.Minute), transitionFailureExhausted)
	pausedTier := mkRec("WARM-Y", "o5", now.Add(-time.Minute), transitionFailurePending)
	for _, rec := range []transitionFailureRecord{past1, past2, future, exh, pausedTier} {
		j.upsert(ctx, rec)
	}

	paused := func(tier string) bool { return tier == "WARM-Y" }
	due := j.due(now, paused, 10)
	if len(due) != 2 {
		t.Fatalf("expected 2 due records, got %d", len(due))
	}
	// Oldest NextRetryAt first.
	if due[0].Object != "o2" || due[1].Object != "o1" {
		t.Fatalf("unexpected due ordering: %s %s", due[0].Object, due[1].Object)
	}

	// Limit honored.
	due = j.due(now, paused, 1)
	if len(due) != 1 || due[0].Object != "o2" {
		t.Fatalf("limit not honored: %+v", due)
	}

	// Counts.
	counts := j.countsByTier()
	if counts["WARM-X"].Pending != 3 || counts["WARM-X"].Exhausted != 1 || counts["WARM-Y"].Pending != 1 {
		t.Fatalf("unexpected counts: %+v", counts)
	}
}

func TestJournalRefresh(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	j := newTransitionFailureJournal(ctx)
	if err := j.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	cfg := testFailureCfg()
	now := time.Now().UTC()

	oi := testObjectInfo("b", "local", "v")
	local := recordTransitionFailure(nil, oi, "WARM-R", false, transitionFailureBackendDown, "err", cfg, now)
	j.upsert(ctx, local)

	// Peer writes a new record directly to the shared store.
	peerOI := testObjectInfo("b", "peer", "v")
	peer := recordTransitionFailure(nil, peerOI, "WARM-R", false, transitionFailurePermission, "perm", cfg, now)
	data, _ := json.Marshal(&peer)
	if err := saveConfig(ctx, store, transitionFailureRecordPath(peer.Tier, peer.ID), data); err != nil {
		t.Fatal(err)
	}
	// Bump mod time beyond the freshness heuristic.
	time.Sleep(20 * time.Millisecond)
	stored := store.objects[transitionFailureRecordPath(peer.Tier, peer.ID)]
	stored.modTime = now.Add(time.Minute)
	store.mu.Lock()
	store.objects[transitionFailureRecordPath(peer.Tier, peer.ID)] = stored
	store.mu.Unlock()

	// Peer deletes the local record remotely.
	if err := deleteConfig(ctx, store, transitionFailureRecordPath(local.Tier, local.ID)); err != nil {
		t.Fatal(err)
	}

	if err := j.refresh(ctx); err != nil {
		t.Fatal(err)
	}
	all := j.list("", "", "")
	if len(all) != 1 || all[0].Object != "peer" {
		t.Fatalf("expected only peer record after refresh, got %+v", all)
	}
}

func TestJournalConcurrentUpsert(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	j := newTransitionFailureJournal(ctx)
	if err := j.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	cfg := testFailureCfg()
	now := time.Now().UTC()
	oi := testObjectInfo("b", "race", "v")

	const n = 16
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := recordTransitionFailure(nil, oi, "WARM-C", false,
				transitionFailureBackendDown, "err", cfg, now.Add(time.Duration(i)*time.Second))
			j.upsert(ctx, rec)
		}(i)
	}
	wg.Wait()

	if store.keyCount() != 1 {
		t.Fatalf("expected one stored record, got %d", store.keyCount())
	}
	got := j.list("WARM-C", "", "")
	if len(got) != 1 {
		t.Fatalf("expected one cached record, got %d", len(got))
	}
	if got[0].Attempts != n {
		t.Fatalf("expected attempts=%d, got %d", n, got[0].Attempts)
	}
}

func TestJournalClear(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	j := newTransitionFailureJournal(ctx)
	if err := j.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	cfg := testFailureCfg()
	now := time.Now().UTC()

	// Pending record.
	pendingOI := testObjectInfo("b", "pending", "v")
	pending := recordTransitionFailure(nil, pendingOI, "WARM-D", false, transitionFailureInternal, "err", cfg, now)
	j.upsert(ctx, pending)

	// Exhausted record.
	exhOI := testObjectInfo("b", "exhausted", "v")
	exh := recordTransitionFailure(nil, exhOI, "WARM-D", false, transitionFailureConfiguration, "cfg", cfg, now)
	exh.Status = transitionFailureExhausted
	exh.NextRetryAt = time.Time{}
	j.upsert(ctx, exh)

	removed, err := j.clear(ctx, "WARM-D", false)
	if err != nil || removed != 1 {
		t.Fatalf("expected 1 exhausted removal, got %d (%v)", removed, err)
	}
	if len(j.list("WARM-D", "", transitionFailureExhausted)) != 0 {
		t.Fatalf("exhausted records should be gone")
	}
	if len(j.list("WARM-D", "", transitionFailurePending)) != 1 {
		t.Fatalf("pending records should remain without all=true")
	}

	removed, err = j.clear(ctx, "WARM-D", true)
	if err != nil || removed != 1 {
		t.Fatalf("expected 1 removal with all=true, got %d (%v)", removed, err)
	}
	if store.keyCount() != 0 {
		t.Fatalf("expected empty store, got %d keys", store.keyCount())
	}
}
