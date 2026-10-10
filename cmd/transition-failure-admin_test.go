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
	"errors"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
)

func setupAdminStoreWithTier(t *testing.T, store transitionFailureStore, tierNames ...string) {
	t.Helper()
	mgr := NewTierConfigMgr()
	for _, name := range tierNames {
		mgr.Tiers[name] = madmin.TierConfig{
			Name: name,
			Type: madmin.S3,
			S3:   &madmin.TierS3{Bucket: "remote-bucket", Region: "us-east-1"},
		}
	}
	data, err := mgr.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if err := saveConfig(context.Background(), store, tierConfigPath, data); err != nil {
		t.Fatal(err)
	}
	old := globalTierConfigMgr
	globalTierConfigMgr = mgr
	t.Cleanup(func() { globalTierConfigMgr = old })
}

func TestAdminListTierFailures(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	j := newTransitionFailureJournal(ctx)
	if err := j.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	now := UTCNow()
	cfg := testFailureCfg()

	put := func(tier, bucket, object string, status transitionFailureStatus) {
		oi := ObjectInfo{Bucket: bucket, Name: object, VersionID: "v", ModTime: now}
		rec := recordTransitionFailure(nil, oi, tier, false,
			transitionFailureBackendDown, "err", cfg, now.Add(-time.Minute))
		rec.Status = status
		if status == transitionFailureExhausted {
			rec.NextRetryAt = time.Time{}
		}
		j.upsert(ctx, rec)
	}
	put("WARM-A", "b1", "o1", transitionFailurePending)
	put("WARM-A", "b2", "o2", transitionFailureExhausted)
	put("WARM-B", "b1", "o3", transitionFailurePending)

	all, err := listTierFailures(ctx, j, "", "", "")
	if err != nil || len(all) != 3 {
		t.Fatalf("expected 3 records, got %d (%v)", len(all), err)
	}
	aOnly, err := listTierFailures(ctx, j, "WARM-A", "", "")
	if err != nil || len(aOnly) != 2 {
		t.Fatalf("expected 2 WARM-A records, got %d (%v)", len(aOnly), err)
	}
	aB1, err := listTierFailures(ctx, j, "WARM-A", "b1", "")
	if err != nil || len(aB1) != 1 || aB1[0].Bucket != "b1" {
		t.Fatalf("tier+bucket filter wrong: %+v (%v)", aB1, err)
	}
	exh, err := listTierFailures(ctx, j, "", "", transitionFailureExhausted)
	if err != nil || len(exh) != 1 || exh[0].Status != "exhausted" {
		t.Fatalf("exhausted filter wrong: %+v (%v)", exh, err)
	}
	if exh[0].Category != "backend_down" {
		t.Fatalf("error category missing from response: %+v", exh[0])
	}
}

// TestAdminListTierFailuresRefresh ensures a record written directly to the
// shared store (e.g. by another node) becomes visible after the handler's
// forced refresh.
func TestAdminListTierFailuresRefresh(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	j := newTransitionFailureJournal(ctx)
	if err := j.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	rec := recordTransitionFailure(nil, ObjectInfo{Bucket: "b", Name: "o", VersionID: "v"},
		"WARM-X", false, transitionFailureInternal, "err", testFailureCfg(), UTCNow())
	data, err := json.Marshal(&rec)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := saveConfig(ctx, store, transitionFailureRecordPath(rec.Tier, rec.ID), data); err != nil {
		t.Fatal(err)
	}

	got, err := listTierFailures(ctx, j, "WARM-X", "", "")
	if err != nil || len(got) != 1 || got[0].Object != "o" {
		t.Fatalf("expected refreshed remote record, got %+v (%v)", got, err)
	}
}

func TestAdminClearTierFailures(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	j := newTransitionFailureJournal(ctx)
	if err := j.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	now := UTCNow()
	mk := func(object, status string) {
		rec := recordTransitionFailure(nil, ObjectInfo{Bucket: "b", Name: object, VersionID: "v"},
			"WARM-C", false, transitionFailureInternal, "err", testFailureCfg(), now)
		rec.Status = transitionFailureStatus(status)
		if status == string(transitionFailureExhausted) {
			rec.NextRetryAt = time.Time{}
		}
		j.upsert(ctx, rec)
	}
	mk("p", string(transitionFailurePending))
	mk("e", string(transitionFailureExhausted))

	n, err := clearTierFailures(ctx, j, "WARM-C", false)
	if err != nil || n != 1 {
		t.Fatalf("expected 1 exhausted cleared, got %d (%v)", n, err)
	}
	if len(j.list("WARM-C", "", transitionFailurePending)) != 1 {
		t.Fatalf("pending record must remain without all=true")
	}
	n, err = clearTierFailures(ctx, j, "", true)
	if err != nil || n != 1 {
		t.Fatalf("expected 1 all-cleared, got %d (%v)", n, err)
	}
	if len(j.list("", "", "")) != 0 {
		t.Fatalf("journal should be empty")
	}
}

func TestAdminTierStatusAndPauseResume(t *testing.T) {
	ctx := context.Background()
	store := newFakeTransitionStore()
	setupAdminStoreWithTier(t, store, "WARM-A", "WARM-B")

	health := newTransitionTierHealthMgr(ctx)
	if err := health.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	j := newTransitionFailureJournal(ctx)
	if err := j.Init(ctx, store); err != nil {
		t.Fatal(err)
	}
	rec := recordTransitionFailure(nil, ObjectInfo{Bucket: "b", Name: "o", VersionID: "v"},
		"WARM-A", false, transitionFailureBackendDown, "down", testFailureCfg(), UTCNow())
	j.upsert(ctx, rec)

	objAPI := store.asObjectLayer()

	// Unknown tier rejected.
	if err := pauseTransitionTier(ctx, objAPI, health, "WARM-NOPE", ""); !errors.Is(err, errTierNotFound) {
		t.Fatalf("expected errTierNotFound, got %v", err)
	}

	// Valid manual pause.
	if err := pauseTransitionTier(ctx, objAPI, health, "WARM-A", "maint"); err != nil {
		t.Fatal(err)
	}
	h, _ := health.Get("WARM-A")
	if !h.Paused || !h.Manual || h.LastErr != "maint" {
		t.Fatalf("unexpected paused state: %+v", h)
	}

	statuses, err := listTierStatus(ctx, health, j, "")
	if err != nil {
		t.Fatal(err)
	}
	byTier := map[string]tierStatusInfo{}
	for _, s := range statuses {
		byTier[s.Tier] = s
	}
	a, ok := byTier["WARM-A"]
	if !ok {
		t.Fatalf("WARM-A missing from status: %+v", statuses)
	}
	if !a.Paused || !a.Manual || a.PendingFailures != 1 || a.Version != 1 {
		t.Fatalf("unexpected WARM-A status: %+v", a)
	}
	if b := byTier["WARM-B"]; b.Paused || b.PendingFailures != 0 {
		t.Fatalf("WARM-B should be healthy: %+v", b)
	}

	// Filter by tier.
	one, err := listTierStatus(ctx, health, j, "WARM-B")
	if err != nil || len(one) != 1 || one[0].Tier != "WARM-B" {
		t.Fatalf("tier filter wrong: %+v (%v)", one, err)
	}

	// Resume.
	if err := resumeTransitionTier(ctx, objAPI, health, "WARM-A"); err != nil {
		t.Fatal(err)
	}
	h, _ = health.Get("WARM-A")
	if h.Paused || h.Manual {
		t.Fatalf("resume did not clear pause: %+v", h)
	}
}
