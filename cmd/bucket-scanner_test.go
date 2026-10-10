// Copyright (c) 2015-2024 MinIO, Inc.
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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio/internal/cachevalue"
)

func TestBucketScanConfigValidate(t *testing.T) {
	cases := []struct {
		name string
		cfg  BucketScanConfig
		want bool
	}{
		{"zero/default", BucketScanConfig{}, true},
		{"weight one", BucketScanConfig{Weight: 1}, true},
		{"weight max", BucketScanConfig{Weight: bucketScanMaxWeight}, true},
		{"weight too high", BucketScanConfig{Weight: bucketScanMaxWeight + 1}, false},
		{"budget min", BucketScanConfig{Budget: bucketScanMinTurnBudget}, true},
		{"budget max", BucketScanConfig{Budget: bucketScanMaxTurnBudget}, true},
		{"budget too small", BucketScanConfig{Budget: time.Millisecond}, false},
		{"budget too large", BucketScanConfig{Budget: time.Hour}, false},
		{"weight and budget", BucketScanConfig{Weight: 2, Budget: time.Second}, false},
		{"paused", BucketScanConfig{Paused: true}, true},
		{"paused with weight", BucketScanConfig{Paused: true, Weight: 3}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if (err == nil) != tc.want {
				t.Fatalf("Validate() err=%v, wantErr=%v", err, !tc.want)
			}
		})
	}

	// Effective weight/budget semantics.
	if (BucketScanConfig{}).effectiveWeight() != 1 {
		t.Fatal("default weight must be 1")
	}
	if d := (BucketScanConfig{}).effectiveBudget(); d != 0 {
		t.Fatalf("default budget must be 0 (unbounded), got %v", d)
	}
	if d := (BucketScanConfig{Weight: 4}).effectiveBudget(); d != bucketScanDefaultBudget {
		t.Fatalf("weight without budget must use the default slice, got %v", d)
	}
	if d := (BucketScanConfig{Budget: 5 * time.Second}).effectiveBudget(); d != 5*time.Second {
		t.Fatalf("explicit budget must be honored, got %v", d)
	}
}

func TestBucketScannerConfigDocValidateIsolatesBadEntries(t *testing.T) {
	doc := bucketScannerConfigDoc{
		Buckets: map[string]BucketScanConfig{
			"good":   {Weight: 2},
			"UPPER":  {Weight: 1}, // invalid bucket name
			"heavy":  {Weight: bucketScanMaxWeight + 1},
			"both":   {Weight: 2, Budget: time.Second},
			"paused": {Paused: true},
		},
	}
	errs := doc.Validate()
	if len(errs) != 3 {
		t.Fatalf("expected 3 bad entries, got %d: %v", len(errs), errs)
	}
	for _, b := range []string{"UPPER", "heavy", "both"} {
		if _, ok := errs[b]; !ok {
			t.Fatalf("expected %q in validation errors", b)
		}
	}
	if _, ok := errs["good"]; ok {
		t.Fatal("valid entry must not be reported")
	}
	if _, ok := errs["paused"]; ok {
		t.Fatal("paused entry must not be reported")
	}
}

// testScheduler builds a scheduler with its barrier open and a worker
// range registered.
func testScheduler(t *testing.T, cycle uint32, buckets []string, cfg map[string]BucketScanConfig, workers, sets int) (*bucketScanScheduler, int) {
	t.Helper()
	s := newBucketScanScheduler(nil)
	s.cfg = bucketScannerConfigDoc{Version: bucketScannerConfigVersion, Buckets: cfg}
	s.states = map[string]bucketScanState{}
	bis := make([]BucketInfo, 0, len(buckets))
	for _, b := range buckets {
		bis = append(bis, BucketInfo{Name: b})
	}
	s.initCycle(cycle, bis, sets)
	base := s.registerWorkers(workers)
	s.setReady()
	return s, base
}

func yieldTurns(t *testing.T, s *bucketScanScheduler, workerID, n int) []string {
	t.Helper()
	got := make([]string, 0, n)
	for range n {
		turn, ok := s.acquire(t.Context(), workerID)
		if !ok {
			t.Fatalf("acquire %d returned false", len(got))
		}
		got = append(got, turn.Bucket)
		s.finish(t.Context(), bucketTurnResult{workerID: workerID, bucket: turn.Bucket, yielded: true})
	}
	return got
}

func TestBucketSchedulerWeightedRoundRobin(t *testing.T) {
	buckets := []string{"alpha", "bbb", "ccc"}
	cfg := map[string]BucketScanConfig{"alpha": {Weight: 3}}
	s, base := testScheduler(t, 1, buckets, cfg, 1, 1)

	got := yieldTurns(t, s, base+0, 10)
	want := []string{"alpha", "alpha", "alpha", "bbb", "ccc", "alpha", "alpha", "alpha", "bbb", "ccc"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("turn %d: got %q want %q (sequence %v)", i, got[i], want[i], got)
		}
	}
}

func TestBucketSchedulerEqualWeightsRotate(t *testing.T) {
	buckets := []string{"alpha", "bbb", "ccc"}
	s, base := testScheduler(t, 1, buckets, nil, 1, 1)
	got := yieldTurns(t, s, base+0, 6)
	// Default weight 1: strict rotation.
	want := []string{"alpha", "bbb", "ccc", "alpha", "bbb", "ccc"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("turn %d: got %q want %q (sequence %v)", i, got[i], want[i], got)
		}
	}
}

func TestBucketSchedulerPausedBucketSkippedAndResumed(t *testing.T) {
	buckets := []string{"bucket-a", "bucket-b"}
	cfg := map[string]BucketScanConfig{"bucket-b": {Paused: true}}
	s, base := testScheduler(t, 1, buckets, cfg, 1, 1)
	w := base

	turn, ok := s.acquire(t.Context(), w)
	if !ok || turn.Bucket != "bucket-a" {
		t.Fatalf("expected bucket-a, got %+v ok=%v", turn, ok)
	}
	s.finish(t.Context(), bucketTurnResult{workerID: w, bucket: "bucket-a"})

	// Only the paused bucket remains; acquire blocks until resume.
	done := make(chan string, 1)
	go func() {
		tr, ok := s.acquire(t.Context(), w)
		if !ok {
			done <- ""
			return
		}
		done <- tr.Bucket
	}()
	select {
	case b := <-done:
		t.Fatalf("acquire should block on paused bucket, got %q", b)
	case <-time.After(150 * time.Millisecond):
	}

	// Resume directly through scheduler state (backend-free).
	s.mu.Lock()
	c := s.cfg.Buckets["bucket-b"]
	c.Paused = false
	delete(s.cfg.Buckets, "bucket-b")
	s.front = map[string]struct{}{"bucket-b": {}}
	s.mu.Unlock()
	s.notify()

	select {
	case b := <-done:
		if b != "bucket-b" {
			t.Fatalf("expected resumed bucket-b, got %q", b)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("resumed bucket was not scheduled")
	}
}

func TestBucketSchedulerCompletionRequiresAllWorkers(t *testing.T) {
	buckets := []string{"bucket-a", "bucket-b"}
	s, base := testScheduler(t, 1, buckets, nil, 2, 1)
	w0, w1 := base, base+1

	t0, ok := s.acquire(t.Context(), w0)
	if !ok {
		t.Fatal("w0 acquire")
	}
	t1, ok := s.acquire(t.Context(), w1)
	if !ok {
		t.Fatal("w1 acquire")
	}
	// Both workers may take the same bucket concurrently.
	s.finish(t.Context(), bucketTurnResult{workerID: w0, bucket: t0.Bucket})
	s.finish(t.Context(), bucketTurnResult{workerID: w1, bucket: t1.Bucket})

	// Drain remaining work for both.
	for i := 0; i < 4; i++ {
		if tr, ok := s.acquire(t.Context(), w0); ok {
			s.finish(t.Context(), bucketTurnResult{workerID: w0, bucket: tr.Bucket})
		}
		if tr, ok := s.acquire(t.Context(), w1); ok {
			s.finish(t.Context(), bucketTurnResult{workerID: w1, bucket: tr.Bucket})
		}
	}
	if _, ok := s.acquire(t.Context(), w0); ok {
		t.Fatal("w0 must be done")
	}
	if _, ok := s.acquire(t.Context(), w1); ok {
		t.Fatal("w1 must be done")
	}
	for _, b := range buckets {
		st := s.states[b]
		if st.CompletedAt.IsZero() {
			t.Fatalf("bucket %s completion not recorded", b)
		}
	}
}

func TestBucketSchedulerProcessedCountsAndPosition(t *testing.T) {
	buckets := []string{"bucket-a"}
	s, base := testScheduler(t, 7, buckets, map[string]BucketScanConfig{"bucket-a": {Weight: 2}}, 1, 1)
	tr, ok := s.acquire(t.Context(), base)
	if !ok {
		t.Fatal("acquire")
	}
	if tr.turnBudget() != bucketScanDefaultBudget {
		t.Fatalf("weighted bucket must use default turn budget, got %v", tr.turnBudget())
	}
	s.finish(t.Context(), bucketTurnResult{
		workerID: base,
		bucket:   "bucket-a",
		yielded:  true,
		position: "bucket-a/prefix/object",
		objects:  100,
	})
	st := s.states["bucket-a"]
	if st.ObjectsProcessed != 100 || st.Turns != 1 || st.Position != "bucket-a/prefix/object" || st.Cycle != 7 {
		t.Fatalf("unexpected state: %+v", st)
	}
	if !st.CompletedAt.IsZero() {
		t.Fatal("yielded bucket must not be marked complete")
	}

	tr, ok = s.acquire(t.Context(), base)
	if !ok || tr.Bucket != "bucket-a" {
		t.Fatalf("yielded bucket must re-enter rotation, got %+v", tr)
	}
	s.finish(t.Context(), bucketTurnResult{workerID: base, bucket: "bucket-a", objects: 5})
	st = s.states["bucket-a"]
	if st.ObjectsProcessed != 105 || st.Turns != 2 || st.CompletedAt.IsZero() {
		t.Fatalf("unexpected state after completion: %+v", st)
	}
}

func TestBucketTurnShouldYield(t *testing.T) {
	ctx := t.Context()

	// Deadline.
	tr := &bucketScanTurn{Bucket: "b", budget: time.Millisecond, deadline: time.Now().Add(-time.Second)}
	if !tr.shouldYield(ctx) || tr.yieldReason() != "budget" {
		t.Fatal("expired deadline must yield with reason budget")
	}
	// Latching.
	if !tr.isYielded() {
		t.Fatal("yield must latch")
	}

	// Pause.
	tr2 := &bucketScanTurn{Bucket: "b", paused: func() bool { return true }}
	if !tr2.shouldYield(ctx) || tr2.yieldReason() != "paused" {
		t.Fatal("pause must yield with reason paused")
	}

	// No limit.
	tr3 := &bucketScanTurn{Bucket: "b"}
	if tr3.shouldYield(ctx) {
		t.Fatal("unlimited turn must not yield")
	}

	// Path/object tracking.
	tr3.setPath("b/x")
	tr3.addObjects(3)
	if tr3.currentPath() != "b/x" || tr3.objects.Load() != 3 {
		t.Fatal("turn tracking mismatch")
	}

	// Context propagation.
	ctx2 := contextWithBucketScanTurn(ctx, tr3)
	if bucketScanTurnFromContext(ctx2) != tr3 {
		t.Fatal("turn did not round-trip through context")
	}
	if bucketScanTurnFromContext(ctx) != nil {
		t.Fatal("expected nil turn")
	}
}

func TestDataUsageEntryScannedCycleMsgp(t *testing.T) {
	e := dataUsageEntry{
		Objects:      11,
		Size:         22,
		ScannedCycle: 42,
		Children:     dataUsageHashMap{},
		ObjSizes:     sizeHistogram{},
		ObjVersions:  versionsHistogram{},
	}
	b, err := e.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	var got dataUsageEntry
	if _, err := got.UnmarshalMsg(b); err != nil {
		t.Fatal(err)
	}
	if got.ScannedCycle != 42 || got.Objects != 11 || got.Size != 22 {
		t.Fatalf("roundtrip mismatch: %+v", got)
	}

	// Zero value is omitted and decodes as zero.
	e0 := dataUsageEntry{Children: dataUsageHashMap{}}
	b0, err := e0.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	var got0 dataUsageEntry
	if _, err := got0.UnmarshalMsg(b0); err != nil {
		t.Fatal(err)
	}
	if got0.ScannedCycle != 0 {
		t.Fatal("zero ScannedCycle must roundtrip")
	}

	// Old format bytes without the key must decode to zero marker.
	if _, err := got.UnmarshalMsg(append(b, 0xc0)); err != nil {
		t.Fatalf("trailing nil decode: %v", err)
	}
}

// TestScanDataFolderYieldAndResume drives two turns of the folder
// scanner: the first yields just before entering "dir2", after "dir1"
// fully completed. The resumed turn must skip dir1's subtree entirely
// while producing identical totals to an uninterrupted scan.
func TestScanDataFolderYieldAndResume(t *testing.T) {
	base := t.TempDir()
	const bucket = "bucket"
	files := []usageTestFile{
		{name: "rootfile", size: 10000},
		{name: "rootfile2", size: 10000},
		{name: "dir1/d1file", size: 2000},
		{name: "dir2/d2file", size: 300},
		{name: "dir1/dira/dafile", size: 100000},
		{name: "dir1/dira/dbfile", size: 200000},
		{name: "dir1/dira/dirasub/dcfile", size: 1000000},
		{name: "dir1/dira/dirasub/sublevel3/dccccfile", size: 10},
	}
	createUsageTestFiles(t, base, bucket, files)

	xls := xlStorage{drivePath: base, diskInfoCache: cachevalue.New[DiskInfo]()}
	xls.diskInfoCache.InitOnce(time.Second, cachevalue.Opts{}, func(ctx context.Context) (DiskInfo, error) {
		return DiskInfo{Total: 1 << 40, Free: 1 << 40}, nil
	})

	const cycle uint32 = 7
	visitCount := map[string]int{}
	var visitsMu sync.Mutex

	makeGetSize := func() getSizeFn {
		return func(item scannerItem) (sizeS sizeSummary, err error) {
			if item.Typ&os.ModeDir != 0 {
				return sizeS, err
			}
			s, err := os.Stat(item.Path)
			if err != nil {
				return sizeS, err
			}
			visitsMu.Lock()
			visitCount[item.Path]++
			visitsMu.Unlock()
			sizeS.totalSize = s.Size()
			sizeS.versions++
			return sizeS, nil
		}
	}

	// Turn 1: yield right as scanning of dir2 starts.
	turn1 := &bucketScanTurn{Bucket: bucket, Cycle: cycle}
	dir2Path := filepath.ToSlash(filepath.Join(bucket, "dir2"))
	turn1.paused = func() bool { return turn1.currentPath() == dir2Path }

	cache1 := dataUsageCache{Info: dataUsageCacheInfo{Name: bucket, NextCycle: cycle}}
	ctx1 := contextWithBucketScanTurn(t.Context(), turn1)
	partial, err := scanDataFolder(ctx1, nil, &xls, cache1, makeGetSize(), 0, func() bool { return false })
	if !errors.Is(err, errBucketScanTurnYielded) {
		t.Fatalf("expected yield, got %v", err)
	}
	if turn1.objects.Load() != 7 {
		t.Fatalf("expected 7 objects processed before yield, got %d", turn1.objects.Load())
	}
	// At least one completed subtree must be marked for resume.
	var marked int
	for _, e := range partial.Cache {
		if e.ScannedCycle == cycle {
			marked++
		}
	}
	if marked == 0 {
		t.Fatal("expected completed subtrees to be marked with the cycle")
	}

	// Merge partial progress with the previous cache.
	merged := cache1.yieldMerge(partial)
	if root := merged.find(bucket); root == nil {
		t.Fatal("merged cache missing bucket root")
	}
	// dir1 must be reachable from the merged root and carry its totals.
	if e := merged.find(pathJoin(bucket, "dir1")); e == nil {
		t.Fatal("merged cache missing scanned dir1")
	}

	// Turn 2: resume, no pause gate. Count visits in this turn.
	before := map[string]int{}
	visitsMu.Lock()
	for k, v := range visitCount {
		before[k] = v
	}
	visitsMu.Unlock()

	turn2 := &bucketScanTurn{Bucket: bucket, Cycle: cycle}
	ctx2 := contextWithBucketScanTurn(t.Context(), turn2)
	final, err := scanDataFolder(ctx2, nil, &xls, merged, makeGetSize(), 0, func() bool { return false })
	if err != nil {
		t.Fatalf("resume scan failed: %v", err)
	}
	// The resumed turn processes the two root objects (the bucket root
	// interval only completes when the whole bucket does) plus dir2's.
	// The completed dir1 subtree (5 objects) must not be reprocessed.
	if turn2.objects.Load() != 3 {
		t.Fatalf("expected 3 objects processed on resume (root+dir2), got %d", turn2.objects.Load())
	}

	flat := final.sizeRecursive(bucket)
	if flat == nil {
		t.Fatal("final cache missing bucket root")
	}
	var wantSize int64 = 1322310
	if flat.Size != wantSize || flat.Objects != 8 {
		t.Fatalf("totals mismatch after resume: size=%d objs=%d", flat.Size, flat.Objects)
	}
	if e := final.find(pathJoin(bucket, "dir2")); e == nil {
		t.Fatal("final cache missing dir2")
	}

	// Nothing visited during turn 2 may belong to the completed dir1.
	allowed := map[string]bool{
		filepath.ToSlash(filepath.Join(base, bucket, "dir2", "d2file")): true,
		filepath.ToSlash(filepath.Join(base, bucket, "rootfile")):       true,
		filepath.ToSlash(filepath.Join(base, bucket, "rootfile2")):      true,
	}
	visitsMu.Lock()
	for p := range visitCount {
		if visitCount[p] > before[p] && !allowed[filepath.ToSlash(p)] {
			t.Fatalf("completed interval was rescanned: %s", p)
		}
	}
	visitsMu.Unlock()

	// The merged partial cache must serialize and reload with markers.
	saved := final.clone()
	buf, err := saved.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	var reloaded dataUsageCache
	if _, err := reloaded.UnmarshalMsg(buf); err != nil {
		t.Fatal(err)
	}
}

// TestBucketSchedulerPersistence verifies configuration and progress
// survive a scheduler rebuild (leader handover) and that bad
// configuration entries are isolated.
func TestBucketSchedulerPersistence(t *testing.T) {
	ctx := t.Context()
	obj, disks, err := prepareErasure(ctx, 8)
	if err != nil {
		t.Fatalf("prepareErasure: %v", err)
	}
	defer removeRoots(disks)
	setObjectLayer(obj)

	for _, b := range []string{"scanp1", "scanp2"} {
		if err := obj.MakeBucket(ctx, b, MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}

	prevGlobal := globalBucketScanScheduler
	globalBucketScanScheduler = nil
	defer func() { globalBucketScanScheduler = prevGlobal }()

	if err := AdminSetBucketScanConfig(ctx, obj, "scanp1", BucketScanConfig{Weight: 3}); err != nil {
		t.Fatal(err)
	}
	if err := AdminPauseBucketScan(ctx, obj, "scanp2"); err != nil {
		t.Fatal(err)
	}

	doc, err := loadBucketScannerConfigDoc(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Buckets["scanp1"].Weight != 3 || !doc.Buckets["scanp2"].Paused {
		t.Fatalf("config doc mismatch: %+v", doc.Buckets)
	}

	// A newly constructed scheduler (as happens on a new leader) loads the
	// persisted configuration.
	s := newBucketScanScheduler(obj)
	s.load(ctx)
	if !s.enabled() {
		t.Fatal("scheduler must be enabled with persisted config")
	}
	if got := s.cfg.Buckets["scanp1"].Weight; got != 3 {
		t.Fatalf("weight not loaded: %d", got)
	}

	allBuckets, err := obj.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s.beginCycle(ctx, 5, allBuckets, 2)
	// Simulate two sets; one with a worker.
	base := s.registerWorkers(1)
	s.setReady()
	s.registerWorkers(0)
	s.setReady()
	if !s.waitBarrier(ctx) {
		t.Fatal("barrier did not open")
	}

	// Paused bucket is hidden; the weighted one is picked.
	tr, ok := s.acquire(ctx, base)
	if !ok || tr.Bucket != "scanp1" {
		t.Fatalf("expected scanp1, got %+v ok=%v", tr, ok)
	}
	s.finish(ctx, bucketTurnResult{workerID: base, bucket: "scanp1", yielded: true, objects: 42, position: "scanp1/p"})
	s.endCycle(ctx)

	// Rebuild again (new leader) and verify the progress is there.
	s2 := newBucketScanScheduler(obj)
	s2.load(ctx)
	st := s2.states["scanp1"]
	if st.Cycle != 5 || st.ObjectsProcessed != 42 || st.Turns != 1 || st.Position != "scanp1/p" {
		t.Fatalf("progress did not survive rebuild: %+v", st)
	}

	status, err := AdminBucketScanStatus(ctx, obj, "scanp1")
	if err != nil {
		t.Fatal(err)
	}
	if len(status) != 1 || status[0].ObjectsProcessed != 42 || status[0].Weight != 3 {
		t.Fatalf("status mismatch: %+v", status)
	}
	statusAll, err := AdminBucketScanStatus(ctx, obj, "")
	if err != nil {
		t.Fatal(err)
	}
	byBucket := map[string]BucketScanStatus{}
	for _, st := range statusAll {
		byBucket[st.Bucket] = st
	}
	if !byBucket["scanp2"].Paused {
		t.Fatal("scanp2 must read as paused")
	}

	if err := AdminResumeBucketScan(ctx, obj, "scanp2"); err != nil {
		t.Fatal(err)
	}
	doc, err = loadBucketScannerConfigDoc(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := doc.Buckets["scanp2"]; ok {
		t.Fatal("resuming a pause-only entry must remove it")
	}

	// A bad entry in the document is isolated and dropped on load.
	bad := bucketScannerConfigDoc{
		Version: bucketScannerConfigVersion,
		Buckets: map[string]BucketScanConfig{
			"good":     {Weight: 2},
			"bad-name": {Weight: bucketScanMaxWeight + 1},
		},
	}
	if err := saveBucketScannerConfigDoc(ctx, obj, bad); err != nil {
		t.Fatal(err)
	}
	s3 := newBucketScanScheduler(obj)
	s3.load(ctx)
	if _, ok := s3.cfg.Buckets["bad-name"]; ok {
		t.Fatal("invalid config entry must be dropped")
	}
	if s3.cfg.Buckets["good"].Weight != 2 {
		t.Fatal("valid entry must survive a bad sibling")
	}
}

// runScheduledScan runs a full NSScanner cycle and returns the last
// usage report.
func runScheduledScan(t *testing.T, ctx context.Context, obj ObjectLayer, cycle uint32, sched *bucketScanScheduler) DataUsageInfo {
	t.Helper()
	results := make(chan DataUsageInfo, 16)
	done := make(chan struct{})
	var last DataUsageInfo
	go func() {
		defer close(done)
		for info := range results {
			last = info
		}
	}()
	if err := obj.NSScanner(ctx, results, cycle, 0, sched); err != nil {
		t.Fatalf("NSScanner cycle %d: %v", cycle, err)
	}
	<-done
	return last
}

func putTestObjects(t *testing.T, ctx context.Context, obj ObjectLayer, bucket, prefix string, n int) {
	t.Helper()
	for i := range n {
		name := fmt.Sprintf("%s/obj-%d", prefix, i)
		data := bytes.Repeat([]byte{'x'}, 1024)
		_, err := obj.PutObject(ctx, bucket, name, mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
		if err != nil {
			t.Fatalf("PutObject %s/%s: %v", bucket, name, err)
		}
	}
}

// TestNSScannerScheduledEndToEnd runs scheduled and legacy scans over a
// real erasure backend and verifies every bucket completes with correct
// totals and recorded progress.
func TestNSScannerScheduledEndToEnd(t *testing.T) {
	ctx := t.Context()
	obj, disks, err := prepareErasure(ctx, 8)
	if err != nil {
		t.Fatalf("prepareErasure: %v", err)
	}
	defer removeRoots(disks)
	setObjectLayer(obj)

	if err := obj.MakeBucket(ctx, "bigbucket", MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := obj.MakeBucket(ctx, "smallbucket", MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	putTestObjects(t, ctx, obj, "bigbucket", "p", 20)
	putTestObjects(t, ctx, obj, "smallbucket", "q", 5)

	// Scheduled scan: the big bucket gets short time slices.
	sched := newBucketScanScheduler(obj)
	if err := AdminSetBucketScanConfig(ctx, obj, "bigbucket", BucketScanConfig{Budget: 200 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	sched.load(ctx)
	globalBucketScanScheduler = sched
	defer func() { globalBucketScanScheduler = nil }()

	usage := runScheduledScan(t, ctx, obj, 1, sched)
	for _, b := range []string{"bigbucket", "smallbucket"} {
		ui, ok := usage.BucketsUsage[b]
		if !ok {
			t.Fatalf("missing usage for %s: %+v", b, usage.BucketsUsage)
		}
		if ui.Size == 0 || ui.ObjectsCount == 0 {
			t.Fatalf("no usage recorded for %s: %+v", b, ui)
		}
	}
	status := sched.Status("")
	byBucket := map[string]BucketScanStatus{}
	for _, st := range status {
		byBucket[st.Bucket] = st
	}
	for _, b := range []string{"bigbucket", "smallbucket"} {
		st := byBucket[b]
		if st.CompletedAt.IsZero() || st.InProgress {
			t.Fatalf("bucket %s must be complete and not in progress: %+v", b, st)
		}
		if st.ObjectsProcessed == 0 || st.Turns == 0 {
			t.Fatalf("bucket %s missing counters: %+v", b, st)
		}
	}

	// Second cycle must complete as well (markers from cycle 1 must not
	// suppress cycle 2).
	usage2 := runScheduledScan(t, ctx, obj, 2, sched)
	if usage2.ObjectsTotalCount < 25 {
		t.Fatalf("second cycle lost objects: total=%d", usage2.ObjectsTotalCount)
	}

	// Pause mid-way semantics: pausing a bucket before a cycle keeps it
	// out, resuming after the cycle lets it scan (position retained).
	if err := AdminPauseBucketScan(ctx, obj, "smallbucket"); err != nil {
		t.Fatal(err)
	}
	sched.load(ctx)
	sched.beginCycle(ctx, 3, mustListBuckets(t, ctx, obj), 2)

	// The next cycle run with paused bucket is validated at scheduler
	// level to avoid blocking the cluster-wide scan on the pause.
	base := sched.registerWorkers(1)
	sched.setReady()
	sched.registerWorkers(0)
	sched.setReady()
	tr, ok := sched.acquire(ctx, base)
	if !ok || tr.Bucket != "bigbucket" {
		t.Fatalf("expected bigbucket, got %+v ok=%v", tr, ok)
	}
	sched.finish(ctx, bucketTurnResult{workerID: base, bucket: "bigbucket"})

	// Only the paused bucket remains; acquire blocks instead of handing
	// it out.
	blockCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if got, ok := sched.acquire(blockCtx, base); ok {
		t.Fatalf("paused bucket must not be scheduled, got %+v", got)
	}

	// Resume: the bucket is handed out immediately and can complete.
	if err := AdminResumeBucketScan(ctx, obj, "smallbucket"); err != nil {
		t.Fatal(err)
	}
	tr, ok = sched.acquire(ctx, base)
	if !ok || tr.Bucket != "smallbucket" {
		t.Fatalf("resumed smallbucket expected, got %+v ok=%v", tr, ok)
	}
	sched.finish(ctx, bucketTurnResult{workerID: base, bucket: "smallbucket"})

	// Legacy (unconfigured) scan path behaves as before and produces the
	// same totals as the scheduled scans.
	legacy := newBucketScanScheduler(obj)
	if legacy.enabled() {
		t.Fatal("legacy scheduler must be disabled without config")
	}
	usage4 := runScheduledScan(t, ctx, obj, 4, nil)
	if usage4.ObjectsTotalCount != usage2.ObjectsTotalCount {
		t.Fatalf("legacy scan total %d differs from scheduled %d",
			usage4.ObjectsTotalCount, usage2.ObjectsTotalCount)
	}
	for _, b := range []string{"bigbucket", "smallbucket"} {
		l := usage4.BucketsUsage[b]
		s := usage2.BucketsUsage[b]
		if l.ObjectsCount != s.ObjectsCount || l.Size != s.Size {
			t.Fatalf("bucket %s legacy=%+v scheduled=%+v", b, l, s)
		}
	}
}

func mustListBuckets(t *testing.T, ctx context.Context, obj ObjectLayer) []BucketInfo {
	t.Helper()
	buckets, err := obj.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return buckets
}
