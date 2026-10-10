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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/minio/madmin-go/v3"
	"github.com/minio/minio/internal/cachevalue"
	"github.com/tinylib/msgp/msgp"
)

// resetScannerLifecycleForTest forces the global lifecycle back to a clean
// idle state regardless of how the previous test left it.
func resetScannerLifecycleForTest(t *testing.T) {
	t.Helper()
	l := globalScannerLifecycle
	l.beginCycle(0, time.Now(), false)
	l.finishCycle(true, time.Now(), "", "")
	if got := l.Status(); got.Phase != scannerPhaseIdle || l.gate.Load() != scannerGateProceed {
		t.Fatalf("lifecycle not reset: %+v", got)
	}
}

func TestScannerLifecyclePauseResumeStop(t *testing.T) {
	resetScannerLifecycleForTest(t)
	l := globalScannerLifecycle
	ctx := t.Context()

	l.beginCycle(1, time.Now(), false)
	defer l.finishCycle(true, time.Now(), "", "")

	// Fast path: boundary does not block while running.
	if err := l.waitAtFolderBoundary(ctx, "bucket/dir"); err != nil {
		t.Fatalf("boundary blocked while running: %v", err)
	}

	// Pause waits until a scan goroutine actually reaches a boundary.
	pauseErr := make(chan error, 1)
	go func() { pauseErr <- PauseScannerCycle(ctx, "unit-pause") }()
	select {
	case err := <-pauseErr:
		t.Fatalf("pause returned before any boundary was reached: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	parked := make(chan error, 1)
	go func() {
		parked <- l.waitAtFolderBoundary(context.Background(), "bucket/dir2")
	}()
	select {
	case err := <-pauseErr:
		if err != nil {
			t.Fatalf("pause: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pause did not settle at a boundary")
	}

	st := l.Status()
	if st.Phase != scannerPhasePaused {
		t.Fatalf("phase = %q, want paused", st.Phase)
	}
	if !st.Active || st.Cycle != 1 || st.Reason != "unit-pause" {
		t.Fatalf("unexpected status: %+v", st)
	}
	select {
	case <-parked:
		t.Fatal("scan goroutine did not park at the paused boundary")
	case <-time.After(50 * time.Millisecond):
	}

	// Resume unblocks the boundary.
	if err := ResumeScannerCycle(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	select {
	case err := <-parked:
		if err != nil {
			t.Fatalf("boundary after resume: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("scan goroutine stayed parked after resume")
	}
	if got := l.Status(); got.Phase != scannerPhaseRunning {
		t.Fatalf("phase = %q, want running", got.Phase)
	}

	// Termination releases boundaries with the sentinel error.
	if err := StopScannerCycle("unit-stop"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if err := l.waitAtBucketBoundary(ctx, "bucket"); !errors.Is(err, errScannerCycleStopped) {
		t.Fatalf("boundary err = %v, want errScannerCycleStopped", err)
	}
	if got := l.Status(); got.Phase != scannerPhaseStopping || got.Reason != "unit-stop" {
		t.Fatalf("unexpected status after stop: %+v", got)
	}
}

func TestScannerLifecyclePendingPauseAndBatches(t *testing.T) {
	resetScannerLifecycleForTest(t)
	l := globalScannerLifecycle
	ctx := t.Context()

	// Pause requested while idle applies to the next cycle.
	if err := PauseScannerCycle(ctx, "pending"); err != nil {
		t.Fatalf("pending pause: %v", err)
	}
	l.beginCycle(3, time.Now(), true)
	defer l.finishCycle(true, time.Now(), "", "")
	if l.gate.Load() != scannerGatePause {
		t.Fatalf("gate = %d, want pause gate", l.gate.Load())
	}
	if !l.isResumingCycle(3) {
		t.Fatal("cycle 3 should be resuming")
	}
	if l.isResumingCycle(4) {
		t.Fatal("cycle 4 must not be resuming")
	}

	reached := make(chan error, 1)
	go func() { reached <- l.waitAtBucketBoundary(context.Background(), "b1") }()
	select {
	case <-reached:
		t.Fatal("new cycle should have parked at its first bucket boundary")
	case <-time.After(50 * time.Millisecond):
	}
	if got := l.Status(); got.Phase != scannerPhasePaused {
		t.Fatalf("phase = %q, want paused", got.Phase)
	}
	if err := ResumeScannerCycle(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	select {
	case err := <-reached:
		if err != nil {
			t.Fatalf("boundary: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("boundary stayed parked after resume")
	}

	// Batch progress accounting.
	l.beginCycle(4, time.Now(), false)
	l.addTotalBatches(2)
	l.addTotalBatches(1)
	l.completeBatch("a")
	l.completeBatch("b")
	st := l.Status()
	if st.TotalBatches != 3 || st.CompletedBatches != 2 || st.RemainingBatches != 1 {
		t.Fatalf("batch counts = %+v, want 3/2/1", st)
	}
	if st.LastCompleted.IsZero() {
		t.Fatal("LastCompleted should be exposed after a completed cycle")
	}
}

// lifecycleTestGetSize mirrors the getSize stub used by TestDataUsageUpdate.
func lifecycleTestGetSize(item scannerItem) (sizeS sizeSummary, err error) {
	if item.Typ&os.ModeDir == 0 {
		var s os.FileInfo
		s, err = os.Stat(item.Path)
		if err != nil {
			return sizeS, err
		}
		sizeS.totalSize = s.Size()
		sizeS.versions++
		return sizeS, nil
	}
	return sizeS, err
}

func lifecycleTestStorage(base string) *xlStorage {
	xls := &xlStorage{drivePath: base, diskInfoCache: cachevalue.New[DiskInfo]()}
	xls.diskInfoCache.InitOnce(time.Second, cachevalue.Opts{}, func(context.Context) (DiskInfo, error) {
		return DiskInfo{Total: 1 << 40, Free: 1 << 40}, nil
	})
	return xls
}

func TestScannerPauseResumeScanFolder(t *testing.T) {
	resetScannerLifecycleForTest(t)
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
	xls := lifecycleTestStorage(base)

	l := globalScannerLifecycle
	// Arm the pause before the scan starts so the cycle parks at its very
	// first directory boundary deterministically.
	if err := PauseScannerCycle(t.Context(), "scan-pending"); err != nil {
		t.Fatal(err)
	}
	l.beginCycle(7, time.Now(), false)
	defer l.finishCycle(true, time.Now(), "", "")

	type scanResult struct {
		cache dataUsageCache
		err   error
	}
	resCh := make(chan scanResult, 1)
	go func() {
		c, err := scanDataFolder(context.Background(), nil, xls,
			dataUsageCache{Info: dataUsageCacheInfo{Name: bucket}},
			lifecycleTestGetSize, 0, func() bool { return false })
		resCh <- scanResult{cache: c, err: err}
	}()

	if err := PauseScannerCycle(t.Context(), "scan-paused"); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if got := l.Status(); got.Phase != scannerPhasePaused {
		t.Fatalf("phase = %q, want paused", got.Phase)
	}
	// Already scanned data is kept; the scan must remain parked, not return.
	select {
	case r := <-resCh:
		t.Fatalf("scan returned while paused: %+v", r)
	case <-time.After(100 * time.Millisecond):
	}

	if err := ResumeScannerCycle(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	select {
	case r := <-resCh:
		if r.err != nil {
			t.Fatalf("scan after resume: %v", r.err)
		}
		e := r.cache.find(bucket)
		if e == nil {
			t.Fatal("bucket entry missing")
		}
		flat := r.cache.flatten(*e)
		if flat.Size != 1322310 || flat.Objects != 8 {
			t.Fatalf("totals after resumed scan = size:%d objects:%d, want 1322310/8", flat.Size, flat.Objects)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("scan did not complete after resume")
	}
}

func TestScannerStopScanFolder(t *testing.T) {
	resetScannerLifecycleForTest(t)
	base := t.TempDir()
	const bucket = "bucket"
	createUsageTestFiles(t, base, bucket, []usageTestFile{
		{name: "rootfile", size: 10000},
		{name: "dir1/d1file", size: 2000},
	})
	xls := lifecycleTestStorage(base)

	l := globalScannerLifecycle
	l.beginCycle(8, time.Now(), false)
	defer l.finishCycle(false, time.Now(), "stop-test", bucket)
	if err := StopScannerCycle("stop-test"); err != nil {
		t.Fatal(err)
	}
	_, err := scanDataFolder(context.Background(), nil, xls,
		dataUsageCache{Info: dataUsageCacheInfo{Name: bucket}},
		lifecycleTestGetSize, 0, func() bool { return false })
	if !errors.Is(err, errScannerCycleStopped) {
		t.Fatalf("scan err = %v, want errScannerCycleStopped", err)
	}
	st := l.Status()
	if st.Phase != scannerPhaseStopping {
		t.Fatalf("phase = %q, want stopping", st.Phase)
	}
}

func TestCurrentScannerCycleLifecycleMsgp(t *testing.T) {
	t1 := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	t2 := time.Now().UTC().Truncate(time.Second)
	want := currentScannerCycle{
		current:        3,
		next:           4,
		started:        t1,
		cycleCompleted: []time.Time{t2},
		state:          scannerCycleStatePaused,
		reason:         "maintenance",
		position:       "bucket/dir1",
		interruptedAt:  t2,
	}
	buf, err := want.MarshalMsg(nil)
	if err != nil {
		t.Fatal(err)
	}
	var got currentScannerCycle
	left, err := got.UnmarshalMsg(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("%d trailing bytes", len(left))
	}
	if got.current != want.current || got.next != want.next ||
		!got.started.Equal(want.started) ||
		len(got.cycleCompleted) != 1 || !got.cycleCompleted[0].Equal(t2) ||
		got.state != want.state || got.reason != want.reason ||
		got.position != want.position || !got.interruptedAt.Equal(want.interruptedAt) {
		t.Fatalf("roundtrip mismatch:\n got  %+v\n want %+v", got, want)
	}

	// An old 4-field payload (no lifecycle fields) must decode cleanly.
	old := []byte{0x84}
	old = append(old, 0xa7)
	old = append(old, "current"...)
	old = msgp.AppendUint64(old, 9)
	old = append(old, 0xa4)
	old = append(old, "next"...)
	old = msgp.AppendUint64(old, 10)
	old = append(old, 0xa7)
	old = append(old, "started"...)
	old = msgp.AppendTime(old, t1)
	old = append(old, 0xae)
	old = append(old, "cycleCompleted"...)
	old = msgp.AppendArrayHeader(old, 0)
	var legacy currentScannerCycle
	if _, err = legacy.UnmarshalMsg(old); err != nil {
		t.Fatalf("legacy payload: %v", err)
	}
	if legacy.current != 9 || legacy.next != 10 || legacy.state != "" {
		t.Fatalf("legacy decode mismatch: %+v", legacy)
	}
}

func TestDataUsageCacheLastScanCycleRoundtrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	src := dataUsageCache{
		Info: dataUsageCacheInfo{
			Name:          "bucket",
			NextCycle:     3,
			LastUpdate:    now,
			SkipHealing:   true,
			LastScanCycle: 3,
		},
		Cache: map[string]dataUsageEntry{
			"bucket": {Size: 1234, Objects: 2},
		},
	}
	var buf bytes.Buffer
	if err := src.serializeTo(&buf); err != nil {
		t.Fatal(err)
	}
	var got dataUsageCache
	if err := got.deserialize(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	if got.Info.Name != "bucket" || got.Info.NextCycle != 3 || !got.Info.SkipHealing {
		t.Fatalf("info mismatch: %+v", got.Info)
	}
	if got.Info.LastScanCycle != 3 {
		t.Fatalf("LastScanCycle = %d, want 3 (resume checkpoint lost)", got.Info.LastScanCycle)
	}
	if !got.Info.LastUpdate.Equal(now) {
		t.Fatalf("LastUpdate = %v, want %v", got.Info.LastUpdate, now)
	}
}

func TestStoreDataUsageStagingAndPublish(t *testing.T) {
	objAPI, disks, err := prepareErasure(t.Context(), 8)
	if err != nil {
		t.Fatalf("prepareErasure: %v", err)
	}
	defer removeRoots(disks)
	setObjectLayer(objAPI)
	ctx := t.Context()

	updates := make(chan dataUsageInfoUpdate)
	done := make(chan struct{})
	go func() {
		storeDataUsageInBackend(ctx, objAPI, updates)
		close(done)
	}()

	partial := DataUsageInfo{LastUpdate: time.Now(), ObjectsTotalCount: 42}
	updates <- dataUsageInfoUpdate{info: partial, final: false}

	// Partial result is staged only; the readable usage stays absent/old.
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, err := readConfig(ctx, objAPI, dataUsagePartialObjNamePath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("partial result was not staged")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := readConfig(ctx, objAPI, dataUsageObjNamePath); !errors.Is(err, errConfigNotFound) {
		t.Fatalf("readable usage exposed a partial round: %v", err)
	}

	// Final result of the complete round gets published.
	final := DataUsageInfo{LastUpdate: time.Now(), ObjectsTotalCount: 84}
	updates <- dataUsageInfoUpdate{info: final, final: true}
	close(updates)
	<-done

	published, err := loadDataUsageFromBackend(ctx, objAPI)
	if err != nil {
		t.Fatalf("load published usage: %v", err)
	}
	if published.ObjectsTotalCount != 84 {
		t.Fatalf("published objects = %d, want 84 (last complete round)", published.ObjectsTotalCount)
	}
}

// TestNSScannerResumeSkipsCompletedBatches drives a real erasure set: the
// first (interrupted) attempt leaves per-bucket completion markers for its
// cycle number; resuming with the same number must skip those buckets instead
// of rescanning them while keeping their totals in the merged result.
func TestNSScannerResumeSkipsCompletedBatches(t *testing.T) {
	resetScannerLifecycleForTest(t)
	ctx := t.Context()

	objAPI, disks, err := prepareErasure(ctx, 8)
	if err != nil {
		t.Fatalf("prepareErasure: %v", err)
	}
	defer removeRoots(disks)
	setObjectLayer(objAPI)
	resetGlobalHealState()
	globalBucketMetadataSys = NewBucketMetadataSys()
	globalBucketObjectLockSys = &BucketObjectLockSys{}
	globalBucketVersioningSys = &BucketVersioningSys{}

	const (
		bucketA = "resume-bucket-a"
		bucketB = "resume-bucket-b"
		cycle   = uint32(5)
	)
	for _, bucket := range []string{bucketA, bucketB} {
		if err = objAPI.MakeBucket(ctx, bucket, MakeBucketOptions{}); err != nil {
			t.Fatalf("MakeBucket(%s): %v", bucket, err)
		}
		for i := range 3 {
			data := bytes.Repeat([]byte{byte('a' + i)}, 512)
			_, err = objAPI.PutObject(ctx, bucket, "obj-"+string(rune('a'+i)),
				mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""), ObjectOptions{})
			if err != nil {
				t.Fatalf("PutObject: %v", err)
			}
		}
	}
	buckets, err := objAPI.ListBuckets(ctx, BucketOptions{})
	if err != nil {
		t.Fatal(err)
	}

	pools, ok := objAPI.(*erasureServerPools)
	if !ok {
		t.Fatalf("unexpected object layer type %T", objAPI)
	}
	er := pools.serverPools[0].sets[0]

	runSetScan := func() ([]dataUsageCache, error) {
		updates := make(chan dataUsageCache, 64)
		var mu sync.Mutex
		var collected []dataUsageCache
		done := make(chan struct{})
		go func() {
			defer close(done)
			for c := range updates {
				mu.Lock()
				collected = append(collected, c.clone())
				mu.Unlock()
			}
		}()
		err := er.nsScanner(ctx, buckets, cycle, updates, madmin.HealNormalScan)
		// nsScanner does not own the channel; let the drainer see the
		// buffered updates and stop.
		time.Sleep(50 * time.Millisecond)
		close(updates)
		<-done
		mu.Lock()
		defer mu.Unlock()
		return collected, err
	}

	// Attempt A: a normal (in production: interrupted just before completion)
	// run persists per-bucket markers for cycle 5.
	globalScannerLifecycle.beginCycle(uint64(cycle), time.Now(), false)
	defer globalScannerLifecycle.finishCycle(true, time.Now(), "", "")
	collectedA, err := runSetScan()
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if st := globalScannerLifecycle.Status(); st.CompletedBatches != 2 || st.RemainingBatches != 0 {
		t.Fatalf("attempt A batches = %+v, want 2 completed", st)
	}
	lastA := collectedA[len(collectedA)-1]
	for _, b := range []string{bucketA, bucketB} {
		if lastA.find(b) == nil {
			t.Fatalf("attempt A result missing bucket %s", b)
		}
	}

	// Attempt B: cycle 5 resumes. Marker buckets must be skipped without
	// scanning a single object, while their totals are replayed.
	globalScannerLifecycle.beginCycle(uint64(cycle), time.Now(), true)
	collectedB, err := runSetScan()
	if err != nil {
		t.Fatalf("resumed scan: %v", err)
	}
	st := globalScannerLifecycle.Status()
	if !st.Resuming {
		t.Fatal("resumed cycle not marked as resuming")
	}
	if st.CompletedBatches != 2 || st.RemainingBatches != 0 {
		t.Fatalf("resumed batches = %+v, want 2 skipped batches", st)
	}
	if st.ObjectsProcessed != 0 {
		t.Fatalf("resumed scan processed %d objects; completed buckets must not be rescanned", st.ObjectsProcessed)
	}
	lastB := collectedB[len(collectedB)-1]
	for _, b := range []string{bucketA, bucketB} {
		e := lastB.find(b)
		if e == nil {
			t.Fatalf("resumed result dropped bucket %s totals", b)
		}
		flat := lastB.flatten(*e)
		if flat.Objects != 3 {
			t.Fatalf("bucket %s objects after resume = %d, want 3", b, flat.Objects)
		}
	}

	// Attempt C: cycle 6 is a fresh number; markers no longer match so the
	// bucket batches are processed again (compact leaves may be skipped by
	// the bloom filter, which is unrelated to the resume checkpoint).
	globalScannerLifecycle.beginCycle(6, time.Now(), false)
	updates := make(chan dataUsageCache, 8)
	go func() {
		for range updates {
		}
	}()
	if err = er.nsScanner(ctx, buckets, 6, updates, madmin.HealNormalScan); err != nil {
		t.Fatalf("fresh cycle scan: %v", err)
	}
	close(updates)
	if st = globalScannerLifecycle.Status(); st.CompletedBatches != 2 {
		t.Fatalf("fresh cycle batches = %+v, want 2 reprocessed batches", st)
	}
	// The fresh cycle moved every bucket's completion marker forward.
	for _, b := range []string{bucketA, bucketB} {
		var bc dataUsageCache
		if err = bc.load(ctx, er, pathJoin(b, dataUsageCacheName)); err != nil {
			t.Fatalf("load bucket cache %s: %v", b, err)
		}
		if bc.Info.LastScanCycle != 6 {
			t.Fatalf("bucket %s marker = %d, want 6 after fresh cycle", b, bc.Info.LastScanCycle)
		}
	}
}
