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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7/pkg/s3utils"
	xioutil "github.com/minio/minio/internal/ioutil"
)

// Per-bucket scan scheduling is driven by two small JSON documents stored
// next to the other scanner state under .minio.sys/buckets/:
//
//   - bucketScannerConfigPath: user supplied configuration (weights, turn
//     budgets and paused state).
//   - bucketScannerStatePath:  runtime progress maintained by the leader,
//     which is what allows a new leader to continue a cycle in progress
//     instead of starting it over.
//
// When no configuration document exists (the common case for existing
// deployments) the scanner takes the legacy path byte-for-byte: every
// bucket is scanned once per cycle, whole bucket at a time, in random
// order. The scheduler only activates once at least one bucket carries a
// non-default configuration.
const (
	bucketScannerConfigPath = bucketMetaPrefix + SlashSeparator + ".bucket-scanner.json"
	bucketScannerStatePath  = bucketMetaPrefix + SlashSeparator + ".bucket-scanner-state.json"

	bucketScannerConfigVersion = 1
	bucketScannerStateVersion  = 1

	// bucketScanMaxWeight bounds how large a share of a cycle a single
	// bucket can win through its weight.
	bucketScanMaxWeight uint32 = 100
	// bucketScanMinTurnBudget/bucketScanMaxTurnBudget bound the wall clock
	// time a single bucket may hold a scanner worker before rotating.
	bucketScanMinTurnBudget   = 100 * time.Millisecond
	bucketScanMaxTurnBudget   = 10 * time.Minute
	bucketScanDefaultBudget   = 10 * time.Second // used when only a weight is configured
	bucketScanStateSaveEvery  = 2 * time.Second  // throttle progress persistence
	bucketScanWaitNotifyEvery = time.Second      // re-check paused state periodically
)

// errBucketScanTurnYielded is returned by a bucket scan when a scheduler
// turn ended before the whole bucket was scanned (turn budget exhausted
// or the bucket got paused).
var errBucketScanTurnYielded = errors.New("bucket scan turn yielded before completion")

// BucketScanConfig is the per-bucket scheduling configuration supplied by
// the operator.
//
// The zero value means "no preference": default weight, a single
// uninterrupted scan per cycle (identical to the legacy scanner behavior).
type BucketScanConfig struct {
	// Weight controls the relative share of scanner turns a bucket wins
	// within a cycle. 0 means the default weight (1).
	Weight uint32 `json:"weight,omitempty"`
	// Budget is the maximum wall-clock duration of a single scanner turn
	// for the bucket. 0 means "scan the whole bucket in one turn" unless
	// Weight is set, in which case the default slice is used.
	Budget time.Duration `json:"budget,omitempty"`
	// Paused removes the bucket from rotation until it is resumed. Its
	// progress position is retained while paused.
	Paused bool `json:"paused,omitempty"`
}

// effectiveWeight returns the scheduling weight used by the round robin.
func (c BucketScanConfig) effectiveWeight() float64 {
	if c.Weight == 0 {
		return 1
	}
	return float64(c.Weight)
}

// effectiveBudget returns the wall-clock budget of a single scanner turn.
// A zero duration means the bucket is scanned uninterrupted (legacy
// behavior).
func (c BucketScanConfig) effectiveBudget() time.Duration {
	switch {
	case c.Budget > 0:
		return c.Budget
	case c.Weight > 0:
		return bucketScanDefaultBudget
	default:
		return 0
	}
}

// isDefault reports whether the configuration changes nothing.
func (c BucketScanConfig) isDefault() bool {
	return c.Weight == 0 && c.Budget == 0 && !c.Paused
}

// Validate rejects illegal weights, budgets and weight/budget combinations.
func (c BucketScanConfig) Validate() error {
	if c.Weight > bucketScanMaxWeight {
		return fmt.Errorf("bucket scan weight must be between 0 and %d, got %d", bucketScanMaxWeight, c.Weight)
	}
	if c.Budget != 0 && (c.Budget < bucketScanMinTurnBudget || c.Budget > bucketScanMaxTurnBudget) {
		return fmt.Errorf("bucket scan budget must be between %s and %s, got %s", bucketScanMinTurnBudget, bucketScanMaxTurnBudget, c.Budget)
	}
	// Weight and budget express the same preference in different units;
	// reject the combination to keep scheduling unambiguous.
	if c.Weight != 0 && c.Budget != 0 {
		return errors.New("bucket scan weight and budget cannot be set together; specify either one")
	}
	return nil
}

// bucketScannerConfigDoc is the on-disk representation of the operator
// configuration.
type bucketScannerConfigDoc struct {
	Version int                         `json:"version"`
	Buckets map[string]BucketScanConfig `json:"buckets"`
}

// Validate validates every bucket entry. It returns a map of per-bucket
// errors so that a single bad entry can be reported (and skipped) without
// invalidating the rest.
func (d *bucketScannerConfigDoc) Validate() map[string]error {
	errs := make(map[string]error)
	for bucket, cfg := range d.Buckets {
		if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
			errs[bucket] = err
			continue
		}
		if err := cfg.Validate(); err != nil {
			errs[bucket] = err
		}
	}
	return errs
}

// bucketScanState is the persisted runtime progress of one bucket.
type bucketScanState struct {
	// Cycle is the scanner cycle this state belongs to.
	Cycle uint32 `json:"cycle"`
	// Position is the last path visited inside the bucket.
	Position string `json:"position"`
	// ObjectsProcessed is a lifetime counter of objects processed.
	ObjectsProcessed uint64 `json:"objectsProcessed"`
	// Turns is a lifetime counter of scanner turns consumed.
	Turns uint64 `json:"turns"`
	// StartedAt is when the bucket was first picked up in its current cycle.
	StartedAt time.Time `json:"startedAt,omitempty"`
	// UpdatedAt is when progress was last recorded.
	UpdatedAt time.Time `json:"updatedAt,omitempty"`
	// CompletedAt is when the bucket finished its most recent cycle.
	CompletedAt time.Time `json:"completedAt,omitempty"`
}

// bucketScannerStateDoc is the on-disk representation of runtime progress.
type bucketScannerStateDoc struct {
	Version   int                        `json:"version"`
	Cycle     uint32                     `json:"cycle"`
	Buckets   map[string]bucketScanState `json:"buckets"`
	UpdatedAt time.Time                  `json:"updatedAt"`
}

// BucketScanStatus is a queryable snapshot of a bucket's configuration and
// progress.
type BucketScanStatus struct {
	Bucket           string        `json:"bucket"`
	Cycle            uint32        `json:"cycle"`
	Weight           uint32        `json:"weight"`
	Budget           time.Duration `json:"budget"`
	Paused           bool          `json:"paused"`
	Position         string        `json:"position"`
	ObjectsProcessed uint64        `json:"objectsProcessed"`
	Turns            uint64        `json:"turns"`
	StartedAt        time.Time     `json:"startedAt,omitempty"`
	UpdatedAt        time.Time     `json:"updatedAt,omitempty"`
	CompletedAt      time.Time     `json:"completedAt,omitempty"`
	// InProgress is true while the bucket has not finished the current
	// cycle on every scanner worker.
	InProgress bool `json:"inProgress"`
}

// bucketScanTurn tracks one worker's time slice of one bucket.
//
// The turn is handed to the storage layer through the context (see
// contextWithBucketScanTurn). The folder scanner checks shouldYield at
// safe boundaries (between folders and periodically between objects), so
// a turn stops without leaving a partially mutated cache behind.
type bucketScanTurn struct {
	Bucket   string
	Cycle    uint32
	budget   time.Duration
	deadline time.Time

	// paused is consulted live on the leader node; it may be nil for
	// remote drives (which use a point-in-time snapshot).
	paused func() bool

	objects atomic.Uint64
	path    atomic.Pointer[string]
	yielded atomic.Bool
	reason  atomic.Pointer[string]
}

// budget returns the configured turn budget (0 means unlimited).
func (t *bucketScanTurn) turnBudget() time.Duration { return t.budget }

func (t *bucketScanTurn) addObjects(n uint64) {
	if t != nil && n != 0 {
		t.objects.Add(n)
	}
}

func (t *bucketScanTurn) setPath(p string) {
	if t != nil {
		t.path.Store(&p)
	}
}

func (t *bucketScanTurn) currentPath() string {
	if t == nil {
		return ""
	}
	if p := t.path.Load(); p != nil {
		return *p
	}
	return ""
}

func (t *bucketScanTurn) markYield(reason string) {
	if t.yielded.CompareAndSwap(false, true) {
		t.reason.Store(&reason)
	}
}

func (t *bucketScanTurn) yieldReason() string {
	if r := t.reason.Load(); r != nil {
		return *r
	}
	return ""
}

// isYielded reports whether the turn has been asked to stop.
func (t *bucketScanTurn) isYielded() bool {
	return t != nil && t.yielded.Load()
}

// shouldYield reports whether scanning should stop at the next safe
// boundary. Once it returns true it keeps returning true.
func (t *bucketScanTurn) shouldYield(ctx context.Context) bool {
	if t == nil {
		return false
	}
	if t.yielded.Load() {
		return true
	}
	switch {
	case ctx.Err() != nil:
		t.markYield("canceled")
	case !t.deadline.IsZero() && !time.Now().Before(t.deadline):
		t.markYield("budget")
	case t.paused != nil && t.paused():
		t.markYield("paused")
	}
	return t.yielded.Load()
}

type bucketScanTurnCtxType struct{}

var bucketScanTurnCtxKey = bucketScanTurnCtxType{}

// contextWithBucketScanTurn attaches a turn to the context.
func contextWithBucketScanTurn(ctx context.Context, t *bucketScanTurn) context.Context {
	if t == nil {
		return ctx
	}
	return context.WithValue(ctx, bucketScanTurnCtxKey, t)
}

// bucketScanTurnFromContext returns the turn attached to the context, if any.
func bucketScanTurnFromContext(ctx context.Context) *bucketScanTurn {
	t, _ := ctx.Value(bucketScanTurnCtxKey).(*bucketScanTurn)
	return t
}

// globalBucketScanScheduler is the leader-local scheduler instance. It is
// recreated by runDataScanner whenever the leader lock is acquired.
var globalBucketScanScheduler *bucketScanScheduler

func loadBucketScannerConfigDoc(ctx context.Context, objAPI ObjectLayer) (bucketScannerConfigDoc, error) {
	doc := bucketScannerConfigDoc{Version: bucketScannerConfigVersion, Buckets: map[string]BucketScanConfig{}}
	buf, err := readConfig(ctx, objAPI, bucketScannerConfigPath)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return doc, nil
		}
		return doc, err
	}
	if len(buf) == 0 {
		return doc, nil
	}
	if err := json.Unmarshal(buf, &doc); err != nil {
		return doc, err
	}
	if doc.Buckets == nil {
		doc.Buckets = map[string]BucketScanConfig{}
	}
	return doc, nil
}

func saveBucketScannerConfigDoc(ctx context.Context, objAPI ObjectLayer, doc bucketScannerConfigDoc) error {
	if doc.Version == 0 {
		doc.Version = bucketScannerConfigVersion
	}
	if doc.Buckets == nil {
		doc.Buckets = map[string]BucketScanConfig{}
	}
	buf, err := json.Marshal(&doc)
	if err != nil {
		return err
	}
	return saveConfig(ctx, objAPI, bucketScannerConfigPath, buf)
}

func loadBucketScannerStateDoc(ctx context.Context, objAPI ObjectLayer) (bucketScannerStateDoc, error) {
	doc := bucketScannerStateDoc{Version: bucketScannerStateVersion, Buckets: map[string]bucketScanState{}}
	buf, err := readConfig(ctx, objAPI, bucketScannerStatePath)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return doc, nil
		}
		return doc, err
	}
	if len(buf) == 0 {
		return doc, nil
	}
	if err := json.Unmarshal(buf, &doc); err != nil {
		return doc, err
	}
	if doc.Buckets == nil {
		doc.Buckets = map[string]bucketScanState{}
	}
	return doc, nil
}

// AdminSetBucketScanConfig validates and stores per-bucket scheduling
// configuration. It applies live when the scheduler runs on this node,
// otherwise it updates the persisted document picked up on the next cycle.
func AdminSetBucketScanConfig(ctx context.Context, objAPI ObjectLayer, bucket string, cfg BucketScanConfig) error {
	if s := globalBucketScanScheduler; s != nil {
		return s.SetConfig(ctx, bucket, cfg)
	}
	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	doc, err := loadBucketScannerConfigDoc(ctx, objAPI)
	if err != nil {
		return err
	}
	if cfg.isDefault() {
		delete(doc.Buckets, bucket)
	} else {
		doc.Buckets[bucket] = cfg
	}
	return saveBucketScannerConfigDoc(ctx, objAPI, doc)
}

// AdminPauseBucketScan pauses a bucket.
func AdminPauseBucketScan(ctx context.Context, objAPI ObjectLayer, bucket string) error {
	if s := globalBucketScanScheduler; s != nil {
		return s.Pause(ctx, bucket)
	}
	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		return err
	}
	doc, err := loadBucketScannerConfigDoc(ctx, objAPI)
	if err != nil {
		return err
	}
	cfg := doc.Buckets[bucket]
	cfg.Paused = true
	doc.Buckets[bucket] = cfg
	return saveBucketScannerConfigDoc(ctx, objAPI, doc)
}

// AdminResumeBucketScan resumes a paused bucket.
func AdminResumeBucketScan(ctx context.Context, objAPI ObjectLayer, bucket string) error {
	if s := globalBucketScanScheduler; s != nil {
		return s.Resume(ctx, bucket)
	}
	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		return err
	}
	doc, err := loadBucketScannerConfigDoc(ctx, objAPI)
	if err != nil {
		return err
	}
	cfg, ok := doc.Buckets[bucket]
	if !ok || !cfg.Paused {
		return nil
	}
	cfg.Paused = false
	if cfg.isDefault() {
		delete(doc.Buckets, bucket)
	} else {
		doc.Buckets[bucket] = cfg
	}
	return saveBucketScannerConfigDoc(ctx, objAPI, doc)
}

// AdminBucketScanStatus returns per-bucket progress and configuration.
// An empty bucket returns every known bucket.
func AdminBucketScanStatus(ctx context.Context, objAPI ObjectLayer, bucket string) ([]BucketScanStatus, error) {
	if s := globalBucketScanScheduler; s != nil {
		return s.Status(bucket), nil
	}
	cfgDoc, err := loadBucketScannerConfigDoc(ctx, objAPI)
	if err != nil {
		return nil, err
	}
	stateDoc, err := loadBucketScannerStateDoc(ctx, objAPI)
	if err != nil {
		return nil, err
	}
	names := []string{}
	if bucket != "" {
		if _, ok := stateDoc.Buckets[bucket]; !ok {
			if _, ok := cfgDoc.Buckets[bucket]; !ok {
				return nil, nil
			}
		}
		names = []string{bucket}
	} else {
		seen := map[string]struct{}{}
		for b := range stateDoc.Buckets {
			seen[b] = struct{}{}
			names = append(names, b)
		}
		for b := range cfgDoc.Buckets {
			if _, ok := seen[b]; !ok {
				names = append(names, b)
			}
		}
		sort.Strings(names)
	}
	out := make([]BucketScanStatus, 0, len(names))
	for _, b := range names {
		cfg := cfgDoc.Buckets[b]
		st := stateDoc.Buckets[b]
		weight := cfg.Weight
		if weight == 0 {
			weight = 1
		}
		out = append(out, BucketScanStatus{
			Bucket:           b,
			Cycle:            st.Cycle,
			Weight:           weight,
			Budget:           cfg.effectiveBudget(),
			Paused:           cfg.Paused,
			Position:         st.Position,
			ObjectsProcessed: st.ObjectsProcessed,
			Turns:            st.Turns,
			StartedAt:        st.StartedAt,
			UpdatedAt:        st.UpdatedAt,
			CompletedAt:      st.CompletedAt,
		})
	}
	return out, nil
}

// bucketTurnResult is the outcome of a worker turn.
type bucketTurnResult struct {
	workerID int
	bucket   string
	yielded  bool
	reason   string
	position string
	objects  uint64
	err      error
}

// bucketScanScheduler schedules scanner turns across buckets for a single
// scan cycle. Every set/disk worker registers with the scheduler and asks
// for the next bucket to work on; a bucket only leaves rotation once all
// registered workers completed it.
type bucketScanScheduler struct {
	objAPI ObjectLayer

	mu sync.Mutex

	// configuration
	cfg bucketScannerConfigDoc

	// runtime progress, persisted to bucketScannerStatePath
	cycle  uint32
	states map[string]bucketScanState

	// per-cycle scheduling state
	totalSets   int
	readySets   int
	workerCount int
	barrierOnce sync.Once
	barrier     chan struct{}

	// pending[B] holds the worker IDs that still need to fully scan B.
	pending map[string]map[int]struct{}
	// credits implement weighted deficit round robin.
	credits map[string]float64
	// ordered bucket names for deterministic tie-breaking.
	ordered []string
	// front buckets jump the queue (resumed buckets, etc.).
	front map[string]struct{}

	// wake is closed and recreated whenever eligibility may have changed
	// (pause/resume/config update).
	wake chan struct{}

	lastStateSave time.Time
}

func newBucketScanScheduler(objAPI ObjectLayer) *bucketScanScheduler {
	return &bucketScanScheduler{
		objAPI: objAPI,
	}
}

// enabled reports whether any bucket carries non-default configuration.
// When false callers must use the legacy scanner path.
func (s *bucketScanScheduler) enabled() bool {
	if s == nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cfg := range s.cfg.Buckets {
		if !cfg.isDefault() {
			return true
		}
	}
	return false
}

// load reads the configuration and runtime state from the backend. Bad
// individual configuration entries are logged and dropped; they never stop
// a scan cycle.
func (s *bucketScanScheduler) load(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cfg = bucketScannerConfigDoc{Version: bucketScannerConfigVersion, Buckets: map[string]BucketScanConfig{}}
	if buf, err := readConfig(ctx, s.objAPI, bucketScannerConfigPath); err != nil {
		if !errors.Is(err, errConfigNotFound) {
			scannerLogIf(ctx, err)
		}
	} else if len(buf) > 0 {
		if err := json.Unmarshal(buf, &s.cfg); err != nil {
			scannerLogOnceIf(ctx, fmt.Errorf("unable to parse %s: %w; ignoring", bucketScannerConfigPath, err), bucketScannerConfigPath)
			s.cfg = bucketScannerConfigDoc{Version: bucketScannerConfigVersion, Buckets: map[string]BucketScanConfig{}}
		}
	}
	if s.cfg.Buckets == nil {
		s.cfg.Buckets = map[string]BucketScanConfig{}
	}
	// Drop and report invalid entries in isolation; one bad bucket must
	// never take the whole scan round down.
	for bucket, cfg := range s.cfg.Buckets {
		if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
			scannerLogOnceIf(ctx, fmt.Errorf("ignoring invalid bucket scanner config for %q: %w", bucket, err), "bucket-scanner-cfg-"+bucket)
			delete(s.cfg.Buckets, bucket)
			continue
		}
		if err := cfg.Validate(); err != nil {
			scannerLogOnceIf(ctx, fmt.Errorf("ignoring invalid bucket scanner config for %q: %w", bucket, err), "bucket-scanner-cfg-"+bucket)
			delete(s.cfg.Buckets, bucket)
			continue
		}
		if cfg.isDefault() {
			delete(s.cfg.Buckets, bucket)
		}
	}

	s.states = map[string]bucketScanState{}
	if buf, err := readConfig(ctx, s.objAPI, bucketScannerStatePath); err != nil {
		if !errors.Is(err, errConfigNotFound) {
			scannerLogIf(ctx, err)
		}
	} else if len(buf) > 0 {
		var doc bucketScannerStateDoc
		if err := json.Unmarshal(buf, &doc); err != nil {
			scannerLogOnceIf(ctx, fmt.Errorf("unable to parse %s: %w; ignoring", bucketScannerStatePath, err), bucketScannerStatePath)
		} else if doc.Buckets != nil {
			s.states = doc.Buckets
			s.cycle = doc.Cycle
		}
	}
}

// beginCycle prepares scheduling for a cycle. It must be called before any
// set starts scanning. numSets is the number of erasure sets that will
// register workers.
func (s *bucketScanScheduler) beginCycle(ctx context.Context, cycle uint32, buckets []BucketInfo, numSets int) {
	s.load(ctx)
	s.initCycle(cycle, buckets, numSets)
}

// initCycle resets per-cycle scheduling state. It does not touch the
// backend and is used directly by tests.
func (s *bucketScanScheduler) initCycle(cycle uint32, buckets []BucketInfo, numSets int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cycle = cycle
	s.totalSets = numSets
	s.readySets = 0
	s.workerCount = 0
	s.barrierOnce = sync.Once{}
	s.barrier = make(chan struct{})
	s.pending = make(map[string]map[int]struct{}, len(buckets))
	s.credits = make(map[string]float64, len(buckets))
	s.front = make(map[string]struct{})
	s.ordered = s.ordered[:0]
	s.wake = make(chan struct{})

	existing := make(map[string]struct{}, len(buckets))
	for _, b := range buckets {
		if isReservedOrInvalidBucket(b.Name, false) {
			continue
		}
		existing[b.Name] = struct{}{}
		s.pending[b.Name] = map[int]struct{}{}
		s.ordered = append(s.ordered, b.Name)
		st := s.states[b.Name]
		// Roll state into the new cycle while keeping lifetime counters
		// and the last known position.
		if st.Cycle != cycle {
			st.Cycle = cycle
			st.StartedAt = time.Time{}
			st.CompletedAt = time.Time{}
			s.states[b.Name] = st
		}
	}
	// Prune state/config for buckets that no longer exist.
	for b := range s.states {
		if _, ok := existing[b]; !ok {
			delete(s.states, b)
		}
	}
	for b := range s.cfg.Buckets {
		if _, ok := existing[b]; !ok {
			delete(s.cfg.Buckets, b)
		}
	}
	sort.Strings(s.ordered)
}

// registerWorkers is called by an erasure set once it has resolved its
// online disks. It assigns a contiguous worker ID range and records that
// every registered worker still needs to scan every bucket. The returned
// base must be added to the per-disk local index to get the global ID.
func (s *bucketScanScheduler) registerWorkers(n int) int {
	s.mu.Lock()
	base := s.workerCount
	s.workerCount += n
	for b, set := range s.pending {
		for i := base; i < base+n; i++ {
			set[i] = struct{}{}
		}
		s.pending[b] = set
	}
	s.mu.Unlock()
	return base
}

// setReady is called by each erasure set (exactly once) after it has
// registered its workers, including sets with zero usable disks.
func (s *bucketScanScheduler) setReady() {
	s.mu.Lock()
	s.readySets++
	ready := s.readySets == s.totalSets
	s.mu.Unlock()
	if ready {
		s.barrierOnce.Do(func() { close(s.barrier) })
	}
}

func (s *bucketScanScheduler) waitBarrier(ctx context.Context) bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	ch := s.barrier
	s.mu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

// configLocked returns the config of a bucket (default if unset).
func (s *bucketScanScheduler) configLocked(bucket string) BucketScanConfig {
	return s.cfg.Buckets[bucket]
}

func (s *bucketScanScheduler) isPaused(bucket string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg.Buckets[bucket].Paused
}

// pickLocked selects the next bucket the worker should scan. Callers must
// hold s.mu. Returns "" when no eligible bucket is pending for the worker.
func (s *bucketScanScheduler) pickLocked(workerID int) string {
	var candidates []string
	for _, b := range s.ordered {
		workers, ok := s.pending[b]
		if !ok {
			continue
		}
		if _, mine := workers[workerID]; !mine {
			continue
		}
		if s.configLocked(b).Paused {
			continue
		}
		candidates = append(candidates, b)
	}
	if len(candidates) == 0 {
		return ""
	}

	// Weighted deficit round robin: periodically add each bucket's weight
	// to its credits, then spend one credit per turn.
	var maxCredit float64
	for _, b := range candidates {
		if c := s.credits[b]; c > maxCredit {
			maxCredit = c
		}
	}
	if maxCredit < 1 {
		for _, b := range s.ordered {
			if s.configLocked(b).Paused {
				continue
			}
			if _, ok := s.pending[b]; ok {
				s.credits[b] += s.configLocked(b).effectiveWeight()
			}
		}
		maxCredit = 0
		for _, b := range candidates {
			if c := s.credits[b]; c > maxCredit {
				maxCredit = c
			}
		}
	}

	pick := ""
	for _, b := range candidates {
		if s.credits[b] < maxCredit {
			continue
		}
		// Front-of-queue buckets always win the tie.
		if pick == "" {
			pick = b
			continue
		}
		_, pickFront := s.front[pick]
		_, bFront := s.front[b]
		if bFront && !pickFront {
			pick = b
		}
	}
	s.credits[pick]--
	return pick
}

// workerHasPending reports whether the worker still has any bucket
// (eligible or paused) assigned to it. Callers must hold s.mu.
func (s *bucketScanScheduler) workerHasPendingLocked(workerID int) bool {
	for _, workers := range s.pending {
		if _, ok := workers[workerID]; ok {
			return true
		}
	}
	return false
}

// acquire returns the next bucket turn for a worker. It blocks while the
// worker only has paused buckets left (those may be resumed). The boolean
// result is false when the worker is done with the cycle or the context
// was canceled.
func (s *bucketScanScheduler) acquire(ctx context.Context, workerID int) (*bucketScanTurn, bool) {
	if !s.waitBarrier(ctx) {
		return nil, false
	}
	for {
		s.mu.Lock()
		b := s.pickLocked(workerID)
		if b != "" {
			cfg := s.configLocked(b)
			budget := cfg.effectiveBudget()
			t := &bucketScanTurn{
				Bucket: b,
				Cycle:  s.cycle,
				budget: budget,
			}
			if budget > 0 {
				t.deadline = time.Now().Add(budget)
			}
			bucket := b
			t.paused = func() bool { return s.isPaused(bucket) }
			st := s.states[b]
			if st.StartedAt.IsZero() {
				st.StartedAt = time.Now()
				st.Cycle = s.cycle
				s.states[b] = st
			}
			s.mu.Unlock()

			return t, true
		}
		// Nothing eligible right now.
		if !s.workerHasPendingLocked(workerID) {
			s.mu.Unlock()
			return nil, false
		}
		wake := s.wake
		s.mu.Unlock()

		// All remaining buckets are paused; wait for resume, but wake up
		// periodically in case the context is canceled.
		timer := time.NewTimer(bucketScanWaitNotifyEvery)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, false
		case <-wake:
		case <-timer.C:
		}
	}
}

// finish records the outcome of a turn. A yielded bucket stays pending
// and re-enters rotation from its saved position; a completed bucket is
// removed from the worker's pending set.
func (s *bucketScanScheduler) finish(ctx context.Context, res bucketTurnResult) {
	s.mu.Lock()
	st := s.states[res.bucket]
	st.Cycle = s.cycle
	if res.objects > 0 {
		st.ObjectsProcessed += res.objects
	}
	st.Turns++
	if res.position != "" {
		st.Position = res.position
	}
	st.UpdatedAt = time.Now()

	if !res.yielded {
		if workers, ok := s.pending[res.bucket]; ok {
			delete(workers, res.workerID)
			if len(workers) == 0 {
				delete(s.pending, res.bucket)
				st.CompletedAt = time.Now()
			} else {
				s.pending[res.bucket] = workers
			}
		}
		delete(s.front, res.bucket)
	}
	s.states[res.bucket] = st

	shouldSave := time.Since(s.lastStateSave) >= bucketScanStateSaveEvery
	if shouldSave {
		s.lastStateSave = time.Now()
	}
	s.mu.Unlock()

	if shouldSave {
		s.saveState(ctx)
	}
}

// notify wakes workers waiting on eligibility changes. Callers must not
// hold s.mu.
func (s *bucketScanScheduler) notify() {
	s.mu.Lock()
	old := s.wake
	s.wake = make(chan struct{})
	s.mu.Unlock()
	if old != nil {
		xioutil.SafeClose(old)
	}
}

// endCycle persists the final progress after all workers drained.
func (s *bucketScanScheduler) endCycle(ctx context.Context) {
	if s == nil {
		return
	}
	s.saveState(ctx)
}

func (s *bucketScanScheduler) saveState(ctx context.Context) {
	if s == nil || s.objAPI == nil {
		return
	}
	s.mu.Lock()
	doc := bucketScannerStateDoc{
		Version:   bucketScannerStateVersion,
		Cycle:     s.cycle,
		Buckets:   make(map[string]bucketScanState, len(s.states)),
		UpdatedAt: time.Now(),
	}
	for b, st := range s.states {
		doc.Buckets[b] = st
	}
	s.mu.Unlock()

	buf, err := json.Marshal(&doc)
	if err != nil {
		bugLogIf(ctx, err)
		return
	}
	if err := saveConfig(ctx, s.objAPI, bucketScannerStatePath, buf); err != nil {
		scannerLogIf(ctx, fmt.Errorf("%w, Object %s", err, bucketScannerStatePath))
	}
}

// saveConfigLocked persists the configuration document. Callers must hold
// s.mu.
func (s *bucketScanScheduler) persistConfigLocked(ctx context.Context) error {
	doc := bucketScannerConfigDoc{
		Version: bucketScannerConfigVersion,
		Buckets: make(map[string]BucketScanConfig, len(s.cfg.Buckets)),
	}
	for b, cfg := range s.cfg.Buckets {
		doc.Buckets[b] = cfg
	}
	buf, err := json.Marshal(&doc)
	if err != nil {
		return err
	}
	return saveConfig(ctx, s.objAPI, bucketScannerConfigPath, buf)
}

// SetConfig validates and stores the scheduling configuration of a bucket.
// Passing the zero value removes the entry.
func (s *bucketScanScheduler) SetConfig(ctx context.Context, bucket string, cfg BucketScanConfig) error {
	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.cfg.Buckets == nil {
		s.cfg.Buckets = map[string]BucketScanConfig{}
	}
	if cfg.isDefault() {
		delete(s.cfg.Buckets, bucket)
	} else {
		s.cfg.Buckets[bucket] = cfg
	}
	err := s.persistConfigLocked(ctx)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	s.notify()
	return nil
}

// GetConfig returns the stored configuration of a bucket.
func (s *bucketScanScheduler) GetConfig(bucket string) (BucketScanConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cfg, ok := s.cfg.Buckets[bucket]
	return cfg, ok
}

// Pause pauses a bucket while retaining its progress. It is a
// convenience over SetConfig.
func (s *bucketScanScheduler) Pause(ctx context.Context, bucket string) error {
	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		return err
	}
	s.mu.Lock()
	cfg := s.cfg.Buckets[bucket]
	cfg.Paused = true
	s.cfg.Buckets[bucket] = cfg
	err := s.persistConfigLocked(ctx)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	s.notify()
	return nil
}

// Resume resumes a paused bucket. The bucket keeps its position and jumps
// the queue so it continues in the current cycle instead of waiting for
// the next one.
func (s *bucketScanScheduler) Resume(ctx context.Context, bucket string) error {
	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		return err
	}
	s.mu.Lock()
	cfg, ok := s.cfg.Buckets[bucket]
	if !ok || !cfg.Paused {
		s.mu.Unlock()
		return nil
	}
	cfg.Paused = false
	if cfg.isDefault() {
		delete(s.cfg.Buckets, bucket)
	} else {
		s.cfg.Buckets[bucket] = cfg
	}
	if s.front == nil {
		s.front = map[string]struct{}{}
	}
	s.front[bucket] = struct{}{}
	err := s.persistConfigLocked(ctx)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	s.notify()
	return nil
}

// Status returns the queryable status of one or all buckets. When bucket
// is empty every known bucket is returned.
func (s *bucketScanScheduler) Status(bucket string) []BucketScanStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	names := []string{}
	if bucket != "" {
		if _, ok := s.states[bucket]; !ok {
			if _, ok := s.cfg.Buckets[bucket]; !ok {
				return nil
			}
		}
		names = []string{bucket}
	} else {
		seen := map[string]struct{}{}
		for b := range s.states {
			seen[b] = struct{}{}
			names = append(names, b)
		}
		for b := range s.cfg.Buckets {
			if _, ok := seen[b]; !ok {
				names = append(names, b)
			}
		}
		sort.Strings(names)
	}

	out := make([]BucketScanStatus, 0, len(names))
	for _, b := range names {
		cfg := s.cfg.Buckets[b]
		st := s.states[b]
		weight := cfg.Weight
		if weight == 0 {
			weight = 1
		}
		_, inProgress := s.pending[b]
		out = append(out, BucketScanStatus{
			Bucket:           b,
			Cycle:            st.Cycle,
			Weight:           weight,
			Budget:           cfg.effectiveBudget(),
			Paused:           cfg.Paused,
			Position:         st.Position,
			ObjectsProcessed: st.ObjectsProcessed,
			Turns:            st.Turns,
			StartedAt:        st.StartedAt,
			UpdatedAt:        st.UpdatedAt,
			CompletedAt:      st.CompletedAt,
			InProgress:       inProgress,
		})
	}
	return out
}
