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
	"fmt"
	"math/rand"
	"net/http"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio/internal/bucket/lifecycle"
	xnet "github.com/minio/pkg/v3/net"
	"github.com/zeebo/xxh3"
)

// Storage layout (all under minioMetaBucket):
//
//	transition-failures/<tier>/<record-id>.json - one object per failed transition
const transitionFailuresPrefix = "transition-failures/"

type transitionFailureStatus string

const (
	transitionFailurePending   transitionFailureStatus = "pending"
	transitionFailureExhausted transitionFailureStatus = "exhausted"
)

// transitionFailureCategory categorizes a transition failure for the
// queryable failure list.
type transitionFailureCategory string

const (
	// transitionFailureBackendDown - remote tier is unreachable or the
	// network/host is down. Retryable.
	transitionFailureBackendDown transitionFailureCategory = "backend_down"
	// transitionFailurePermission - remote tier rejected the operation
	// due to credentials/permissions. Retryable (credentials may be fixed).
	transitionFailurePermission transitionFailureCategory = "permission"
	// transitionFailureConfiguration - local tier configuration problem
	// (unknown tier, invalid config). Not retried.
	transitionFailureConfiguration transitionFailureCategory = "configuration"
	// transitionFailureInternal - anything else (local quorum, unexpected
	// remote errors etc). Retryable.
	transitionFailureInternal transitionFailureCategory = "internal"
)

func (c transitionFailureCategory) String() string { return string(c) }

// transitionFailureRecord is one entry in the per-tier list of failed
// transition tasks. It is persisted as JSON in the cluster shared store.
type transitionFailureRecord struct {
	ID        string `json:"id"`
	Bucket    string `json:"bucket"`
	Object    string `json:"object"`
	VersionID string `json:"versionId"`
	Tier      string `json:"tier"`

	Size       int64     `json:"size"`
	ETag       string    `json:"etag"`
	ModTime    time.Time `json:"lastModified"`
	Noncurrent bool      `json:"noncurrent"`

	Category transitionFailureCategory `json:"category"`
	LastErr  string                    `json:"lastErr"`

	Attempts      int                     `json:"attempts"`
	FirstFailedAt time.Time               `json:"firstFailedAt"`
	LastFailedAt  time.Time               `json:"lastFailedAt"`
	NextRetryAt   time.Time               `json:"nextRetryAt"`
	Status        transitionFailureStatus `json:"status"`
}

// transitionFailureRecordID returns a deterministic ID for one object
// version transitioning to one tier, so repeated failures merge into the
// same record instead of producing duplicates.
func transitionFailureRecordID(tier, bucket, object, versionID string) string {
	var sb strings.Builder
	sb.WriteString(tier)
	sb.WriteByte('|')
	sb.WriteString(bucket)
	sb.WriteByte('|')
	sb.WriteString(object)
	sb.WriteByte('|')
	sb.WriteString(versionID)
	return strconv.FormatUint(xxh3.HashString(sb.String()), 16)
}

func transitionFailureRecordPath(tier, id string) string {
	return transitionFailuresPrefix + tier + SlashSeparator + id + ".json"
}

// classifyTransitionError categorizes a transition error.
//
//	recordable: whether the failure must be archived in the failure list.
//	retryable:  whether scheduling another attempt is meaningful.
func classifyTransitionError(err error) (cat transitionFailureCategory, recordable, retryable bool) {
	if err == nil {
		return "", false, false
	}
	// Object/version is gone - nothing to transition, behave exactly like
	// the pre-existing worker path (drop silently).
	if isErrObjectNotFound(err) || isErrVersionNotFound(err) {
		return "", false, false
	}
	// Process is shutting down; do not archive or retry.
	if errors.Is(err, context.Canceled) {
		return "", false, false
	}
	var backendDown BackendDown
	if errors.As(err, &backendDown) || xnet.IsNetworkOrHostDown(err, false) ||
		strings.Contains(err.Error(), "use of closed network connection") {
		return transitionFailureBackendDown, true, true
	}
	var permErr tierPermErr
	if errors.As(err, &permErr) {
		return transitionFailurePermission, true, true
	}
	if errors.Is(err, errTierNotFound) || errors.Is(err, errTierInvalidConfig) {
		return transitionFailureConfiguration, true, false
	}
	return transitionFailureInternal, true, true
}

// backoffDelay computes the exponential backoff delay for the given attempt
// number. jitterFactor is in [-1, 1] and scales the delay by +/-20%.
func backoffDelay(attempts int, base, max time.Duration, jitterFactor float64) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	d := base
	for i := 0; i < attempts-1; i++ {
		if d >= max {
			d = max
			break
		}
		d *= 2
		if d > max || d <= 0 {
			d = max
			break
		}
	}
	j := 1 + 0.2*jitterFactor
	out := time.Duration(float64(d) * j)
	if out <= 0 {
		out = d
	}
	if out > max {
		out = max
	}
	return out
}

// transitionBackoff returns the backoff delay with randomized +/-20% jitter.
func transitionBackoff(attempts int, base, max time.Duration) time.Duration {
	return backoffDelay(attempts, base, max, rand.Float64()*2-1)
}

// recordTransitionFailure builds the new state of a failure record after a
// failed attempt. prev is nil on the first failure. The returned record has
// monotonically increasing Attempts, preserved FirstFailedAt and an
// exponential NextRetryAt; records that reach the configured attempt cap are
// moved to the exhausted (dead-letter) state.
func recordTransitionFailure(prev *transitionFailureRecord, oi ObjectInfo, tier string, noncurrent bool,
	cat transitionFailureCategory, errMsg string, cfg transitionFailureConfig, now time.Time,
) transitionFailureRecord {
	var rec transitionFailureRecord
	if prev != nil {
		rec = *prev
		rec.Attempts++
	} else {
		rec.Attempts = 1
		rec.FirstFailedAt = now
		rec.ID = transitionFailureRecordID(tier, oi.Bucket, oi.Name, oi.VersionID)
		rec.Bucket = oi.Bucket
		rec.Object = oi.Name
		rec.VersionID = oi.VersionID
		rec.Tier = tier
		rec.Size = oi.Size
		rec.ETag = oi.ETag
		rec.ModTime = oi.ModTime
		rec.Noncurrent = noncurrent
	}

	rec.Category = cat
	rec.LastErr = errMsg
	rec.LastFailedAt = now

	if cfg.MaxAttempts > 0 && rec.Attempts >= cfg.MaxAttempts {
		rec.Status = transitionFailureExhausted
		rec.NextRetryAt = time.Time{}
		return rec
	}
	rec.Status = transitionFailurePending
	rec.NextRetryAt = now.Add(transitionBackoff(rec.Attempts, cfg.RetryDelay, cfg.RetryMaxDelay))
	return rec
}

// isNoncurrentTransitionTask reports whether the transition task targets a
// noncurrent object version.
func isNoncurrentTransitionTask(event lifecycle.Event) bool {
	return event.Action == lifecycle.TransitionVersionAction
}

// transitionFailureStore is the subset of ObjectLayer used by the failure
// journal. ObjectLayer satisfies it; tests may provide a narrower fake.
type transitionFailureStore interface {
	ListObjects(ctx context.Context, bucket, prefix, marker, delimiter string, maxKeys int) (ListObjectsInfo, error)
	GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, opts ObjectOptions) (gr *GetObjectReader, err error)
	PutObject(ctx context.Context, bucket, object string, r *PutObjReader, opts ObjectOptions) (ObjectInfo, error)
	DeleteObject(ctx context.Context, bucket, object string, opts ObjectOptions) (ObjectInfo, error)
	NewNSLock(bucket string, objects ...string) RWLocker
}

// transitionFailureCounts is the per-tier tally of records in each state.
type transitionFailureCounts struct {
	Pending   int `json:"pending"`
	Exhausted int `json:"exhausted"`
}

// transitionFailureJournal is the cluster-shared, per-tier archive of failed
// transition tasks. Records live as one JSON object per failure under
// minioMetaBucket/transition-failures/<tier>/ and are mirrored in an in-memory
// index that is bootstrapped at startup and kept converged by periodic refresh.
type transitionFailureJournal struct {
	ctx   context.Context
	store transitionFailureStore

	cfgFn func() transitionFailureConfig

	mu      sync.RWMutex
	records map[string]transitionFailureRecord
}

func newTransitionFailureJournal(ctx context.Context) *transitionFailureJournal {
	return &transitionFailureJournal{
		ctx:     ctx,
		records: make(map[string]transitionFailureRecord),
		cfgFn: func() transitionFailureConfig {
			return globalILMConfig.getTransitionFailureConfig()
		},
	}
}

// Init bootstraps the in-memory index from the cluster shared store.
func (j *transitionFailureJournal) Init(ctx context.Context, store transitionFailureStore) error {
	j.store = store
	return j.refreshLocked(ctx, true)
}

// loadRecord reads one record from the shared store. A missing record returns
// nil with no error.
func (j *transitionFailureJournal) loadRecord(ctx context.Context, p string) (*transitionFailureRecord, error) {
	data, err := readConfig(ctx, j.store, p)
	if err != nil {
		if errors.Is(err, errConfigNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var rec transitionFailureRecord
	if err = json.Unmarshal(data, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

// withRecordLock serializes read-modify-write of one record cluster-wide.
func (j *transitionFailureJournal) withRecordLock(ctx context.Context, id string, fn func(ctx context.Context)) {
	lk := j.store.NewNSLock(minioMetaBucket, transitionFailuresPrefix+id)
	lkctx, err := lk.GetLock(ctx, sharedLockTimeout)
	if err != nil {
		transitionLogIf(ctx, fmt.Errorf("failed to acquire transition failure record lock for %s: %w", id, err))
		return
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx)
	fn(ctx)
}

// upsert archives (or advances) a failure record. All persistence failures
// are logged only - failure journaling must never propagate into foreground
// paths.
func (j *transitionFailureJournal) upsert(ctx context.Context, rec transitionFailureRecord) {
	if j == nil || j.store == nil || rec.ID == "" {
		return
	}
	j.withRecordLock(ctx, rec.ID, func(ctx context.Context) {
		p := transitionFailureRecordPath(rec.Tier, rec.ID)
		incomingExhausted := rec.Status == transitionFailureExhausted
		if prev, err := j.loadRecord(ctx, p); err == nil && prev != nil {
			// Build on the persisted attempt count; a concurrent retry
			// may know of more attempts than the persisted copy. First
			// failure time always comes from the persisted record.
			base := prev
			if rec.Attempts-1 > base.Attempts {
				base.Attempts = rec.Attempts - 1
			}
			rec = recordTransitionFailure(base, ObjectInfo{
				Bucket:    rec.Bucket,
				Name:      rec.Object,
				VersionID: rec.VersionID,
				Size:      rec.Size,
				ETag:      rec.ETag,
				ModTime:   rec.ModTime,
			}, rec.Tier, rec.Noncurrent, rec.Category, rec.LastErr, j.cfgFn(), rec.LastFailedAt)
		} else if err != nil {
			transitionLogIf(ctx, err)
		}
		if incomingExhausted {
			// Non-retryable failure (e.g. configuration error): archive as
			// exhausted regardless of attempt count.
			rec.Status = transitionFailureExhausted
			rec.NextRetryAt = time.Time{}
		}
		data, err := json.Marshal(&rec)
		if err != nil {
			transitionLogIf(ctx, err)
			return
		}
		if err = saveConfig(ctx, j.store, p, data); err != nil {
			transitionLogIf(ctx, err)
			return
		}
		j.mu.Lock()
		j.records[rec.ID] = rec
		j.mu.Unlock()
	})
}

// succeed removes the record after a successful (re)attempt. Missing records
// are treated as success.
func (j *transitionFailureJournal) succeed(ctx context.Context, id string) {
	if j == nil || j.store == nil || id == "" {
		return
	}
	j.withRecordLock(ctx, id, func(ctx context.Context) {
		j.mu.RLock()
		rec, ok := j.records[id]
		j.mu.RUnlock()
		if !ok {
			// Nothing locally known; verify against the shared store by
			// listing prefix is unnecessary - absent cache means it was
			// either never here or already removed by refresh.
			return
		}
		if err := deleteConfig(ctx, j.store, transitionFailureRecordPath(rec.Tier, id)); err != nil &&
			!errors.Is(err, errConfigNotFound) {
			transitionLogIf(ctx, err)
			return
		}
		j.mu.Lock()
		delete(j.records, id)
		j.mu.Unlock()
	})
}

// list returns copies of records matching the filters. Empty tier lists all
// tiers.
func (j *transitionFailureJournal) list(tier, bucket string, status transitionFailureStatus) []transitionFailureRecord {
	if j == nil {
		return nil
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	var out []transitionFailureRecord
	for _, rec := range j.records {
		if tier != "" && rec.Tier != tier {
			continue
		}
		if bucket != "" && rec.Bucket != bucket {
			continue
		}
		if status != "" && rec.Status != status {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, k int) bool {
		if out[i].LastFailedAt.Equal(out[k].LastFailedAt) {
			return out[i].ID < out[k].ID
		}
		return out[i].LastFailedAt.After(out[k].LastFailedAt)
	})
	return out
}

// due returns up to limit pending records whose NextRetryAt is due for tiers
// that are not paused, ordered by NextRetryAt (oldest first).
func (j *transitionFailureJournal) due(now time.Time, isTierPaused func(string) bool, limit int) []transitionFailureRecord {
	if j == nil || limit <= 0 {
		return nil
	}
	j.mu.RLock()
	var out []transitionFailureRecord
	for _, rec := range j.records {
		if rec.Status != transitionFailurePending || rec.NextRetryAt.IsZero() || rec.NextRetryAt.After(now) {
			continue
		}
		if isTierPaused != nil && isTierPaused(rec.Tier) {
			continue
		}
		out = append(out, rec)
	}
	j.mu.RUnlock()
	sort.Slice(out, func(i, k int) bool {
		if out[i].NextRetryAt.Equal(out[k].NextRetryAt) {
			return out[i].ID < out[k].ID
		}
		return out[i].NextRetryAt.Before(out[k].NextRetryAt)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}

// countsByTier returns the record counts grouped by tier and state.
func (j *transitionFailureJournal) countsByTier() map[string]transitionFailureCounts {
	counts := make(map[string]transitionFailureCounts)
	if j == nil {
		return counts
	}
	j.mu.RLock()
	defer j.mu.RUnlock()
	for _, rec := range j.records {
		c := counts[rec.Tier]
		switch rec.Status {
		case transitionFailureExhausted:
			c.Exhausted++
		default:
			c.Pending++
		}
		counts[rec.Tier] = c
	}
	return counts
}

// clear deletes records matching the filters and returns the number removed.
// When all is false only exhausted records are eligible.
func (j *transitionFailureJournal) clear(ctx context.Context, tier string, all bool) (int, error) {
	if j == nil || j.store == nil {
		return 0, nil
	}
	var toDelete []transitionFailureRecord
	j.mu.RLock()
	for _, rec := range j.records {
		if tier != "" && rec.Tier != tier {
			continue
		}
		if !all && rec.Status != transitionFailureExhausted {
			continue
		}
		toDelete = append(toDelete, rec)
	}
	j.mu.RUnlock()

	removed := 0
	for _, rec := range toDelete {
		var deleteErr error
		j.withRecordLock(ctx, rec.ID, func(ctx context.Context) {
			if err := deleteConfig(ctx, j.store, transitionFailureRecordPath(rec.Tier, rec.ID)); err != nil &&
				!errors.Is(err, errConfigNotFound) {
				deleteErr = err
				return
			}
			j.mu.Lock()
			delete(j.records, rec.ID)
			j.mu.Unlock()
		})
		if deleteErr != nil {
			return removed, deleteErr
		}
		removed++
	}
	return removed, nil
}

// refresh converges the in-memory index with the shared store.
func (j *transitionFailureJournal) refresh(ctx context.Context) error {
	return j.refreshLocked(ctx, false)
}

func (j *transitionFailureJournal) refreshLocked(ctx context.Context, bootstrap bool) error {
	if j == nil || j.store == nil {
		return nil
	}
	seen := make(map[string]struct{})
	marker := ""
	for {
		res, err := j.store.ListObjects(ctx, minioMetaBucket, transitionFailuresPrefix, marker, "", maxObjectList)
		if err != nil {
			return err
		}
		for _, obj := range res.Objects {
			id := strings.TrimSuffix(path.Base(obj.Name), ".json")
			seen[id] = struct{}{}
			j.mu.RLock()
			cached, ok := j.records[id]
			j.mu.RUnlock()
			if ok && !bootstrap && !cached.LastFailedAt.IsZero() && !obj.ModTime.After(cached.LastFailedAt) {
				// Our cached copy is at least as fresh.
				continue
			}
			rec, rerr := j.loadRecord(ctx, obj.Name)
			if rerr != nil || rec == nil {
				if rerr != nil && !errors.Is(rerr, errConfigNotFound) {
					transitionLogIf(ctx, rerr)
				}
				continue
			}
			j.mu.Lock()
			j.records[rec.ID] = *rec
			j.mu.Unlock()
		}
		if !res.IsTruncated {
			break
		}
		if res.NextMarker != "" {
			marker = res.NextMarker
			continue
		}
		if len(res.Objects) == 0 {
			break
		}
		marker = res.Objects[len(res.Objects)-1].Name
	}

	// Drop locally cached records that no longer exist in the shared store.
	j.mu.Lock()
	for id := range j.records {
		if _, ok := seen[id]; !ok {
			delete(j.records, id)
		}
	}
	j.mu.Unlock()
	return nil
}
