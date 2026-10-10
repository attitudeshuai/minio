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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path"
	"sort"
	"time"

	"github.com/minio/minio/internal/bucket/lifecycle"
)

const (
	// transitionPendingDir is the per-node local-drive directory holding the
	// crash-safe snapshot of queued and in-flight transition tasks.
	transitionPendingDir      = bucketMetaPrefix + SlashSeparator + "transition"
	transitionPendingListFile = "pending-list.json"

	// transitionPendingSnapshotInterval is the periodic snapshot interval;
	// graceful shutdown always performs a final flush.
	transitionPendingSnapshotInterval = 30 * time.Second
)

func transitionPendingPath() string {
	return path.Join(transitionPendingDir, transitionPendingListFile)
}

// pendingTransitionEntry is the compact representation of an unfinished
// transition task persisted across process restarts.
type pendingTransitionEntry struct {
	Bucket     string    `json:"bucket"`
	Object     string    `json:"object"`
	VersionID  string    `json:"versionId"`
	Tier       string    `json:"tier"`
	Size       int64     `json:"size"`
	ETag       string    `json:"etag"`
	ModTime    time.Time `json:"lastModified"`
	Noncurrent bool      `json:"noncurrent"`
	Attempts   int       `json:"attempts"`
}

// collectPendingSnapshot returns a deterministic snapshot of all queued and
// in-flight transition tasks on this node.
func (t *transitionState) collectPendingSnapshot() []pendingTransitionEntry {
	t.pendingMu.Lock()
	keys := make([]uint64, 0, len(t.pendingTasks))
	for k := range t.pendingTasks {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, k int) bool { return keys[i] < keys[k] })
	entries := make([]pendingTransitionEntry, 0, len(keys))
	for _, k := range keys {
		task := t.pendingTasks[k]
		entries = append(entries, pendingTransitionEntry{
			Bucket:     task.objInfo.Bucket,
			Object:     task.objInfo.Name,
			VersionID:  task.objInfo.VersionID,
			Tier:       task.event.StorageClass,
			Size:       task.objInfo.Size,
			ETag:       task.objInfo.ETag,
			ModTime:    task.objInfo.ModTime,
			Noncurrent: isNoncurrentTransitionTask(task.event),
			Attempts:   task.attempts,
		})
	}
	t.pendingMu.Unlock()
	return entries
}

func marshalPendingEntries(entries []pendingTransitionEntry) ([]byte, error) {
	return json.Marshal(entries)
}

func unmarshalPendingEntries(data []byte) ([]pendingTransitionEntry, error) {
	var entries []pendingTransitionEntry
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

// saveTransitionPendingSnapshotTo writes the pending task snapshot onto any
// one available local drive, mirroring the MRF persistence approach.
func saveTransitionPendingSnapshotTo(ctx context.Context, localDrives []StorageAPI, entries []pendingTransitionEntry) error {
	data, err := marshalPendingEntries(entries)
	if err != nil {
		return err
	}
	if len(localDrives) == 0 {
		return nil
	}

	var lastErr error
	for _, localDrive := range localDrives {
		if localDrive == nil {
			continue
		}
		r := io.NopCloser(bytes.NewReader(data))
		if err := localDrive.CreateFile(ctx, "", minioMetaBucket, transitionPendingPath(), int64(len(data)), r); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// deleteTransitionPendingSnapshotOn removes the snapshot file from local drives.
func deleteTransitionPendingSnapshotOn(ctx context.Context, localDrives []StorageAPI) {
	for _, localDrive := range localDrives {
		if localDrive == nil {
			continue
		}
		localDrive.Delete(ctx, minioMetaBucket, transitionPendingPath(), DeleteOptions{})
	}
}

// loadTransitionPendingSnapshotFrom reads a pending task snapshot from the
// given local drives, if one exists.
func loadTransitionPendingSnapshotFrom(ctx context.Context, localDrives []StorageAPI) ([]pendingTransitionEntry, bool) {
	for _, localDrive := range localDrives {
		if localDrive == nil {
			continue
		}
		rc, err := localDrive.ReadFileStream(ctx, minioMetaBucket, transitionPendingPath(), 0, -1)
		if err != nil {
			continue
		}
		data, rerr := io.ReadAll(rc)
		rc.Close()
		if rerr != nil {
			continue
		}
		entries, uerr := unmarshalPendingEntries(data)
		if uerr != nil {
			transitionLogIf(ctx, uerr)
			continue
		}
		return entries, true
	}
	return nil, false
}

// saveTransitionPendingSnapshot writes the pending task snapshot onto local
// drives of this node.
func saveTransitionPendingSnapshot(ctx context.Context, entries []pendingTransitionEntry) error {
	globalLocalDrivesMu.RLock()
	localDrives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()
	return saveTransitionPendingSnapshotTo(ctx, localDrives, entries)
}

// deleteTransitionPendingSnapshot removes the snapshot file from local drives.
func deleteTransitionPendingSnapshot(ctx context.Context) {
	globalLocalDrivesMu.RLock()
	localDrives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()
	deleteTransitionPendingSnapshotOn(ctx, localDrives)
}

// loadTransitionPendingSnapshot reads a pending task snapshot from local
// drives of this node, if one exists.
func loadTransitionPendingSnapshot(ctx context.Context) ([]pendingTransitionEntry, bool) {
	globalLocalDrivesMu.RLock()
	localDrives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()
	return loadTransitionPendingSnapshotFrom(ctx, localDrives)
}

// replayTransitionPendingTasksFrom re-enqueues tasks found in the on-disk
// snapshot and then removes the snapshot.
func replayTransitionPendingTasksFrom(ctx context.Context, localDrives []StorageAPI, enqueue func(task transitionTask)) int {
	entries, ok := loadTransitionPendingSnapshotFrom(ctx, localDrives)
	if !ok {
		return 0
	}
	for _, e := range entries {
		action := lifecycle.TransitionAction
		if e.Noncurrent {
			action = lifecycle.TransitionVersionAction
		}
		oi := ObjectInfo{
			Bucket:    e.Bucket,
			Name:      e.Object,
			VersionID: e.VersionID,
			Size:      e.Size,
			ETag:      e.ETag,
			ModTime:   e.ModTime,
		}
		enqueue(transitionTask{
			objInfo:  oi,
			src:      lcEventSrc_Scanner,
			event:    lifecycle.Event{Action: action, StorageClass: e.Tier},
			attempts: e.Attempts,
		})
	}
	deleteTransitionPendingSnapshotOn(ctx, localDrives)
	return len(entries)
}

// replayTransitionPendingTasks re-enqueues tasks found in the on-disk
// snapshot and then removes the snapshot. It is called once, on startup.
func replayTransitionPendingTasks(ctx context.Context, objAPI ObjectLayer) {
	if globalTransitionState == nil {
		return
	}
	globalLocalDrivesMu.RLock()
	localDrives := cloneDrives(globalLocalDrivesMap)
	globalLocalDrivesMu.RUnlock()
	n := replayTransitionPendingTasksFrom(ctx, localDrives, func(task transitionTask) {
		globalTransitionState.enqueue(task, false)
	})
	if n > 0 {
		// The replayed tasks will be re-snapshotted by the periodic loop if
		// they remain unfinished.
		globalTransitionState.pendingDirty.Store(true)
	}
}

// runTransitionPendingSnapshotLoop periodically flushes queued/in-flight
// tasks so an ungraceful restart loses at most one snapshot interval.
func runTransitionPendingSnapshotLoop(ctx context.Context, objAPI ObjectLayer) {
	ticker := time.NewTicker(transitionPendingSnapshotInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if globalTransitionState == nil || !globalTransitionState.pendingDirty.CompareAndSwap(true, false) {
				continue
			}
			entries := globalTransitionState.collectPendingSnapshot()
			if len(entries) == 0 {
				deleteTransitionPendingSnapshot(ctx)
				continue
			}
			if err := saveTransitionPendingSnapshot(ctx, entries); err != nil {
				transitionLogIf(ctx, err)
			}
		}
	}
}

// shutdownTransitionPending performs the final on-disk flush of pending tasks
// during a graceful shutdown.
func shutdownTransitionPending(ctx context.Context) {
	if globalTransitionState == nil {
		return
	}
	if !globalTransitionState.getFailureCfg().Enabled {
		return
	}
	entries := globalTransitionState.collectPendingSnapshot()
	if len(entries) == 0 {
		deleteTransitionPendingSnapshot(ctx)
		return
	}
	if err := saveTransitionPendingSnapshot(ctx, entries); err != nil {
		transitionLogIf(ctx, err)
	}
}
