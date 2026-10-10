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
	"testing"
	"time"

	"github.com/minio/minio/internal/bucket/lifecycle"
)

func TestPendingSnapshotSaveReplayDelete(t *testing.T) {
	ctx := context.Background()
	st := newTestTransitionState(t, true)
	drive := newFakeTransitionDrive()
	drives := []StorageAPI{drive}

	now := time.Now().UTC().Truncate(time.Second)
	t1 := transitionTask{
		objInfo: ObjectInfo{Bucket: "b1", Name: "o1", VersionID: "v1", Size: 10, ETag: "e1", ModTime: now},
		src:     lcEventSrc_Scanner,
		event:   transitionEventFor("WARM-A", false),
	}
	t2 := transitionTask{
		objInfo: ObjectInfo{Bucket: "b2", Name: "o2", VersionID: "v2", Size: 20, ETag: "e2", ModTime: now.Add(time.Minute)},
		src:     lcEventSrc_Scanner,
		event:   transitionEventFor("WARM-B", true), attempts: 2,
	}
	st.trackEnqueued(t1)
	st.trackEnqueued(t2)

	entries := st.collectPendingSnapshot()
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if err := saveTransitionPendingSnapshotTo(ctx, drives, entries); err != nil {
		t.Fatal(err)
	}
	if drive.fileCount() != 1 {
		t.Fatalf("expected one snapshot file, got %d", drive.fileCount())
	}

	var replayed []transitionTask
	n := replayTransitionPendingTasksFrom(ctx, drives, func(task transitionTask) { replayed = append(replayed, task) })
	if n != 2 || len(replayed) != 2 {
		t.Fatalf("expected 2 replayed tasks, got %d/%d", n, len(replayed))
	}
	if drive.hasFile(minioMetaBucket + "/" + transitionPendingPath()) {
		t.Fatalf("snapshot file must be removed after replay")
	}

	byKey := map[string]transitionTask{}
	for _, task := range replayed {
		byKey[task.objInfo.Bucket+"/"+task.objInfo.Name] = task
	}
	got1 := byKey["b1/o1"]
	if got1.event.StorageClass != "WARM-A" || got1.objInfo.VersionID != "v1" || got1.attempts != 0 ||
		got1.event.Action != lifecycle.TransitionAction {
		t.Fatalf("unexpected replayed task 1: %+v", got1)
	}
	got2 := byKey["b2/o2"]
	if got2.event.StorageClass != "WARM-B" || got2.attempts != 2 ||
		got2.event.Action != lifecycle.TransitionVersionAction {
		t.Fatalf("unexpected replayed task 2: %+v", got2)
	}

	// Second replay finds nothing (idempotent).
	if n := replayTransitionPendingTasksFrom(ctx, drives, func(task transitionTask) {}); n != 0 {
		t.Fatalf("second replay should be a no-op, got %d", n)
	}
}
