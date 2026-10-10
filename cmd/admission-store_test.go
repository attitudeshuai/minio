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
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio/internal/admission"
)

// memAdmissionStore is an in-memory admissionStore for handler/store tests.
type memAdmissionStore struct {
	mu          sync.Mutex
	objs        map[string][]byte
	knownBucket map[string]bool
	errBuckets  map[string]bool // buckets with a transient backend error
}

func newMemAdmissionStore() *memAdmissionStore {
	return &memAdmissionStore{
		objs:        map[string][]byte{},
		knownBucket: map[string]bool{"my-bucket": true},
		errBuckets:  map[string]bool{},
	}
}

func (s *memAdmissionStore) key(bucket, object string) string { return bucket + "/" + object }

func (s *memAdmissionStore) GetObjectNInfo(ctx context.Context, bucket, object string, _ *HTTPRangeSpec, _ http.Header, opts ObjectOptions) (*GetObjectReader, error) {
	s.mu.Lock()
	data, ok := s.objs[s.key(bucket, object)]
	s.mu.Unlock()
	if !ok {
		return nil, ObjectNotFound{Bucket: bucket, Object: object}
	}
	oi := ObjectInfo{Bucket: bucket, Name: object, Size: int64(len(data))}
	return NewGetObjectReaderFromReader(bytes.NewReader(data), oi, opts)
}

func (s *memAdmissionStore) PutObject(ctx context.Context, bucket, object string, r *PutObjReader, _ ObjectOptions) (ObjectInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return ObjectInfo{}, err
	}
	s.mu.Lock()
	s.objs[s.key(bucket, object)] = data
	s.mu.Unlock()
	return ObjectInfo{Bucket: bucket, Name: object, Size: int64(len(data))}, nil
}

func (s *memAdmissionStore) DeleteObject(ctx context.Context, bucket, object string, _ ObjectOptions) (ObjectInfo, error) {
	s.mu.Lock()
	delete(s.objs, s.key(bucket, object))
	s.mu.Unlock()
	return ObjectInfo{Bucket: bucket, Name: object}, nil
}

func (s *memAdmissionStore) GetBucketInfo(ctx context.Context, bucket string, _ BucketOptions) (BucketInfo, error) {
	if s.errBuckets[bucket] {
		return BucketInfo{}, StorageErr("simulated transient backend error")
	}
	if s.knownBucket[bucket] {
		return BucketInfo{Name: bucket}, nil
	}
	return BucketInfo{}, BucketNotFound{Bucket: bucket}
}

func (s *memAdmissionStore) hasFile() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.objs[minioMetaBucket+"/"+admissionRulesConfigFile]
	return ok
}

func (s *memAdmissionStore) writeRaw(p string, data []byte) {
	s.mu.Lock()
	s.objs[minioMetaBucket+"/"+p] = data
	s.mu.Unlock()
}

func TestAdmissionStoreUpsertReloadDelete(t *testing.T) {
	store := newMemAdmissionStore()
	ctx := context.Background()
	sys := &admissionSys{mgr: admission.NewManager(0)}

	wait := 250 * time.Millisecond
	rule := admission.Rule{
		Scope: admission.ScopeCaller,
		Name:  "alice",
		Total: &admission.ClassQuota{Rate: 10, Burst: 20, Wait: &wait},
	}
	if err := sys.upsertRule(ctx, store, rule); err != nil {
		t.Fatal(err)
	}
	if !store.hasFile() {
		t.Fatal("rules file must be persisted")
	}

	// A fresh system loads the same rules from the store (incl. wait_ms).
	sys2 := &admissionSys{mgr: admission.NewManager(0)}
	sys2.initFromStore(ctx, store)
	rules := sys2.currentRules()
	if len(rules) != 1 || rules[0].Name != "alice" || rules[0].Total.Rate != 10 ||
		rules[0].Total.Burst != 20 || rules[0].Total.Wait == nil || *rules[0].Total.Wait != 250*time.Millisecond {
		t.Fatalf("reloaded rules mismatch: %+v", rules)
	}
	if !sys2.mgr.Enabled() {
		t.Fatal("manager must be enabled after loading rules")
	}

	// Replace the same (scope,name).
	rule2 := rule
	rule2.Total = &admission.ClassQuota{Rate: 40, Burst: 80}
	if err := sys.upsertRule(ctx, store, rule2); err != nil {
		t.Fatal(err)
	}
	if got := sys.currentRules(); len(got) != 1 || got[0].Total.Rate != 40 {
		t.Fatalf("replace failed: %+v", got)
	}

	// Delete persists removal and disables the manager.
	if err := sys.deleteRule(ctx, store, admission.ScopeCaller, "alice"); err != nil {
		t.Fatal(err)
	}
	if store.hasFile() {
		t.Fatal("rules file must be removed when empty")
	}
	if sys.mgr.Enabled() {
		t.Fatal("manager must be disabled with no rules")
	}
	if err := sys.deleteRule(ctx, store, admission.ScopeCaller, "alice"); err != errAdmissionRuleNotFound {
		t.Fatalf("second delete: want not-found, got %v", err)
	}
}

func TestAdmissionStoreInvalidKeepsOld(t *testing.T) {
	store := newMemAdmissionStore()
	ctx := context.Background()
	sys := &admissionSys{mgr: admission.NewManager(0)}

	good := admission.Rule{
		Scope: admission.ScopeCaller, Name: "alice",
		Total: &admission.ClassQuota{Rate: 10, Burst: 10},
	}
	if err := sys.upsertRule(ctx, store, good); err != nil {
		t.Fatal(err)
	}

	// Invalid quota must be rejected and the old rule must stay.
	bad := admission.Rule{
		Scope: admission.ScopeCaller, Name: "bob",
		Total: &admission.ClassQuota{Rate: -1, Burst: 0},
	}
	if err := sys.upsertRule(ctx, store, bad); err == nil {
		t.Fatal("invalid rule must be rejected")
	}
	got := sys.currentRules()
	if len(got) != 1 || got[0].Name != "alice" {
		t.Fatalf("old rules must remain, got %+v", got)
	}

	// Rule without any quota.
	empty := admission.Rule{Scope: admission.ScopeCaller, Name: "carol"}
	if err := sys.upsertRule(ctx, store, empty); err == nil {
		t.Fatal("rule without quotas must be rejected")
	}

	// Invalid scope and invalid bucket names are rejected early.
	for _, r := range []admission.Rule{
		{Scope: "weird", Name: "x", Total: &admission.ClassQuota{Rate: 1, Burst: 1}},
		{Scope: admission.ScopeBucket, Name: "INVALID_UPPER", Total: &admission.ClassQuota{Rate: 1, Burst: 1}},
		{Scope: admission.ScopeBucket, Name: "missing-bucket", Total: &admission.ClassQuota{Rate: 1, Burst: 1}},
	} {
		if err := sys.upsertRule(ctx, store, r); err == nil {
			t.Fatalf("expected rejection for %+v", r)
		}
	}
	if len(sys.currentRules()) != 1 {
		t.Fatal("no failed upsert may mutate the rule set")
	}

	// Existing bucket passes target validation.
	bucketRule := admission.Rule{
		Scope: admission.ScopeBucket, Name: "my-bucket",
		Total: &admission.ClassQuota{Rate: 1, Burst: 1},
	}
	if err := sys.upsertRule(ctx, store, bucketRule); err != nil {
		t.Fatalf("existing bucket rule: %v", err)
	}
}

func TestAdmissionStoreCorruptFileFailOpen(t *testing.T) {
	store := newMemAdmissionStore()
	ctx := context.Background()
	store.writeRaw(admissionRulesConfigFile, []byte("{not json"))

	rules, err := loadAdmissionRulesFromStore(ctx, store)
	if err == nil {
		t.Fatal("corrupt file must return error")
	}
	if rules != nil {
		t.Fatal("no rules on corrupt file")
	}

	// initFromStore logs and stays disabled instead of failing.
	sys := &admissionSys{mgr: admission.NewManager(0)}
	sys.initFromStore(ctx, store)
	if sys.mgr.Enabled() {
		t.Fatal("corrupt rules must leave the layer disabled (fail open)")
	}
}

func TestAdmissionRulesFileJSONShape(t *testing.T) {
	// Body of set-request-admission parses into the quota structure.
	body := `{"reads":{"rate":5,"burst":10,"wait_ms":100},"total":{"rate":20,"burst":40}}`
	var req setRequestAdmissionReq
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}
	if req.Reads == nil || req.Reads.Rate != 5 || req.Reads.Burst != 10 ||
		req.Reads.Wait == nil || *req.Reads.Wait != 100*time.Millisecond {
		t.Fatalf("reads parse: %+v", req.Reads)
	}
	if req.Writes != nil || req.Lists != nil {
		t.Fatal("absent classes must stay nil")
	}
}
