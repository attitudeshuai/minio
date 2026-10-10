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
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio/internal/admission"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/policy"
)

// admissionDefaultWait is the default bounded queue time for a request when
// a rule does not specify its own wait.
const admissionDefaultWait = 250 * time.Millisecond

// admissionSys is the node-local per-bucket/per-caller request admission
// layer. It is a thin wrapper around the pure admission.Manager; persistence
// and cluster exchange live in admission-store.go / admission-peer.go.
type admissionSys struct {
	mgr *admission.Manager

	// upsert serializes rule mutations (admin set/delete, reaper).
	upsert sync.Mutex

	// rules is the last applied full rule set, kept so admin handlers and
	// the reaper can inspect/mutate/persist it without touching limiters.
	rules atomic.Pointer[admission.Rules]

	// agg is the cluster usage aggregator (nil on single-node / tests).
	agg atomic.Pointer[admission.Aggregator]

	// store is the metadata store, set by startAdmissionSystem. Delete
	// hooks fall back to newObjectLayerFn() when it is unset.
	store atomic.Pointer[admissionStore]
}

// setStore records the backing metadata store for the system.
func (sys *admissionSys) setStore(s admissionStore) { sys.store.Store(&s) }

func (sys *admissionSys) backingStore() admissionStore {
	if p := sys.store.Load(); p != nil {
		return *p
	}
	return newObjectLayerFn()
}

func (sys *admissionSys) setAggregator(a *admission.Aggregator) {
	sys.agg.Store(a)
}

// mergeSnapshot stores one peer snapshot in the cluster aggregator.
func (sys *admissionSys) mergeSnapshot(snap admission.PeerSnap) error {
	if a := sys.agg.Load(); a != nil {
		return a.Merge(snap)
	}
	return nil
}

// mergeAllocation stores a leader allocation push in the cluster aggregator.
func (sys *admissionSys) mergeAllocation(al admission.Allocation) error {
	if a := sys.agg.Load(); a != nil {
		return a.MergeAllocation(al)
	}
	return nil
}

// globalAdmissionSys is always non-nil: with no rules configured the
// manager is disabled and admission is a single atomic-load no-op.
var globalAdmissionSys = newAdmissionSys()

func newAdmissionSys() *admissionSys {
	return &admissionSys{
		mgr: admission.NewManager(admissionDefaultWait,
			admission.WithNodeCount(func() int { return totalNodeCount() }),
		),
	}
}

// replaceRules validates, stores and applies a full rule set. It also starts
// the background cluster workers on the first rule and stops them when the
// set becomes empty (only when a backing store is wired, i.e. real server).
func (sys *admissionSys) replaceRules(rules admission.Rules) error {
	if err := rules.Validate(); err != nil {
		return err
	}
	if err := sys.mgr.ApplyRules(rules); err != nil {
		return err
	}
	cp := rules.Clone()
	sys.rules.Store(&cp)

	// Worker lifecycle: only for the production system with an explicit
	// store, never for test-built sys instances.
	if sys == globalAdmissionSys && sys.store.Load() != nil {
		if len(rules) > 0 {
			startAdmissionWorkers()
		} else {
			stopAdmissionWorkers()
		}
	}
	return nil
}

// currentRules returns a copy of the active rule set (nil when empty).
func (sys *admissionSys) currentRules() admission.Rules {
	r := sys.rules.Load()
	if r == nil {
		return nil
	}
	return r.Clone()
}

// admit performs the bucket- and caller-dimension admission checks for one
// authenticated S3 request. It is called after signature and policy checks
// have succeeded; the identity in reqInfo is therefore verified.
func (sys *admissionSys) admit(ctx context.Context, r *http.Request, action policy.Action, bucket string) APIErrorCode {
	if sys == nil || !sys.mgr.Enabled() {
		return ErrNone
	}
	reqInfo := logger.GetReqInfo(ctx)
	if reqInfo == nil {
		// Fail open for unexpected internal call paths without ReqInfo.
		return ErrNone
	}
	class := requestActionClass(r, action)

	// Bucket dimension (also covers anonymous requests).
	tookBucket := false
	if bucket != "" {
		if s3Err := sys.admitOne(ctx, reqInfo, admission.ScopeBucket, bucket, class); s3Err != ErrNone {
			return s3Err
		}
		tookBucket = true
	}

	// Caller dimension (anonymous requests have no caller). If it rejects
	// (or the client goes away while queued), refund the bucket tokens so a
	// request that never executes consumes nothing from either dimension.
	if caller := admissionCaller(reqInfo.Cred); caller != "" {
		if s3Err := sys.admitOne(ctx, reqInfo, admission.ScopeCaller, caller, class); s3Err != ErrNone {
			if tookBucket {
				sys.mgr.Refund(admission.ScopeBucket, bucket, class)
			}
			return s3Err
		}
	}
	return ErrNone
}

func (sys *admissionSys) admitOne(ctx context.Context, reqInfo *logger.ReqInfo, scope admission.Scope, name string, class admission.ActionClass) APIErrorCode {
	d := sys.mgr.Admit(ctx, scope, name, class)
	if d.Allowed {
		return ErrNone
	}
	if d.Canceled {
		// Client went away while queued; do not count or emit a rejection.
		return ErrClientDisconnected
	}
	reqInfo.AdmissionRateLimited = true
	reqInfo.AdmissionRetryAfter = d.RetryAfter
	reqInfo.AdmissionLimit = d.Limit
	reqInfo.AdmissionRemaining = d.Remaining

	// Dedicated audit event with the rejecting dimension (the regular
	// access audit entry records the 429 response itself).
	retrySecs := (int64(d.RetryAfter) + int64(time.Second) - 1) / int64(time.Second)
	if retrySecs < 1 {
		retrySecs = 1
	}
	auditLogInternal(ctx, AuditLogOptions{
		Event:   "s3:RequestRateLimited",
		APIName: reqInfo.API,
		Status:  "429",
		Bucket:  reqInfo.BucketName,
		Object:  reqInfo.ObjectName,
		Error:   requestRateLimitedCode,
		Tags: map[string]string{
			"scope":      string(scope),
			"quotaName":  name,
			"class":      string(class),
			"retryAfter": strconv.FormatInt(retrySecs, 10),
			"accessKey":  reqInfo.Cred.AccessKey,
			"parentUser": reqInfo.Cred.ParentUser,
		},
	})
	return ErrAdmissionRateLimited
}

// admitRequest is the single chokepoint entry used by the S3 auth layer.
func admitRequest(ctx context.Context, r *http.Request, action policy.Action, bucket string) APIErrorCode {
	return globalAdmissionSys.admit(ctx, r, action, bucket)
}
