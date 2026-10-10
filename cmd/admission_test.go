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
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/minio/minio/internal/admission"
	"github.com/minio/minio/internal/auth"
	"github.com/minio/minio/internal/logger"
	"github.com/minio/pkg/v3/policy"
)

func qPtr(q admission.ClassQuota) *admission.ClassQuota { return &q }
func waitPtrMs(ms int64) *time.Duration {
	d := time.Duration(ms) * time.Millisecond
	return &d
}

func newTestAdmissionSys(t *testing.T, rules admission.Rules) *admissionSys {
	t.Helper()
	sys := &admissionSys{mgr: admission.NewManager(0)}
	if err := sys.replaceRules(rules); err != nil {
		t.Fatal(err)
	}
	return sys
}

func admitCtx(cred auth.Credentials, bucket string) (context.Context, *logger.ReqInfo) {
	ri := &logger.ReqInfo{Cred: cred, BucketName: bucket}
	return logger.SetReqInfo(context.Background(), ri), ri
}

func req(method string) *http.Request {
	return httptest.NewRequest(method, "/b/o", nil)
}

func TestAdmissionDisabledByDefault(t *testing.T) {
	sys := &admissionSys{mgr: admission.NewManager(0)}
	ctx, _ := admitCtx(auth.Credentials{AccessKey: "alice"}, "b")
	if ec := sys.admit(ctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrNone {
		t.Fatalf("disabled system must admit, got %v", ec)
	}
	// Package-global default is disabled too.
	if ec := admitRequest(ctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrNone {
		t.Fatalf("global default must admit, got %v", ec)
	}
}

func TestAdmissionBucketAndCallerDimensions(t *testing.T) {
	rules := admission.Rules{
		{
			Scope: admission.ScopeBucket,
			Name:  "b",
			Reads: qPtr(admission.ClassQuota{Rate: 1, Burst: 1, Wait: waitPtrMs(0)}),
		},
		{
			Scope: admission.ScopeCaller,
			Name:  "alice",
			Total: qPtr(admission.ClassQuota{Rate: 1, Burst: 5, Wait: waitPtrMs(0)}),
		},
	}
	sys := newTestAdmissionSys(t, rules)
	cred := auth.Credentials{AccessKey: "alice"}

	// 1st read: both dimensions allow.
	ctx, ri := admitCtx(cred, "b")
	if ec := sys.admit(ctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrNone {
		t.Fatalf("first read allowed, got %v", ec)
	}

	// 2nd read: bucket read burst (1) exhausted, caller total still has room.
	ctx, ri = admitCtx(cred, "b")
	if ec := sys.admit(ctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrAdmissionRateLimited {
		t.Fatalf("second read must be rejected by bucket quota, got %v", ec)
	}
	if !ri.AdmissionRateLimited || ri.AdmissionRetryAfter < time.Second || ri.AdmissionRemaining != 0 {
		t.Fatalf("rejection hint not recorded: %+v", ri)
	}

	// Write to b: bucket rule only constrains reads; caller total applies.
	ctx, _ = admitCtx(cred, "b")
	if ec := sys.admit(ctx, req(http.MethodPut), policy.PutObjectAction, "b"); ec != ErrNone {
		t.Fatalf("write not constrained by read quota, got %v", ec)
	}

	// Other bucket: bucket dimension absent; caller total still applies.
	ctx, _ = admitCtx(cred, "other")
	if ec := sys.admit(ctx, req(http.MethodGet), policy.GetObjectAction, "other"); ec != ErrNone {
		t.Fatalf("other bucket must pass bucket dimension, got %v", ec)
	}

	// Different caller is unaffected.
	ctx, _ = admitCtx(auth.Credentials{AccessKey: "bob"}, "b")
	if ec := sys.admit(ctx, req(http.MethodPut), policy.PutObjectAction, "b"); ec != ErrNone {
		t.Fatalf("other caller must be unaffected, got %v", ec)
	}
}

func TestAdmissionListBucketsCallerOnly(t *testing.T) {
	rules := admission.Rules{{
		Scope: admission.ScopeCaller,
		Name:  "alice",
		Total: qPtr(admission.ClassQuota{Rate: 1, Burst: 1, Wait: waitPtrMs(0)}),
	}}
	sys := newTestAdmissionSys(t, rules)
	cred := auth.Credentials{AccessKey: "alice"}

	ctx, _ := admitCtx(cred, "")
	if ec := sys.admit(ctx, req(http.MethodGet), policy.ListAllMyBucketsAction, ""); ec != ErrNone {
		t.Fatalf("first ListBuckets allowed, got %v", ec)
	}
	ctx, ri := admitCtx(cred, "")
	if ec := sys.admit(ctx, req(http.MethodGet), policy.ListAllMyBucketsAction, ""); ec != ErrAdmissionRateLimited {
		t.Fatalf("second ListBuckets rejected by caller total, got %v", ec)
	}
	if ri.BucketName != "" {
		t.Fatalf("bucket must stay empty for ListAllMyBuckets")
	}
}

func TestAdmissionAnonymousBucketOnly(t *testing.T) {
	rules := admission.Rules{{
		Scope: admission.ScopeBucket,
		Name:  "b",
		Total: qPtr(admission.ClassQuota{Rate: 1, Burst: 1, Wait: waitPtrMs(0)}),
	}}
	sys := newTestAdmissionSys(t, rules)

	ctx, _ := admitCtx(auth.Credentials{}, "b") // anonymous
	if ec := sys.admit(ctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrNone {
		t.Fatalf("first anonymous read allowed, got %v", ec)
	}
	ctx, _ = admitCtx(auth.Credentials{}, "b")
	if ec := sys.admit(ctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrAdmissionRateLimited {
		t.Fatalf("second anonymous read rejected by bucket, got %v", ec)
	}
}

func TestAdmissionCanceledWhileQueued(t *testing.T) {
	rules := admission.Rules{{
		Scope: admission.ScopeBucket,
		Name:  "b",
		Total: qPtr(admission.ClassQuota{Rate: 1, Burst: 1, Wait: waitPtrMs(10000)}),
	}}
	sys := newTestAdmissionSys(t, rules)

	// Consume the burst token.
	ctx, _ := admitCtx(auth.Credentials{}, "b")
	if ec := sys.admit(ctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrNone {
		t.Fatalf("burst token: %v", ec)
	}

	// Already-canceled context must surface as client disconnect, not rejection.
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	ri := &logger.ReqInfo{BucketName: "b"}
	cctx = logger.SetReqInfo(cctx, ri)
	if ec := sys.admit(cctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrClientDisconnected {
		t.Fatalf("want ErrClientDisconnected, got %v", ec)
	}
	if ri.AdmissionRateLimited {
		t.Fatal("canceled request must not record a rejection decision")
	}
}

// A client that goes away while queued on the caller dimension must not
// consume the token already taken from the bucket dimension.
func TestAdmissionCallerCancelRefundsBucket(t *testing.T) {
	rules := admission.Rules{
		{
			Scope: admission.ScopeBucket,
			Name:  "b",
			Total: qPtr(admission.ClassQuota{Rate: 0, Burst: 2, Wait: waitPtrMs(0)}),
		},
		{
			Scope: admission.ScopeCaller,
			Name:  "alice",
			Total: qPtr(admission.ClassQuota{Rate: 0, Burst: 1, Wait: waitPtrMs(10000)}),
		},
	}
	sys := newTestAdmissionSys(t, rules)

	// First request consumes one token from each dimension (bucket left 1).
	ctx, _ := admitCtx(auth.Credentials{AccessKey: "alice"}, "b")
	if ec := sys.admit(ctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrNone {
		t.Fatalf("first request allowed, got %v", ec)
	}

	// Second request: bucket has a token, caller does not; the already-canceled
	// context surfaces as disconnect while queued on the caller dimension.
	cctx, cancel := context.WithCancel(context.Background())
	cancel()
	cctx = logger.SetReqInfo(cctx, &logger.ReqInfo{
		Cred: auth.Credentials{AccessKey: "alice"}, BucketName: "b",
	})
	if ec := sys.admit(cctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrClientDisconnected {
		t.Fatalf("want ErrClientDisconnected, got %v", ec)
	}

	// A different caller (no caller rule) now gets the refunded bucket token.
	ctx2, _ := admitCtx(auth.Credentials{AccessKey: "bob"}, "b")
	if ec := sys.admit(ctx2, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrNone {
		t.Fatalf("bucket token must have been refunded, got %v", ec)
	}

	// And the bucket is now exhausted: a third caller is rejected.
	ctx3, _ := admitCtx(auth.Credentials{AccessKey: "carol"}, "b")
	if ec := sys.admit(ctx3, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrAdmissionRateLimited {
		t.Fatalf("bucket burst exhausted, got %v", ec)
	}
}

func TestAdmissionNilReqInfoFailOpen(t *testing.T) {
	rules := admission.Rules{{
		Scope: admission.ScopeBucket,
		Name:  "b",
		Total: qPtr(admission.ClassQuota{Rate: 1, Burst: 1, Wait: waitPtrMs(0)}),
	}}
	sys := newTestAdmissionSys(t, rules)
	if ec := sys.admit(context.Background(), req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrNone {
		t.Fatalf("missing ReqInfo must fail open, got %v", ec)
	}
}

// Counters must flow into the admin GET response and reflect accept/reject.
func TestAdmissionStatsInGetResponse(t *testing.T) {
	rules := admission.Rules{{
		Scope: admission.ScopeBucket,
		Name:  "b",
		Total: qPtr(admission.ClassQuota{Rate: 1, Burst: 1, Wait: waitPtrMs(0)}),
	}}
	sys := newTestAdmissionSys(t, rules)
	cred := auth.Credentials{AccessKey: "alice"}

	ctx, _ := admitCtx(cred, "b")
	if ec := sys.admit(ctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrNone {
		t.Fatalf("accepted: %v", ec)
	}
	ctx, _ = admitCtx(cred, "b")
	if ec := sys.admit(ctx, req(http.MethodGet), policy.GetObjectAction, "b"); ec != ErrAdmissionRateLimited {
		t.Fatalf("rejected: %v", ec)
	}

	var accepted, rejected, queued, waiting int64
	for _, u := range sys.mgr.SnapshotStats() {
		for _, s := range u.Slots {
			accepted += s.Stats.Accepted
			rejected += s.Stats.Rejected
			queued += s.Stats.Queued
			waiting += s.Stats.Waiting
		}
	}
	if accepted < 1 || rejected < 1 {
		t.Fatalf("counters accepted=%d rejected=%d", accepted, rejected)
	}
	if waiting != 0 {
		t.Fatalf("no request should be waiting, got %d", waiting)
	}

	// Same payload the admin GET handler marshals.
	resp := getRequestAdmissionResp{Rules: sys.currentRules(), Stats: sys.mgr.SnapshotStats()}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"stats"`, `"accepted"`, `"rejected"`, `"queued"`, `"waiting"`} {
		if !bytes.Contains(data, []byte(field)) {
			t.Fatalf("GET response missing %s: %s", field, data)
		}
	}
	_ = queued
}
